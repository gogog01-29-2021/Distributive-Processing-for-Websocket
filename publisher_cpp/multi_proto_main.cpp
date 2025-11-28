// main.cpp - Multi-exchange L2 order book with Protobuf & Optimization
//
// Build (MSVC + vcpkg):
//   cl /nologo /EHsc /Zi /std:c++17 /DWIN32_LEAN_AND_MEAN /D_WIN32_WINNT=0x0A00 ^
//      main.cpp orderbook.pb.cc ^
//      /I "C:\vcpkg\installed\x64-windows\include" ^
//      /Fo".\build\" /Fe"orderbook_rt.exe" ^
//      /link /LIBPATH:"C:\vcpkg\installed\x64-windows\lib" ^
//      libssl.lib libcrypto.lib ws2_32.lib crypt32.lib ^
//      aws-cpp-sdk-kinesis.lib aws-cpp-sdk-core.lib ^
//      libprotobuf.lib

#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include <unordered_map>
#include <stdexcept>
#include <ctime>
#include <list>

#include <aws/core/Aws.h>
#include <aws/kinesis/KinesisClient.h>
#include <aws/kinesis/model/PutRecordRequest.h>
#include <aws/kinesis/model/PutRecordsRequest.h> // [FIX] 배치 전송용

// ✅ Protobuf 헤더
#include "orderbook.pb.h"

#ifdef _WIN32
#  include <windows.h>
#endif

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/lockfree/spsc_queue.hpp>
#include <nlohmann/json.hpp>

namespace fs = std::filesystem;
namespace asio = boost::asio;
namespace ssl = asio::ssl;
namespace beast = boost::beast;
namespace websocket = beast::websocket;
namespace http = beast::http;
using tcp = asio::ip::tcp;
using json = nlohmann::json;

// ========== Pinning helpers ==========
static inline void pin_current_thread(const char* role, int core_id) {
#ifdef _WIN32
    if (core_id < 0 || core_id >= (int)(sizeof(DWORD_PTR) * 8)) {
        // std::cerr << "[WARN] pin_current_thread(" << role << "): invalid core_id=" << core_id << "\n";
        return;
    }
    DWORD_PTR mask = (DWORD_PTR)1 << core_id;
    HANDLE h = GetCurrentThread();
    DWORD_PTR res = SetThreadAffinityMask(h, mask);
    if (res == 0) {
        std::cerr << "[WARN] pin_current_thread(" << role << "): SetThreadAffinityMask failed (core=" << core_id << ")\n";
    }
    else {
        std::cout << "[pin] " << role << " pinned to core#" << core_id << "\n";
    }
#else
    (void)role;
    (void)core_id;
#endif
}

// ========== json helpers ==========
static inline bool get_num(const json& v, double& out) {
    if (v.is_number_float()) { out = v.get<double>(); return true; }
    if (v.is_number_integer()) { out = static_cast<double>(v.get<int64_t>()); return true; }
    if (v.is_number_unsigned()) { out = static_cast<double>(v.get<uint64_t>()); return true; }
    if (v.is_string()) { 
        try { out = std::stod(v.get<std::string>()); return true; } 
        catch(...) { return false; }
    }
    return false;
}

static inline bool get_i64(const json& v, int64_t& out) {
    if (v.is_number_integer()) { out = v.get<int64_t>(); return true; }
    if (v.is_number_unsigned()) { out = static_cast<int64_t>(v.get<uint64_t>()); return true; }
    if (v.is_string()) {
        try { out = std::stoll(v.get<std::string>()); return true; }
        catch(...) { return false; }
    }
    return false;
}

// ========== time helpers ==========
static inline double now_sec() {
    using namespace std::chrono;
    return duration<double>(std::chrono::system_clock::now().time_since_epoch()).count();
}

static inline uint64_t now_ms() {
    using namespace std::chrono;
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
}

// ========== types ==========
struct Delta {
    std::string exchange;
    std::string instance;
    std::string symbol;
    int64_t     seq{ 0 };
    double      event_ts{ 0.0 };
    double      recv_ts{ 0.0 };
    std::vector<std::pair<double, double>> bids;
    std::vector<std::pair<double, double>> asks;

    // [IMPROVE] 재사용을 위한 reset (현재 코드에서는 make_shared로 매번 생성하지만 구조상 준비)
    void clear() {
        seq = 0; event_ts = 0.0; recv_ts = 0.0;
        bids.clear();
        asks.clear();
    }
};
using DeltaPtr = std::shared_ptr<Delta>;

// [IMPROVE] 간단한 Object Pool Wrapper
class DeltaPool {
public:
    static DeltaPtr acquire() {
        // 실제 HFT 환경에서는 boost::pool이나 lock-free stack을 사용해 
        // 미리 할당된 객체를 재사용해야 함. 여기서는 인터페이스만 도입.
        return std::make_shared<Delta>(); 
    }
};

template<size_t Capacity>
using SPSC = boost::lockfree::spsc_queue<DeltaPtr, boost::lockfree::capacity<Capacity>>;

// ========== publisher interface ==========
struct IPublisher {
    virtual ~IPublisher() = default;
    virtual void publish(const Delta& ev) = 0;
    virtual void flush() {} // [FIX] 배치 전송을 위한 flush 메소드 추가
};

// ========== file publisher (JSON) ==========
class FilePublisher : public IPublisher {
public:
    explicit FilePublisher(const fs::path& p) {
        fs::create_directories(p.parent_path());
        out_.open(p, std::ios::app);
        if (!out_) throw std::runtime_error("Failed to open output " + p.string());
    }

    void publish(const Delta& ev) override {
        json j;
        j["exchange"] = ev.exchange;
        j["instance"] = ev.instance;
        j["symbol"] = ev.symbol;
        j["seq"] = ev.seq;
        j["event_ts"] = ev.event_ts;
        j["recv_ts"] = ev.recv_ts;
        auto arr = [](const std::vector<std::pair<double, double>>& v) {
            json a = json::array();
            for (auto& pr : v) a.push_back({ pr.first, pr.second });
            return a;
        };
        j["bids"] = arr(ev.bids);
        j["asks"] = arr(ev.asks);
        out_ << j.dump() << "\n";
    }

private:
    std::ofstream out_;
};

// ========== Kinesis Publisher (Protobuf Binary + Batching) ==========
class KinesisPublisher : public IPublisher {
public:
    KinesisPublisher(const std::string& symbol,
                     const std::string& region = "ap-northeast-2",
                     bool use_protobuf = true)
        : symbol_(symbol)
        , use_protobuf_(use_protobuf)
    {
        stream_name_ = "orderbook-" + symbol;
        
        Aws::Client::ClientConfiguration cfg;
        cfg.region = region.c_str();
        // [IMPROVE] 타임아웃 및 커넥션 설정 최적화
        cfg.connectTimeoutMs = 1000;
        cfg.requestTimeoutMs = 1000;
        cfg.maxConnections = 50; 

        client_ = std::make_unique<Aws::Kinesis::KinesisClient>(cfg);
        
        std::cout << "[KinesisPublisher] Initialized for symbol=" << symbol_
                  << " stream=" << stream_name_
                  << " format=" << (use_protobuf_ ? "protobuf" : "json") << "\n";
    }

    // [FIX] 단건 전송 대신 내부 버퍼에 쌓음
    void publish(const Delta& ev) override {
        std::string payload;
        
        if (use_protobuf_) {
            orderbook::OrderbookDelta pb_msg;
            pb_msg.set_exchange(ev.exchange);
            pb_msg.set_instance(ev.instance);
            pb_msg.set_symbol(ev.symbol);
            pb_msg.set_seq(ev.seq);
            pb_msg.set_event_ts(ev.event_ts);
            pb_msg.set_recv_ts(ev.recv_ts);
            
            // Bids
            for (const auto& [price, qty] : ev.bids) {
                auto* level = pb_msg.add_bids();
                level->set_price(price);
                level->set_quantity(qty);
            }
            // Asks
            for (const auto& [price, qty] : ev.asks) {
                auto* level = pb_msg.add_asks();
                level->set_price(price);
                level->set_quantity(qty);
            }
            
            if (!pb_msg.SerializeToString(&payload)) {
                std::cerr << "[KinesisPublisher:" << symbol_ << "] Protobuf serialization failed\n";
                return;
            }
        } else {
            // JSON fallback
            json j;
            j["exchange"] = ev.exchange;
            j["instance"] = ev.instance;
            j["symbol"] = ev.symbol;
            j["seq"] = ev.seq;
            j["event_ts"] = ev.event_ts;
            j["recv_ts"] = ev.recv_ts;
            auto arr = [](const std::vector<std::pair<double, double>>& v) {
                json a = json::array();
                for (auto& pr : v) a.push_back({pr.first, pr.second});
                return a;
            };
            j["bids"] = arr(ev.bids);
            j["asks"] = arr(ev.asks);
            payload = j.dump();
        }

        // [FIX] Kinesis PutRecordsEntry 생성 및 버퍼링
        Aws::Kinesis::Model::PutRecordsRequestEntry entry;
        Aws::Utils::ByteBuffer buf(
            reinterpret_cast<const unsigned char*>(payload.data()),
            static_cast<unsigned int>(payload.size())
        );
        entry.SetData(buf);
        // [CRITICAL FIX] 파티션 키를 심볼 단위로 고정하여 순서 보장 및 샤드 분산
        entry.SetPartitionKey(ev.exchange.c_str()); 

        std::lock_guard<std::mutex> lock(buffer_mutex_);
        pending_records_.push_back(std::move(entry));

        // 배치 크기 도달 시 전송 (예: 50개)
        if (pending_records_.size() >= 50) {
            flush_internal();
        }
    }

    // 주기적으로 혹은 종료 시 호출
    void flush() override {
        std::lock_guard<std::mutex> lock(buffer_mutex_);
        if (!pending_records_.empty()) {
            flush_internal();
        }
    }

private:
    void flush_internal() {
        // [NOTE] 실제로는 이 부분도 비동기(Async)로 돌려야 Writer 스레드가 안 멈춤.
        // 여기서는 예시를 위해 동기 호출하되, PutRecords(Batch) 사용하여 RTT 비용 최소화.
        if (pending_records_.empty()) return;

        Aws::Kinesis::Model::PutRecordsRequest req;
        req.SetStreamName(stream_name_.c_str());
        req.SetRecords(pending_records_);

        auto outcome = client_->PutRecords(req);
        if (!outcome.IsSuccess()) {
             std::cerr << "[Kinesis Error:" << symbol_ << "] " << outcome.GetError().GetMessage() << "\n";
        } else {
             auto result = outcome.GetResult();
             if (result.GetFailedRecordCount() > 0) {
                 std::cerr << "[Kinesis Warn:" << symbol_ << "] Partial failure: " 
                           << result.GetFailedRecordCount() << " records failed.\n";
             }
             total_sent_ += (pending_records_.size() - result.GetFailedRecordCount());
             
             // 주기적 로그
             auto now = std::chrono::steady_clock::now();
             if (now - last_log_time_ >= std::chrono::seconds(10)) {
                 std::cout << "[KinesisPublisher:" << symbol_ 
                           << "] Total sent: " << total_sent_.load() << "\n";
                 last_log_time_ = now;
             }
        }
        pending_records_.clear();
    }

    std::string symbol_;
    std::string stream_name_;
    bool use_protobuf_;
    std::unique_ptr<Aws::Kinesis::KinesisClient> client_;
    std::atomic<uint64_t> total_sent_{0};
    std::chrono::steady_clock::time_point last_log_time_;
    
    std::vector<Aws::Kinesis::Model::PutRecordsRequestEntry> pending_records_;
    std::mutex buffer_mutex_;
};

// ========== MultiPublisher ==========
class MultiPublisher : public IPublisher {
public:
    void add(IPublisher* p) { targets_.push_back(p); }

    void publish(const Delta& ev) override {
        for (auto* p : targets_) {
            if (p) p->publish(ev);
        }
    }
    
    void flush() override {
        for (auto* p : targets_) {
            if (p) p->flush();
        }
    }

private:
    std::vector<IPublisher*> targets_;
};

// ========== HTTPS GET ==========
static json https_get_json(asio::io_context& ioc, ssl::context& ssl_ctx,
    const std::string& host, const std::string& target) {
    beast::ssl_stream<beast::tcp_stream> stream(ioc, ssl_ctx);
    if (!SSL_set_tlsext_host_name(stream.native_handle(), host.c_str()))
        throw std::runtime_error("SNI set failed");

    tcp::resolver resolver(ioc);
    auto results = resolver.resolve(host, "443");
    beast::get_lowest_layer(stream).connect(results);
    stream.handshake(ssl::stream_base::client);

    http::request<http::string_body> req{ http::verb::get, target, 11 };
    req.set(http::field::host, host);
    req.set(http::field::user_agent, "obrt/1.0");
    http::write(stream, req);

    beast::flat_buffer buffer;
    http::response<http::string_body> res;
    http::read(stream, buffer, res);

    beast::error_code ec;
    stream.shutdown(ec);

    if (res.result() != http::status::ok) {
        std::ostringstream oss;
        oss << "HTTP " << static_cast<int>(res.result()) << " for " << target;
        throw std::runtime_error(oss.str());
    }
    return json::parse(res.body());
}

// ========== Bybit snapshot ==========
struct Snapshot {
    std::vector<std::pair<double, double>> bids;
    std::vector<std::pair<double, double>> asks;
    int64_t seq{ 0 };
    double event_ts{ 0.0 };
};

static Snapshot fetch_bybit_snapshot(asio::io_context& ioc, ssl::context& ssl_ctx,
    const std::string& symbol) {
    std::ostringstream tgt;
    tgt << "/v5/market/orderbook?category=spot&symbol=" << symbol << "&limit=200";

    json j = https_get_json(ioc, ssl_ctx, "api.bybit.com", tgt.str());
    if (j.value("retCode", -1) != 0) {
        throw std::runtime_error("bybit REST retCode != 0");
    }
    json res = j["result"];

    Snapshot s;
    if (res.contains("b")) {
        for (auto& e : res["b"]) {
            double p = std::stod(e[0].get<std::string>());
            double q = std::stod(e[1].get<std::string>());
            s.bids.emplace_back(p, q);
        }
    }
    if (res.contains("a")) {
        for (auto& e : res["a"]) {
            double p = std::stod(e[0].get<std::string>());
            double q = std::stod(e[1].get<std::string>());
            s.asks.emplace_back(p, q);
        }
    }
    double ts_ms = 0.0;
    if (res.contains("ts")) {
        get_num(res["ts"], ts_ms);
    }
    s.event_ts = ts_ms / 1000.0;
    s.seq = static_cast<int64_t>(ts_ms);
    return s;
}

// ========== Validator ==========
class Validator {
public:
    Validator(std::string exchange,
        std::string symbol,
        SPSC<4096>* qa,
        SPSC<4096>* qb,
        SPSC<4096>* clean_q,
        std::chrono::milliseconds peer_wait = std::chrono::milliseconds(15),
        std::chrono::milliseconds idle_flush = std::chrono::milliseconds(20),
        std::atomic<bool>* running = nullptr,
        int core_id = -1)
        : exch_(std::move(exchange))
        , sym_(std::move(symbol))
        , qa_(qa)
        , qb_(qb)
        , clean_q_(clean_q)
        , peer_wait_(peer_wait)
        , idle_flush_(idle_flush)
        , running_(running ? *running : dummy_running_)
        , core_id_(core_id)
    {
    }

    void start() {
        th_ = std::thread([this] {
            if (core_id_ >= 0) {
                std::string role = "validator:" + exch_ + ":" + sym_;
                pin_current_thread(role.c_str(), core_id_);
            }
            std::cout << "[validator:" << exch_ << ":" << sym_
                << "] thread start (tid=" << std::this_thread::get_id() << ")\n";
            run();
        });
    }

    void join() {
        if (th_.joinable()) th_.join();
    }

private:
    std::map<int64_t, DeltaPtr> hold_;
    std::map<int64_t, uint64_t> first_seen_ms_;

    std::string exch_;
    std::string sym_;
    SPSC<4096>* qa_;
    SPSC<4096>* qb_;
    SPSC<4096>* clean_q_;

    std::chrono::milliseconds peer_wait_;
    std::chrono::milliseconds idle_flush_;
    std::atomic<bool> dummy_running_{ true };
    std::atomic<bool>& running_;
    std::thread th_;
    int core_id_;

    int64_t   last_seq_ = 0;
    uint64_t  gap_count_ = 0;
    uint64_t  dup_count_ = 0;
    uint64_t  out_count_ = 0;
    uint64_t  last_log_ms_ = 0;

    void emit(const DeltaPtr& d) {
        while (running_.load() && !clean_q_->push(d)) {
            std::this_thread::sleep_for(std::chrono::microseconds(1));
        }

        if (last_seq_ != 0) {
            int64_t diff = d->seq - last_seq_;

            if (exch_ == "bybit") {
                if (diff > 1) {
                    ++gap_count_;
                    std::cerr << "[GAP:" << exch_ << ":" << sym_
                              << "] last=" << last_seq_ << " curr=" << d->seq << "\n";
                } else if (diff <= 0) {
                    ++dup_count_;
                }
            } else if (exch_ == "binance" || exch_ == "okx") {
                const int64_t RESET_THRESHOLD = 1'000'000;
                if (diff < 0 && std::llabs(diff) > RESET_THRESHOLD) {
                    std::cerr << "[SEQ_RESET:" << exch_ << ":" << sym_
                              << "] last=" << last_seq_ << " curr=" << d->seq << "\n";
                    last_seq_ = d->seq;
                }
                else if (diff < 0) {
                    ++dup_count_;
                }
            }
        }

        last_seq_ = d->seq;
        ++out_count_;

        uint64_t now = now_ms();
        if (now - last_log_ms_ >= 1000) {
            std::cout << "[STAT:validator:" << exch_ << ":" << sym_
                      << "] out=" << out_count_
                      << " gap=" << gap_count_
                      << " dup/oop=" << dup_count_
                      << "\n";
            out_count_ = 0;
            gap_count_ = 0; // [추가] 0으로 리셋하면 현재 발생하는 Gap만 볼 수 있음
            dup_count_ = 0;
            last_log_ms_ = now;
        }
    }

    void insert_or_choose(const DeltaPtr& d) {
        auto it = hold_.find(d->seq);
        if (it == hold_.end()) {
            hold_.emplace(d->seq, d);
            first_seen_ms_.emplace(d->seq, now_ms());
            return;
        }
        auto& cur = it->second;
        bool replace = false;
        if (d->event_ts > cur->event_ts) replace = true;
        else if (d->event_ts == cur->event_ts && d->recv_ts < cur->recv_ts) replace = true;
        else if (d->event_ts == cur->event_ts && d->recv_ts == cur->recv_ts) {
            if (cur->instance != "A" && d->instance == "A") replace = true;
        }
        if (replace) it->second = d;
    }

    void flush_ready(uint64_t now_ms_val) {
        while (!hold_.empty()) {
            auto it = hold_.begin();
            int64_t seq = it->first;
            uint64_t seen = first_seen_ms_[seq];
            if (now_ms_val - seen < (uint64_t)peer_wait_.count())
                break;
            emit(it->second);
            first_seen_ms_.erase(seq);
            hold_.erase(it);
        }
    }

    void run() {
        uint64_t last_idle_flush = now_ms();

        while (running_.load()) {
            DeltaPtr d;
            bool got = false;
            if (qa_ && qa_->pop(d)) {
                insert_or_choose(d);
                got = true;
            }
            if (qb_ && qb_->pop(d)) {
                insert_or_choose(d);
                got = true;
            }

            uint64_t nowv = now_ms();
            flush_ready(nowv);

            if (nowv - last_idle_flush >= (uint64_t)idle_flush_.count()) {
                flush_ready(nowv);
                last_idle_flush = nowv;
            }

            if (!got) {
                std::this_thread::sleep_for(std::chrono::microseconds(100)); // Sleep 줄임
            }
        }

        for (auto& kv : hold_) {
            emit(kv.second);
        }
        hold_.clear();
        first_seen_ms_.clear();
    }
};

// ========== Writer ==========
class Writer {
public:
    Writer(std::string exchange,
        std::string symbol,
        SPSC<4096>* clean_q,
        IPublisher* publisher,
        std::atomic<bool>& running,
        int core_id = -1)
        : exch_(std::move(exchange))
        , sym_(std::move(symbol))
        , clean_q_(clean_q)
        , pub_(publisher)
        , running_(running)
        , core_id_(core_id)
    {
    }

    void start() {
        th_ = std::thread([this] {
            if (core_id_ >= 0) {
                std::string role = "writer:" + exch_ + ":" + sym_;
                pin_current_thread(role.c_str(), core_id_);
            }
            std::cout << "[writer:" << exch_ << ":" << sym_
                << "] thread start (tid=" << std::this_thread::get_id() << ")\n";
            this->run();
        });
    }

    void join() {
        if (th_.joinable()) th_.join();
    }

private:
    std::string        exch_;
    std::string        sym_;
    SPSC<4096>* clean_q_;
    IPublisher* pub_;
    std::atomic<bool>& running_;
    std::thread        th_;
    int                core_id_;

    void run() {
        uint64_t last_ts = now_ms();
        uint64_t last_flush = now_ms();
        uint64_t counter = 0;

        while (running_.load()) {
            DeltaPtr d;
            bool popped = false;
            
            // [FIX] 한 번에 여러 개 처리 (Batching 효과 증대)
            while (clean_q_ && clean_q_->pop(d)) {
                if (pub_ && d) {
                    pub_->publish(*d);
                    ++counter;
                }
                popped = true;
                
                // 너무 오래 락을 잡지 않도록 루프 제한
                if (counter % 100 == 0) break; 
            }

            uint64_t now = now_ms();
            
            // [FIX] 주기적 Flush (예: 100ms 마다)
            if (now - last_flush >= 100) {
                if (pub_) pub_->flush();
                last_flush = now;
            }

            if (!popped) {
                std::this_thread::sleep_for(std::chrono::microseconds(10)); // Sleep 최소화
            }

            if (now - last_ts >= 1000) {
                std::cout << "[TPS:" << exch_ << ":" << sym_
                    << "] " << counter << " events/sec\n";
                counter = 0;
                last_ts = now;
            }
        }

        if (clean_q_) {
            DeltaPtr d;
            while (clean_q_->pop(d)) {
                if (pub_ && d) pub_->publish(*d);
            }
        }
        if (pub_) pub_->flush();
    }
};

// ========== Ingestor Base ==========
class IngestorBase {
public:
    IngestorBase(std::string exchange,
        asio::io_context& ioc,
        ssl::context& ssl_ctx,
        std::string instance_id,
        std::unordered_map<std::string, SPSC<4096>*> routes,
        std::atomic<bool>& running,
        int core_id = -1)
        : exch_(std::move(exchange))
        , ioc_(ioc)
        , ssl_(ssl_ctx)
        , inst_(std::move(instance_id))
        , routes_(std::move(routes))
        , running_(running)
        , core_id_(core_id)
    {
    }

    virtual ~IngestorBase() = default;

    void start() {
        th_ = std::thread([this] {
            if (core_id_ >= 0) {
                std::string role = "ingestor:" + exch_ + ":" + inst_;
                pin_current_thread(role.c_str(), core_id_);
            }
            std::cout << "[ingestor:" << exch_ << ":" << inst_
                << "] thread start (tid=" << std::this_thread::get_id() << ")\n";
            this->run();
        });
    }

    void join() {
        if (th_.joinable()) th_.join();
    }

protected:
    std::string exch_;
    asio::io_context& ioc_;
    ssl::context& ssl_;
    std::string       inst_;
    std::unordered_map<std::string, SPSC<4096>*> routes_;
    std::atomic<bool>& running_;
    std::thread       th_;
    int               core_id_;

    std::atomic<uint64_t> produced_count_{ 0 };
    uint64_t last_tps_log_ms_{ 0 };

    void on_published_one() {
        produced_count_.fetch_add(1, std::memory_order_relaxed);
        uint64_t now = now_ms();
        if (last_tps_log_ms_ == 0) {
            last_tps_log_ms_ = now;
            return;
        }
        if (now - last_tps_log_ms_ >= 1000) {
            auto n = produced_count_.exchange(0);
            std::cout << "[TPS:ingestor:" << exch_ << ":" << inst_
                << "] " << n << " events/sec\n";
            last_tps_log_ms_ = now;
        }
    }

    virtual void run() = 0;
};

// ========== Bybit Ingestor ==========
class BybitIngestor : public IngestorBase {
public:
    using IngestorBase::IngestorBase;

private:
    std::string symbol_from_topic(const std::string& topic) {
        auto pos = topic.rfind('.');
        if (pos == std::string::npos) return topic;
        return topic.substr(pos + 1);
    }

    void push_snapshot_for_all() {
        for (auto& kv : routes_) {
            const std::string& sym = kv.first;
            SPSC<4096>* q = kv.second;
            if (!q) continue;
            try {
                Snapshot s = fetch_bybit_snapshot(ioc_, ssl_, sym);
                // [FIX] Use Object Pool
                auto d = DeltaPool::acquire();
                d->exchange = "bybit";
                d->instance = inst_;
                d->symbol = sym;
                d->seq = s.seq;
                d->event_ts = s.event_ts;
                d->recv_ts = now_sec();
                d->bids = std::move(s.bids);
                d->asks = std::move(s.asks);
                if (!q->push(d)) {
                    std::cerr << "[WARN:bybit:" << inst_ << ":" << sym << "] snapshot drop (queue full)\n";
                }
                else {
                    std::cout << "[ingestor:bybit:" << inst_ << ":" << sym << "] snapshot pushed\n";
                }
            }
            catch (const std::exception& e) {
                std::cerr << "[ingestor:bybit:" << inst_ << ":" << sym << "] snapshot error: " << e.what() << "\n";
            }
        }
    }

    void run() override {
        push_snapshot_for_all();

        while (running_.load()) {
            try {
                tcp::resolver resolver(ioc_);
                auto results = resolver.resolve("stream.bybit.com", "443");

                beast::ssl_stream<beast::tcp_stream> ss(ioc_, ssl_);
                if (!SSL_set_tlsext_host_name(ss.native_handle(), "stream.bybit.com"))
                    throw std::runtime_error("SNI set failed");
                beast::get_lowest_layer(ss).connect(results);
                ss.handshake(ssl::stream_base::client);

                websocket::stream<beast::ssl_stream<beast::tcp_stream>> ws(std::move(ss));
                ws.set_option(websocket::stream_base::timeout::suggested(beast::role_type::client));
                ws.set_option(websocket::stream_base::decorator(
                    [](websocket::request_type& req) { req.set(http::field::user_agent, "obrt/1.0"); }
                ));
                ws.handshake("stream.bybit.com", "/v5/public/spot");

                json args = json::array();
                for (auto& kv : routes_) {
                    args.push_back("orderbook.50." + kv.first);
                }
                json sub = { {"op","subscribe"}, {"args", args} };
                ws.write(asio::buffer(sub.dump()));

                beast::flat_buffer buffer;

                while (running_.load()) {
                    buffer.clear();
                    ws.read(buffer);
                    auto s = beast::buffers_to_string(buffer.data());
                    if (s.empty()) continue;

                    json m = json::parse(s, nullptr, false);
                    if (m.is_discarded()) continue;
                    if (m.contains("op")) continue;
                    if (!m.contains("topic")) continue;

                    auto topic = m["topic"].get<std::string>();
                    std::string sym = symbol_from_topic(topic);

                    auto itRoute = routes_.find(sym);
                    if (itRoute == routes_.end()) continue;
                    SPSC<4096>* outQ = itRoute->second;
                    if (!outQ) continue;

                    json data = m.value("data", json::object());
                    double t_ms = 0.0; bool has_ts = false;
                    if (data.contains("ts"))           has_ts = get_num(data["ts"], t_ms);
                    if (!has_ts && m.contains("ts"))   has_ts = get_num(m["ts"], t_ms);
                    if (!has_ts && data.contains("t")) has_ts = get_num(data["t"], t_ms);

                    int64_t u = 0; bool has_seq = false;
                    if (data.contains("u"))            has_seq = get_i64(data["u"], u);
                    if (!has_seq && m.contains("seq")) has_seq = get_i64(m["seq"], u);

                    // [FIX] Use Object Pool
                    auto d = DeltaPool::acquire();
                    d->exchange = "bybit";
                    d->instance = inst_;
                    d->symbol = sym;
                    d->seq = u;
                    d->event_ts = has_ts ? (t_ms / 1000.0) : 0.0;
                    d->recv_ts = now_sec();

                    if (data.contains("b")) {
                        for (auto& row : data["b"]) {
                            double p, q;
                            get_num(row[0], p); get_num(row[1], q);
                            d->bids.emplace_back(p, q);
                        }
                    }
                    if (data.contains("a")) {
                        for (auto& row : data["a"]) {
                            double p, q;
                            get_num(row[0], p); get_num(row[1], q);
                            d->asks.emplace_back(p, q);
                        }
                    }

                    if (!outQ->push(d)) {
                        std::cerr << "[RING_FULL:" << exch_ << ":" << inst_ << ":" << sym
                            << "] dropping delta seq=" << d->seq << "\n";
                    }
                    else {
                        on_published_one();
                    }
                }
            }
            catch (const std::exception& e) {
                std::cerr << "[ingestor:bybit:" << inst_ << "] ws error: "
                    << e.what() << " -> reconnect after 5s\n";
                std::this_thread::sleep_for(std::chrono::seconds(5));
            }
        }
    }
};

// ========== Binance Ingestor ==========
class BinanceIngestor : public IngestorBase {
public:
    using IngestorBase::IngestorBase;

private:
    std::string symbol_from_stream(const std::string& stream) {
        auto pos = stream.find('@');
        std::string s = (pos == std::string::npos) ? stream : stream.substr(0, pos);
        std::string upper;
        upper.reserve(s.size());
        for (char c : s) upper.push_back(::toupper((unsigned char)c));
        return upper;
    }

    std::string build_ws_path() {
        std::ostringstream oss;
        oss << "/stream?streams=";
        bool first = true;
        for (auto& kv : routes_) {
            if (!first) oss << "/";
            first = false;
            std::string sym_lower;
            for (char c : kv.first) sym_lower.push_back(::tolower((unsigned char)c));
            oss << sym_lower << "@depth@100ms";
        }
        return oss.str();
    }

    void run() override {
        while (running_.load()) {
            try {
                tcp::resolver resolver(ioc_);
                auto results = resolver.resolve("stream.binance.com", "9443");

                beast::ssl_stream<beast::tcp_stream> ss(ioc_, ssl_);
                if (!SSL_set_tlsext_host_name(ss.native_handle(), "stream.binance.com"))
                    throw std::runtime_error("SNI set failed");
                beast::get_lowest_layer(ss).connect(results);
                ss.handshake(ssl::stream_base::client);

                websocket::stream<beast::ssl_stream<beast::tcp_stream>> ws(std::move(ss));
                ws.set_option(websocket::stream_base::timeout::suggested(beast::role_type::client));
                ws.set_option(websocket::stream_base::decorator(
                    [](websocket::request_type& req) { req.set(http::field::user_agent, "obrt/1.0"); }
                ));

                auto path = build_ws_path();
                ws.handshake("stream.binance.com", path);

                beast::flat_buffer buffer;

                while (running_.load()) {
                    buffer.clear();
                    ws.read(buffer);
                    auto s = beast::buffers_to_string(buffer.data());
                    if (s.empty()) continue;

                    json m = json::parse(s, nullptr, false);
                    if (m.is_discarded()) continue;
                    if (!m.contains("stream") || !m.contains("data")) continue;

                    std::string stream_name = m["stream"].get<std::string>();
                    json data = m["data"];
                    std::string sym = symbol_from_stream(stream_name);

                    auto itRoute = routes_.find(sym);
                    if (itRoute == routes_.end()) continue;
                    SPSC<4096>* outQ = itRoute->second;
                    if (!outQ) continue;

                    double t_ms = 0.0;
                    get_num(data["E"], t_ms);
                    int64_t u = 0;
                    get_i64(data["u"], u);

                    // [FIX] Use Object Pool
                    auto d = DeltaPool::acquire();
                    d->exchange = "binance";
                    d->instance = inst_;
                    d->symbol = sym;
                    d->seq = u;
                    d->event_ts = t_ms / 1000.0;
                    d->recv_ts = now_sec();

                    if (data.contains("b")) {
                        for (auto& row : data["b"]) {
                            double p, q;
                            get_num(row[0], p); get_num(row[1], q);
                            d->bids.emplace_back(p, q);
                        }
                    }
                    if (data.contains("a")) {
                        for (auto& row : data["a"]) {
                            double p, q;
                            get_num(row[0], p); get_num(row[1], q);
                            d->asks.emplace_back(p, q);
                        }
                    }

                    if (!outQ->push(d)) {
                        std::cerr << "[RING_FULL:" << exch_ << ":" << inst_ << ":" << sym
                            << "] dropping delta seq=" << d->seq << "\n";
                    }
                    else {
                        on_published_one();
                    }
                }
            }
            catch (const std::exception& e) {
                std::cerr << "[ingestor:binance:" << inst_ << "] ws error: "
                    << e.what() << " -> reconnect after 5s\n";
                std::this_thread::sleep_for(std::chrono::seconds(5));
            }
        }
    }
};

// ========== OKX Ingestor ==========
class OkxIngestor : public IngestorBase {
public:
    using IngestorBase::IngestorBase;

private:
    std::string instId_from_arg(const json& arg) {
        if (!arg.contains("instId")) return "";
        std::string instId = arg["instId"].get<std::string>();
        std::string sym;
        sym.reserve(instId.size());
        for (char c : instId) {
            if (c != '-') sym.push_back(c);
        }
        return sym;
    }

    void run() override {
        while (running_.load()) {
            try {
                tcp::resolver resolver(ioc_);
                auto results = resolver.resolve("ws.okx.com", "8443");

                beast::ssl_stream<beast::tcp_stream> ss(ioc_, ssl_);
                if (!SSL_set_tlsext_host_name(ss.native_handle(), "ws.okx.com"))
                    throw std::runtime_error("SNI set failed");
                beast::get_lowest_layer(ss).connect(results);
                ss.handshake(ssl::stream_base::client);

                websocket::stream<beast::ssl_stream<beast::tcp_stream>> ws(std::move(ss));
                ws.set_option(websocket::stream_base::timeout::suggested(beast::role_type::client));
                ws.set_option(websocket::stream_base::decorator(
                    [](websocket::request_type& req) { req.set(http::field::user_agent, "obrt/1.0"); }
                ));

                ws.handshake("ws.okx.com", "/ws/v5/public");

                json args = json::array();
                for (auto& kv : routes_) {
                    std::string instId;
                    const std::string& sym = kv.first;
                    for (size_t i = 0; i < sym.size(); ++i) {
                        if (i == 3) instId.push_back('-');
                        instId.push_back(sym[i]);
                    }
                    args.push_back({
                        {"channel", "books"},
                        {"instId", instId}
                    });
                }
                json sub = { {"op","subscribe"}, {"args", args} };
                ws.write(asio::buffer(sub.dump()));

                beast::flat_buffer buffer;

                while (running_.load()) {
                    buffer.clear();
                    ws.read(buffer);
                    auto s = beast::buffers_to_string(buffer.data());
                    if (s.empty()) continue;

                    json m = json::parse(s, nullptr, false);
                    if (m.is_discarded()) continue;
                    if (m.contains("event")) continue;
                    if (!m.contains("arg") || !m.contains("data")) continue;

                    json arg = m["arg"];
                    json datas = m["data"];
                    if (!datas.is_array() || datas.empty()) continue;
                    json data = datas[0];

                    std::string sym = instId_from_arg(arg);
                    auto itRoute = routes_.find(sym);
                    if (itRoute == routes_.end()) continue;
                    SPSC<4096>* outQ = itRoute->second;
                    if (!outQ) continue;

                    double t_ms = 0.0;
                    if (data.contains("ts")) {
                        std::string ts_str = data["ts"].get<std::string>();
                        t_ms = std::stod(ts_str);
                    }

                    int64_t u = 0;
                    if (data.contains("seqId")) {
                        get_i64(data["seqId"], u);
                    }

                    // [FIX] Use Object Pool
                    auto d = DeltaPool::acquire();
                    d->exchange = "okx";
                    d->instance = inst_;
                    d->symbol = sym;
                    d->seq = u;
                    d->event_ts = t_ms / 1000.0;
                    d->recv_ts = now_sec();

                    if (data.contains("bids")) {
                        for (auto& row : data["bids"]) {
                            double p = 0, q = 0;
                            get_num(row[0], p); get_num(row[1], q);
                            d->bids.emplace_back(p, q);
                        }
                    }
                    if (data.contains("asks")) {
                        for (auto& row : data["asks"]) {
                            double p = 0, q = 0;
                            get_num(row[0], p); get_num(row[1], q);
                            d->asks.emplace_back(p, q);
                        }
                    }

                    if (!outQ->push(d)) {
                        std::cerr << "[RING_FULL:" << exch_ << ":" << inst_ << ":" << sym
                            << "] dropping delta seq=" << d->seq << "\n";
                    }
                    else {
                        on_published_one();
                    }
                }
            }
            catch (const std::exception& e) {
                std::cerr << "[ingestor:okx:" << inst_ << "] ws error: "
                    << e.what() << " -> reconnect after 5s\n";
                std::this_thread::sleep_for(std::chrono::seconds(5));
            }
        }
    }
};

// ========== global ==========
static std::atomic<bool> g_running(true);

void on_sigint(int) {
    g_running.store(false);
}

// ========== main ==========
int main(int argc, char** argv)
{
    // [FIX] Protobuf & AWS Init
    Aws::SDKOptions aws_options;
    Aws::InitAPI(aws_options);
    GOOGLE_PROTOBUF_VERIFY_VERSION;

    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " {bybit|binance|okx}\n";
        google::protobuf::ShutdownProtobufLibrary();
        Aws::ShutdownAPI(aws_options);
        return 1;
    }

    std::string target = argv[1];
    for (auto& c : target) c = static_cast<char>(::tolower(static_cast<unsigned char>(c)));

    bool enable_bybit = (target == "bybit");
    bool enable_binance = (target == "binance");
    bool enable_okx = (target == "okx");

    if (!enable_bybit && !enable_binance && !enable_okx) {
        std::cerr << "Invalid exchange '" << target << "'. Use one of: bybit, binance, okx\n";
        google::protobuf::ShutdownProtobufLibrary();
        Aws::ShutdownAPI(aws_options);
        return 1;
    }

    {
        std::signal(SIGINT, on_sigint);
#ifdef SIGTERM
        std::signal(SIGTERM, on_sigint);
#endif

        std::vector<std::string> symbols = {
            "BTCUSDT",
            "ETHUSDT",
            "SOLUSDT"
        };

        std::cout << "Running with Protobuf binary format & Kinesis Batching...\n";

        try {
            asio::io_context ioc;
            ssl::context ssl_ctx(ssl::context::tls_client);
            ssl_ctx.set_default_verify_paths();
            ssl_ctx.set_verify_mode(ssl::verify_peer);

            try {
                ssl_ctx.load_verify_file("cacert.pem");
            }
            catch (...) {
                std::cerr << "[WARN] cacert.pem not found. Using OS trust store.\n";
            }

            // ======================
            //  심볼별 Publisher 생성
            // ======================
            std::unordered_map<std::string, std::unique_ptr<FilePublisher>> file_pubs;
            std::unordered_map<std::string, std::unique_ptr<KinesisPublisher>> kinesis_pubs;
            std::unordered_map<std::string, std::unique_ptr<MultiPublisher>> multi_pubs;

            for (const auto& sym : symbols) {
                fs::path out_path = fs::path("data") / ("orderbook_" + sym + ".jsonl");
                file_pubs[sym] = std::make_unique<FilePublisher>(out_path);
                
                kinesis_pubs[sym] = std::make_unique<KinesisPublisher>(
                    sym,
                    "ap-northeast-2",
                    true  // use_protobuf = true
                );
                
                multi_pubs[sym] = std::make_unique<MultiPublisher>();
                multi_pubs[sym]->add(file_pubs[sym].get());
                multi_pubs[sym]->add(kinesis_pubs[sym].get());
            }

            // 파이프 구성
            struct ExchSymbolPipe {
                std::string exch;
                std::string sym;
                std::unique_ptr<SPSC<4096>> qa;
                std::unique_ptr<SPSC<4096>> qb;
                std::unique_ptr<SPSC<4096>> qc;
                std::unique_ptr<Validator> val;
                std::unique_ptr<Writer>    writer;
            };

            std::vector<ExchSymbolPipe> pipes;

            for (auto& sym : symbols)
            {
                ExchSymbolPipe p;
                p.exch = target;
                p.sym = sym;
                p.qa = std::make_unique<SPSC<4096>>();
                p.qb = std::make_unique<SPSC<4096>>();
                p.qc = std::make_unique<SPSC<4096>>();

                p.val = std::make_unique<Validator>(
                    target, sym,
                    p.qa.get(), p.qb.get(), p.qc.get(),
                    std::chrono::milliseconds(15),
                    std::chrono::milliseconds(20),
                    &g_running, -1
                );

                p.writer = std::make_unique<Writer>(
                    target, sym,
                    p.qc.get(),
                    multi_pubs[sym].get(),
                    g_running, -1
                );

                pipes.emplace_back(std::move(p));
            }

            // Ingestor 라우팅
            std::unordered_map<std::string, SPSC<4096>*> routesA;
            std::unordered_map<std::string, SPSC<4096>*> routesB;

            for (auto& p : pipes) {
                routesA[p.sym] = p.qa.get();
                routesB[p.sym] = p.qb.get();
            }

            // Ingestor 생성
            std::unique_ptr<IngestorBase> ingestorA;
            std::unique_ptr<IngestorBase> ingestorB;

            if (enable_bybit) {
                ingestorA = std::make_unique<BybitIngestor>(
                    "bybit", ioc, ssl_ctx, "A", routesA, g_running
                );
                ingestorB = std::make_unique<BybitIngestor>(
                    "bybit", ioc, ssl_ctx, "B", routesB, g_running
                );
            }
            else if (enable_binance) {
                ingestorA = std::make_unique<BinanceIngestor>(
                    "binance", ioc, ssl_ctx, "A", routesA, g_running
                );
                ingestorB = std::make_unique<BinanceIngestor>(
                    "binance", ioc, ssl_ctx, "B", routesB, g_running
                );
            }
            else if (enable_okx) {
                ingestorA = std::make_unique<OkxIngestor>(
                    "okx", ioc, ssl_ctx, "A", routesA, g_running
                );
                ingestorB = std::make_unique<OkxIngestor>(
                    "okx", ioc, ssl_ctx, "B", routesB, g_running
                );
            }

            // 시작
            // [NOTE] 동기 방식 Ingestor이므로 ioc.run()은 resolver 등 내부용으로만 쓰임
            std::thread io_thread([&]() { 
                asio::executor_work_guard<asio::io_context::executor_type> work(ioc.get_executor());
                ioc.run(); 
            });

            if (ingestorA) ingestorA->start();
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
            if (ingestorB) ingestorB->start();

            for (auto& p : pipes) {
                p.val->start();
                p.writer->start();
            }

            std::cout << "Running... Press Ctrl+C to stop.\n";

            while (g_running.load())
                std::this_thread::sleep_for(std::chrono::seconds(1));

            // 종료
            ioc.stop();
            if (ingestorA) ingestorA->join();
            if (ingestorB) ingestorB->join();
            for (auto& p : pipes) {
                p.val->join();
                p.writer->join();
            }
            if (io_thread.joinable()) io_thread.join();

            std::cout << "done\n";
        }
        catch (const std::exception& e) {
            std::cerr << "FATAL: " << e.what() << "\n";
        }
    }

    Aws::ShutdownAPI(aws_options);
    google::protobuf::ShutdownProtobufLibrary();
    
    return 0;
}