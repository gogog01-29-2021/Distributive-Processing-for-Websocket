// main.cpp - Multi-exchange (Bybit / Binance / OKX) L2 order book:
//            Ingestor A/B (multi-symbol) + per-symbol validators + writers (per exchange)
//
// Build (MSVC + vcpkg):
//   cl /nologo /EHsc /Zi /std:c++17 /DWIN32_LEAN_AND_MEAN /D_WIN32_WINNT=0x0A00 main.cpp ^
//      /I "C:\BIGDATA3\bigdata\vcpkg\installed\x64-windows\include" ^
//      /Fo".\build\" /Fe"orderbook_rt.exe" ^
//      /link /LIBPATH:"C:\BIGDATA3\bigdata\vcpkg\installed\x64-windows\lib" ^
//      libssl.lib libcrypto.lib ws2_32.lib crypt32.lib ^
//      aws-cpp-sdk-kinesis.lib aws-cpp-sdk-core.lib
//
// 런타임: 실행 폴더에 'cacert.pem' 있으면 로딩 시도(없어도 OS 루트 저장소로 검증 시도)

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

#include <aws/core/Aws.h>
#include <aws/kinesis/KinesisClient.h>
#include <aws/kinesis/model/PutRecordRequest.h>

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
        std::cerr << "[WARN] pin_current_thread(" << role << "): invalid core_id=" << core_id << "\n";
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
    if (v.is_string()) { out = std::stod(v.get<std::string>()); return true; }
    return false;
}

static inline bool get_i64(const json& v, int64_t& out) {
    if (v.is_number_integer()) { out = v.get<int64_t>(); return true; }
    if (v.is_number_unsigned()) { out = static_cast<int64_t>(v.get<uint64_t>()); return true; }
    if (v.is_string()) { out = std::stoll(v.get<std::string>()); return true; }
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

static inline std::string today_date() {
    std::time_t t = std::time(nullptr);
    std::tm tm{};
#ifdef _WIN32
    localtime_s(&tm, &t);
#else
    localtime_r(&t, &tm);
#endif
    char buf[16];
    std::strftime(buf, sizeof(buf), "%Y-%m-%d", &tm);
    return std::string(buf);
}

// ========== types ==========
struct Delta {
    std::string exchange;            // "bybit"/"binance"/"okx"/"multi"
    std::string instance;            // "A"/"B"
    std::string symbol;              // "BTCUSDT", ...
    int64_t     seq{ 0 };              // exchange-specific sequence / update id
    double      event_ts{ 0.0 };       // exchange event time (sec)
    double      recv_ts{ 0.0 };        // local receive time (sec)
    std::vector<std::pair<double, double>> bids;
    std::vector<std::pair<double, double>> asks;
};
using DeltaPtr = std::shared_ptr<Delta>;

// Single Producer / Single Consumer lock-free queue
template<size_t Capacity>
using SPSC = boost::lockfree::spsc_queue<DeltaPtr, boost::lockfree::capacity<Capacity>>;

// ---------- publisher interface ----------
struct IPublisher {
    virtual ~IPublisher() = default;
    virtual void publish(const Delta& ev) = 0;
};

// ========== file publisher ==========
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
        // 필요하면 flush 활성화
        // out_.flush();
    }

private:
    std::ofstream out_;
};

// ---------- Kinesis publisher ----------
class KinesisPublisher : public IPublisher {
public:
    KinesisPublisher(const std::string& stream_name,
        const std::string& region = "ap-northeast-2")
        : stream_name_(stream_name)
    {
        Aws::Client::ClientConfiguration cfg;
        cfg.region = region.c_str();
        client_ = std::make_unique<Aws::Kinesis::KinesisClient>(cfg);
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

        std::string payload = j.dump();

        Aws::Kinesis::Model::PutRecordRequest req;
        req.SetStreamName(stream_name_.c_str());

        // 파티션 키: 거래소 + 심볼 기준
        std::string partition_key = ev.exchange + ":" + ev.symbol;
        req.SetPartitionKey(partition_key.c_str());

        Aws::Utils::ByteBuffer buf(
            reinterpret_cast<const unsigned char*>(payload.data()),
            static_cast<unsigned int>(payload.size())
        );
        req.SetData(buf);

        auto outcome = client_->PutRecord(req);
        if (!outcome.IsSuccess()) {
            std::cerr << "[KinesisPublisher] PutRecord failed: "
                << outcome.GetError().GetMessage() << "\n";
        }
    }

private:
    std::string stream_name_;
    std::unique_ptr<Aws::Kinesis::KinesisClient> client_;
};

// ---------- MultiPublisher ----------
class MultiPublisher : public IPublisher {
public:
    void add(IPublisher* p) { targets_.push_back(p); }

    void publish(const Delta& ev) override {
        for (auto* p : targets_) {
            if (p) p->publish(ev);
        }
    }

private:
    std::vector<IPublisher*> targets_;
};

// 공통 HTTPS GET (주로 스냅샷용; Bybit에서 사용)
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

// ========== Bybit snapshot helper ==========
struct Snapshot {
    std::vector<std::pair<double, double>> bids;
    std::vector<std::pair<double, double>> asks;
    int64_t seq{ 0 };
    double event_ts{ 0.0 };
};

static Snapshot fetch_bybit_snapshot(asio::io_context& ioc, ssl::context& ssl_ctx,
    const std::string& symbol) {
    // GET https://api.bybit.com/v5/market/orderbook?category=spot&symbol=BTCUSDT&limit=200
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

// ========== Validator (merge A/B with timeout) ==========
class Validator {
public:
    Validator(std::string exchange,
        std::string symbol,
        SPSC<4096>* qa,
        SPSC<4096>* qb,
        SPSC<4096>* clean_q,
        std::chrono::milliseconds peer_wait = std::chrono::milliseconds(5),
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
    // 시퀀스별 후보( A/B 중 더 좋은 것 ) 저장
    std::map<int64_t, DeltaPtr> hold_;
    std::map<int64_t, uint64_t> first_seen_ms_;

    std::string exch_;
    std::string sym_;
    SPSC<4096>* qa_;         // Ingestor A → Validator
    SPSC<4096>* qb_;         // Ingestor B → Validator
    SPSC<4096>* clean_q_;    // Validator → Writer

    std::chrono::milliseconds peer_wait_;
    std::chrono::milliseconds idle_flush_;
    std::atomic<bool> dummy_running_{ true };
    std::atomic<bool>& running_;
    std::thread th_;
    int core_id_;

    // gap / 성능 통계
    int64_t   last_seq_ = 0;
    uint64_t  gap_count_ = 0;
    uint64_t  dup_count_ = 0;
    uint64_t  out_count_ = 0;
    uint64_t  last_log_ms_ = 0;

    void emit(const DeltaPtr& d) {
        // clean 링 버퍼로 push (Writer가 소비)
        while (running_.load() && !clean_q_->push(d)) {
            std::this_thread::sleep_for(std::chrono::microseconds(1));
        }

        // gap / 중복 체크
        if (last_seq_ != 0) {
            if (d->seq > last_seq_ + 1) {
                ++gap_count_;
                std::cerr << "[GAP:" << exch_ << ":" << sym_
                    << "] last=" << last_seq_
                    << " curr=" << d->seq << "\n";
            }
            else if (d->seq <= last_seq_) {
                ++dup_count_;
            }
        }
        last_seq_ = d->seq;
        ++out_count_;

        if (!d->bids.empty() || !d->asks.empty()) {
            std::cout << "[clean:" << exch_ << ":" << sym_
                << "] seq=" << d->seq
                << " t=" << std::fixed << std::setprecision(3) << d->event_ts
                << " recv=" << d->recv_ts << "\n";
        }

        // 1초마다 통계 로그
        uint64_t now = now_ms();
        if (now - last_log_ms_ >= 1000) {
            std::cout << "[STAT:validator:" << exch_ << ":" << sym_
                << "] out=" << out_count_
                << " gap=" << gap_count_
                << " dup/oop=" << dup_count_
                << "\n";
            out_count_ = 0;
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
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        }

        // 종료 시 남은 것 모두 방출
        for (auto& kv : hold_) {
            emit(kv.second);
        }
        hold_.clear();
        first_seen_ms_.clear();
    }
};


// ========== Writer (Validator → Publisher I/O 전담) ==========
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
        uint64_t counter = 0; // 초당 처리량 측정용

        while (running_.load()) {
            DeltaPtr d;
            if (clean_q_ && clean_q_->pop(d)) {
                if (pub_ && d) {
                    pub_->publish(*d);
                    ++counter;
                }
            }
            else {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }

            uint64_t now = now_ms();
            if (now - last_ts >= 1000) {
                std::cout << "[TPS:" << exch_ << ":" << sym_
                    << "] " << counter << " events/sec\n";
                counter = 0;
                last_ts = now;
            }
        }

        // 종료 시 큐에 남아있는 데이터도 모두 처리
        if (clean_q_) {
            DeltaPtr d;
            while (clean_q_->pop(d)) {
                if (pub_ && d) {
                    pub_->publish(*d);
                }
            }
        }
    }
};

// ========== 공통 Ingestor base ==========
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
    int core_id_;

    std::atomic<uint64_t> produced_count_{ 0 };
    uint64_t last_tps_log_ms_{ 0 };

    // 심볼별 push 성공 시 호출
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
        // "orderbook.50.BTCUSDT" → "BTCUSDT"
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
                auto d = std::make_shared<Delta>();
                d->exchange = "bybit";
                d->instance = inst_;
                d->symbol = sym;
                d->seq = s.seq;
                d->event_ts = s.event_ts;
                d->recv_ts = now_sec();
                d->bids = std::move(s.bids);
                d->asks = std::move(s.asks);
                if (!q->push(d)) {
                    std::cerr << "[WARN:bybit:" << inst_ << ":" << sym
                        << "] snapshot drop (queue full)\n";
                }
                else {
                    std::cout << "[ingestor:bybit:" << inst_ << ":" << sym
                        << "] snapshot pushed\n";
                }
            }
            catch (const std::exception& e) {
                std::cerr << "[ingestor:bybit:" << inst_ << ":" << sym
                    << "] snapshot error: " << e.what() << "\n";
            }
        }
    }

    void run() override {
        // 초기 스냅샷
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
                std::cout << "[ingestor:bybit:" << inst_ << "] subscribed symbols: ";
                for (auto& kv : routes_) std::cout << kv.first << " ";
                std::cout << "\n";

                beast::flat_buffer buffer;
                static std::atomic<bool> debug_once{ false };

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

                    auto d = std::make_shared<Delta>();
                    d->exchange = "bybit";
                    d->instance = inst_;
                    d->symbol = sym;
                    d->seq = u;
                    d->event_ts = has_ts ? (t_ms / 1000.0) : 0.0;
                    d->recv_ts = now_sec();

                    if (data.contains("b")) {
                        for (auto& row : data["b"]) {
                            double p, q;
                            get_num(row[0], p);
                            get_num(row[1], q);
                            d->bids.emplace_back(p, q);
                        }
                    }
                    if (data.contains("a")) {
                        for (auto& row : data["a"]) {
                            double p, q;
                            get_num(row[0], p);
                            get_num(row[1], q);
                            d->asks.emplace_back(p, q);
                        }
                    }

                    if (!debug_once.exchange(true)) {
                        std::cout << "[DEBUG:bybit:" << inst_ << "] keys top=";
                        for (auto& it : m.items()) std::cout << it.key() << " ";
                        std::cout << " | data=";
                        for (auto& it : data.items()) std::cout << it.key() << " ";
                        std::cout << "\n";
                    }

                    if (!has_ts) {
                        std::cerr << "[WARN:bybit:" << inst_ << "] no event_ts for " << sym
                            << " seq=" << d->seq << " (publishing anyway)\n";
                    }

                    if (!outQ->push(d)) {
                        // 링버퍼가 꽉 차서 drop
                        std::cerr << "[RING_FULL:" << exch_ << ":" << inst_ << ":" << sym
                            << "] dropping delta seq=" << d->seq << "\n";
                    }
                    else {
                        on_published_one(); // TPS 계측
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
        // "btcusdt@depth@100ms" → "BTCUSDT"
        auto pos = stream.find('@');
        std::string s = (pos == std::string::npos) ? stream : stream.substr(0, pos);
        std::string upper;
        upper.reserve(s.size());
        for (char c : s) upper.push_back(::toupper((unsigned char)c));
        return upper;
    }

    std::string build_ws_path() {
        // /stream?streams=btcusdt@depth@100ms/ethusdt@depth@100ms/...
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

                std::cout << "[ingestor:binance:" << inst_ << "] subscribed path=" << path << "\n";
                beast::flat_buffer buffer;
                static std::atomic<bool> debug_once{ false };

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
                    get_num(data["E"], t_ms);  // event time
                    int64_t u = 0;
                    get_i64(data["u"], u);     // last update id

                    auto d = std::make_shared<Delta>();
                    d->exchange = "binance";
                    d->instance = inst_;
                    d->symbol = sym;
                    d->seq = u;
                    d->event_ts = t_ms / 1000.0;
                    d->recv_ts = now_sec();

                    if (data.contains("b")) {
                        for (auto& row : data["b"]) {
                            double p, q;
                            get_num(row[0], p);
                            get_num(row[1], q);
                            d->bids.emplace_back(p, q);
                        }
                    }
                    if (data.contains("a")) {
                        for (auto& row : data["a"]) {
                            double p, q;
                            get_num(row[0], p);
                            get_num(row[1], q);
                            d->asks.emplace_back(p, q);
                        }
                    }

                    if (!debug_once.exchange(true)) {
                        std::cout << "[DEBUG:binance:" << inst_ << "] keys data=";
                        for (auto& it : data.items()) std::cout << it.key() << " ";
                        std::cout << "\n";
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
                std::cout << "[ingestor:okx:" << inst_ << "] subscribed symbols: ";
                for (auto& kv : routes_) std::cout << kv.first << " ";
                std::cout << "\n";

                beast::flat_buffer buffer;
                static std::atomic<bool> debug_once{ false };

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

                    auto d = std::make_shared<Delta>();
                    d->exchange = "okx";
                    d->instance = inst_;
                    d->symbol = sym;
                    d->seq = u;
                    d->event_ts = t_ms / 1000.0;
                    d->recv_ts = now_sec();

                    if (data.contains("bids")) {
                        for (auto& row : data["bids"]) {
                            double p = 0, q = 0;
                            get_num(row[0], p);
                            get_num(row[1], q);
                            d->bids.emplace_back(p, q);
                        }
                    }
                    if (data.contains("asks")) {
                        for (auto& row : data["asks"]) {
                            double p = 0, q = 0;
                            get_num(row[0], p);
                            get_num(row[1], q);
                            d->asks.emplace_back(p, q);
                        }
                    }

                    if (!debug_once.exchange(true)) {
                        std::cout << "[DEBUG:okx:" << inst_ << "] keys data=";
                        for (auto& it : data.items()) std::cout << it.key() << " ";
                        std::cout << "\n";
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

// ========== global running flag ==========
static std::atomic<bool> g_running(true);

void on_sigint(int) {
    g_running.store(false);
}

// ========== main ==========
int main(int argc, char** argv)
{
    Aws::SDKOptions aws_options;
    Aws::InitAPI(aws_options);

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

        std::cout << "Running (Binance + OKX + Bybit / Ingestor A/B / Validator + Writer per symbol)...\n";

        try {
            // ======================
            //  TLS + IO Context
            // ======================
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
            //  Publisher 구성 (거래소별)
            // ======================

            // ---- Bybit ----
            fs::path out_bybit = fs::path("data") / "orderbook_bybit.jsonl";
            FilePublisher file_pub_bybit(out_bybit);

            KinesisPublisher kinesis_pub_bybit(
                "orderbook-bybit",
                "ap-northeast-2"
            );

            MultiPublisher multi_pub_bybit;
            multi_pub_bybit.add(&file_pub_bybit);
            multi_pub_bybit.add(&kinesis_pub_bybit);

            // ---- Binance ----
            fs::path out_binance = fs::path("data") / "orderbook_binance.jsonl";
            FilePublisher file_pub_binance(out_binance);

            KinesisPublisher kinesis_pub_binance(
                "orderbook-binance",
                "ap-northeast-2"
            );

            MultiPublisher multi_pub_binance;
            multi_pub_binance.add(&file_pub_binance);
            multi_pub_binance.add(&kinesis_pub_binance);

            // ---- OKX ----
            fs::path out_okx = fs::path("data") / "orderbook_okx.jsonl";
            FilePublisher file_pub_okx(out_okx);

            KinesisPublisher kinesis_pub_okx(
                "orderbook-okx",
                "ap-northeast-2"
            );

            MultiPublisher multi_pub_okx;
            multi_pub_okx.add(&file_pub_okx);
            multi_pub_okx.add(&kinesis_pub_okx);

            // ======================
            //  (거래소, 심볼) 별 파이프 구성
            // ======================
            struct ExchSymbolPipe {
                std::string exch;   // "bybit", "binance", "okx"
                std::string sym;    // "BTCUSDT", ...

                // Ingestor A/B → Validator
                std::unique_ptr<SPSC<4096>> qa;
                std::unique_ptr<SPSC<4096>> qb;

                // Validator → Writer
                std::unique_ptr<SPSC<4096>> qc;

                std::unique_ptr<Validator> val;
                std::unique_ptr<Writer>    writer;
            };

            std::vector<ExchSymbolPipe> pipes_bybit;
            std::vector<ExchSymbolPipe> pipes_binance;
            std::vector<ExchSymbolPipe> pipes_okx;

            pipes_bybit.reserve(symbols.size());
            pipes_binance.reserve(symbols.size());
            pipes_okx.reserve(symbols.size());

            // ======================
            //  거래소별 SPSC 및 Validator/Writer 구성
            // ======================

            for (auto& sym : symbols)
            {
                // ---- Bybit ----
                {
                    ExchSymbolPipe p;
                    p.exch = "bybit";
                    p.sym = sym;
                    p.qa = std::make_unique<SPSC<4096>>();
                    p.qb = std::make_unique<SPSC<4096>>();
                    p.qc = std::make_unique<SPSC<4096>>();

                    // A/B -> Validator -> qc
                    p.val = std::make_unique<Validator>(
                        "bybit",
                        sym,
                        p.qa.get(),                      // A
                        p.qb.get(),                      // B
                        p.qc.get(),                      // clean_q
                        std::chrono::milliseconds(5),    // peer_wait
                        std::chrono::milliseconds(20),   // idle_flush
                        &g_running,
                        -1                               // core_id (코어 핀 안 쓸 거면 -1)
                    );

                    // qc -> Writer -> MultiPublisher(파일 + Kinesis)
                    p.writer = std::make_unique<Writer>(
                        "bybit",
                        sym,
                        p.qc.get(),                      // Validator → qc → Writer
                        &multi_pub_bybit,
                        g_running,
                        -1
                    );

                    pipes_bybit.emplace_back(std::move(p));
                }

                // ---- Binance ----
                {
                    ExchSymbolPipe p;
                    p.exch = "binance";
                    p.sym = sym;
                    p.qa = std::make_unique<SPSC<4096>>();
                    p.qb = std::make_unique<SPSC<4096>>();
                    p.qc = std::make_unique<SPSC<4096>>();

                    p.val = std::make_unique<Validator>(
                        "binance",
                        sym,
                        p.qa.get(),
                        p.qb.get(),
                        p.qc.get(),
                        std::chrono::milliseconds(5),
                        std::chrono::milliseconds(20),
                        &g_running,
                        -1
                    );

                    p.writer = std::make_unique<Writer>(
                        "binance",
                        sym,
                        p.qc.get(),
                        &multi_pub_binance,
                        g_running,
                        -1
                    );

                    pipes_binance.emplace_back(std::move(p));
                }


                // ---- OKX ----
                {
                    ExchSymbolPipe p;
                    p.exch = "okx";
                    p.sym = sym;
                    p.qa = std::make_unique<SPSC<4096>>();
                    p.qb = std::make_unique<SPSC<4096>>();
                    p.qc = std::make_unique<SPSC<4096>>();

                    p.val = std::make_unique<Validator>(
                        "okx",
                        sym,
                        p.qa.get(),
                        p.qb.get(),
                        p.qc.get(),
                        std::chrono::milliseconds(5),
                        std::chrono::milliseconds(20),
                        &g_running,
                        -1
                    );

                    p.writer = std::make_unique<Writer>(
                        "okx",
                        sym,
                        p.qc.get(),
                        &multi_pub_okx,
                        g_running,
                        -1
                    );

                    pipes_okx.emplace_back(std::move(p));
                }
            }

            // ======================
            //  거래소별 Ingestor 라우팅 테이블 생성
            // ======================

            // Bybit
            std::unordered_map<std::string, SPSC<4096>*> routesA_bybit;
            std::unordered_map<std::string, SPSC<4096>*> routesB_bybit;

            // Binance
            std::unordered_map<std::string, SPSC<4096>*> routesA_binance;
            std::unordered_map<std::string, SPSC<4096>*> routesB_binance;

            // OKX
            std::unordered_map<std::string, SPSC<4096>*> routesA_okx;
            std::unordered_map<std::string, SPSC<4096>*> routesB_okx;

            for (auto& p : pipes_bybit) {
                routesA_bybit[p.sym] = p.qa.get();
                routesB_bybit[p.sym] = p.qb.get();
            }
            for (auto& p : pipes_binance) {
                routesA_binance[p.sym] = p.qa.get();
                routesB_binance[p.sym] = p.qb.get();
            }
            for (auto& p : pipes_okx) {
                routesA_okx[p.sym] = p.qa.get();
                routesB_okx[p.sym] = p.qb.get();
            }

            // ======================
            // 3개 거래소 Ingestor A/B 생성
            // ======================
            BybitIngestor   bybitA("bybit", ioc, ssl_ctx, "A", routesA_bybit, g_running);
            BybitIngestor   bybitB("bybit", ioc, ssl_ctx, "B", routesB_bybit, g_running);

            BinanceIngestor binanceA("binance", ioc, ssl_ctx, "A", routesA_binance, g_running);
            BinanceIngestor binanceB("binance", ioc, ssl_ctx, "B", routesB_binance, g_running);

            OkxIngestor     okxA("okx", ioc, ssl_ctx, "A", routesA_okx, g_running);
            OkxIngestor     okxB("okx", ioc, ssl_ctx, "B", routesB_okx, g_running);

            // ======================
            // io_context 스레드 시작
            // ======================
            std::thread io_thread([&]() {
                ioc.run();
                });

            // ======================
            // Ingestor 순차 시작
            // ======================
            bybitA.start();
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
            bybitB.start();

            binanceA.start();
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
            binanceB.start();

            okxA.start();
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
            okxB.start();

            // ======================
            // Validator & Writer 시작
            // ======================
            for (auto& p : pipes_bybit)   p.val->start();
            for (auto& p : pipes_binance) p.val->start();
            for (auto& p : pipes_okx)     p.val->start();

            for (auto& p : pipes_bybit)   p.writer->start();
            for (auto& p : pipes_binance) p.writer->start();
            for (auto& p : pipes_okx)     p.writer->start();

            std::cout << "Running... Press Ctrl + C to stop.\n";

            // 메인 스레드 대기
            while (g_running.load())
                std::this_thread::sleep_for(std::chrono::seconds(1));

            // ======================
            // 종료 처리
            // ======================
            ioc.stop();

            bybitA.join();   bybitB.join();
            binanceA.join(); binanceB.join();
            okxA.join();     okxB.join();

            for (auto& p : pipes_bybit)   p.val->join();
            for (auto& p : pipes_binance) p.val->join();
            for (auto& p : pipes_okx)     p.val->join();

            for (auto& p : pipes_bybit)   p.writer->join();
            for (auto& p : pipes_binance) p.writer->join();
            for (auto& p : pipes_okx)     p.writer->join();

            if (io_thread.joinable())
                io_thread.join();

            std::cout << "done\n";

        }
        catch (const std::exception& e) {
            std::cerr << "FATAL: " << e.what() << "\n";
        }
    }

    Aws::ShutdownAPI(aws_options);
    return 0;
}