// main.cpp - Binance Spot L2 (8-Thread / 9-Ring Arch) - FINAL CORRECTED VERSION
// 
// 아키텍처 (총 8+1 스레드):
//   - 1x I/O Thread (Boost.asio)
//   - 2x Ingestor Threads (A/B, All Symbols, Pinned) -> 6x Ingest SPSC Rings
//   - 3x Validator Threads (BTC/ETH/SOL, Pinned)  -> 3x Clean SPSC Rings
//   - 3x Writer Threads (BTC/ETH/SOL, Pinned)     -> 3x .jsonl Files
//
// 빌드 (MSVC + vcpkg):
//   cl /nologo /EHsc /Zi /std:c++17 /DWIN32_LEAN_AND_MEAN /D_WIN32_WINNT=0x0A00 main.cpp ^
//      /I "[vcpkg_root]\installed\x64-windows\include" ^
//      /Fo".\build\main.obj" /Fe".\build\orderbook_rt.exe" ^
//      /link /LIBPATH:"[vcpkg_root]\installed\x64-windows\lib" ^
//      libssl.lib libcrypto.lib ws2_32.lib crypt32.lib
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
#include <cctype> // <-- 수정 1: tolower를 위해 헤더 추가

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/lockfree/spsc_queue.hpp>
#include <nlohmann/json.hpp>

// CPU 피닝을 위한 헤더
#ifdef _WIN32
#include <windows.h>
#else
#include <pthread.h>
#endif

// --- 수정 2: 네임스페이스 선언 (오류가 없도록 std:: 사용) ---
namespace fs = std::filesystem;
namespace asio = boost::asio;
namespace ssl = asio::ssl;
namespace beast = boost::beast;
namespace websocket = beast::websocket;
namespace http = beast::http;
using tcp = asio::ip::tcp;
using json = nlohmann::json;

// --- 유틸리티 함수 ---
static inline bool get_num(const json& v, double& out) {
    if (v.is_number_float()) { out = v.get<double>(); return true; }
    if (v.is_number_integer()) { out = static_cast<double>(v.get<int64_t>()); return true; }
    if (v.is_number_unsigned()) { out = static_cast<double>(v.get<uint64_t>()); return true; }
    if (v.is_string()) { try { out = std::stod(v.get<std::string>()); return true; } catch (...) { return false; } }
    return false;
}
static inline bool get_i64(const json& v, int64_t& out) {
    if (v.is_number_integer()) { out = v.get<int64_t>(); return true; }
    if (v.is_number_unsigned()) { out = static_cast<int64_t>(v.get<uint64_t>()); return true; }
    if (v.is_string()) { try { out = std::stoll(v.get<std::string>()); return true; } catch (...) { return false; } }
    return false;
}
static inline double now_sec() {
    using namespace std::chrono;
    return duration<double>(std::chrono::system_clock::now().time_since_epoch()).count();
}
static inline uint64_t now_ms() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

// 스레드 CPU 피닝(고정) 유틸리티
void pin_thread_to_core(int core_id) {
    if (core_id < 0) return; // 코어 ID가 음수면 피닝 스킵

#ifdef _WIN32
    DWORD_PTR mask = (static_cast<DWORD_PTR>(1) << core_id);
    if (!SetThreadAffinityMask(GetCurrentThread(), mask)) {
        std::cerr << "WARN: Failed to set thread affinity for core " << core_id << "\n";
    }
#else
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);
    if (pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset) != 0) {
        std::cerr << "WARN: Failed to set thread affinity for core " << core_id << "\n";
    }
#endif
}

// --- 공통 타입 정의 ---
struct Delta {
    std::string exchange{ "binance" };
    std::string ingestor_id;         // "A" / "B"
    std::string symbol;              // "BTCUSDT"
    int64_t     seq{ 0 };              // from Binance 'u' (final update ID)
    int64_t     first_seq{ 0 };        // from Binance 'U' (first update ID)
    double      event_ts{ 0.0 };       // from Binance 'E' (ms->sec)
    double      recv_ts{ 0.0 };        // local receive time (sec)
    std::vector<std::pair<double, double>> bids;
    std::vector<std::pair<double, double>> asks;
};
using DeltaPtr = std::shared_ptr<Delta>;

// 링 버퍼: Single Producer / Single Consumer lock-free queue
// 버퍼 크기를 65536으로 늘려 Gap 발생 확률 감소
template<size_t Capacity = 65536>
using SPSC_Ring = boost::lockfree::spsc_queue<DeltaPtr, boost::lockfree::capacity<Capacity>>;

// --- File Publisher (Writer 스레드가 사용) ---
class FilePublisher {
public:
    explicit FilePublisher(const fs::path& p) {
        fs::create_directories(p.parent_path());
        out_.open(p, std::ios::app); // 이어쓰기 모드
        if (!out_) throw std::runtime_error("Failed to open output " + p.string());

        std::cout << "    [FilePublisher] Outputting to: " << p.string() << "\n";
    }

    void publish(const Delta& ev) {
        json j;
        j["exchange"] = ev.exchange;
        j["ingestor"] = ev.ingestor_id; // "A" or "B" (Winner)
        j["symbol"] = ev.symbol;
        j["seq"] = ev.seq;
        j["first_seq"] = ev.first_seq;
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

    // Writer 스레드의 빠른 종료를 위해 flush를 가끔만 호출
    void flush_if_needed() {
        auto now = now_ms();
        if (now - last_flush_ms_ > 1000) { // 1초에 한 번
            out_.flush();
            last_flush_ms_ = now;
        }
    }

private:
    std::ofstream out_;
    uint64_t last_flush_ms_ = 0;
};


// --- HTTPS GET (Binance Snapshot용) ---
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
        oss << "HTTP " << (int)res.result() << " for " << target;
        throw std::runtime_error(oss.str());
    }
    return json::parse(res.body());
}

// --- Ingestor 스레드 (모든 심볼 수신 및 분배) ---
class BinanceIngestor {
public:
    BinanceIngestor(asio::io_context& ioc, ssl::context& ssl_ctx,
        std::string ingestor_id,
        const std::vector<std::string>& symbols,
        std::map<std::string, SPSC_Ring<>*> output_queues,
        std::atomic<bool>& running)
        : ioc_(ioc), ssl_(ssl_ctx), ingestor_id_(std::move(ingestor_id)),
        symbols_(symbols), output_queues_(std::move(output_queues)), running_(running)
    {
        ws_target_ = "/stream?streams=";
        for (size_t i = 0; i < symbols_.size(); ++i) {
            std::string sym_lower = symbols_[i];
            // --- 수정 3: ::tolower를 std::tolower로 변경 ---
            std::transform(sym_lower.begin(), sym_lower.end(), sym_lower.begin(),
                [](unsigned char c) { return std::tolower(c); });
            ws_target_ += sym_lower + "@depth";
            if (i < symbols_.size() - 1) ws_target_ += "/";
        }

        for (const auto& sym_upper : symbols_) {
            std::string sym_lower = sym_upper;
            // --- 수정 3: ::tolower를 std::tolower로 변경 ---
            std::transform(sym_lower.begin(), sym_lower.end(), sym_lower.begin(),
                [](unsigned char c) { return std::tolower(c); });
            stream_map_[sym_lower + "@depth"] = sym_upper;
        }
    }

    void start(int core_id) {
        th_ = std::thread([this, core_id] {
            pin_thread_to_core(core_id);
            std::cout << "[Ingestor-" << ingestor_id_ << ":" << "Core " << core_id << "] thread start (tid="
                << std::this_thread::get_id() << ")\n";
            this->run();
            });
    }
    void join() { if (th_.joinable()) th_.join(); }

private:
    void run() {
        const std::string host = "fstream.binance.com";

        while (running_.load()) {
            try {
                tcp::resolver resolver(ioc_);
                auto results = resolver.resolve(host, "443");

                beast::ssl_stream<beast::tcp_stream> ss(ioc_, ssl_);
                if (!SSL_set_tlsext_host_name(ss.native_handle(), host.c_str()))
                    throw std::runtime_error("SNI set failed");
                beast::get_lowest_layer(ss).connect(results);
                ss.handshake(ssl::stream_base::client);

                websocket::stream<beast::ssl_stream<beast::tcp_stream>> ws(std::move(ss));
                ws.set_option(websocket::stream_base::timeout::suggested(beast::role_type::client));
                ws.set_option(websocket::stream_base::decorator(
                    [](websocket::request_type& req) { req.set(http::field::user_agent, "obrt/1.0"); }
                ));
                ws.handshake(host, ws_target_);

                std::cout << "[Ingestor-" << ingestor_id_ << "] subscribed to " << host << ws_target_ << "\n";

                beast::flat_buffer buffer;
                while (running_.load()) {
                    buffer.clear();
                    ws.read(buffer);
                    auto s = beast::buffers_to_string(buffer.data());
                    if (s.empty()) continue;

                    json m = json::parse(s, nullptr, false);
                    if (m.is_discarded()) continue;

                    std::string stream_name = m.value("stream", "");
                    if (stream_name.empty()) continue;

                    auto map_it = stream_map_.find(stream_name);
                    if (map_it == stream_map_.end()) continue;

                    std::string symbol = map_it->second;
                    SPSC_Ring<>* outQ = output_queues_.at(symbol);

                    json data = m.value("data", json::object());
                    if (!data.contains("u") || !data.contains("E")) continue;

                    auto d = std::make_shared<Delta>();
                    d->ingestor_id = ingestor_id_;
                    d->symbol = symbol;
                    d->recv_ts = now_sec();

                    double t_ms = 0.0;
                    get_i64(data["u"], d->seq);
                    get_i64(data["U"], d->first_seq);
                    get_num(data["E"], t_ms);
                    d->event_ts = t_ms / 1000.0;

                    if (data.contains("b")) {
                        for (auto& row : data["b"]) {
                            double p, q; get_num(row[0], p); get_num(row[1], q);
                            d->bids.emplace_back(p, q);
                        }
                    }
                    if (data.contains("a")) {
                        for (auto& row : data["a"]) {
                            double p, q; get_num(row[0], p); get_num(row[1], q);
                            d->asks.emplace_back(p, q);
                        }
                    }
                    outQ->push(d);
                }
            }
            catch (const std::exception& e) {
                std::cerr << "[Ingestor-" << ingestor_id_ << "] ws error: "
                    << e.what() << " -> reconnect after 5s\n";
                std::this_thread::sleep_for(std::chrono::seconds(5));
            }
        }
    }

    asio::io_context& ioc_;
    ssl::context& ssl_;
    std::string       ingestor_id_;
    std::vector<std::string> symbols_;
    std::string       ws_target_;
    std::map<std::string, std::string> stream_map_;
    std::map<std::string, SPSC_Ring<>*> output_queues_;
    std::atomic<bool>& running_;
    std::thread       th_;
};

// --- Validator 스레드 (A/B 수신 -> Clean Ring 전송) [순수 CPU-Bound] ---
class Validator {
public:
    Validator(std::string symbol,
        SPSC_Ring<>* qa, SPSC_Ring<>* qb,
        SPSC_Ring<>* clean_q, // 출력 링 버퍼
        asio::io_context& ioc,
        ssl::context& ssl_ctx,
        std::chrono::milliseconds peer_wait = std::chrono::milliseconds(5),
        std::atomic<bool>* running = nullptr)
        : sym_(std::move(symbol)), qa_(qa), qb_(qb), clean_q_(clean_q),
        ioc_(ioc), ssl_(ssl_ctx),
        next_seq_(0),
        peer_wait_(peer_wait),
        running_(*running)
    {
    }

    void start(int core_id) {
        th_ = std::thread([this, core_id] {
            pin_thread_to_core(core_id);
            std::cout << "[Validator-" << sym_ << ":" << "Core " << core_id << "] thread start (tid="
                << std::this_thread::get_id() << ")\n" << std::flush;

            try {
                load_snapshot_and_set_seq();
            }
            catch (const std::exception& e) {
                std::cerr << "[Validator-" << sym_ << "] FATAL: Snapshot load failed: " << e.what()
                    << ". Exiting thread.\n";
                return;
            }
            this->run();
            });
    }
    void join() { if (th_.joinable()) th_.join(); }

private:
    void load_snapshot_and_set_seq() {
        std::string host = "fapi.binance.com";
        std::string target = "/fapi/v1/depth?symbol=" + sym_ + "&limit=1000";

        std::cout << "[Validator-" << sym_ << "] Fetching snapshot from " << host << target << "...\n";
        json j = https_get_json(ioc_, ssl_, host, target);

        if (!j.contains("lastUpdateId")) {
            throw std::runtime_error("Snapshot JSON missing 'lastUpdateId'");
        }
        int64_t last_id = 0;
        get_i64(j["lastUpdateId"], last_id);
        next_seq_ = last_id + 1;
        std::cout << "[Validator-" << sym_ << "] Snapshot OK. Start seq set to: " << next_seq_ << "\n";
    }

    std::map<int64_t, DeltaPtr> hold_;          // seq -> Delta
    std::map<int64_t, uint64_t> first_seen_ms_; // seq -> first seen timestamp
    SPSC_Ring<>* clean_q_; // 검증된 데이터가 나갈 큐

    // maybe_emit: 순서가 맞는 데이터를 'Clean Ring'으로 푸시 (I/O 없음)
    void maybe_emit() {
        if (next_seq_ == 0) return;

        while (!hold_.empty()) {
            auto it = hold_.find(next_seq_);
            if (it == hold_.end()) break;

            // FilePublisher::publish() 대신 Clean 큐로 PUSH
            clean_q_->push(it->second);

            hold_.erase(it);
            first_seen_ms_.erase(next_seq_);
            ++next_seq_;
        }
    }

    void insert_or_choose(DeltaPtr d) {
        if (d->seq < next_seq_ && d->first_seq < next_seq_) {
            return; // 스냅샷 이전의 오래된 데이터 버림
        }

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
            if (cur->ingestor_id != "A" && d->ingestor_id == "A") replace = true;
        }
        if (replace) it->second = d;
    }

    void run() {
        while (running_.load()) {
            DeltaPtr d;
            bool got_a = qa_->pop(d);
            if (got_a) insert_or_choose(d);

            bool got_b = qb_->pop(d);
            if (got_b) insert_or_choose(d);

            if (!hold_.empty()) {
                auto first_buffered_seq = hold_.begin()->first;

                if (first_buffered_seq == next_seq_) {
                    auto seen = first_seen_ms_[first_buffered_seq];
                    auto now = now_ms();
                    // 피어 대기 시간이 지났으면 즉시 방출
                    if (now - seen >= (uint64_t)peer_wait_.count()) {
                        maybe_emit();
                    }
                }
                else if (first_buffered_seq > next_seq_) {
                    // Gap 감지: 링버퍼가 오버플로우됨
                    std::cerr << "[Validator-" << sym_ << "] Gap detected! Expected " << next_seq_
                        << " got " << first_buffered_seq << ". Jumping seq.\n";
                    next_seq_ = first_buffered_seq;
                    maybe_emit();
                }
                // else (first_buffered_seq < next_seq_): 스냅샷 동기화 중 버려지는 데이터 (정상)
            }

            // CPU 100% 방지
            if (!got_a && !got_b) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        }
    }

    std::string sym_;
    SPSC_Ring<>* qa_;
    SPSC_Ring<>* qb_;
    asio::io_context& ioc_;
    ssl::context& ssl_;

    int64_t next_seq_;
    std::chrono::milliseconds peer_wait_;
    std::atomic<bool>& running_;
    std::thread       th_;
};

// --- (NEW) Writer 스레드 (Clean Ring -> File) [순수 I/O-Bound] ---
class FileWriter {
public:
    FileWriter(std::string symbol,
        SPSC_Ring<>* clean_q, // 입력 링 버퍼
        const std::string& output_filename,
        std::atomic<bool>& running)
        : sym_(std::move(symbol)), clean_q_(clean_q), running_(running)
    {
        publisher_ = std::make_unique<FilePublisher>(fs::path(output_filename));
    }

    void start(int core_id) {
        th_ = std::thread([this, core_id] {
            pin_thread_to_core(core_id);
            std::cout << "[Writer-" << sym_ << ":" << "Core " << core_id << "] thread start (tid="
                << std::this_thread::get_id() << ")\n" << std::flush;
            this->run();
            });
    }
    void join() { if (th_.joinable()) th_.join(); }

private:
    void run() {
        uint64_t msg_count = 0;
        uint64_t last_log_ms = now_ms();

        while (running_.load()) {
            DeltaPtr d;
            bool got_data = clean_q_->pop(d);

            if (got_data) {
                publisher_->publish(*d);
                msg_count++;
            }
            else {
                // 큐가 비었으면 휴식
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }

            // 1초에 한 번씩 로깅 및 파일 flush
            uint64_t now = now_ms();
            if (now - last_log_ms > 1000) {
                if (msg_count > 0) {
                    std::cout << "[Writer-" << sym_ << "] " << msg_count << " msgs/sec written.\n";
                    publisher_->flush_if_needed();
                    msg_count = 0;
                }
                last_log_ms = now;
            }
        }
    }

    std::string sym_;
    SPSC_Ring<>* clean_q_;
    std::unique_ptr<FilePublisher> publisher_;
    std::atomic<bool>& running_;
    std::thread th_;
};


// ---------- 전역 변수 및 시그널 핸들러 ----------
static std::atomic<bool> g_running(true);

void on_sigint(int) {
    g_running.store(false);
}

// ---------- main ----------
int main(int argc, char** argv) {
    std::signal(SIGINT, on_sigint);
#ifdef SIGTERM
    std::signal(SIGTERM, on_sigint);
#endif

    // --- 1. 설정: 심볼 및 CPU 코어 할당 ---
    std::vector<std::string> symbols = { "BTCUSDT", "ETHUSDT", "SOLUSDT" };
    const int CORE_IO = 0;
    const int CORE_INGESTOR_A = 1;
    const int CORE_INGESTOR_B = 2;
    const int CORE_VALIDATOR_BTC = 3;
    const int CORE_VALIDATOR_ETH = 4;
    const int CORE_VALIDATOR_SOL = 5;
    const int CORE_WRITER_BTC = 6; // 신규
    const int CORE_WRITER_ETH = 7; // 신규
    const int CORE_WRITER_SOL = 8; // 신규

    std::cout << "Starting 8-Thread / 9-Ring Architecture (Binance -> File Output)\n";
    std::cout << "Build: " << __DATE__ << " " << __TIME__ << ". (Press Ctrl+C to terminate)\n";
    std::cout << "Output directory: .\\data\\\n";

    fs::create_directory("data"); // 출력 디렉토리 생성

    try {
        // --- 2. TLS 및 I/O 컨텍스트 준비 ---
        asio::io_context ioc;
        ssl::context ssl_ctx(ssl::context::tls_client);
        ssl_ctx.set_default_verify_paths();
        ssl_ctx.set_verify_mode(ssl::verify_peer);
        try {
            ssl_ctx.load_verify_file("cacert.pem");
        }
        catch (...) {
            std::cerr << "[WARN] cacert.pem not loaded, using OS trust store\n";
        }

        // --- 3. 9개의 링 버퍼(SPSC 큐) 생성 ---
        SPSC_Ring<> btc_a_ring, btc_b_ring;
        SPSC_Ring<> eth_a_ring, eth_b_ring;
        SPSC_Ring<> sol_a_ring, sol_b_ring;
        SPSC_Ring<> btc_clean_ring, eth_clean_ring, sol_clean_ring; // 신규 Clean Rings

        std::map<std::string, SPSC_Ring<>*> queues_a = {
            {"BTCUSDT", &btc_a_ring}, {"ETHUSDT", &eth_a_ring}, {"SOLUSDT", &sol_a_ring}
        };
        std::map<std::string, SPSC_Ring<>*> queues_b = {
            {"BTCUSDT", &btc_b_ring}, {"ETHUSDT", &eth_b_ring}, {"SOLUSDT", &sol_b_ring}
        };

        // --- 4. 8개의 핵심 스레드 객체 생성 ---

        // 2x Ingestors
        BinanceIngestor ingestor_a(ioc, ssl_ctx, "A", symbols, std::move(queues_a), g_running);
        BinanceIngestor ingestor_b(ioc, ssl_ctx, "B", symbols, std::move(queues_b), g_running);

        // 3x Validators (출력이 Clean Ring으로 변경됨)
        Validator validator_btc("BTCUSDT", &btc_a_ring, &btc_b_ring, &btc_clean_ring, ioc, ssl_ctx, std::chrono::milliseconds(5), &g_running);
        Validator validator_eth("ETHUSDT", &eth_a_ring, &eth_b_ring, &eth_clean_ring, ioc, ssl_ctx, std::chrono::milliseconds(5), &g_running);
        Validator validator_sol("SOLUSDT", &sol_a_ring, &sol_b_ring, &sol_clean_ring, ioc, ssl_ctx, std::chrono::milliseconds(5), &g_running);

        // 3x Writers (신규)
        FileWriter writer_btc("BTCUSDT", &btc_clean_ring, "data/BTCUSDT_clean.jsonl", g_running);
        FileWriter writer_eth("ETHUSDT", &eth_clean_ring, "data/ETHUSDT_clean.jsonl", g_running);
        FileWriter writer_sol("SOLUSDT", &sol_clean_ring, "data/SOLUSDT_clean.jsonl", g_running);

        // --- 5. 스레드 시작 ---

        std::thread ioth([&ioc, CORE_IO] {
            pin_thread_to_core(CORE_IO);
            std::cout << "[ASIO-IO:" << "Core " << CORE_IO << "] thread start (tid="
                << std::this_thread::get_id() << ")\n";
            ioc.run();
            });

        ingestor_a.start(CORE_INGESTOR_A);
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
        ingestor_b.start(CORE_INGESTOR_B);

        std::cout << "Waiting for Ingestors to connect...\n";
        std::this_thread::sleep_for(std::chrono::seconds(1));

        validator_btc.start(CORE_VALIDATOR_BTC);
        validator_eth.start(CORE_VALIDATOR_ETH);
        validator_sol.start(CORE_VALIDATOR_SOL);

        writer_btc.start(CORE_WRITER_BTC);
        writer_eth.start(CORE_WRITER_ETH);
        writer_sol.start(CORE_WRITER_SOL);

        // --- 6. 메인 스레드 대기 ---
        while (g_running.load()) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        // --- 7. 종료 절차 ---
        std::cout << "\nShutting down... sending stop signal...\n";
        ioc.stop();

        ingestor_a.join();
        ingestor_b.join();
        std::cout << "Ingestors joined.\n";

        validator_btc.join();
        validator_eth.join();
        validator_sol.join();
        std::cout << "Validators joined.\n";

        writer_btc.join();
        writer_eth.join();
        writer_sol.join();
        std::cout << "Writers joined.\n";

        if (ioth.joinable()) ioth.join();
        std::cout << "I/O thread joined.\n";

        std::cout << "Shutdown complete.\n";
        return 0;

    }
    catch (const std::exception& e) {
        std::cerr << "FATAL Error in main: " << e.what() << "\n";
        return 1;
    }
}
