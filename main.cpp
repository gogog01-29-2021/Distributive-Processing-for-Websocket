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
// main.cpp - Binance Spot L2 (8-Thread / 9-Ring Arch) - 100ms & OPTIMIZED BUFFER
// 
// [최적화 설정]
// 1. 스트림: "@depth@100ms" (0.1초 단위 수신, 안정적이고 충분히 빠름)
// 2. 버퍼 크기: 65,536개 (약 1MB/큐)
//    - 100ms 데이터 기준, 약 1.8시간 분량의 버퍼링 가능. 메모리 효율 최적.
// 3. 쓰기 최적화: 64KB 배치 쓰기 유지.
//
// [빌드 (최적화 옵션 /O2 필수)]
//   cl /nologo /EHsc /Zi /O2 /std:c++17 /DWIN32_LEAN_AND_MEAN /D_WIN32_WINNT=0x0A00 main.cpp ^
//      /I ".\vcpkg\installed\x64-windows\include" ^
//      /Fo".\build\main.obj" /Fe".\build\orderbook_rt.exe" ^
//      /link /LIBPATH:".\vcpkg\installed\x64-windows\lib" ^
//      libssl.lib libcrypto.lib ws2_32.lib crypt32.lib

// main.cpp - Binance Spot L2 (8-Thread / 9-Ring Arch) - 100ms & OPTIMIZED BUFFER
// 
// [최적화 설정]
// 1. 스트림: "@depth@100ms" (0.1초 단위 수신, 안정적이고 충분히 빠름)
// 2. 버퍼 크기: 65,536개 (약 1MB/큐)
//    - 100ms 데이터 기준, 약 1.8시간 분량의 버퍼링 가능. 메모리 효율 최적.
// 3. 쓰기 최적화: 64KB 배치 쓰기 유지.
//
// [빌드 (최적화 옵션 /O2 필수)]
//   cl /nologo /EHsc /Zi /O2 /std:c++17 /DWIN32_LEAN_AND_MEAN /D_WIN32_WINNT=0x0A00 main.cpp ^
//      /I ".\vcpkg\installed\x64-windows\include" ^
//      /Fo".\build\main.obj" /Fe".\build\orderbook_rt.exe" ^
//      /link /LIBPATH:".\vcpkg\installed\x64-windows\lib" ^
//      libssl.lib libcrypto.lib ws2_32.lib crypt32.lib

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
#include <cctype>

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/lockfree/spsc_queue.hpp>
#include <nlohmann/json.hpp>

#ifdef _WIN32
#include <windows.h>
#else
#include <pthread.h>
#endif

namespace fs = std::filesystem;
namespace asio = boost::asio;
namespace ssl = asio::ssl;
namespace beast = boost::beast;
namespace websocket = beast::websocket;
namespace http = beast::http;
using tcp = asio::ip::tcp;
using json = nlohmann::json;

// --- 유틸리티 함수 ---
inline bool get_num(const json& v, double& out) {
    if (v.is_number_float()) { out = v.get<double>(); return true; }
    if (v.is_number_integer()) { out = static_cast<double>(v.get<int64_t>()); return true; }
    if (v.is_number_unsigned()) { out = static_cast<double>(v.get<uint64_t>()); return true; }
    if (v.is_string()) { try { out = std::stod(v.get<std::string>()); return true; } catch (...) { return false; } }
    return false;
}
inline bool get_i64(const json& v, int64_t& out) {
    if (v.is_number_integer()) { out = v.get<int64_t>(); return true; }
    if (v.is_number_unsigned()) { out = static_cast<int64_t>(v.get<uint64_t>()); return true; }
    if (v.is_string()) { try { out = std::stoll(v.get<std::string>()); return true; } catch (...) { return false; } }
    return false;
}
inline double now_sec() {
    using namespace std::chrono;
    return duration<double>(std::chrono::system_clock::now().time_since_epoch()).count();
}
inline uint64_t now_ms() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

void pin_thread_to_core(int core_id) {
    if (core_id < 0) return;
#ifdef _WIN32
    DWORD_PTR mask = (static_cast<DWORD_PTR>(1) << core_id);
    SetThreadAffinityMask(GetCurrentThread(), mask);
#endif
}

// --- 공통 타입 정의 ---
struct Delta {
    std::string exchange{ "binance" };
    std::string ingestor_id;
    std::string symbol;
    int64_t     seq{ 0 };
    int64_t     first_seq{ 0 };
    double      event_ts{ 0.0 };
    double      recv_ts{ 0.0 };
    std::vector<std::pair<double, double>> bids;
    std::vector<std::pair<double, double>> asks;
};
using DeltaPtr = std::shared_ptr<Delta>;

// [설정 변경] 링 버퍼 크기: 65,536 (약 1MB)
// 100ms 데이터 기준 초당 10개 생성 -> 65536개면 약 6500초(1.8시간) 버퍼링 가능.
// 메모리 사용량도 적고 안정성도 충분함.
template<size_t Capacity = 65536>
using SPSC_Ring = boost::lockfree::spsc_queue<DeltaPtr, boost::lockfree::capacity<Capacity>>;

// --- File Publisher ---
class FilePublisher {
public:
    explicit FilePublisher(const fs::path& p) {
        fs::create_directories(p.parent_path());
        out_.open(p, std::ios::app | std::ios::binary);
        if (!out_) throw std::runtime_error("Failed to open output " + p.string());
        buffer_.reserve(65536);
        std::cout << "    [FilePublisher] Outputting to: " << p.string() << "\n";
    }

    ~FilePublisher() {
        flush_buffer();
    }

    void publish(const Delta& ev) {
        json j;
        j["e"] = ev.exchange;
        j["i"] = ev.ingestor_id;
        j["s"] = ev.symbol;
        j["u"] = ev.seq;
        j["U"] = ev.first_seq;
        j["E"] = ev.event_ts;
        j["R"] = ev.recv_ts;

        auto arr = [](const std::vector<std::pair<double, double>>& v) {
            json a = json::array();
            for (auto& pr : v) a.push_back({ pr.first, pr.second });
            return a;
            };
        if (!ev.bids.empty()) j["b"] = arr(ev.bids);
        if (!ev.asks.empty()) j["a"] = arr(ev.asks);

        std::string s = j.dump();
        if (buffer_.size() + s.size() + 1 > 65536) {
            flush_buffer();
        }
        buffer_.append(s);
        buffer_.push_back('\n');
    }

    void flush_if_needed() {
        auto now = now_ms();
        if (now - last_flush_ms_ > 2000) {
            out_.flush();
            last_flush_ms_ = now;
        }
    }

private:
    void flush_buffer() {
        if (buffer_.empty()) return;
        out_.write(buffer_.c_str(), buffer_.size());
        buffer_.clear();
    }

    std::ofstream out_;
    std::string buffer_;
    uint64_t last_flush_ms_ = 0;
};

// --- HTTPS GET ---
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

// --- Ingestor ---
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
            std::transform(sym_lower.begin(), sym_lower.end(), sym_lower.begin(),
                [](unsigned char c) { return std::tolower(c); });

            // [설정 변경] @depth@100ms (안정적, 효율적)
            ws_target_ += sym_lower + "@depth@100ms";
            if (i < symbols_.size() - 1) ws_target_ += "/";
        }

        for (const auto& sym_upper : symbols_) {
            std::string sym_lower = sym_upper;
            std::transform(sym_lower.begin(), sym_lower.end(), sym_lower.begin(),
                [](unsigned char c) { return std::tolower(c); });
            // 맵핑 키 일치
            stream_map_[sym_lower + "@depth@100ms"] = sym_upper;
        }
    }

    void start(int core_id) {
        th_ = std::thread([this, core_id] {
            pin_thread_to_core(core_id);
            std::cout << "[Ingestor-" << ingestor_id_ << ":" << "Core " << core_id << "] start\n";
            this->run();
            });
    }
    void join() { if (th_.joinable()) th_.join(); }

private:
    void run() {
        const std::string host = "stream.binance.com";

        while (running_.load()) {
            try {
                tcp::resolver resolver(ioc_);
                auto results = resolver.resolve(host, "9443");
                beast::ssl_stream<beast::tcp_stream> ss(ioc_, ssl_);
                if (!SSL_set_tlsext_host_name(ss.native_handle(), host.c_str())) throw std::runtime_error("SNI failed");
                beast::get_lowest_layer(ss).connect(results);
                ss.handshake(ssl::stream_base::client);
                websocket::stream<beast::ssl_stream<beast::tcp_stream>> ws(std::move(ss));
                ws.set_option(websocket::stream_base::timeout::suggested(beast::role_type::client));
                ws.handshake(host, ws_target_);
                std::cout << "[Ingestor-" << ingestor_id_ << "] connected to " << ws_target_ << "\n";

                beast::flat_buffer buffer;
                while (running_.load()) {
                    buffer.clear();
                    ws.read(buffer);
                    auto s = beast::buffers_to_string(buffer.data());
                    if (s.empty()) continue;

                    json m = json::parse(s, nullptr, false);
                    if (m.is_discarded()) continue;

                    std::string stream_name = m.value("stream", "");
                    auto map_it = stream_map_.find(stream_name);
                    if (map_it == stream_map_.end()) continue;

                    std::string symbol = map_it->second;
                    json data = m.value("data", json::object());
                    if (!data.contains("u")) continue;

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

                    auto* q = output_queues_.at(symbol);
                    // Backpressure는 유지하되, 큐가 작아도 100ms 데이터는 충분히 처리 가능
                    while (!q->push(d) && running_.load()) {
                        std::this_thread::sleep_for(std::chrono::microseconds(1));
                    }
                }
            }
            catch (const std::exception& e) {
                std::cerr << "[Ingestor-" << ingestor_id_ << "] error: " << e.what() << " -> retry 5s\n";
                std::this_thread::sleep_for(std::chrono::seconds(5));
            }
        }
    }
    asio::io_context& ioc_;
    ssl::context& ssl_;
    std::string ingestor_id_;
    std::vector<std::string> symbols_;
    std::string ws_target_;
    std::map<std::string, std::string> stream_map_;
    std::map<std::string, SPSC_Ring<>*> output_queues_;
    std::atomic<bool>& running_;
    std::thread th_;
};

// --- Validator ---
class Validator {
public:
    Validator(std::string symbol, SPSC_Ring<>* qa, SPSC_Ring<>* qb, SPSC_Ring<>* clean_q,
        asio::io_context& ioc, ssl::context& ssl_ctx, std::atomic<bool>* running)
        : sym_(std::move(symbol)), qa_(qa), qb_(qb), clean_q_(clean_q),
        ioc_(ioc), ssl_(ssl_ctx), running_(*running) {
    }

    void start(int core_id) {
        th_ = std::thread([this, core_id] {
            pin_thread_to_core(core_id);
            std::cout << "[Validator-" << sym_ << "] start\n";
            try { load_snapshot(); }
            catch (const std::exception& e) {
                std::cerr << "[Validator-" << sym_ << "] Snap fail: " << e.what() << "\n";
            }
            this->run();
            });
    }
    void join() { if (th_.joinable()) th_.join(); }

private:
    void load_snapshot() {
        std::string host = "api.binance.com";
        std::string target = "/api/v3/depth?symbol=" + sym_ + "&limit=1000";

        json j = https_get_json(ioc_, ssl_, host, target);
        if (!j.contains("lastUpdateId")) throw std::runtime_error("No lastUpdateId");
        int64_t last_id = 0;
        get_i64(j["lastUpdateId"], last_id);
        next_seq_ = last_id + 1;
        std::cout << "[Validator-" << sym_ << "] Snapshot OK. Expect U <= " << next_seq_ << "\n";
    }

    int64_t next_seq_ = 0;
    std::map<int64_t, DeltaPtr> hold_;
    SPSC_Ring<>* clean_q_;

    void maybe_emit() {
        if (next_seq_ == 0) return;

        while (!hold_.empty()) {
            auto it = hold_.begin();
            DeltaPtr d = it->second;

            if (d->seq < next_seq_) {
                hold_.erase(it);
                continue;
            }

            // 100ms 스트림은 U-u 범위로 오므로 범위 검사 필수
            if (d->first_seq <= next_seq_) {
                while (!clean_q_->push(d) && running_.load()) {
                    std::this_thread::sleep_for(std::chrono::microseconds(1));
                }
                next_seq_ = d->seq + 1;
                hold_.erase(it);
            }
            else {
                // Gap 발생: 버퍼가 꽉 찰 정도면 강제 점프 (여기서는 5000개 기준)
                // 100ms 모드에서는 이 상황이 거의 발생하지 않음
                if (hold_.size() > 5000) {
                    std::cerr << "[Validator-" << sym_ << "] FORCE JUMP: exp " << next_seq_
                        << " -> got range [" << d->first_seq << "-" << d->seq << "]\n";
                    next_seq_ = d->seq + 1;
                    while (!clean_q_->push(d) && running_.load())
                        std::this_thread::sleep_for(std::chrono::microseconds(1));
                    hold_.erase(it);
                }
                else {
                    break;
                }
            }
        }
    }

    void insert(DeltaPtr d) {
        if (d->seq < next_seq_) return;
        auto it = hold_.find(d->seq);
        if (it == hold_.end()) {
            hold_.emplace(d->seq, d);
        }
    }

    void run() {
        while (running_.load()) {
            DeltaPtr d;
            bool work = false;
            if (qa_->pop(d)) { insert(d); work = true; }
            if (qb_->pop(d)) { insert(d); work = true; }

            if (work) maybe_emit();

            if (!work) std::this_thread::sleep_for(std::chrono::microseconds(1));
        }
    }

    std::string sym_;
    SPSC_Ring<>* qa_;
    SPSC_Ring<>* qb_;
    asio::io_context& ioc_;
    ssl::context& ssl_;
    std::atomic<bool>& running_;
    std::thread th_;
};

// --- Writer ---
class FileWriter {
public:
    FileWriter(std::string symbol, SPSC_Ring<>* clean_q, const std::string& fname, std::atomic<bool>& running)
        : sym_(std::move(symbol)), clean_q_(clean_q), running_(running) {
        publisher_ = std::make_unique<FilePublisher>(fs::path(fname));
    }
    void start(int core_id) {
        th_ = std::thread([this, core_id] {
            pin_thread_to_core(core_id);
            std::cout << "[Writer-" << sym_ << ":" << "Core " << core_id << "] start\n";
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
            if (clean_q_->pop(d)) {
                publisher_->publish(*d);
                msg_count++;
            }
            else {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }

            uint64_t now = now_ms();
            if (now - last_log_ms > 1000) {
                if (msg_count > 0) {
                    std::cout << "[Writer-" << sym_ << "] " << msg_count << " msgs/sec\n";
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

static std::atomic<bool> g_running(true);
void on_sigint(int) { g_running.store(false); }

int main(int argc, char** argv) {
    std::signal(SIGINT, on_sigint);
    std::vector<std::string> symbols = { "BTCUSDT", "ETHUSDT", "SOLUSDT" };
    const int CORE_IO = 0;
    const int CORE_INGESTOR_A = 1;
    const int CORE_INGESTOR_B = 2;
    const int CORE_VALIDATOR_BTC = 3;
    const int CORE_VALIDATOR_ETH = 4;
    const int CORE_VALIDATOR_SOL = 5;
    const int CORE_WRITER_BTC = 6;
    const int CORE_WRITER_ETH = 7;
    const int CORE_WRITER_SOL = 8;

    std::cout << "Starting 8-Thread / 9-Ring Arch (Binance Spot)\n";
    std::cout << "Mode: @depth@100ms (Optimized Buffer)\n";
    fs::create_directory("data");

    try {
        asio::io_context ioc;
        ssl::context ssl_ctx(ssl::context::tls_client);
        ssl_ctx.set_default_verify_paths();
        ssl_ctx.set_verify_mode(ssl::verify_peer);
        try { ssl_ctx.load_verify_file("cacert.pem"); }
        catch (...) {}

        // 힙에 할당 (65,536 size)
        auto btc_a_ring = std::make_unique<SPSC_Ring<>>();
        auto btc_b_ring = std::make_unique<SPSC_Ring<>>();
        auto eth_a_ring = std::make_unique<SPSC_Ring<>>();
        auto eth_b_ring = std::make_unique<SPSC_Ring<>>();
        auto sol_a_ring = std::make_unique<SPSC_Ring<>>();
        auto sol_b_ring = std::make_unique<SPSC_Ring<>>();

        auto btc_clean_ring = std::make_unique<SPSC_Ring<>>();
        auto eth_clean_ring = std::make_unique<SPSC_Ring<>>();
        auto sol_clean_ring = std::make_unique<SPSC_Ring<>>();

        std::map<std::string, SPSC_Ring<>*> queues_a = {
            {"BTCUSDT", btc_a_ring.get()}, {"ETHUSDT", eth_a_ring.get()}, {"SOLUSDT", sol_a_ring.get()}
        };
        std::map<std::string, SPSC_Ring<>*> queues_b = {
            {"BTCUSDT", btc_b_ring.get()}, {"ETHUSDT", eth_b_ring.get()}, {"SOLUSDT", sol_b_ring.get()}
        };

        BinanceIngestor ingestor_a(ioc, ssl_ctx, "A", symbols, std::move(queues_a), g_running);
        BinanceIngestor ingestor_b(ioc, ssl_ctx, "B", symbols, std::move(queues_b), g_running);

        Validator validator_btc("BTCUSDT", btc_a_ring.get(), btc_b_ring.get(), btc_clean_ring.get(), ioc, ssl_ctx, &g_running);
        Validator validator_eth("ETHUSDT", eth_a_ring.get(), eth_b_ring.get(), eth_clean_ring.get(), ioc, ssl_ctx, &g_running);
        Validator validator_sol("SOLUSDT", sol_a_ring.get(), sol_b_ring.get(), sol_clean_ring.get(), ioc, ssl_ctx, &g_running);

        FileWriter writer_btc("BTCUSDT", btc_clean_ring.get(), "data/BTCUSDT_clean.jsonl", g_running);
        FileWriter writer_eth("ETHUSDT", eth_clean_ring.get(), "data/ETHUSDT_clean.jsonl", g_running);
        FileWriter writer_sol("SOLUSDT", sol_clean_ring.get(), "data/SOLUSDT_clean.jsonl", g_running);

        std::thread ioth([&ioc, CORE_IO] {
            pin_thread_to_core(CORE_IO);
            std::cout << "[ASIO-IO] start\n";
            ioc.run();
            });

        ingestor_a.start(CORE_INGESTOR_A);
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
        ingestor_b.start(CORE_INGESTOR_B);

        std::this_thread::sleep_for(std::chrono::seconds(2));

        validator_btc.start(CORE_VALIDATOR_BTC);
        validator_eth.start(CORE_VALIDATOR_ETH);
        validator_sol.start(CORE_VALIDATOR_SOL);

        writer_btc.start(CORE_WRITER_BTC);
        writer_eth.start(CORE_WRITER_ETH);
        writer_sol.start(CORE_WRITER_SOL);

        while (g_running.load()) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        ioc.stop();
        if (ioth.joinable()) ioth.join();
        ingestor_a.join(); ingestor_b.join();
        validator_btc.join(); validator_eth.join(); validator_sol.join();
        writer_btc.join(); writer_eth.join(); writer_sol.join();
    }
    catch (const std::exception& e) {
        std::cerr << "FATAL Error in main: " << e.what() << "\n";
        return 1;
    }
    return 0;
}
