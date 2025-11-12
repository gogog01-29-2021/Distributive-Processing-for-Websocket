// main.cpp - Bybit Spot(v5) L2 order book: A/B collectors + validator merge (3 symbols = 9 threads)
// Build (MSVC + vcpkg):
//   cl /nologo /EHsc /Zi /std:c++17 /DWIN32_LEAN_AND_MEAN /D_WIN32_WINNT=0x0A00 main.cpp ^
//      /I "C:\BIGDATA3\bigdata\vcpkg\installed\x64-windows\include" ^
//      /Fo".\build\" /Fe"orderbook_rt.exe" ^
//      /link /LIBPATH:"C:\BIGDATA3\bigdata\vcpkg\installed\x64-windows\lib" ^
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

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/lockfree/spsc_queue.hpp>
#include <nlohmann/json.hpp>

namespace fs = std::filesystem;
namespace asio = boost::asio;
namespace ssl  = asio::ssl;
namespace beast = boost::beast;
namespace websocket = beast::websocket;
namespace http = beast::http;
using tcp = asio::ip::tcp;
using json = nlohmann::json;

static inline bool get_num(const json& v, double& out) {
    if (v.is_number_float())   { out = v.get<double>(); return true; }
    if (v.is_number_integer()) { out = static_cast<double>(v.get<int64_t>()); return true; }
    if (v.is_number_unsigned()){ out = static_cast<double>(v.get<uint64_t>()); return true; }
    if (v.is_string())         { out = std::stod(v.get<std::string>()); return true; }
    return false;
}
static inline bool get_i64(const json& v, int64_t& out) {
    if (v.is_number_integer())  { out = v.get<int64_t>(); return true; }
    if (v.is_number_unsigned()) { out = static_cast<int64_t>(v.get<uint64_t>()); return true; }
    if (v.is_string())          { out = std::stoll(v.get<std::string>()); return true; }
    return false;
}
// ---------- time helpers ----------
static inline double now_sec() {
    using namespace std::chrono;
    return duration<double>(std::chrono::system_clock::now().time_since_epoch()).count();
}
static inline uint64_t now_ms() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
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

// ---------- types ----------
struct Delta {
    std::string exchange{"bybit"};
    std::string instance;            // "A" / "B"
    std::string symbol;              // "BTCUSDT"
    int64_t     seq{0};              // from Bybit 'u'
    double      event_ts{0.0};       // from Bybit 't' (ms->sec)
    double      recv_ts{0.0};        // local receive time (sec)
    std::vector<std::pair<double,double>> bids;
    std::vector<std::pair<double,double>> asks;
};
using DeltaPtr = std::shared_ptr<Delta>;

// Single Producer / Single Consumer lock-free queue (pointer to avoid big copies)
template<size_t Capacity>
using SPSC = boost::lockfree::spsc_queue<DeltaPtr, boost::lockfree::capacity<Capacity>>;

// ---------- file publisher ----------
class FilePublisher {
public:
    explicit FilePublisher(const fs::path& p) {
        fs::create_directories(p.parent_path());
        out_.open(p, std::ios::app);
        if(!out_) throw std::runtime_error("Failed to open output " + p.string());
    }
    void publish(const Delta& ev) {
        json j;
        j["exchange"] = ev.exchange;
        j["instance"] = ev.instance;
        j["symbol"]   = ev.symbol;
        j["seq"]      = ev.seq;
        j["event_ts"] = ev.event_ts;
        j["recv_ts"]  = ev.recv_ts;
        auto arr = [](const std::vector<std::pair<double,double>>& v){
            json a = json::array();
            for (auto &pr : v) a.push_back({pr.first, pr.second});
            return a;
        };
        j["bids"] = arr(ev.bids);
        j["asks"] = arr(ev.asks);
        out_ << j.dump() << "\n";
        // out_.flush(); // 필요시 주석 해제
    }
private:
    std::ofstream out_;
};

// ---------- HTTPS GET (for optional snapshot) ----------
static json https_get_json(asio::io_context& ioc, ssl::context& ssl_ctx,
                           const std::string& host, const std::string& target) {
    beast::ssl_stream<beast::tcp_stream> stream(ioc, ssl_ctx);
    if(!SSL_set_tlsext_host_name(stream.native_handle(), host.c_str()))
        throw std::runtime_error("SNI set failed");

    tcp::resolver resolver(ioc);
    auto results = resolver.resolve(host, "443");
    beast::get_lowest_layer(stream).connect(results);
    stream.handshake(ssl::stream_base::client);

    http::request<http::string_body> req{http::verb::get, target, 11};
    req.set(http::field::host, host);
    req.set(http::field::user_agent, "obrt/1.0");
    http::write(stream, req);

    beast::flat_buffer buffer;
    http::response<http::string_body> res;
    http::read(stream, buffer, res);

    beast::error_code ec;
    stream.shutdown(ec);

    if(res.result() != http::status::ok) {
        std::ostringstream oss;
        oss << "HTTP " << (int)res.result() << " for " << target;
        throw std::runtime_error(oss.str());
    }
    return json::parse(res.body());
}

// ---------- Bybit collector (single symbol, single instance) ----------
class BybitCollector {
public:
    BybitCollector(asio::io_context& ioc, ssl::context& ssl_ctx,
                   std::string instance_id, std::string symbol,
                   SPSC<4096>* outQ,
                   std::atomic<bool>& running)
    : ioc_(ioc), ssl_(ssl_ctx), inst_(std::move(instance_id)),
      sym_(std::move(symbol)), outQ_(outQ), running_(running) {}

    void start() {
    th_ = std::thread([this]{
        std::cout << "[bybit:" << inst_ << ":" << sym_ << "] thread start (tid="
                  << std::this_thread::get_id() << ")\n";
        this->run();
    });
    }
    void join() { if(th_.joinable()) th_.join(); }

private:
    void run() {
        // Optional: do an initial REST snapshot (not strictly required for validator to start)
        try {
            auto s = fetch_snapshot_once(sym_);
            auto d = std::make_shared<Delta>();
            d->instance = inst_;
            d->symbol   = sym_;
            d->seq      = s.seq;      // using ts as provisional seq (if needed)
            d->event_ts = s.event_ts;
            d->recv_ts  = now_sec();
            d->bids     = std::move(s.bids);
            d->asks     = std::move(s.asks);
            // snapshot은 validator에서 특별 취급하지 않고 참고용으로만 소비해도 됨
            outQ_->push(d);
            std::cout << "[bybit:" << inst_ << ":" << sym_ << "] snapshot pushed\n";
        } catch (const std::exception& e) {
            std::cerr << "[bybit:" << inst_ << ":" << sym_ << "] snapshot error: " << e.what() << "\n";
        }

        while (running_.load()) {
            try {
                tcp::resolver resolver(ioc_);
                auto results = resolver.resolve("stream.bybit.com", "443");

                beast::ssl_stream<beast::tcp_stream> ss(ioc_, ssl_);
                if(!SSL_set_tlsext_host_name(ss.native_handle(), "stream.bybit.com"))
                    throw std::runtime_error("SNI set failed");
                beast::get_lowest_layer(ss).connect(results);
                ss.handshake(ssl::stream_base::client);

                websocket::stream<beast::ssl_stream<beast::tcp_stream>> ws(std::move(ss));
                ws.set_option(websocket::stream_base::timeout::suggested(beast::role_type::client));
                ws.set_option(websocket::stream_base::decorator(
                    [](websocket::request_type& req){ req.set(http::field::user_agent, "obrt/1.0"); }
                ));
                ws.handshake("stream.bybit.com", "/v5/public/spot");

                // subscribe
                json sub = { {"op","subscribe"}, {"args", {"orderbook.50." + sym_}} };
                ws.write(asio::buffer(sub.dump()));
                std::cout << "[bybit:" << inst_ << ":" << sym_ << "] subscribed\n";

                beast::flat_buffer buffer;
                while (running_.load()) {
                    buffer.clear();
                    ws.read(buffer);
                    auto s = beast::buffers_to_string(buffer.data());
                    if (s.empty()) continue;

                    json m = json::parse(s, nullptr, false);
                    if (m.is_discarded()) continue;
                    if (m.contains("op")) continue;               // ack/ping-pong
                    if (!m.contains("topic")) continue;
                    auto topic = m["topic"].get<std::string>();
                    if (topic.find("orderbook.") != 0) continue;

                    json data = m.value("data", json::object());
                    double t_ms = 0.0; bool has_ts = false;
                    if (data.contains("ts"))           has_ts = get_num(data["ts"], t_ms);
                    if (!has_ts && m.contains("ts"))   has_ts = get_num(m["ts"], t_ms);
                    if (!has_ts && data.contains("t")) has_ts = get_num(data["t"], t_ms);

                    // ---- seq 우선순위: data.u -> top-level seq ----
                    int64_t u = 0; bool has_seq = false;
                    if (data.contains("u"))            has_seq = get_i64(data["u"], u);
                    if (!has_seq && m.contains("seq")) has_seq = get_i64(m["seq"], u);

                    // Delta 생성
                    auto d = std::make_shared<Delta>();
                    d->instance = inst_;
                    d->symbol   = sym_;
                    d->seq      = u;                       // 없으면 0 (Validator가 처리)
                    d->event_ts = has_ts ? (t_ms / 1000.0) : 0.0;
                    d->recv_ts  = now_sec();

                    // 호가 파싱
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

                    // (선택) 한 번만 키 구조 로깅
                    static std::atomic<bool> once{false};
                    if (!once.exchange(true)) {
                        std::cout << "[DEBUG] keys top=";
                        for (auto& it: m.items()) std::cout << it.key() << " ";
                        std::cout << " | data=";
                        for (auto& it: data.items()) std::cout << it.key() << " ";
                        std::cout << "\n";
                    }

                    // (선택) event_ts 없는 프레임 경고
                    if (!has_ts) {
                        std::cerr << "[WARN] no event_ts for " << sym_
                                << " seq=" << d->seq << " (publishing anyway)\n";
                    }

                    outQ_->push(d);
                }
            } catch (const std::exception& e) {
                std::cerr << "[bybit:" << inst_ << ":" << sym_ << "] ws error: "
                          << e.what() << " -> reconnect after 5s\n";
                std::this_thread::sleep_for(std::chrono::seconds(5));
            }
        }
    }

    struct Snapshot {
        std::vector<std::pair<double,double>> bids;
        std::vector<std::pair<double,double>> asks;
        int64_t seq{0};
        double event_ts{0.0};
    };

    Snapshot fetch_snapshot_once(const std::string& symbol) {
        // GET https://api.bybit.com/v5/market/orderbook?category=spot&symbol=BTCUSDT&limit=200
        std::ostringstream tgt;
        tgt << "/v5/market/orderbook?category=spot&symbol=" << symbol << "&limit=200";
        json j = https_get_json(ioc_, ssl_, "api.bybit.com", tgt.str());
        if (j.value("retCode", -1) != 0) throw std::runtime_error("bybit REST retCode != 0");
        json res = j["result"];

        Snapshot s;
        if (res.contains("b")) {
            for (auto& e : res["b"]) {
                double p = std::stod(e[0].get<std::string>());
                double q = std::stod(e[1].get<std::string>());
                s.bids.emplace_back(p,q);
            }
        }
        if (res.contains("a")) {
            for (auto& e : res["a"]) {
                double p = std::stod(e[0].get<std::string>());
                double q = std::stod(e[1].get<std::string>());
                s.asks.emplace_back(p,q);
            }
        }
        double ts_ms = res.value("ts", 0.0);
        s.event_ts = ts_ms / 1000.0;
        s.seq      = static_cast<int64_t>(ts_ms); // provisional seq (WS 'u'와 별도이므로 validator가 유연 처리)
        return s;
    }

    asio::io_context& ioc_;
    ssl::context&     ssl_;
    std::string       inst_;
    std::string       sym_;
    SPSC<4096>*       outQ_{nullptr};
    std::atomic<bool>& running_;
    std::thread       th_;
};

// ---------- Validator (merge A/B by seq with timeout) ----------
class Validator {
public:
    Validator(std::string symbol,
              SPSC<4096>* qa, SPSC<4096>* qb,
              FilePublisher* publisher,
              int64_t start_seq = 0,
              std::chrono::milliseconds peer_wait = std::chrono::milliseconds(5),
              std::chrono::milliseconds idle_flush = std::chrono::milliseconds(20),
              std::atomic<bool>* running = nullptr)
    : sym_(std::move(symbol)), qa_(qa), qb_(qb), pub_(publisher),
      next_seq_(start_seq), peer_wait_(peer_wait), idle_flush_(idle_flush),
      running_(*running) {}

    void start() {
        th_ = std::thread([this]{
            // 스레드 안에서 찍어야 올바른 tid가 나옵니다.
            std::cout << "[validator:" << sym_ << "] thread start (tid="
                    << std::this_thread::get_id() << ")\n" << std::flush;
            this->run();
        });
    }
    void join()  { if(th_.joinable()) th_.join(); }

private:
    // 간단한 보류 버퍼(시퀀스→이벤트), A/B 모두 공동 버퍼에 넣고 seq 키로만 본다
    std::map<int64_t, DeltaPtr> hold_;
    std::map<int64_t, uint64_t> first_seen_ms_; // peer wait 타이머용

    void maybe_emit() {
        // next_seq_가 0이면 가장 작은 키부터 시작(스냅샷 기반 없이 유연하게)
        if (next_seq_ == 0 && !hold_.empty()) {
            next_seq_ = hold_.begin()->first;
        }
        // 연속 seq 출력
        while (!hold_.empty()) {
            auto it = hold_.find(next_seq_);
            if (it == hold_.end()) break;
            emit(*(it->second));
            hold_.erase(it);
            first_seen_ms_.erase(next_seq_);
            ++next_seq_;
        }
    }

    void emit(const Delta& d) {
        // 역행/중복 방지(보수적): 파일 퍼블리시
        pub_->publish(d);
        // Top-of-book 로그 간단 출력
        if (!d.bids.empty() || !d.asks.empty()) {
            double best_bid = NAN, best_ask = NAN;
            if (!d.bids.empty()) best_bid = d.bids[0].first;            // 이미 정렬돼있지 않을 수 있으니 신뢰 X
            if (!d.asks.empty()) best_ask = d.asks[0].first;
            std::cout << "[clean:" << sym_ << "] seq=" << d.seq
                      << " t=" << std::fixed << std::setprecision(3) << d.event_ts
                      << " recv=" << d.recv_ts << "\n";
        }
    }

    void insert_or_choose(DeltaPtr d) {
        // 동일 seq가 이미 있으면 승자 선택(기준: event_ts → recv_ts → instance A 우선)
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
            // instance A 우선 (deterministic)
            if (cur->instance != "A" && d->instance == "A") replace = true;
        }
        if (replace) it->second = d;
    }

    void run() {
        uint64_t last_idle_flush = now_ms();

        while (running_.load()) {
            // 1) 두 큐에서 최대 한 개씩 가져온다(SPSC 특성상 try_pop)
            DeltaPtr d;
            if (qa_->pop(d)) insert_or_choose(d);
            if (qb_->pop(d)) insert_or_choose(d);

            // 2) peer wait (다음 seq가 비워져 있을 때 일정 시간 지난 작은 seq부터 방출)
            if (!hold_.empty()) {
                auto first = hold_.begin()->first;
                auto seen  = first_seen_ms_[first];
                auto now   = now_ms();
                // next_seq_가 0이거나, 첫 키가 next_seq_보다 앞서면 next_seq_ 조정
                if (next_seq_ == 0 || first < next_seq_) next_seq_ = first;
                if (now - seen >= (uint64_t)peer_wait_.count()) {
                    // 시간 초과 → next_seq부터 가능한 만큼 emit
                    maybe_emit();
                }
            }

            // 3) 주기적 idle flush
            uint64_t now = now_ms();
            if (now - last_idle_flush >= (uint64_t)idle_flush_.count()) {
                maybe_emit();
                last_idle_flush = now;
            }

            // 4) 너무 바쁘지 않게 슬립 살짝
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        // 종료 시 남은 것 방출 시도
        maybe_emit();
    }

    std::string sym_;
    SPSC<4096>* qa_;
    SPSC<4096>* qb_;
    FilePublisher* pub_;
    int64_t next_seq_;
    std::chrono::milliseconds peer_wait_;
    std::chrono::milliseconds idle_flush_;
    std::atomic<bool>& running_;
    std::thread th_;
};

// ---------- main ----------
static std::atomic<bool> g_running(true);

void on_sigint(int) {
    g_running.store(false);
}

int main(int argc, char** argv) {
    std::signal(SIGINT, on_sigint);
#ifdef SIGTERM
    std::signal(SIGTERM, on_sigint);
#endif

    // 설정: 원하는 심볼을 여기서 조정
    std::vector<std::string> symbols = {"BTCUSDT", "ETHUSDT", "SOLUSDT"};
    
    std::cout << "Running... build " << __DATE__ << " " << __TIME__
              << "  (Press Ctrl+C to terminate)\n";
    try {
        // TLS
        asio::io_context ioc;
        ssl::context ssl_ctx(ssl::context::tls_client);
        ssl_ctx.set_default_verify_paths();
        ssl_ctx.set_verify_mode(ssl::verify_peer);
        try {
            ssl_ctx.load_verify_file("cacert.pem"); // 있으면 사용
        } catch(...) {
            std::cerr << "[WARN] cacert.pem not loaded, using OS trust store\n";
        }

        // 출력 파일 준비
        fs::path outp = fs::path("data") / "orderbook_clean.jsonl";
        FilePublisher publisher(outp);

        // 심볼별 큐 2개(A/B)와 콜렉터/밸리데이터
        struct Pipes {
            std::unique_ptr<SPSC<4096>> qa;
            std::unique_ptr<SPSC<4096>> qb;
            std::unique_ptr<BybitCollector> ca;
            std::unique_ptr<BybitCollector> cb;
            std::unique_ptr<Validator> val;
        };
        std::vector<Pipes> pipes;
        pipes.reserve(symbols.size());

        for (auto& sym : symbols) {
            Pipes p;
            p.qa = std::make_unique<SPSC<4096>>();
            p.qb = std::make_unique<SPSC<4096>>();

            p.ca = std::make_unique<BybitCollector>(ioc, ssl_ctx, "A", sym, p.qa.get(), g_running);
            p.cb = std::make_unique<BybitCollector>(ioc, ssl_ctx, "B", sym, p.qb.get(), g_running);

            // start_seq=0: 최초 도달 프레임 기준으로 시퀀스 정렬 시작
            p.val = std::make_unique<Validator>(sym, p.qa.get(), p.qb.get(),
                                                &publisher, /*start_seq*/0,
                                                std::chrono::milliseconds(5),
                                                std::chrono::milliseconds(20),
                                                &g_running);
            pipes.emplace_back(std::move(p));
        }

        // 스레드 시작
        std::thread ioth([&]{ ioc.run(); });
        for (auto& p : pipes) { 
            p.ca->start(); 
            std::this_thread::sleep_for(std::chrono::milliseconds(300)); 
            p.cb->start(); 
        }
        for (auto& p : pipes) { p.val->start(); }

        std::cout << "Running... Ctrl+C로 종료\n";
        while (g_running.load()) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        // 종료 절차
        ioc.stop();
        for (auto& p : pipes) { p.ca->join(); p.cb->join(); }
        for (auto& p : pipes) { p.val->join(); }
        if (ioth.joinable()) ioth.join();

        std::cout << "done\n";
        return 0;

    } catch (const std::exception& e) {
        std::cerr << "FATAL: " << e.what() << "\n";
        return 1;
    }
}
