// main.cpp - Real-time OrderBook (Bybit Spot v5) snapshot+delta, JSONL store
// C++17 OK. Build: link with OpenSSL (libssl, libcrypto), Boost (Beast/Asio), nlohmann/json
// Run example:
//   orderbook_rt.exe --symbols BTCUSDT,ETHUSDT,SOLUSDT --instance A --seconds 0

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <mutex>
#include <optional>
#include <queue>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket.hpp>
#include <nlohmann/json.hpp>

namespace fs = std::filesystem;
namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
namespace websocket = beast::websocket;
using tcp = asio::ip::tcp;
using json = nlohmann::json;

// ---------- time helpers ----------
static inline double now_sec() {
    using namespace std::chrono;
    return duration<double>(std::chrono::system_clock::now().time_since_epoch()).count();
}
static inline std::string now_date() {
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

// ---------- MPSC queue ----------
template <typename T>
class MPSCQueue {
public:
    void push(T v) {
        {
            std::lock_guard<std::mutex> lk(mu_);
            q_.push(std::move(v));
        }
        cv_.notify_one();
    }
    bool pop(T& out) {
        std::unique_lock<std::mutex> lk(mu_);
        cv_.wait(lk, [&] { return !q_.empty() || stopped_; });
        if (q_.empty()) return false;
        out = std::move(q_.front());
        q_.pop();
        return true;
    }
    void stop() {
        {
            std::lock_guard<std::mutex> lk(mu_);
            stopped_ = true;
        }
        cv_.notify_all();
    }
private:
    std::queue<T> q_;
    std::mutex mu_;
    std::condition_variable cv_;
    bool stopped_ = false;
};

// ---------- Event ----------
struct OrderBookEvent {
    std::string exchange;     // "bybit"
    std::string instance_id;  // "A"/"B"
    std::string symbol;       // "BTCUSDT"
    double event_ts = 0.0;    // exchange ts (sec)
    double recv_ts = 0.0;     // local recv ts (sec)
    long long seq = 0;        // simple seq (we use ts for snapshot)

    // deltas (price, qty)
    std::vector<std::pair<double, double>> bids;
    std::vector<std::pair<double, double>> asks;
};

// ---------- LocalOrderBook ----------
class LocalOrderBook {
public:
    explicit LocalOrderBook(std::string exch, std::string sym)
        : exchange_(std::move(exch)), symbol_(std::move(sym)) {}

    void apply(const OrderBookEvent& ev) {
        for (size_t i = 0; i < ev.bids.size(); ++i) {
            double p = ev.bids[i].first, q = ev.bids[i].second;
            if (q == 0.0) bids_.erase(p); else bids_[p] = q;
        }
        for (size_t i = 0; i < ev.asks.size(); ++i) {
            double p = ev.asks[i].first, q = ev.asks[i].second;
            if (q == 0.0) asks_.erase(p); else asks_[p] = q;
        }
        last_seq_ = ev.seq;
    }
    void overwrite_from_snapshot(const std::vector<std::pair<double, double>>& b,
        const std::vector<std::pair<double, double>>& a,
        long long seq) {
        bids_.clear(); asks_.clear();
        for (size_t i = 0; i < b.size(); ++i) bids_[b[i].first] = b[i].second;
        for (size_t i = 0; i < a.size(); ++i) asks_[a[i].first] = a[i].second;
        last_seq_ = seq;
    }
    std::vector<std::pair<double, double>> top_bids(int n) const {
        std::vector<std::pair<double, double>> v; v.reserve(n);
        for (auto it = bids_.rbegin(); it != bids_.rend() && (int)v.size() < n; ++it)
            v.emplace_back(it->first, it->second);
        return v;
    }
    std::vector<std::pair<double, double>> top_asks(int n) const {
        std::vector<std::pair<double, double>> v; v.reserve(n);
        for (auto it = asks_.begin(); it != asks_.end() && (int)v.size() < n; ++it)
            v.emplace_back(it->first, it->second);
        return v;
    }
    void print_top1() const {
        auto b = top_bids(1), a = top_asks(1);
        double bid = b.empty() ? NAN : b[0].first;
        double ask = a.empty() ? NAN : a[0].first;
        std::cout << "[" << exchange_ << ":" << symbol_ << "] "
            << "bid=" << (std::isnan(bid) ? -1 : bid)
            << " ask=" << (std::isnan(ask) ? -1 : ask)
            << " (seq=" << last_seq_ << ")\n";
    }
    const std::string& symbol() const { return symbol_; }
    long long last_seq() const { return last_seq_; }
    const std::map<double, double>& bids_map() const { return bids_; }
    const std::map<double, double>& asks_map() const { return asks_; }
private:
    std::string exchange_;
    std::string symbol_;
    std::map<double, double> bids_; // price asc
    std::map<double, double> asks_; // price asc
    long long last_seq_ = 0;
};

// ---------- JSON helpers ----------
static json pairs_to_json_array(const std::vector<std::pair<double, double>>& v) {
    json arr = json::array();
    for (size_t i = 0; i < v.size(); ++i) arr.push_back({ v[i].first, v[i].second });
    return arr;
}
static json map_topN_to_json(const std::map<double, double>& m, bool reverse, int n) {
    json arr = json::array();
    if (reverse) {
        int c = 0; for (auto it = m.rbegin(); it != m.rend() && c < n; ++it, ++c) arr.push_back({ it->first,it->second });
    }
    else {
        int c = 0; for (auto it = m.begin(); it != m.end() && c < n; ++it, ++c) arr.push_back({ it->first,it->second });
    }
    return arr;
}
static fs::path devdb_path(const OrderBookEvent& ev) {
    fs::path base = fs::path("data") / "devdb" / now_date() / ev.exchange / ev.symbol;
    fs::create_directories(base);
    return base / (ev.instance_id + ".jsonl");
}

// ---------- Broadcaster & Workers ----------
static void broadcaster(MPSCQueue<OrderBookEvent>& src,
    MPSCQueue<OrderBookEvent>& rtQ,
    MPSCQueue<OrderBookEvent>& stQ,
    std::atomic<bool>& running) {
    while (running.load()) {
        OrderBookEvent ev;
        if (!src.pop(ev)) break;
        rtQ.push(ev);
        stQ.push(std::move(ev));
    }
}
static void realtime_worker(MPSCQueue<OrderBookEvent>& q,
    std::map<std::string, LocalOrderBook>& books,
    std::atomic<bool>& running) {
    std::unordered_map<std::string, double> last_seen;
    while (running.load()) {
        OrderBookEvent ev;
        if (!q.pop(ev)) break;
        std::string key = ev.exchange + ":" + ev.symbol + ":" + ev.instance_id;
        std::map<std::string, LocalOrderBook>::iterator it = books.find(key);
        if (it == books.end()) it = books.emplace(key, LocalOrderBook(ev.exchange, ev.symbol)).first;
        it->second.apply(ev);

        double now = ev.recv_ts;
        double prev = now;
        if (last_seen.find(key) != last_seen.end()) prev = last_seen[key];
        last_seen[key] = now;

        std::vector<std::pair<double, double>> tb = it->second.top_bids(1);
        std::vector<std::pair<double, double>> ta = it->second.top_asks(1);
        double bid = tb.empty() ? NAN : tb[0].first;
        double ask = ta.empty() ? NAN : ta[0].first;

        std::cout << "[" << ev.exchange << ":" << ev.instance_id << ":" << ev.symbol
            << "] Δt=" << std::fixed << std::setprecision(3) << (now - prev)
            << "s bid=" << (std::isnan(bid) ? -1 : bid)
            << " ask=" << (std::isnan(ask) ? -1 : ask) << "\n";
    }
}
static void store_worker(MPSCQueue<OrderBookEvent>& q,
    std::map<std::string, LocalOrderBook>& books,
    std::atomic<bool>& running) {
    fs::create_directories("data");
    std::ofstream fout_all(fs::path("data") / "orderbook_stream.jsonl", std::ios::app);
    std::unordered_map<std::string, std::unique_ptr<std::ofstream>> cache;

    while (running.load()) {
        OrderBookEvent ev;
        if (!q.pop(ev)) break;

        std::string key = ev.exchange + ":" + ev.symbol + ":" + ev.instance_id;
        LocalOrderBook* lob = nullptr;
        std::map<std::string, LocalOrderBook>::iterator it = books.find(key);
        if (it != books.end()) lob = &it->second;

        json j;
        j["exchange"] = ev.exchange;
        j["instance_id"] = ev.instance_id;
        j["symbol"] = ev.symbol;
        j["event_ts"] = ev.event_ts;
        j["recv_ts"] = ev.recv_ts;
        j["seq"] = ev.seq;
        j["raw"] = { {"b", pairs_to_json_array(ev.bids)},
                     {"a", pairs_to_json_array(ev.asks)} };

        if (lob) {
            j["depth"] = {
                {"symbol", lob->symbol()},
                {"last_seq", lob->last_seq()},
                {"bids", map_topN_to_json(lob->bids_map(), true, 20)},
                {"asks", map_topN_to_json(lob->asks_map(), false, 20)}
            };
        }

        fout_all << j.dump() << "\n";
        fout_all.flush();

        fs::path p = devdb_path(ev);
        std::string pk = p.string();
        if (cache.find(pk) == cache.end())
            cache[pk] = std::unique_ptr<std::ofstream>(new std::ofstream(p, std::ios::app));
        *(cache[pk]) << j.dump() << "\n";
        cache[pk]->flush();
    }
}

// ---------- HTTPS GET (OpenSSL + Beast) ----------
static json https_get_json(asio::io_context& ioc, asio::ssl::context& ssl_ctx,
    const std::string& host, const std::string& target) {
    beast::ssl_stream<beast::tcp_stream> stream(ioc, ssl_ctx);
    if (!SSL_set_tlsext_host_name(stream.native_handle(), host.c_str()))
        throw std::runtime_error("SNI set failed");

    tcp::resolver resolver(ioc);
    auto results = resolver.resolve(host, "443");
    beast::get_lowest_layer(stream).connect(results);
    stream.handshake(asio::ssl::stream_base::client);

    http::request<http::string_body> req{ http::verb::get, target, 11 };
    req.set(http::field::host, host);
    req.set(http::field::user_agent, "obrt/1.0");
    http::write(stream, req);

    beast::flat_buffer buffer;
    http::response<http::string_body> res;
    http::read(stream, buffer, res);

    beast::error_code ec;
    stream.shutdown(ec); // ignore shutdown error

    if (res.result() != http::status::ok) {
        std::ostringstream oss;
        oss << "HTTP " << (int)res.result() << " for " << target;
        throw std::runtime_error(oss.str());
    }
    return json::parse(res.body());
}

// ---------- Bybit Spot Session ----------
class BybitSession : public std::enable_shared_from_this<BybitSession> {
public:
    BybitSession(asio::io_context& ioc, asio::ssl::context& ssl,
        std::string instance_id,
        std::vector<std::string> symbols,
        MPSCQueue<OrderBookEvent>& out)
        : ioc_(ioc), ssl_(ssl), inst_(instance_id), syms_(symbols), out_(out) {}

    void run() {
        for (size_t i = 0; i < syms_.size(); ++i) {
            std::string s = syms_[i];
            std::thread th([self = shared_from_this(), s] { self->run_one(s); });
            th.detach();
        }
    }

private:
    struct Snapshot {
        std::vector<std::pair<double, double>> bids;
        std::vector<std::pair<double, double>> asks;
        long long seq = 0;
        double event_ts = 0.0;
    };

    Snapshot bybit_snapshot(const std::string& symbol) {
        // GET https://api.bybit.com/v5/market/orderbook?category=spot&symbol=BTCUSDT&limit=200
        std::ostringstream tgt;
        tgt << "/v5/market/orderbook?category=spot&symbol=" << symbol << "&limit=200";
        json j = https_get_json(ioc_, ssl_, "api.bybit.com", tgt.str());
        if (j.value("retCode", -1) != 0) throw std::runtime_error("bybit REST retCode != 0");

        json res = j["result"];
        Snapshot s;
        if (res.contains("b")) {
            for (size_t i = 0; i < res["b"].size(); ++i) {
                const json& e = res["b"][i];
                double p = std::stod(e[0].get<std::string>());
                double q = std::stod(e[1].get<std::string>());
                s.bids.emplace_back(p, q);
            }
        }
        if (res.contains("a")) {
            for (size_t i = 0; i < res["a"].size(); ++i) {
                const json& e = res["a"][i];
                double p = std::stod(e[0].get<std::string>());
                double q = std::stod(e[1].get<std::string>());
                s.asks.emplace_back(p, q);
            }
        }
        double ts_ms = 0.0;
        if (res.contains("ts")) ts_ms = res["ts"].get<double>();
        s.event_ts = ts_ms / 1000.0;
        s.seq = static_cast<long long>(ts_ms); // 임시 seq
        return s;
    }

    void run_one(const std::string symbol) {
        // 1) snapshot 먼저
        try {
            Snapshot snap = bybit_snapshot(symbol);
            OrderBookEvent ev;
            ev.exchange = "bybit";
            ev.instance_id = inst_;
            ev.symbol = symbol;
            ev.event_ts = snap.event_ts;
            ev.recv_ts = now_sec();
            ev.seq = snap.seq;
            ev.bids = snap.bids;
            ev.asks = snap.asks;
            out_.push(ev);
            std::cout << "[bybit:" << inst_ << ":" << symbol << "] snapshot loaded\n";
        }
        catch (const std::exception& e) {
            std::cerr << "[bybit:" << inst_ << ":" << symbol << "] REST error: " << e.what() << "\n";
        }

        // 2) websocket: wss://stream.bybit.com/v5/public/spot
        for (;;) {
            try {
                tcp::resolver resolver(ioc_);
                auto results = resolver.resolve("stream.bybit.com", "443");

                beast::ssl_stream<beast::tcp_stream> ss(ioc_, ssl_);
                if (!SSL_set_tlsext_host_name(ss.native_handle(), "stream.bybit.com"))
                    throw std::runtime_error("SNI set failed");
                beast::get_lowest_layer(ss).connect(results);
                ss.handshake(asio::ssl::stream_base::client);

                websocket::stream<beast::ssl_stream<beast::tcp_stream>> ws(std::move(ss));
                ws.set_option(websocket::stream_base::timeout::suggested(beast::role_type::client));
                ws.set_option(websocket::stream_base::decorator(
                    [](websocket::request_type& req) {
                        req.set(http::field::user_agent, "obrt/1.0");
                    }
                ));
                ws.handshake("stream.bybit.com", "/v5/public/spot");

                json sub = { {"op","subscribe"}, {"args", {"orderbook.50." + symbol}} };
                ws.write(asio::buffer(sub.dump()));
                std::cout << "[bybit:" << inst_ << ":" << symbol << "] subscribed " << sub.dump() << "\n";

                beast::flat_buffer buffer;
                for (;;) {
                    buffer.clear();
                    ws.read(buffer);
                    std::string s = beast::buffers_to_string(buffer.data());
                    if (s.empty()) continue;

                    json m = json::parse(s, nullptr, false);
                    if (m.is_discarded()) continue;
                    if (m.contains("op")) continue;                 // ping/pong/ack
                    if (!m.contains("topic")) continue;
                    std::string topic = m["topic"].get<std::string>();
                    if (topic.find("orderbook.") != 0) continue;

                    json data = m.value("data", json::object());
                    std::vector<std::pair<double, double>> b, a;
                    double ts_ms = 0.0;
                    long long seq = 0;

                    if (data.contains("t")) ts_ms = data["t"].get<double>();
                    if (data.contains("u")) seq = data["u"].get<long long>();
                    if (data.contains("b")) {
                        for (size_t i = 0; i < data["b"].size(); ++i) {
                            const json& row = data["b"][i];
                            double p = std::stod(row[0].get<std::string>());
                            double q = std::stod(row[1].get<std::string>());
                            b.emplace_back(p, q);
                        }
                    }
                    if (data.contains("a")) {
                        for (size_t i = 0; i < data["a"].size(); ++i) {
                            const json& row = data["a"][i];
                            double p = std::stod(row[0].get<std::string>());
                            double q = std::stod(row[1].get<std::string>());
                            a.emplace_back(p, q);
                        }
                    }

                    OrderBookEvent ev;
                    ev.exchange = "bybit";
                    ev.instance_id = inst_;
                    ev.symbol = symbol;
                    ev.event_ts = ts_ms / 1000.0;
                    ev.recv_ts = now_sec();
                    ev.seq = seq;
                    ev.bids = b;
                    ev.asks = a;
                    out_.push(ev);
                }
            }
            catch (const std::exception& e) {
                std::cerr << "[bybit:" << inst_ << ":" << symbol << "] ws error: "
                    << e.what() << " → reconnect in 5s\n";
                std::this_thread::sleep_for(std::chrono::seconds(5));
            }
        }
    }

private:
    asio::io_context& ioc_;
    asio::ssl::context& ssl_;
    std::string inst_;
    std::vector<std::string> syms_;
    MPSCQueue<OrderBookEvent>& out_;
};

// ---------- CLI ----------
struct Args {
    std::vector<std::string> symbols;
    std::string instance_id = "A";
    int run_seconds = 0; // 0 = run forever
    std::string cafile;
};



static Args parse_args(int argc, char** argv) {
    Args a;
    a.symbols = { "BTCUSDT","ETHUSDT","SOLUSDT" };
    for (int i = 1; i < argc; ++i) {
        std::string s = argv[i];
        if (s == "--symbols" && i + 1 < argc) {
            a.symbols.clear();
            std::string v = argv[++i];
            std::stringstream ss(v);
            std::string tok;
            while (std::getline(ss, tok, ',')) if (!tok.empty()) a.symbols.push_back(tok);
        }
        else if (s == "--instance" && i + 1 < argc) {
            a.instance_id = argv[++i];
        }
        else if (s == "--seconds" && i + 1 < argc) {
            a.run_seconds = std::stoi(argv[++i]);
        }
        else if (s == "--cafile" && i + 1 < argc) {
            a.cafile = argv[++i];
        }
    }
    return a;
}

// ---------- main ----------
int main(int argc, char** argv) {
    Args args = parse_args(argc, argv);

    // queues & LOB maps
    MPSCQueue<OrderBookEvent> centralQ, realtimeQ, storeQ;
    std::map<std::string, LocalOrderBook> books_rt, books_st;
    std::atomic<bool> running(true);

    // TLS
    asio::io_context ioc;
    asio::ssl::context ssl_ctx(asio::ssl::context::tls_client);
    ssl_ctx.set_default_verify_paths();
    ssl_ctx.set_verify_mode(asio::ssl::verify_peer);

    // 루트 인증서(cacert.pem) 로드 (실행 파일 위치 기준)
    try {
        // 프로젝트 루트나 실행 폴더 둘 중 하나에 두었다는 가정
        ssl_ctx.load_verify_file("cacert.pem");
    }
    catch (const std::exception& e) {
        std::cerr << "[WARN] Failed to load cacert.pem: " << e.what()
            << "\n       Place 'cacert.pem' next to the EXE or project root.\n";
    }

    // Bybit (Spot)
    std::shared_ptr<BybitSession> bybit(new BybitSession(ioc, ssl_ctx, args.instance_id, args.symbols, centralQ));
    bybit->run();

    // threads
    std::thread ioth([&] { ioc.run(); });
    std::thread fanout([&] { broadcaster(centralQ, realtimeQ, storeQ, running); });
    std::thread rt([&] { realtime_worker(realtimeQ, books_rt, running); });
    std::thread st([&] { store_worker(storeQ, books_st, running); });

    if (args.run_seconds > 0) {
        std::this_thread::sleep_for(std::chrono::seconds(args.run_seconds));
        running.store(false);
        centralQ.stop(); realtimeQ.stop(); storeQ.stop();
        ioc.stop();
    }
    else {
        std::cout << "Running... Press Ctrl+C to terminate.\n";
        // 간단 대기 (운영 시 서비스 매니저 사용 권장)
        std::this_thread::sleep_for(std::chrono::hours(24 * 365));
    }

    if (ioth.joinable()) ioth.join();
    if (fanout.joinable()) fanout.join();
    if (rt.joinable()) rt.join();
    if (st.joinable()) st.join();

    std::cout << "done\n";
    return 0;
}
