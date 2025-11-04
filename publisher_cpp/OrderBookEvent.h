// OrderBookEvent.h
#pragma once

#include <string>
#include <vector>
#include <utility>
#include <cstdint>

// ====== 공통 이벤트 ======
struct OrderBookEvent {
    std::string exchange;   // "binance", "okx", "bybit"
    std::string symbol;     // "BTC-USD", ...
    std::string instance_id;// "A" / "B"  (인스턴스 구분)
    double      event_ts;   // exchange가 준 시각 (sec)
    double      recv_ts;    // 우리가 받은 시각 (sec, system_clock)
    std::uint64_t seq;      // exchange 시퀀스

    std::vector<std::pair<double, double>> bids;
    std::vector<std::pair<double, double>> asks;
};
