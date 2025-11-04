#pragma once

#include <iostream> 
#include <string>
#include <map>
#include <vector>
#include <utility>
#include <functional>   // std::greater
#include "OrderBookEvent.h"

// ====== 로컬 오더북 ======
struct LocalOrderBook {
    std::string exchange;
    std::string symbol;
    // price -> qty
    std::map<double, double, std::greater<double>> bids; // 내림차순
    std::map<double, double> asks;                       // 오름차순
    std::uint64_t last_seq = 0;

    LocalOrderBook() = default;
    LocalOrderBook(std::string ex, std::string sym)
        : exchange(std::move(ex)), symbol(std::move(sym)) {}

    void apply(const OrderBookEvent& ev) {
        // 시퀀스 체크
        if (ev.seq && ev.seq < last_seq) {
            // 뒤로 간 이벤트면 무시
            return;
        }

        // 아주 단순하게 덮어쓰기
        for (auto& [p, q] : ev.bids) {
            if (q == 0.0) bids.erase(p);
            else bids[p] = q;
        }
        for (auto& [p, q] : ev.asks) {
            if (q == 0.0) asks.erase(p);
            else asks[p] = q;
        }
        last_seq = ev.seq;
    }

    void print_top1() const {
        double best_bid = bids.empty() ? 0.0 : bids.begin()->first;
        double best_ask = asks.empty() ? 0.0 : asks.begin()->first;
        std::cout << "[" << exchange << ":" << symbol << "] "
            << "bid=" << best_bid << " ask=" << best_ask
            << " seq=" << last_seq << "\n";
    }
};
