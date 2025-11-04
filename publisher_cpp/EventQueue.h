#pragma once
#include <queue>
#include <mutex>
#include <condition_variable>
#include "OrderBookEvent.h"

// ====== simple MPSC queue ======
class EventQueue {
public:
    // lvalue push
    void push(const OrderBookEvent& ev) {
        {
            std::lock_guard<std::mutex> lg(m_);
            q_.push(ev);
        }
        cv_.notify_one();
    }
    // rvalue push (불필요한 복사 제거)
    void push(OrderBookEvent&& ev) {
        {
            std::lock_guard<std::mutex> lg(m_);
            q_.push(std::move(ev));
        }
        cv_.notify_one();
    }

    // block pop: true면 out에 유효 이벤트, false면 종료
    bool pop(OrderBookEvent& out) {
        std::unique_lock<std::mutex> lk(m_);
        cv_.wait(lk, [&] { return stop_ || !q_.empty(); }); // ✅ 스푸리어스 방지
        if (stop_ && q_.empty()) return false;             // ✅ 종료 안전하게
        out = std::move(q_.front());                       // ✅ move
        q_.pop();
        return true;
    }

    // 타임아웃 pop (필요시 사용)
    bool pop_for(OrderBookEvent& out, std::chrono::milliseconds dur) {
        std::unique_lock<std::mutex> lk(m_);
        if (!cv_.wait_for(lk, dur, [&] { return stop_ || !q_.empty(); }))
            return false; // timeout
        if (stop_ && q_.empty()) return false;
        out = std::move(q_.front());
        q_.pop();
        return true;
    }

    void stop() {
        {
            std::lock_guard<std::mutex> lg(m_);
            stop_ = true;
        }
        cv_.notify_all();
    }

private:
    std::queue<OrderBookEvent> q_;
    std::mutex m_;
    std::condition_variable cv_;
    bool stop_ = false;
};