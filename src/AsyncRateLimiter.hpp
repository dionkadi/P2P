// Utils/AsyncRateLimiter.hpp
#pragma once

#include <boost/asio.hpp>
#include <deque>

namespace asio = boost::asio;

class AsyncRateLimiter {
public:
    AsyncRateLimiter(asio::io_context& io_context, uint64_t rate_bps, uint64_t capacity_factor = 2)
        : strand_(asio::make_strand(io_context)),
          timer_(strand_), // Bind the timer to the strand
          rate_bytes_per_second_(rate_bps),
          capacity_(rate_bps * capacity_factor),
          tokens_(capacity_) {
        
        if (rate_bytes_per_second_ > 0) {
            asio::post(strand_, [this] { refill_tokens(); });
        }
    }

    asio::awaitable<void> await_tokens(size_t amount) {
        // If rate limiting is disabled, complete immediately.
        if (rate_bytes_per_second_ == 0) {
            co_return;
        }

        co_await asio::async_initiate<void(boost::system::error_code)>(
            [this, amount](auto&& completion_handler) {
                // Dispatch the logic onto strand to ensure thread safety.
                asio::dispatch(strand_, [this, amount, h = std::move(completion_handler)]() mutable {
                    if (tokens_ >= amount) {
                        // We have enough tokens, complete the operation now.
                        tokens_ -= amount;
                        // Post the handler to avoid calling it from within the initiation function.
                        asio::post(strand_, [handler = std::move(h)]() mutable {
                            handler(boost::system::error_code{});
                        });
                    } else {
                        // Not enough tokens, suspend the operation by enqueuing the handler.
                        waiters_.emplace_back(amount, std::move(h));
                    }
                });
            },
            asio::use_awaitable
        );
    }

private:
    void refill_tokens() {
        timer_.expires_after(std::chrono::milliseconds(100));
        timer_.async_wait([this](const boost::system::error_code& ec) {
            if (ec) return; // Timer was cancelled

            uint64_t refill_amount = (rate_bytes_per_second_ * 100) / 1000;
            tokens_ = std::min(tokens_ + refill_amount, capacity_);

            // Resume any waiters that can now be satisfied
            while (!waiters_.empty() && tokens_ >= waiters_.front().first) {
                auto [amount, handler] = std::move(waiters_.front());
                waiters_.pop_front();
                
                tokens_ -= amount;
                
                // The handler is already bound to the correct executor, so we can just post it.
                asio::post(strand_, [h = std::move(handler)]() mutable {
                    h(boost::system::error_code{}); // Pass empty error_code for success
                });
            }

            refill_tokens(); // Schedule the next refill
        });
    }

    asio::strand<asio::io_context::executor_type> strand_;
    asio::steady_timer timer_;
    const uint64_t rate_bytes_per_second_;
    const uint64_t capacity_;
    uint64_t tokens_;

    // The queue stores {amount_needed, completion_handler}
    using Waiter = std::pair<size_t, asio::any_completion_handler<void(boost::system::error_code)>>;
    std::deque<Waiter> waiters_;
};
