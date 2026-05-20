// Utils/AsyncRateLimiter.hpp
#pragma once

#include <boost/asio.hpp>
#include <boost/asio/bind_executor.hpp>
#include <deque>

#include "Utils.hpp"

namespace asio = boost::asio;

template<typename TokenCountType = uint64_t>
class AsyncRateLimiter {
    static_assert(std::is_unsigned_v<TokenCountType>, "TokenCountType must be an unsigned integer type.");
public:
    static constexpr std::chrono::milliseconds refill_interval = std::chrono::milliseconds(100);

    AsyncRateLimiter(asio::io_context& io_context, uint64_t rate_bps, uint64_t capacity_factor = 1)
        : strand_(asio::make_strand(io_context)),
          timer_(strand_), // Bind the timer to the strand
          rate_bytes_per_second_(rate_bps),
          capacity_(rate_bps * capacity_factor),
          tokens_(capacity_) {
        
        if (rate_bytes_per_second_ > 0) {
            asio::post(strand_, [this] { refill_tokens(); });
        }
    }

    ~AsyncRateLimiter() {
        for (auto& [amount, handler] : waiters_) {
            asio::post(strand_, [h = std::move(handler)]() mutable {
                h(boost::asio::error::operation_aborted);
            });
        }
        waiters_.clear();
    }

    asio::awaitable<void> await_tokens(size_t amount) {
        if (rate_bytes_per_second_ == 0) {
            co_return;  // If rate limiting is disabled, complete immediately.
        }

        co_await asio::async_initiate<void(boost::system::error_code)>(
            asio::bind_executor(strand_, // Ensure the logic runs on the strand
            [this, amount](auto&& completion_handler) mutable {
                if (tokens_ >= amount) {
                    tokens_ -= amount;
                    std::move(completion_handler)(boost::system::error_code{});
                } else {
                    waiters_.emplace_back(amount, std::move(completion_handler));
                }
            }),
            asio::use_awaitable
        );
    }

    void set_rate(uint64_t bps) noexcept {
        rate_bytes_per_second_ = bps;
        capacity_ = bps;  // Reset capacity to match new rate
        tokens_ = std::min(tokens_, capacity_);
        if (rate_bytes_per_second_ > 0) {
            asio::post(strand_, [this] { refill_tokens(); });
        }
    }

private:
    void refill_tokens() {
        timer_.expires_after(refill_interval);
        timer_.async_wait(asio::bind_executor(strand_, 
        [this](const boost::system::error_code& ec) {
            if (ec == boost::asio::error::operation_aborted) {
                return;
            }
            if (ec) {
                LOGERR("Error when refilling tokens: {}", ec.message());
                return;
            }

            TokenCountType refill_amount = static_cast<TokenCountType>(
                (static_cast<long double>(rate_bytes_per_second_) * refill_interval.count()) / 1000.0
            );
            tokens_ = std::min(tokens_ + refill_amount, capacity_);

            // Resume any waiters that can now be satisfied
            while (!waiters_.empty() && tokens_ >= waiters_.front().first) {
                auto [amount, handler] = std::move(waiters_.front());
                waiters_.pop_front();
                
                tokens_ -= static_cast<TokenCountType>(amount);
                
                // The handler is already bound to the correct executor, so we can just post it.
                asio::post(strand_, [h = std::move(handler)]() mutable {
                    h(boost::system::error_code{}); // Pass empty error_code for success
                });
            }

            refill_tokens(); // Schedule the next refill
        }));
    }

    asio::strand<asio::io_context::executor_type> strand_;
    asio::steady_timer timer_;
    TokenCountType rate_bytes_per_second_;
    TokenCountType capacity_;
    TokenCountType tokens_;

    // The queue stores {amount_needed, completion_handler}
    using Waiter = std::pair<size_t, asio::any_completion_handler<void(boost::system::error_code)>>;
    std::deque<Waiter> waiters_;
};
