// Utils/AsyncRateLimiter.hpp
#pragma once

#include <boost/asio.hpp>
#include <deque>
#include <limits>
#include <memory>
#include <mutex>
#include <vector>

#include "Utils.hpp"

namespace asio = boost::asio;

template<typename TokenCountType = uint64_t>
class AsyncRateLimiter {
    static_assert(std::is_unsigned_v<TokenCountType>, "TokenCountType must be an unsigned integer type.");

    using Waiter = std::pair<size_t, asio::any_completion_handler<void(boost::system::error_code)>>;

    struct State {
        explicit State(asio::io_context& io_context, uint64_t rate_bps, uint64_t capacity_factor)
            : timer(io_context),
              rate_bytes_per_second(rate_bps),
              capacity(rate_bps * capacity_factor),
              tokens(capacity) {}

        std::mutex mutex;
        asio::steady_timer timer;
        TokenCountType rate_bytes_per_second;
        TokenCountType capacity;
        TokenCountType tokens;
        bool refill_active{false};
        bool shutting_down{false};
        std::deque<Waiter> waiters;
    };

public:
    static constexpr std::chrono::milliseconds refill_interval = std::chrono::milliseconds(100);

    AsyncRateLimiter(asio::io_context& io_context, uint64_t rate_bps, uint64_t capacity_factor = 1)
        : state_(std::make_shared<State>(io_context, rate_bps, capacity_factor)) {}

    ~AsyncRateLimiter() {
        stop();
    }

    asio::awaitable<void> await_tokens(size_t amount) {
        auto state = state_;
        co_await asio::async_initiate<void(boost::system::error_code)>(
            [state, amount](auto&& completion_handler) mutable {
                boost::system::error_code result;
                bool complete_now = false;

                {
                    std::lock_guard lock(state->mutex);

                    if (state->shutting_down) {
                        result = boost::asio::error::make_error_code(boost::asio::error::operation_aborted);
                        complete_now = true;
                    } else if (state->rate_bytes_per_second == 0) {
                        complete_now = true;
                    } else {
                        maybe_start_refill_locked(state);
                        if (state->tokens >= amount) {
                            state->tokens -= amount;
                            complete_now = true;
                        } else {
                            state->waiters.emplace_back(amount, std::move(completion_handler));
                            return;
                        }
                    }
                }

                if (complete_now) {
                    std::move(completion_handler)(result);
                }
            },
            asio::use_awaitable
        );
    }

    void set_rate(uint64_t bps) noexcept {
        auto state = state_;
        std::vector<Waiter> ready_waiters;
        bool cancel_timer = false;

        {
            std::lock_guard lock(state->mutex);

            if (state->shutting_down) {
                return;
            }

            state->rate_bytes_per_second = bps;
            state->capacity = bps;

            if (state->rate_bytes_per_second == 0) {
                state->tokens = std::numeric_limits<TokenCountType>::max();
                state->refill_active = false;
                cancel_timer = true;
                while (!state->waiters.empty()) {
                    ready_waiters.push_back(std::move(state->waiters.front()));
                    state->waiters.pop_front();
                }
            } else {
                state->tokens = std::min(state->tokens, state->capacity);
                maybe_start_refill_locked(state);
                collect_ready_waiters_locked(state, ready_waiters);
            }
        }

        if (cancel_timer) {
            state->timer.cancel();
        }

        for (auto& [amount, handler] : ready_waiters) {
            handler(boost::system::error_code{});
        }
    }

    void stop() noexcept {
        auto state = state_;
        std::vector<Waiter> pending_waiters;

        {
            std::lock_guard lock(state->mutex);

            if (state->shutting_down) {
                return;
            }

            state->shutting_down = true;
            state->refill_active = false;
            while (!state->waiters.empty()) {
                pending_waiters.push_back(std::move(state->waiters.front()));
                state->waiters.pop_front();
            }
        }

        state->timer.cancel();

        auto aborted = boost::asio::error::make_error_code(boost::asio::error::operation_aborted);
        for (auto& [amount, handler] : pending_waiters) {
            handler(aborted);
        }
    }

private:
    static void maybe_start_refill_locked(const std::shared_ptr<State>& state) {
        if (state->shutting_down || state->refill_active || state->rate_bytes_per_second == 0) {
            return;
        }

        state->refill_active = true;
        schedule_refill(state);
    }

    static void collect_ready_waiters_locked(const std::shared_ptr<State>& state, std::vector<Waiter>& ready_waiters) {
        while (!state->waiters.empty() && state->tokens >= state->waiters.front().first) {
            auto [amount, handler] = std::move(state->waiters.front());
            state->waiters.pop_front();
            state->tokens -= static_cast<TokenCountType>(amount);
            ready_waiters.emplace_back(amount, std::move(handler));
        }
    }

    static void schedule_refill(const std::shared_ptr<State>& state) {
        state->timer.expires_after(refill_interval);
        state->timer.async_wait([state](const boost::system::error_code& ec) {
            on_refill(state, ec);
        });
    }

    static void on_refill(const std::shared_ptr<State>& state, const boost::system::error_code& ec) {
        std::vector<Waiter> ready_waiters;
        bool continue_refill = false;

        {
            std::lock_guard lock(state->mutex);

            if (ec == boost::asio::error::operation_aborted) {
                state->refill_active = false;
                return;
            }

            if (ec) {
                state->refill_active = false;
                LOGERR("Error when refilling tokens: {}", ec.message());
                return;
            }

            if (state->shutting_down) {
                state->refill_active = false;
                return;
            }

            TokenCountType refill_amount = static_cast<TokenCountType>(
                (static_cast<long double>(state->rate_bytes_per_second) * refill_interval.count()) / 1000.0
            );
            state->tokens = std::min(state->tokens + refill_amount, state->capacity);
            collect_ready_waiters_locked(state, ready_waiters);
            continue_refill = !state->shutting_down && state->rate_bytes_per_second > 0;
            if (!continue_refill) {
                state->refill_active = false;
            }
        }

        for (auto& [amount, handler] : ready_waiters) {
            handler(boost::system::error_code{});
        }

        if (continue_refill) {
            schedule_refill(state);
        }
    }

    std::shared_ptr<State> state_;
};
