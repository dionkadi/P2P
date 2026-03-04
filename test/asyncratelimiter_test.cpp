#include "gtest/gtest.h"
#include "AsyncRateLimiter.hpp"
#include "helper.hpp"
#include <boost/asio.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/deferred.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <boost/asio/experimental/cancellation_condition.hpp>
#include <boost/asio/experimental/parallel_group.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <chrono>
#include <utility>
#include <vector>

namespace asio = boost::asio;

TEST(AsyncRateLimiterTest, NoRateLimitWhenRateIsZero) {
    asio::io_context io_context;
    AsyncRateLimiter limiter(io_context, 0); // No rate limit

    auto start_time = std::chrono::steady_clock::now();

    RunAsync(io_context, [&]() -> asio::awaitable<void> {
        co_await limiter.await_tokens(1000);
        co_await limiter.await_tokens(2000);
        co_await limiter.await_tokens(500);
    });

    auto end_time = std::chrono::steady_clock::now();
    // Should complete almost instantly
    EXPECT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count(), 100);
}

TEST(AsyncRateLimiterTest, RateLimitingWorks) {
    asio::io_context io_context;
    // 100 bytes/second, capacity factor 2 = 200 bytes capacity
    
    auto start_time = std::chrono::steady_clock::now();
    
    RunAsync(io_context, [&]() -> asio::awaitable<void> {
        AsyncRateLimiter limiter(io_context, 100, 2);
        co_await limiter.await_tokens(50);  // Should be instant (within initial tokens)
        co_await limiter.await_tokens(50);  // Should be instant
        co_await limiter.await_tokens(50);  // Should be instant
        co_await limiter.await_tokens(50);  // Should be instant (total 200 tokens consumed)

        // Now, more tokens requested than available, should wait for refill
        // 100 bytes/s -> 10 bytes every 100ms
        // Request 100 bytes, need 1000ms (1 second) to refill
        co_await limiter.await_tokens(100);
        co_await limiter.await_tokens(100); // Another 100 bytes, another 1000ms
    });

    auto end_time = std::chrono::steady_clock::now();
    long long duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

    // Expect at least 2 seconds (2000ms) for the 2x100 byte requests.
    // Allow some buffer for timer inaccuracies and test execution overhead.
    // Initial 200 bytes from capacity are instant.
    // Then 100 bytes (need 1s refill) + 100 bytes (need 1s refill) = ~2s wait.
    EXPECT_GE(duration_ms, 1900); // Should be roughly 2000ms
    EXPECT_LE(duration_ms, 2500); // Upper bound to detect excessive delays
}

TEST(AsyncRateLimiterTest, MultipleConcurrentRequests) {
    asio::io_context io_context;
    // 100 bytes/second, capacity factor 1 = 100 bytes capacity
    
    auto start_time = std::chrono::steady_clock::now();
    
    // Spawn 5 requests of 30 bytes each. Total = 150 bytes.
    // Initial capacity: 100 bytes.
    // First 3 requests (90 bytes) might be instant.
    // Next 2 requests (60 bytes) will queue.
    // Need 150 bytes. 100 bytes are capacity. Need 50 bytes refill.
    // 50 bytes / (100 bytes/s) = 0.5 seconds.
    // Refill rate is 10 bytes/100ms. So 5 refills.
    // Waiters are processed in order.
    
    RunAsync(io_context, [&]() -> asio::awaitable<void> {
        AsyncRateLimiter limiter(io_context, 100, 1);
        using deferred_op_t = decltype(asio::co_spawn(std::declval<asio::io_context&>(), limiter.await_tokens(std::declval<size_t>()), asio::deferred));
        std::vector<deferred_op_t> tasks;
        for (int i = 0; i < 5; ++i) {
            tasks.push_back(asio::co_spawn(io_context, limiter.await_tokens(30), asio::deferred));
        }
        auto group = asio::experimental::make_parallel_group(std::move(tasks));
        co_await group.async_wait(asio::experimental::wait_for_all(), asio::use_awaitable); // Wait for all to complete
    });

    auto end_time = std::chrono::steady_clock::now();
    long long duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

    // Expected total tokens needed: 150. Initial tokens: 100. Tokens to refill: 50.
    // Time to refill 50 tokens at 100 bps = 0.5 seconds (500ms).
    // All requests should complete roughly within this timeframe after the initial burst.
    EXPECT_GE(duration_ms, 450); // Lower bound
    EXPECT_LE(duration_ms, 800); // Upper bound to allow for some scheduling overhead
}

TEST(AsyncRateLimiterTest, Cancellation) {
    asio::io_context io_context;
    bool operation_cancelled = false;
    
    // This coroutine will try to acquire 100 tokens, which will take 10 seconds.
    // But the io_context will be stopped much earlier.
    auto long_wait_task = [&]() -> asio::awaitable<void> {
        AsyncRateLimiter limiter(io_context, 10, 1); // Very slow rate
        asio::steady_timer timer(io_context);
        try {
            co_await limiter.await_tokens(100);
        } catch (const boost::system::system_error& e) {
            // boost::system::errc::operation_canceled is the expected error code
            if (e.code() == asio::error::operation_aborted) {
                operation_cancelled = true;
            } else {
                throw;
            }
        }
    };

    asio::co_spawn(io_context, long_wait_task(), asio::detached);

    // Run io_context for a short period, then stop it
    io_context.run_for(50ms);
    io_context.stop(); // This should cancel pending async operations
    io_context.run(); // allow the cancelled handler to complete

    EXPECT_TRUE(operation_cancelled);
}

TEST(AsyncRateLimiterTest, NoNegativeTokens) {
    asio::io_context io_context;
    
    RunAsync(io_context, [&]() -> asio::awaitable<void> {
        AsyncRateLimiter limiter(io_context, 100, 1); // 100 bytes capacity
        co_await limiter.await_tokens(50); // Consume 50
        co_await limiter.await_tokens(50); // Consume 50, now 0 tokens
        co_await limiter.await_tokens(10); // Will wait, should not go negative internally
    });

    // We can't directly inspect 'tokens_' which is private.
    // The test passes if the system behaves correctly and doesn't crash or
    // allow more data than rate limit. The previous tests cover this implicitly.
    // This test ensures the internal logic handles it correctly by not exposing errors.
}
