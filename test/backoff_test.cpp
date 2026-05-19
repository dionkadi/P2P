#include <gtest/gtest.h>
#include <chrono>

#include "Utils.hpp"

using namespace std::chrono_literals;

TEST(BackoffTest, CalculateDelayIncreasesExponentially) {
    // attempt=0: 30s * 2^0 = 30s
    EXPECT_EQ(calculate_backoff_delay(0, 30s, 30min), 30s);
    // attempt=1: 30s * 2^1 = 60s
    EXPECT_EQ(calculate_backoff_delay(1, 30s, 30min), 60s);
    // attempt=2: 30s * 2^2 = 120s
    EXPECT_EQ(calculate_backoff_delay(2, 30s, 30min), 120s);
    // attempt=3: 30s * 2^3 = 240s
    EXPECT_EQ(calculate_backoff_delay(3, 30s, 30min), 240s);
    // attempt=4: 30s * 2^4 = 480s = 8min
    EXPECT_EQ(calculate_backoff_delay(4, 30s, 30min), 480s);
    // attempt=5: 30s * 2^5 = 960s = 16min
    EXPECT_EQ(calculate_backoff_delay(5, 30s, 30min), 960s);
    // attempt=6: 30s * 2^6 = 1920s = 32min, capped at 30min = 1800s
    EXPECT_EQ(calculate_backoff_delay(6, 30s, 30min), 1800s);
    // attempt=10: way past cap
    EXPECT_EQ(calculate_backoff_delay(10, 30s, 30min), 1800s);
}

TEST(BackoffTest, CalculateDelayRespectsMaxDelay) {
    // With 10s initial delay and 60s max
    EXPECT_EQ(calculate_backoff_delay(0, 10s, 60s), 10s);
    EXPECT_EQ(calculate_backoff_delay(1, 10s, 60s), 20s);
    EXPECT_EQ(calculate_backoff_delay(2, 10s, 60s), 40s);
    // attempt=3 would be 80s, capped at 60s
    EXPECT_EQ(calculate_backoff_delay(3, 10s, 60s), 60s);
    EXPECT_EQ(calculate_backoff_delay(4, 10s, 60s), 60s);
}

TEST(BackoffTest, InitialStateNotInBackoff) {
    BackoffState state;
    EXPECT_FALSE(state.is_in_backoff());
    EXPECT_EQ(state.attempt_count_, 0);
}

TEST(BackoffTest, OnSuccessResetsCount) {
    BackoffState state;
    state.on_failure();
    EXPECT_EQ(state.attempt_count_, 1);
    EXPECT_TRUE(state.is_in_backoff());

    state.on_success();
    EXPECT_EQ(state.attempt_count_, 0);
    EXPECT_FALSE(state.is_in_backoff());
}

TEST(BackoffTest, OnFailureSetsNextRetryAt) {
    BackoffState state;
    auto before = std::chrono::steady_clock::now();
    state.on_failure(10s);
    auto after = std::chrono::steady_clock::now();

    EXPECT_EQ(state.attempt_count_, 1);
    // Delay should be 10s * 2^1 = 20s
    auto expected_next = before + 20s;
    EXPECT_GE(state.next_retry_at_, expected_next);
    EXPECT_LE(state.next_retry_at_, after + 20s);
    EXPECT_TRUE(state.is_in_backoff());
}

TEST(BackoffTest, RepeatedFailuresIncreaseDelay) {
    BackoffState state;
    state.on_failure(10s);
    auto t1 = state.next_retry_at_;

    state.on_failure(10s);
    auto t2 = state.next_retry_at_;

    state.on_failure(10s);
    auto t3 = state.next_retry_at_;

    // Each failure should push next_retry_at further out
    EXPECT_GT(t2, t1);
    EXPECT_GT(t3, t2);
    EXPECT_EQ(state.attempt_count_, 3);
}

TEST(BackoffTest, IsInBackoffFalseWhenNoAttempts) {
    BackoffState state;
    EXPECT_FALSE(state.is_in_backoff());
}

TEST(BackoffTest, IsInBackoffTrueAfterFailure) {
    BackoffState state;
    state.on_failure();
    EXPECT_TRUE(state.is_in_backoff());
}

TEST(BackoffTest, IsInBackoffFalseAfterSuccess) {
    BackoffState state;
    state.on_failure();
    EXPECT_TRUE(state.is_in_backoff());
    state.on_success();
    EXPECT_FALSE(state.is_in_backoff());
}

TEST(BackoffTest, CheckAndResetIfIdleResetsAfterInactivity) {
    BackoffState state;
    state.on_success(); // Set last_success_at_ to now
    state.on_failure(); // Now attempt_count_ = 1

    EXPECT_TRUE(state.is_in_backoff());

    // Manually set last_success_at_ to be far in the past (beyond the 1h idle window)
    state.last_success_at_ = std::chrono::steady_clock::now() - BackoffState::kResetAfterIdle - 1h;

    bool was_reset = state.check_and_reset_if_idle();
    EXPECT_TRUE(was_reset);
    EXPECT_EQ(state.attempt_count_, 0);
    EXPECT_FALSE(state.is_in_backoff());
}

TEST(BackoffTest, CheckAndResetIfIdleDoesNotResetBeforeInactivity) {
    BackoffState state;
    state.on_success();
    state.on_failure();

    EXPECT_TRUE(state.is_in_backoff());

    // last_success_at_ is very recent, should NOT reset
    bool was_reset = state.check_and_reset_if_idle();
    EXPECT_FALSE(was_reset);
    EXPECT_EQ(state.attempt_count_, 1);
    EXPECT_TRUE(state.is_in_backoff());
}

TEST(BackoffTest, CheckAndResetIfIdleNoopWhenNoSuccessYet) {
    BackoffState state;
    state.on_failure();

    EXPECT_TRUE(state.is_in_backoff());
    // No last_success_at_ set, should not reset
    bool was_reset = state.check_and_reset_if_idle();
    EXPECT_FALSE(was_reset);
    EXPECT_EQ(state.attempt_count_, 1);
}

TEST(BackoffTest, IsInBackoffReturnsFalseWhenIdleExpired) {
    BackoffState state;
    state.on_success();
    state.on_failure();

    // Manually set last_success_at_ to far in the past
    state.last_success_at_ = std::chrono::steady_clock::now() - BackoffState::kResetAfterIdle - 1h;

    // is_in_backoff() should return false (idle timeout triggered)
    EXPECT_FALSE(state.is_in_backoff());
}

TEST(BackoffTest, GetDelayReturnsCorrectValues) {
    BackoffState state;
    // Before any failures, delay for attempt 0
    EXPECT_EQ(state.get_delay(30s), 30s);

    state.on_failure(30s);
    // After 1 failure (attempt=1), delay for next: 30s * 2^1 = 60s
    EXPECT_EQ(state.get_delay(30s), 60s);

    state.on_failure(30s);
    // After 2 failures (attempt=2), delay for next: 30s * 2^2 = 120s
    EXPECT_EQ(state.get_delay(30s), 120s);
}

TEST(BackoffTest, MultipleSuccessesKeepCountZero) {
    BackoffState state;
    state.on_success();
    EXPECT_EQ(state.attempt_count_, 0);

    state.on_success();
    EXPECT_EQ(state.attempt_count_, 0);

    state.on_failure();
    EXPECT_EQ(state.attempt_count_, 1);

    state.on_success();
    EXPECT_EQ(state.attempt_count_, 0);
}

TEST(BackoffTest, OnFailureWithCustomInitialDelay) {
    BackoffState state;
    state.on_failure(5s);
    // attempt=1: 5s * 2^1 = 10s
    auto now = std::chrono::steady_clock::now();
    EXPECT_GE(state.next_retry_at_, now + 9s);
    EXPECT_LE(state.next_retry_at_, now + 11s);
    EXPECT_EQ(state.attempt_count_, 1);
}
