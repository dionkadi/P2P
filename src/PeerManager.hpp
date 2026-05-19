#pragma once

#include <atomic>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <cstddef>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <memory>
#include <mutex>

#include "PeerConnection.hpp"
#include "SessionState.hpp"

struct BannedPeer {
    std::string ip;
    TimePoint ban_time;
    TimePoint expiry_time;
};

struct PeerMisbehavior {
    uint32_t corrupt_pieces{0};
    uint32_t protocol_violations{0};
    uint32_t timeouts{0};
    uint32_t connection_failures{0};

    bool has_exceeded_thresholds() const noexcept {
        return corrupt_pieces >= 3 || protocol_violations >= 5 || timeouts >= 10 || connection_failures >= 5;
    }
};

class PeerManager : public std::enable_shared_from_this<PeerManager> {
public:
    PeerManager(asio::io_context& io_context, std::shared_ptr<SessionState> state, std::chrono::seconds choke_interval = 10s) noexcept;

    PeerManager(const PeerManager&) = delete;
    PeerManager& operator=(const PeerManager&) = delete;
    PeerManager(PeerManager&&) noexcept = delete;
    PeerManager& operator=(PeerManager&&) noexcept = delete;

    bool add_connection(const PeerId& id, std::shared_ptr<PeerConnection> conn);
    void remove_connection(const PeerId& id) {
        std::lock_guard lock(mutex_);
        active_connections_.erase(id);
    }
    void remove_all_connections() {
        std::lock_guard lock(mutex_);
        active_connections_.clear();
    }
    std::shared_ptr<PeerConnection> get_connection(const PeerId& id) const noexcept {
        std::lock_guard lock(mutex_);
        if (active_connections_.count(id)) {
            return active_connections_.at(id);
        }
        return nullptr;
    }
    std::vector<std::shared_ptr<PeerConnection>> get_all_connections() const {
        std::lock_guard lock(mutex_);
        std::vector<std::shared_ptr<PeerConnection>> all_conns;
        std::ranges::copy(active_connections_ | std::views::values, std::back_inserter(all_conns));
        return all_conns;
    }
    void close(const PeerId& id) {
        std::lock_guard lock(mutex_);
        if (!active_connections_.count(id)) {
            return;
        }
        active_connections_[id]->close();
    }
    void close_all() {
        std::lock_guard lock(mutex_);
        std::ranges::for_each(active_connections_ | std::views::values, [] (std::shared_ptr<PeerConnection>& conn) { conn->close(); });
    }
    bool contains_peer(const PeerId& id) {
        std::lock_guard lock(mutex_);
        return active_connections_.count(id) > 0;
    }
    bool contains_peer_addr(const std::string& peer_addr) {
        std::lock_guard lock(mutex_);
        return std::ranges::any_of(active_connections_ | std::views::values, 
                                   [&peer_addr](const std::shared_ptr<PeerConnection>& conn) {
                                       return conn->peer_addr() == peer_addr;
                                   });
    }
    size_t connection_count() {
        std::lock_guard lock(mutex_);
        return active_connections_.size();
    }
    
    // Connection limit accessors
    size_t max_total_connections() const noexcept { return max_total_connections_; }
    size_t max_connections_per_ip() const noexcept { return max_connections_per_ip_; }
    size_t max_half_open_connections() const noexcept { return max_half_open_connections_; }
    size_t half_open_connections() const noexcept { return half_open_connections_.load(); }

    void set_max_total_connections(size_t n) noexcept { max_total_connections_ = n; }
    void set_max_connections_per_ip(size_t n) noexcept { max_connections_per_ip_ = n; }
    void set_max_half_open_connections(size_t n) noexcept { max_half_open_connections_ = n; }

    asio::awaitable<void> choke_loop();
    asio::awaitable<void> send_have_message_to_all(size_t piece_index);

    asio::awaitable<void> pex_loop();

    void add_active_peer(const EndPoint& ep) {
        std::lock_guard lock(active_mutex_);
        active_peers_.insert(ep);
    }
    void add_discovered_peer(const EndPoint& ep) {
        std::lock_guard lock(discovered_mutex_);
        discovered_peers_.insert(ep);
    }
    void remove_active_peer(const EndPoint& ep) {
        std::lock_guard lock(active_mutex_);
        active_peers_.erase(ep);
    }
    void remove_discovered_peer(const EndPoint& ep) {
        std::lock_guard lock(discovered_mutex_);
        discovered_peers_.erase(ep);
    }
    void add_dropped_peer(const EndPoint& ep) {
        std::lock_guard lock(dropped_mutex_);
        dropped_peers_.push_back(ep);
    }

    std::unordered_set<EndPoint> get_discovered_peers() const {
        std::lock_guard lock(discovered_mutex_);
        return discovered_peers_;
    }

    std::vector<PeerId> get_unchoked_peers() const {
        std::lock_guard lock(mutex_);
        std::vector<PeerId> result;
        std::ranges::for_each(active_connections_, [&result](const auto& p) {
            const auto& [pid, conn] = p;
            if (!conn->am_choking()) {
                result.push_back(pid);
            }
        });
        return result;
    }

    void cancel() noexcept {
        LOGDBG("PeerManager: Cancelling pex_timer_...");
        pex_timer_.cancel();
        LOGDBG("PeerManager: pex_timer_ cancelled. Cancelling choke_timer_...");
        choke_timer_.cancel();
        LOGDBG("PeerManager: choke_timer_ cancelled. Cancelling backoff_retry_timer_...");
        backoff_retry_timer_.cancel();
        LOGDBG("PeerManager: backoff_retry_timer_ cancelled. Cancelling ban_cleanup_timer_...");
        ban_cleanup_timer_.cancel();
        LOGDBG("PeerManager: ban_cleanup_timer_ cancelled.");
    }

    // --- Ban management ---
    bool is_banned(const std::string& ip) const;
    void ban_peer_by_ip(const std::string& ip);
    void report_corrupt_piece(const std::string& peer_addr);
    void report_protocol_violation(const std::string& peer_addr);
    void report_timeout(const std::string& peer_addr);
    asio::awaitable<void> ban_cleanup_loop();

    void report_connection_success(const std::string& peer_addr);
    void report_connection_failure(const std::string& peer_addr);

    asio::awaitable<std::optional<AsyncSocket>> connect_to_peer(const std::string& peer_addr);
    asio::awaitable<std::vector<std::shared_ptr<PeerConnection>>> available_peers(size_t piece_index) const;

    static std::string extract_ip_from_addr(const std::string& peer_addr);

private:
    asio::io_context& io_context_;
    asio::strand<asio::any_io_executor> strand_;
    asio::steady_timer pex_timer_;
    asio::steady_timer choke_timer_;
    std::map<PeerId, std::shared_ptr<PeerConnection>> active_connections_;
    std::shared_ptr<SessionState> state_;
    std::unordered_set<EndPoint> active_peers_;
    std::unordered_set<EndPoint> discovered_peers_;
    std::deque<EndPoint> dropped_peers_;
    size_t choke_loop_counter_{0};
    std::chrono::seconds choke_interval_;
    mutable std::mutex mutex_;
    mutable std::mutex active_mutex_;
    mutable std::mutex discovered_mutex_;
    mutable std::mutex dropped_mutex_;

    // Connection limit fields
    size_t max_total_connections_{200};
    size_t max_connections_per_ip_{2};
    size_t max_half_open_connections_{40};
    std::atomic<size_t> half_open_connections_{0};

    std::shared_ptr<PeerConnection> find_worst_peer_locked();

    std::string populate_added(size_t max_peers = 50);
    std::string populate_dropped();

    // Backoff state for peer connection retries
    std::unordered_map<std::string, BackoffState> backoff_states_;
    mutable std::mutex backoff_mutex_;
    asio::steady_timer backoff_retry_timer_;

    // Ban state (mutable for const method read + lazy expiry cleanup)
    mutable std::unordered_map<std::string, BannedPeer> banned_peers_;
    mutable std::unordered_map<std::string, PeerMisbehavior> peer_misbehavior_;
    mutable std::mutex ban_mutex_;
    asio::steady_timer ban_cleanup_timer_;
};