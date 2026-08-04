#pragma once

#include <atomic>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <cstddef>
#include <map>
#include <vector>
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
        // connection_failures raised from 5 to 10: transient handshake/NAT failures are
        // extremely common in public swarms and should not trigger a 1h ban. The global
        // fail-cache (in report_connection_failure) already backs off dead peers; the
        // ban is reserved for peers that persistently fail across many attempts.
        return corrupt_pieces >= 3 || protocol_violations >= 5 || timeouts >= 10 || connection_failures >= 10;
    }
};

class PeerManager : public std::enable_shared_from_this<PeerManager> {
public:
    PeerManager(asio::io_context& io_context, std::shared_ptr<SessionState> state, std::chrono::milliseconds choke_interval = 10s) noexcept;

    PeerManager(const PeerManager&) = delete;
    PeerManager& operator=(const PeerManager&) = delete;
    PeerManager(PeerManager&&) noexcept = delete;
    PeerManager& operator=(PeerManager&&) noexcept = delete;

    bool add_connection(const PeerId& id, std::shared_ptr<PeerConnection> conn);
    void remove_connection(const PeerId& id, const PeerConnection* conn = nullptr) {
        std::lock_guard lock(mutex_);
        // If a specific connection is given, only erase the map entry when it
        // still refers to THAT connection. A stale connection whose id was
        // replaced by dedup must not remove the live replacement.
        if (conn) {
            auto it = active_connections_.find(id);
            if (it == active_connections_.end() || it->second.get() != conn) {
                return;
            }
        }
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
    void close_all();
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
    bool contains_peer_ip(const std::string& ip) {
        std::lock_guard lock(mutex_);
        return std::ranges::any_of(active_connections_ | std::views::values, 
                                   [&ip](const std::shared_ptr<PeerConnection>& conn) {
                                       return extract_ip_from_addr(conn->peer_addr()) == ip;
                                   });
    }
    size_t connection_count() {
        std::lock_guard lock(mutex_);
        return active_connections_.size();
    }
    
    // Connection limit accessors
    static constexpr size_t kUnchokeSlots = 4;
    size_t max_total_connections() const noexcept { return max_total_connections_; }
    size_t max_connections_per_ip() const noexcept { return max_connections_per_ip_; }
    size_t max_half_open_connections() const noexcept { return max_half_open_connections_; }
    size_t half_open_connections() const noexcept { return half_open_connections_.load(); }
    void release_half_open() noexcept {
        if (half_open_connections_.load() > 0) {
            --half_open_connections_;
        }
    }

    void set_max_total_connections(size_t n) noexcept { max_total_connections_ = n; }
    void set_max_connections_per_ip(size_t n) noexcept { max_connections_per_ip_ = n; }
    void set_max_half_open_connections(size_t n) noexcept { max_half_open_connections_ = n; }

    asio::awaitable<void> choke_loop();
    void poke_choke_loop() noexcept;
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

    // Count of peers that have UNCHOKED US (download capacity). The in-flight
    // piece window is scaled to this: committing a fixed 32-piece window when
    // only 1 peer has unchoked us floods that single peer (observed 446-block
    // timeout burst), because the rest of the swarm unchokes us staggered over
    // the next ~12s — far too late to absorb an already-locked window.
    size_t unchoked_by_peer_count() const {
        std::lock_guard lock(mutex_);
        size_t n = 0;
        for (const auto& [pid, conn] : active_connections_) {
            if (!conn->peer_is_choking()) {
                ++n;
            }
        }
        return n;
    }

    void cancel() noexcept {
        shutting_down_.store(true, std::memory_order_release);
        LOGDBG("PeerManager: Cancelling pex_timer_...");
        pex_timer_.cancel();
        LOGDBG("PeerManager: pex_timer_ cancelled. Cancelling choke_timer_...");
        choke_timer_.cancel();
        LOGDBG("PeerManager: choke_timer_ cancelled. Cancelling backoff_retry_timer_...");
        backoff_retry_timer_.cancel();
        LOGDBG("PeerManager: backoff_retry_timer_ cancelled. Cancelling ban_cleanup_timer_...");
        ban_cleanup_timer_.cancel();
        LOGDBG("PeerManager: ban_cleanup_timer_ cancelled.");

        std::vector<std::shared_ptr<asio::ip::tcp::socket>> pending_sockets;
        {
            std::lock_guard lock(pending_connect_mutex_);
            pending_sockets.swap(pending_connect_sockets_);
        }
        for (const auto& socket : pending_sockets) {
            boost::system::error_code ec;
            socket->cancel(ec);
        }
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
    std::atomic_bool choke_poke_{false};
    std::chrono::milliseconds choke_interval_;
    mutable std::mutex mutex_;
    mutable std::mutex active_mutex_;
    mutable std::mutex discovered_mutex_;
    mutable std::mutex dropped_mutex_;

    // Connection limit fields. The download ceiling is the peer UNCHOKE set:
    // seeders only grant upload slots via tit-for-tat + one rotating optimistic
    // slot, so a 0-upload leecher is served by only a handful of peers no matter
    // how many are connected. More connections = more optimistic-slot lottery
    // tickets; 500 mirrors qBittorrent's default (200 starved the swarm).
    size_t max_total_connections_{500};
    // 2 per IP silently excluded legitimate seedboxes/CDNs on shared IPs.
    size_t max_connections_per_ip_{4};
    // Concurrent in-flight connects. libtorrent 2.x removed the hard half-open
    // cap (replaced by ~30 connects/sec throttling); a fixed cap of 100
    // serialized acquisition: with trackers/DHT/PEX returning hundreds of peers
    // (mostly dead) the pool pinned at 100/100 for 15s each, so the client
    // could never reach the ~500-live-peer cross-section that gives an unchoke
    // lottery ticket per seed. Match qBittorrent's no-cap behavior.
    size_t max_half_open_connections_{500};
    std::atomic<size_t> half_open_connections_{0};

    std::shared_ptr<PeerConnection> find_worst_peer_locked();

    std::string populate_added(size_t max_peers = 50);
    std::string populate_dropped();

    // Backoff state for peer connection retries
    std::unordered_map<std::string, BackoffState> backoff_states_;
    mutable std::mutex backoff_mutex_;
    asio::steady_timer backoff_retry_timer_;


    mutable std::mutex pending_connect_mutex_;
    std::vector<std::shared_ptr<asio::ip::tcp::socket>> pending_connect_sockets_;

    // Ban state (mutable for const method read + lazy expiry cleanup)
    mutable std::unordered_map<std::string, BannedPeer> banned_peers_;
    mutable std::unordered_map<std::string, PeerMisbehavior> peer_misbehavior_;
    mutable std::mutex ban_mutex_;
    std::atomic<bool> shutting_down_{false};
    asio::steady_timer ban_cleanup_timer_;
};
