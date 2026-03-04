#pragma once

#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <cstddef>
#include <map>
#include <unordered_set>
#include <memory>
#include <mutex>

#include "PeerConnection.hpp"
#include "SessionState.hpp"

class PeerManager{
public:
    PeerManager(asio::io_context& io_context, std::shared_ptr<SessionState> state, std::chrono::seconds choke_interval = 10s) noexcept;

    PeerManager(const PeerManager&) = delete;
    PeerManager& operator=(const PeerManager&) = delete;
    PeerManager(PeerManager&&) noexcept = delete;
    PeerManager& operator=(PeerManager&&) noexcept = delete;

    void add_connection(const PeerId& id, std::shared_ptr<PeerConnection> conn) {
        std::lock_guard lock(mutex_);
        active_connections_[id] = std::move(conn);
    }
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

    asio::awaitable<std::optional<AsyncSocket>> connect_to_peer(const std::string& peer_addr);
    asio::awaitable<std::vector<std::shared_ptr<PeerConnection>>> available_peers(size_t piece_index) const;

private:
    asio::io_context& io_context_;
    asio::strand<asio::any_io_executor> strand_;
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

    std::string populate_added(size_t max_peers = 50);
    std::string populate_dropped();
};