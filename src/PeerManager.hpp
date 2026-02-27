#pragma once

#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <cstddef>
#include <map>
#include <memory>
#include <mutex>

#include "PeerConnection.hpp"
#include "SessionState.hpp"

class PeerManager{
public:
    PeerManager(asio::io_context& io_context, std::shared_ptr<SessionState> state) noexcept;

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

    asio::awaitable<std::optional<AsyncSocket>> connect_to_peer(const std::string& peer_addr);
    asio::awaitable<std::vector<std::shared_ptr<PeerConnection>>> available_peers(size_t piece_index) const;

private:
    asio::io_context& io_context_;
    asio::strand<asio::any_io_executor> strand_;
    std::map<PeerId, std::shared_ptr<PeerConnection>> active_connections_;
    std::shared_ptr<SessionState> state_;
    size_t choke_loop_counter_{0};
    mutable std::mutex mutex_;
};