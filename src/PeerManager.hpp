#pragma once

#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <cstddef>
#include <map>
#include <memory>

#include "PeerConnection.hpp"
#include "SessionState.hpp"

class PeerManager{
public:
    PeerManager(asio::io_context& io_context, std::shared_ptr<SessionState> state);

    std::map<PeerId, std::shared_ptr<PeerConnection>>& active_connections() { return active_connections_; }
    const std::map<PeerId, std::shared_ptr<PeerConnection>>& active_connections() const { return active_connections_; }
    std::shared_ptr<PeerConnection> connection(const PeerId& peer_id) { return active_connections_.at(peer_id); }

    asio::awaitable<std::optional<AsyncSocket>> connect_to_peer(const std::string& peer_addr);
    asio::awaitable<void> choke_loop();
    asio::awaitable<void> send_have_message_to_all(size_t piece_index);

    std::vector<std::shared_ptr<PeerConnection>> available_peers(size_t piece_index);

private:
    asio::io_context& io_context_;
    std::map<PeerId, std::shared_ptr<PeerConnection>> active_connections_;
    std::shared_ptr<SessionState> state_;

    size_t choke_loop_counter_{0};
};