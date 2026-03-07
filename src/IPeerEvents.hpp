#pragma once
#include <boost/asio/awaitable.hpp>
#include <cstddef>
#include <span>

#include <boost/asio.hpp>
namespace asio = boost::asio;

// Forward declaration
class PeerConnection;

class IPeerConnectionEvents {
public:
    virtual ~IPeerConnectionEvents() = default;

    // Called when a full piece block is received
    virtual asio::awaitable<void> on_piece_block(
        std::shared_ptr<PeerConnection> conn, 
        size_t piece_index, 
        uint32_t begin, 
        std::span<const std::byte> block_data
    ) = 0;

    // Called when a peer sends a request for a block
    virtual asio::awaitable<void> on_block_request(
        std::shared_ptr<PeerConnection> conn, 
        size_t piece_index, 
        uint32_t begin, 
        uint32_t length
    ) = 0;

    // Called when a peer sends its bitfield or a HAVE message
    virtual asio::awaitable<void> on_peer_has_piece(
        std::shared_ptr<PeerConnection> conn, 
        size_t piece_index
    ) = 0;

    virtual asio::awaitable<void> on_peer_bitfield(
        std::shared_ptr<PeerConnection> conn,
        std::span<const std::byte> bitfield
    ) = 0;
    
    // Called when a peer chokes or unchokes us
    virtual asio::awaitable<void> on_choke_status_changed(
        std::shared_ptr<PeerConnection> conn, 
        bool is_choking
    ) = 0;

    // Called when a connection is terminated or fails
    virtual asio::awaitable<void> on_disconnect(std::shared_ptr<PeerConnection> conn) = 0;
    
    virtual asio::awaitable<void> on_extended_message(
        std::shared_ptr<PeerConnection> conn, 
        std::span<const std::byte> payload
    ) = 0;
};