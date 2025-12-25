#pragma once

#include <atomic>
#include <boost/asio/io_context.hpp>
#include <boost/asio/strand.hpp>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <span>
#include <utility>
#include <vector>

#include "Types.hpp"
#include "AsyncSocket.hpp"
#include "SessionState.hpp"
#include "IPeerEvents.hpp"

class PeerManager;

class PeerConnection: public std::enable_shared_from_this<PeerConnection> {
public:
    static asio::awaitable<std::shared_ptr<PeerConnection>> create(
        asio::io_context& io_context,
        AsyncSocket socket,
        std::string peer_addr,
        const PeerId& my_id,
        std::shared_ptr<SessionState> state,
        std::shared_ptr<IPeerConnectionEvents> events
    );

    bool has_piece(size_t index) const noexcept;
    void set_has_piece(size_t index) noexcept;

    bool am_choking() const noexcept { return am_choking_; }
    bool peer_is_choking() const noexcept { return peer_is_choking_; };
    bool am_interested() const noexcept { return am_interested_; }
    bool peer_is_interested() const noexcept { return peer_is_interested_; }
    size_t bitfield_size() const noexcept { return bitfield_.size(); }
    uint64_t bytes_downloaded() const noexcept { return bytes_downloaded_.load(std::memory_order_relaxed); } 
    uint64_t bytes_uploaded() const noexcept { return bytes_uploaded_.load(std::memory_order_relaxed); }
    const PeerId& peer_id() const noexcept { return peer_id_; }
    const std::string& peer_addr() const noexcept { return peer_addr_; }
    ExtendedMessageType extension_type(uint8_t index) const { return remote_extension_map_.at(index); }

    void am_choking(bool val) noexcept { am_choking_ = val; }
    void peer_is_choking(bool val) noexcept { peer_is_choking_ = val; }
    void am_interested(bool val) noexcept { am_interested_ = val; }
    void peer_is_interested(bool val) noexcept { peer_is_interested_ = val; }
    void bytes_downloaded(uint64_t val) noexcept { bytes_downloaded_.store(val, std::memory_order_relaxed); } 
    void bytes_uploaded(uint64_t val) noexcept { bytes_uploaded_.store(val, std::memory_order_relaxed); }
    void add_bytes_downloaded(uint64_t val) noexcept { bytes_downloaded_.fetch_add(val, std::memory_order_relaxed); } 
    void add_bytes_uploaded(uint64_t val) noexcept { bytes_uploaded_.fetch_add(val, std::memory_order_relaxed); }
    void update_extension_type(uint8_t index, ExtendedMessageType type) { remote_extension_map_[index] = type; }

    template<typename T>
    void bitfield(T&& other) noexcept { bitfield_ = std::forward<T>(other); } 

    void close() noexcept { socket_.close(); }
    
    asio::awaitable<void> send_simple_message(MessageType type);
    asio::awaitable<void> send_request(size_t index, uint32_t begin, uint32_t length);
    asio::awaitable<void> send_piece(size_t index, uint32_t begin, std::span<const std::byte> block_data);
    asio::awaitable<void> send_bitfield(const std::vector<uint8_t>& bitfield_data);
    asio::awaitable<void> send_cancel(size_t index, uint32_t begin, uint32_t length);
    asio::awaitable<void> send_have(size_t index);

private:
    PeerConnection(
        asio::io_context& io_context, AsyncSocket socket, std::string peer_addr, 
        std::shared_ptr<SessionState> state, std::shared_ptr<IPeerConnectionEvents> events
    ) noexcept;

    asio::awaitable<bool> perform_handshake(const PeerId& my_peer_id);
    asio::awaitable<void> message_loop();
    asio::awaitable<void> keep_alive_loop();

    void start_loops();

    asio::io_context& io_context_;
    AsyncSocket socket_;
    std::string peer_addr_;
    PeerId peer_id_{};
    std::shared_ptr<SessionState> state_;
    std::shared_ptr<IPeerConnectionEvents> events_;

    std::vector<uint8_t> bitfield_;
    std::map<uint8_t, ExtendedMessageType> remote_extension_map_;

    bool am_choking_{true};
    bool peer_is_choking_{true};
    bool am_interested_{false};
    bool peer_is_interested_{false};

    std::atomic_uint64_t bytes_downloaded_{0};
    std::atomic_uint64_t bytes_uploaded_{0};
};