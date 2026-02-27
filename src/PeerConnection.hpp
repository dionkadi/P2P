#pragma once

#include <atomic>
#include <boost/asio/io_context.hpp>
#include <boost/asio/strand.hpp>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <span>
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

    PeerConnection(const PeerConnection&) = delete;
    PeerConnection& operator=(const PeerConnection&) = delete;
    PeerConnection(PeerConnection&&) noexcept = delete;
    PeerConnection& operator=(PeerConnection&&) noexcept = delete;

    bool has_piece(size_t index) const noexcept {
        std::lock_guard lock(mutex_);
        if (bitfield_.empty() || index / 8 >= bitfield_.size()) {
            return false;
        }
        return (bitfield_[index / 8] >> (7 - (index % 8))) & 1;
    }
    void set_has_piece(size_t index) noexcept {
        std::lock_guard lock(mutex_);
        if (bitfield_.empty() || index / 8 >= bitfield_.size()) {
            LOGWARN("Attempted to set piece {} out of bitfield bounds (size: {}) for peer {}", index, bitfield_.size(), peer_id_);
            return ;
        }
        bitfield_[index / 8] |= (1 << (7 - (index % 8)));
    }

    bool am_choking() const noexcept { return am_choking_.load(std::memory_order_relaxed); }
    bool peer_is_choking() const noexcept { return peer_is_choking_.load(std::memory_order_relaxed); };
    bool am_interested() const noexcept { return am_interested_.load(std::memory_order_relaxed); }
    bool peer_is_interested() const noexcept { return peer_is_interested_.load(std::memory_order_relaxed); }

    size_t bitfield_size() const noexcept {
        std::lock_guard lock(mutex_);
        return bitfield_.size();
    }

    uint64_t bytes_downloaded() const noexcept { return bytes_downloaded_.load(std::memory_order_relaxed); } 
    uint64_t bytes_uploaded() const noexcept { return bytes_uploaded_.load(std::memory_order_relaxed); }
    const PeerId& peer_id() const noexcept { return peer_id_; }
    const std::string& peer_addr() const noexcept { return peer_addr_; }

    void am_choking(bool val) noexcept { am_choking_.store(val, std::memory_order_relaxed); }
    void peer_is_choking(bool val) noexcept { peer_is_choking_.store(val, std::memory_order_relaxed); }
    void am_interested(bool val) noexcept { am_interested_.store(val, std::memory_order_relaxed); }
    void peer_is_interested(bool val) noexcept { peer_is_interested_.store(val, std::memory_order_relaxed); }
    void bytes_downloaded(uint64_t val) noexcept { bytes_downloaded_.store(val, std::memory_order_relaxed); } 
    void bytes_uploaded(uint64_t val) noexcept { bytes_uploaded_.store(val, std::memory_order_relaxed); }
    void add_bytes_downloaded(uint64_t val) noexcept { bytes_downloaded_.fetch_add(val, std::memory_order_relaxed); } 
    void add_bytes_uploaded(uint64_t val) noexcept { bytes_uploaded_.fetch_add(val, std::memory_order_relaxed); }
    
    ExtendedMessageType extension_type(uint8_t index) const noexcept {
        std::lock_guard lock(mutex_);
        if (!remote_extension_map_.count(index)) {
            return ExtendedMessageType::UNKNOWN;
        }
        return remote_extension_map_.at(index);
    }
    void update_extension_type(uint8_t index, ExtendedMessageType type) noexcept {
        std::lock_guard lock(mutex_);
        remote_extension_map_[index] = type;
    }
    template<typename T>
    void bitfield(T&& other) {
        std::lock_guard lock(mutex_);
        bitfield_ = std::forward<T>(other);
    }

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
    asio::strand<asio::io_context::executor_type> strand_;
    AsyncSocket socket_;
    std::string peer_addr_;
    PeerId peer_id_{};
    std::shared_ptr<SessionState> state_;
    std::shared_ptr<IPeerConnectionEvents> events_;

    std::vector<uint8_t> bitfield_;
    std::map<uint8_t, ExtendedMessageType> remote_extension_map_;

    std::atomic_bool am_choking_{true};
    std::atomic_bool peer_is_choking_{true};
    std::atomic_bool am_interested_{false};
    std::atomic_bool peer_is_interested_{false};

    std::atomic_uint64_t bytes_downloaded_{0};
    std::atomic_uint64_t bytes_uploaded_{0};

    mutable std::mutex mutex_;
};