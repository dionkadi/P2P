#pragma once

#include <atomic>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <span>
#include <vector>

#include "Utils.hpp"
#include "AsyncRateLimiter.hpp"
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

    std::chrono::steady_clock::time_point last_data_received() const noexcept { return last_data_received_; }
    void last_data_received(std::chrono::steady_clock::time_point tp) noexcept { last_data_received_ = tp; }
    
    ExtendedMessageType extension_type(uint8_t index) const noexcept {
        std::lock_guard lock(mutex_);
        if (!remote_extension_.count(index)) {
            return ExtendedMessageType::UNKNOWN;
        }
        return remote_extension_.at(index);
    }
    void update_extension_type(uint8_t index, ExtendedMessageType type) noexcept {
        std::lock_guard lock(mutex_);
        remote_extension_[index] = type;
    }
    template<typename T>
    void bitfield(T&& other) {
        std::lock_guard lock(mutex_);
        bitfield_ = std::forward<T>(other);
    }

    bool supported_pex() noexcept {
        std::call_once(pex_flag_, [self = shared_from_this()](){
            for (const auto& [k, v] : self->remote_extension_) {
                if (v == ExtendedMessageType::ut_pex) {
                    self->supported_pex_ = true;
                    return;
                }
            }
            self->supported_pex_ = false;
        });
        return supported_pex_;
    }

    void close() noexcept {
        asio::post(strand_, [self = shared_from_this()] {
            self->socket_.close();
            self->keep_alive_timer_.cancel();
        });
    }
    
    asio::awaitable<void> send_simple_message(MessageType type);
    // Returns true if sent immediately, false if queued due to pipeline limit
    asio::awaitable<bool> send_request(size_t index, uint32_t begin, uint32_t length);
    asio::awaitable<void> send_piece(size_t index, uint32_t begin, std::span<const std::byte> block_data);
    asio::awaitable<void> send_bitfield(const std::vector<uint8_t>& bitfield_data);
    asio::awaitable<void> send_cancel(size_t index, uint32_t begin, uint32_t length);
    asio::awaitable<void> send_have(size_t index);
    asio::awaitable<void> send_reject(size_t index, uint32_t begin, uint32_t length);
    asio::awaitable<void> send_extended_message(uint8_t type_id, std::span<const std::byte> payload);
    asio::awaitable<void> send_metadata_request(uint8_t ext_id, int piece);

    void flush_pending_requests();
    void on_request_completed(uint32_t length);
    void on_request_rejected(uint32_t length);
    const std::deque<RequestPayload>& pending_requests() const noexcept { return pending_requests_; }

    void set_upload_rate(uint64_t bps) noexcept;
    void set_download_rate(uint64_t bps) noexcept;

    // Pipeline state introspection (for testing/monitoring)
    size_t max_outstanding_requests() const noexcept { return max_outstanding_requests_; }
    size_t outstanding_request_count() const noexcept { return outstanding_request_count_; }
    uint64_t total_bytes_requested() const noexcept { return total_bytes_requested_; }
    uint64_t total_bytes_received() const noexcept { return total_bytes_received_; }
    size_t pending_request_count() const noexcept { return pending_requests_.size(); }

    static constexpr uint64_t MAX_PIPELINE_BUFFER = 256 * 1024;

    bool fast_extension_supported() const noexcept { return fast_extension_supported_; }
    void fast_extension_supported(bool val) noexcept { fast_extension_supported_ = val; }

    bool supports_metadata() const noexcept { return metadata_ext_id_ != 0; }
    uint8_t metadata_ext_id() const noexcept { return metadata_ext_id_; }
    void metadata_ext_id(uint8_t id) noexcept { metadata_ext_id_ = id; }
    int32_t metadata_size() const noexcept { return metadata_size_; }
    void metadata_size(int32_t size) noexcept { metadata_size_ = size; }

protected:
    PeerConnection(
        asio::io_context& io_context, AsyncSocket socket, std::string peer_addr, 
        std::shared_ptr<SessionState> state, std::shared_ptr<IPeerConnectionEvents> events
    ) noexcept;

    asio::io_context& io_context_;
    asio::strand<asio::io_context::executor_type> strand_;
    asio::steady_timer keep_alive_timer_;
    AsyncSocket socket_;
    std::string peer_addr_;
    PeerId peer_id_{};

    std::shared_ptr<AsyncRateLimiter<>> upload_limiter_;
    std::shared_ptr<AsyncRateLimiter<>> download_limiter_;

    std::atomic_uint64_t bytes_downloaded_{0};
    std::atomic_uint64_t bytes_uploaded_{0};

    std::chrono::steady_clock::time_point last_data_received_{};

    // Request pipelining
    std::deque<RequestPayload> pending_requests_;
    size_t max_outstanding_requests_ = 5;
    size_t outstanding_request_count_ = 0;
    uint64_t total_bytes_requested_ = 0;
    uint64_t total_bytes_received_ = 0;

private:
    asio::awaitable<bool> perform_handshake(const PeerId& my_peer_id);
    asio::awaitable<void> message_loop();
    asio::awaitable<void> keep_alive_loop();

    void start_loops();

    std::shared_ptr<SessionState> state_;
    std::shared_ptr<IPeerConnectionEvents> events_;

    std::vector<uint8_t> bitfield_;
    std::map<uint8_t, ExtendedMessageType> remote_extension_;

    std::atomic_bool am_choking_{true};
    std::atomic_bool peer_is_choking_{true};
    std::atomic_bool am_interested_{false};
    std::atomic_bool peer_is_interested_{false};

    std::once_flag pex_flag_;
    bool supported_pex_;
    bool fast_extension_supported_{false};
    uint8_t metadata_ext_id_{0};
    int32_t metadata_size_{0};

    mutable std::mutex mutex_;
};