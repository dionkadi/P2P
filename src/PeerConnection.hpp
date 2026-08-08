#pragma once

#include <atomic>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
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
    using RequestSentHook = std::function<void(uint32_t index, uint32_t begin, uint32_t length, const PeerId& peer_id)>;
    using InterestChangeHook = std::function<void()>;

    void set_interest_change_hook(InterestChangeHook hook) noexcept { interest_change_hook_ = std::move(hook); }

    static asio::awaitable<std::shared_ptr<PeerConnection>> create(
        asio::io_context& io_context,
        AsyncSocket socket,
        std::string peer_addr,
        const PeerId& my_id,
        std::shared_ptr<SessionState> state,
        std::shared_ptr<IPeerConnectionEvents> events,
        bool mse_enabled,
        bool inbound
    );

    PeerConnection(const PeerConnection&) = delete;
    PeerConnection& operator=(const PeerConnection&) = delete;
    PeerConnection(PeerConnection&&) noexcept = delete;
    PeerConnection& operator=(PeerConnection&&) noexcept = delete;
    ~PeerConnection();

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
    std::vector<uint8_t> bitfield_copy() const {
        std::lock_guard lock(mutex_);
        return bitfield_;
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
    std::chrono::steady_clock::time_point last_unchoke_time() const noexcept { return last_unchoke_time_; }
    void last_unchoke_time(std::chrono::steady_clock::time_point tp) noexcept { last_unchoke_time_ = tp; }
    
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
    bool inventory_pending_metadata() const noexcept {
        std::lock_guard lock(mutex_);
        return inventory_pending_metadata_;
    }
    void inventory_pending_metadata(bool val) noexcept {
        std::lock_guard lock(mutex_);
        inventory_pending_metadata_ = val;
    }
    bool peer_has_all_hint() const noexcept {
        std::lock_guard lock(mutex_);
        return peer_has_all_hint_;
    }
    void peer_has_all_hint(bool val) noexcept {
        std::lock_guard lock(mutex_);
        peer_has_all_hint_ = val;
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
        if (upload_limiter_) {
            upload_limiter_->stop();
        }
        if (download_limiter_) {
            download_limiter_->stop();
        }
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
    // True if requests are queued (unsent) waiting for a free pipeline slot.
    bool has_pending_requests() const noexcept {
        std::lock_guard lock(pipeline_mutex_);
        return !pending_requests_.empty();
    }
    // Drops unsent queued requests. Called when the peer chokes us: those
    // blocks are re-requested through the piece resume path instead, so the
    // queue can't re-flood a peer that already refused us.
    void drop_pending_requests() noexcept {
        std::lock_guard lock(pipeline_mutex_);
        pending_requests_.clear();
    }
    const std::deque<RequestPayload>& pending_requests() const noexcept { return pending_requests_; }
    // Thread-safe snapshot of the unsent request queue. on_disconnect must
    // iterate across co_await suspension points; the live deque is mutated by
    // other coroutines under pipeline_mutex_ (send_request push_back,
    // flush_pending_requests pop_front, send_cancel erase, drop clear), so
    // iterating the live reference races concurrent mutation (dangling
    // iterator → SEGV). Elements are 12 bytes and the queue is bounded by the
    // pipeline limits, so the copy is negligible.
    std::deque<RequestPayload> pending_requests_snapshot() const {
        std::lock_guard lock(pipeline_mutex_);
        return pending_requests_;
    }

    void set_upload_rate(uint64_t bps) noexcept;
    void set_download_rate(uint64_t bps) noexcept;
    void set_request_sent_hook(RequestSentHook hook) {
        std::lock_guard lock(mutex_);
        request_sent_hook_ = std::move(hook);
    }

    // Pipeline state introspection (for testing/monitoring)
    size_t max_outstanding_requests() const noexcept { return max_outstanding_requests_; }
    size_t outstanding_request_count() const noexcept {
        std::lock_guard lock(pipeline_mutex_);
        return outstanding_request_count_;
    }
    uint64_t total_bytes_requested() const noexcept {
        std::lock_guard lock(pipeline_mutex_);
        return total_bytes_requested_;
    }
    uint64_t total_bytes_received() const noexcept {
        std::lock_guard lock(pipeline_mutex_);
        return total_bytes_received_;
    }
    size_t pending_request_count() const noexcept {
        std::lock_guard lock(pipeline_mutex_);
        return pending_requests_.size();
    }

    // qBittorrent pipelines up to max_out_request_queue=500 requests per peer
    // (16 KiB blocks → ~8 MiB in flight per connection), filling every
    // unchoked peer's pipe simultaneously. 64 requests (~1 MiB) under-utilized
    // the few peers that did unchoke us, capping per-peer throughput. 500
    // matches libtorrent's request_queue_size default.
    static constexpr uint64_t MAX_PIPELINE_BUFFER = 8 * 1024 * 1024;

    bool fast_extension_supported() const noexcept { return fast_extension_supported_; }
    void fast_extension_supported(bool val) noexcept { fast_extension_supported_ = val; }

    bool supports_metadata() const noexcept { return metadata_ext_id_ != 0; }
    uint8_t metadata_ext_id() const noexcept { return metadata_ext_id_; }
    void metadata_ext_id(uint8_t id) noexcept { metadata_ext_id_ = id; }
    int32_t metadata_size() const noexcept { return metadata_size_; }
    void metadata_size(int32_t size) noexcept { metadata_size_ = size; }
    bool metadata_requested() const noexcept { return metadata_requested_; }
    void metadata_requested(bool v) noexcept { metadata_requested_ = v; }

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
    bool mse_enabled_{false};
    bool inbound_{false};

    std::shared_ptr<AsyncRateLimiter<>> upload_limiter_;
    std::shared_ptr<AsyncRateLimiter<>> download_limiter_;

    std::atomic_uint64_t bytes_downloaded_{0};
    std::atomic_uint64_t bytes_uploaded_{0};

    std::chrono::steady_clock::time_point last_data_received_{};
    // When this peer last unchoked us (epoch if never). Written on the
    // message-loop thread, read on the PieceManager strand — same benign
    // pattern as last_data_received_. Lets block picks discover freshly
    // unchoked seeders before they have any delivery evidence.
    std::chrono::steady_clock::time_point last_unchoke_time_{};

    // Request pipelining. The old window (5 requests / 256 KiB in flight) kept
    // throughput far below what peers can deliver over LAN/domestic links;
    // qBittorrent pipelines dozens of blocks per connection. 500 requests of
    // 16 KiB = ~8 MiB per peer in flight, capped by MAX_PIPELINE_BUFFER.
    std::deque<RequestPayload> pending_requests_;
    size_t max_outstanding_requests_ = 500;
    size_t outstanding_request_count_ = 0;
    uint64_t total_bytes_requested_ = 0;
    uint64_t total_bytes_received_ = 0;

private:
    void notify_request_sent(uint32_t index, uint32_t begin, uint32_t length) {
        RequestSentHook hook;
        PeerId peer_id{};
        {
            std::lock_guard lock(mutex_);
            hook = request_sent_hook_;
            peer_id = peer_id_;
        }
        if (hook) {
            hook(index, begin, length, peer_id);
        }
    }

    asio::awaitable<bool> perform_handshake(const PeerId& my_peer_id);
    asio::awaitable<void> message_loop();
    asio::awaitable<void> keep_alive_loop();

    void start_loops();

    std::shared_ptr<SessionState> state_;
    std::shared_ptr<IPeerConnectionEvents> events_;

    std::vector<uint8_t> bitfield_;
    std::map<uint8_t, ExtendedMessageType> remote_extension_;
    RequestSentHook request_sent_hook_;
    InterestChangeHook interest_change_hook_;

    std::atomic_bool am_choking_{true};
    std::atomic_bool peer_is_choking_{true};
    std::atomic_bool am_interested_{false};
    std::atomic_bool peer_is_interested_{false};

    std::once_flag pex_flag_;
    bool supported_pex_;
    bool fast_extension_supported_{false};
    uint8_t metadata_ext_id_{0};
    int32_t metadata_size_{0};
    bool metadata_requested_{false};
    bool inventory_pending_metadata_{false};
    bool peer_has_all_hint_{false};

    mutable std::mutex mutex_;

    // Protects pipeline state: pending_requests_, outstanding_request_count_,
    // total_bytes_requested_, total_bytes_received_
    mutable std::mutex pipeline_mutex_;
};
