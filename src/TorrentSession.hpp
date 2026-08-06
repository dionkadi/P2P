#pragma once

#include "Utils.hpp"
#include "AsyncRateLimiter.hpp"
#include "SessionState.hpp"
#include "PieceManager.hpp"
#include "FileManager.hpp"
#include "PeerManager.hpp"
#include "IPeerEvents.hpp"
#include "TrackerClient.hpp"
#include "Kademlia.hpp"
#include "LsdDiscovery.hpp"

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>

static constexpr size_t METADATA_PIECE_SIZE = 16384; // 16 KiB metadata pieces (BEP-10)

class TorrentSession : public IPeerConnectionEvents, public std::enable_shared_from_this<TorrentSession> {
public:
    TorrentSession(
        asio::io_context& io_context, 
        PeerId my_peer_id, 
        const std::filesystem::path& torrent_path, 
        const std::filesystem::path& save_path, 
        int peer_port, 
        Mode mode,
        uint64_t upload_rate_bps = 512 * 1024, 
        uint64_t download_rate_bps = 2048 * 1024
    );

    // Factory: create from a magnet URI (BEP-9)
    static std::shared_ptr<TorrentSession> create_from_magnet(
        asio::io_context& io_context,
        PeerId my_peer_id,
        const std::string& magnet_uri,
        const std::filesystem::path& save_path,
        int peer_port,
        Mode mode,
        uint64_t upload_rate_bps = 512 * 1024,
        uint64_t download_rate_bps = 2048 * 1024
    );

    static std::shared_ptr<TorrentSession> create_from_magnet_with_metadata(
        asio::io_context& io_context,
        PeerId my_peer_id,
        const std::string& magnet_uri,
        const std::filesystem::path& save_path,
        int peer_port,
        Mode mode,
        const std::vector<std::byte>& info_bencoded,
        uint64_t upload_rate_bps = 512 * 1024,
        uint64_t download_rate_bps = 2048 * 1024
    );

    TorrentSession(const TorrentSession&) = delete;
    TorrentSession& operator=(const TorrentSession&) = delete;

    asio::awaitable<void> run();
    asio::awaitable<void> stop();

    void set_on_complete(std::function<void()> cb) { on_complete_ = std::move(cb); }
    void set_tracker_announce_interval(std::chrono::seconds interval) { tracker_announce_interval_ = interval; }
    void set_enable_dht(bool enabled) noexcept { enable_dht_ = enabled; }
    void set_enable_lsd(bool enabled) noexcept { enable_lsd_ = enabled; }
    // Use a shared DHT node (managed externally by ClientApp) instead of creating one.
    // The external node must already be started and will not be stopped by this session.
    void set_shared_dht_node(std::shared_ptr<DHTNode> node) noexcept { dht_node_ = node; external_dht_node_ = node; }

    const std::shared_ptr<PeerManager>& peer_manager() const noexcept { return peer_manager_; }
    // Apply connection-limit config from ClientConfig. Without this the PeerManager
    // keeps hardcoded defaults (200/2/40) and --max-connections / --max-half-open /
    // --max-connections-per-ip on the CLI/config are silently ignored.
    void set_connection_limits(size_t max_total, size_t max_per_ip, size_t max_half_open) noexcept {
        peer_manager_->set_max_total_connections(max_total);
        peer_manager_->set_max_connections_per_ip(max_per_ip);
        peer_manager_->set_max_half_open_connections(max_half_open);
    }
    std::shared_ptr<SessionState> get_state() const noexcept { return state_; }
    Mode get_mode() const noexcept { return mode_; }
    uint16_t get_port() const noexcept { return peer_port_; }
    bool is_stopped() const noexcept { return shutting_down_.load(std::memory_order_acquire); }
    const std::vector<std::byte>& get_info_hash() const noexcept { return state_->info_hash(); }
    const TorrentInfo& get_torrent_info() const noexcept { return state_->torrent_info(); }
    const std::string& get_display_name() const noexcept { return state_->torrent_info().name; }

    void add_tracker_url(const std::string& url);
    void add_tracker_url_direct(const std::string& url);
    // Returns true and remembers `url` if it was not seen before; false if it
    // is a duplicate. Prevents the same tracker being configured twice
    // (observed: tracker.publicbt.com:80 registered twice from state restore),
    // which doubled announce traffic and wasted connection slots.
    bool is_duplicate_tracker_url(const std::string& url);
    // Count trackers that have had at least one successful announce
    size_t connected_tracker_count() const noexcept {
        std::lock_guard lock(tracker_backoff_mutex_);
        size_t count = 0;
        for (const auto& [url, state] : tracker_backoff_states_) {
            if (state.last_success_at_ != TimePoint{}) {
                ++count;
            }
        }
        return count;
    }

    // IPeerConnectionEvents implementation
    asio::awaitable<void> on_piece_block(std::shared_ptr<PeerConnection> conn, size_t piece_index, uint32_t begin, std::span<const std::byte> block_data) override;
    asio::awaitable<void> on_block_request(std::shared_ptr<PeerConnection> conn, size_t piece_index, uint32_t begin, uint32_t length) override;
    asio::awaitable<void> on_peer_has_piece(std::shared_ptr<PeerConnection> conn, size_t piece_index) override;
    asio::awaitable<void> on_peer_has_all(std::shared_ptr<PeerConnection> conn) override;
    asio::awaitable<void> on_peer_has_none(std::shared_ptr<PeerConnection> conn) override;
    asio::awaitable<void> on_peer_bitfield(std::shared_ptr<PeerConnection> conn, std::span<const std::byte> bitfield) override;
    asio::awaitable<void> on_choke_status_changed(std::shared_ptr<PeerConnection> conn, bool is_choking) override;
    asio::awaitable<void> on_piece_rejected(std::shared_ptr<PeerConnection> conn, size_t piece_index, uint32_t begin, uint32_t length) override;
    asio::awaitable<void> on_disconnect(std::shared_ptr<PeerConnection> conn) override;
    asio::awaitable<void> on_extended_message(std::shared_ptr<PeerConnection> conn, std::span<const std::byte> payload) override;

private:
    asio::awaitable<bool> init();

    asio::awaitable<void> await_upload_tokens(size_t amount);
    asio::awaitable<void> await_download_tokens(size_t amount);

    // Internal constructor for magnet-link sessions (no .torrent file)
    TorrentSession(
        asio::io_context& io_context,
        PeerId my_peer_id,
        std::shared_ptr<SessionState> state,
        int peer_port,
        Mode mode,
        uint64_t upload_rate_bps,
        uint64_t download_rate_bps
    );

    asio::awaitable<void> load_session_state(const ResumeData& data);
    asio::awaitable<void> save_session_state();

    asio::awaitable<void> tracker_announce_loop();
    asio::awaitable<void> announce_tracker_for(std::string event = "");
    asio::awaitable<void> discovered_peers_loop();
    asio::awaitable<void> handle_new_connection(AsyncSocket socket, std::string peer_addr);
    asio::awaitable<void> periodically_save();
    asio::awaitable<void> dht_announce_loop();
    asio::awaitable<void> request_metadata_from_peer(std::shared_ptr<PeerConnection> conn);
    asio::awaitable<void> metadata_retry_loop();
    asio::awaitable<void> on_metadata_complete();

    asio::awaitable<void> send_cancel_for_block(uint32_t piece_index, uint32_t block_index, const PeerId& exclude_peer_id);
    void record_request_sent(size_t piece_index, uint32_t begin, uint32_t length, const PeerId& peer_id);

    void reset() noexcept;

    asio::io_context& io_context_;
    asio::strand<asio::any_io_executor> strand_;
    asio::steady_timer save_timer_;
    asio::steady_timer tracker_announce_timer_;
    asio::steady_timer discovered_peers_timer_;
    PeerId my_peer_id_;
    uint16_t peer_port_;
    Mode mode_;
    std::vector<std::vector<std::shared_ptr<ITrackerClient>>> tracker_clients_by_tier_;
    std::unordered_map<std::string, BackoffState> tracker_backoff_states_;
    mutable std::mutex tracker_backoff_mutex_;
    std::unordered_set<std::string> tracker_urls_;
    std::mutex tracker_urls_mutex_;

    std::shared_ptr<SessionState> state_;
    std::shared_ptr<PieceManager> piece_manager_;
    std::shared_ptr<PeerManager> peer_manager_;
    std::unique_ptr<AsyncServerSocket> peer_server_;
    std::unique_ptr<FileManager> file_manager_;
    std::shared_ptr<DHTNode> dht_node_;
    std::shared_ptr<DHTNode> external_dht_node_; // non-owning: lifecycle managed by ClientApp
    std::shared_ptr<LsdDiscovery> lsd_discovery_;
    asio::steady_timer dht_announce_timer_;
    int empty_dht_lookups_{0}; // consecutive get_peers returning 0 peers
    std::atomic<bool> dht_bootstrap_in_progress_{false}; // one bootstrap round at a time
    std::vector<std::string> dht_bootstrap_nodes_;
    bool enable_dht_{true};
    bool enable_lsd_{true};

    bool metadata_download_active_{false};
    std::vector<std::byte> metadata_buffer_;
    std::atomic<size_t> metadata_pieces_received_{0};
    std::once_flag metadata_buffer_init_flag_;
    asio::steady_timer metadata_retry_timer_;
    AsyncRateLimiter<> upload_limiter_;
    AsyncRateLimiter<> download_limiter_;
    std::atomic<bool> shutting_down_{false};
    size_t chain_counter_{0}; // 1/kChainDivisor of completions re-seed the completing primary
    asio::steady_timer completion_timer_;
    std::function<void()> on_complete_;
    std::chrono::seconds tracker_announce_interval_;
    int peerless_announces_{0}; // consecutive fast re-announces while peerless
};
