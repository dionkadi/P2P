#pragma once

#include "Types.hpp"
#include "AsyncRateLimiter.hpp"
#include "SessionState.hpp"
#include "PieceManager.hpp"
#include "FileManager.hpp"
#include "PeerManager.hpp"
#include "IPeerEvents.hpp"
#include "TrackerClient.hpp"

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/strand.hpp>
#include <cstdint>
#include <memory>

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

    asio::awaitable<void> run();
    asio::awaitable<void> stop();

    // IPeerConnectionEvents implementation
    asio::awaitable<void> on_piece_block(std::shared_ptr<PeerConnection> conn, size_t piece_index, uint32_t begin, std::span<const std::byte> block_data) override;
    asio::awaitable<void> on_block_request(std::shared_ptr<PeerConnection> conn, size_t piece_index, uint32_t begin, uint32_t length) override;
    asio::awaitable<void> on_peer_has_piece(std::shared_ptr<PeerConnection> conn, size_t piece_index) override;
    asio::awaitable<void> on_peer_bitfield(std::shared_ptr<PeerConnection> conn, std::span<const std::byte> bitfield) override;
    asio::awaitable<void> on_choke_status_changed(std::shared_ptr<PeerConnection> conn, bool is_choking) override;
    asio::awaitable<void> on_disconnect(std::shared_ptr<PeerConnection> conn) override;
    asio::awaitable<void> on_extended_message(std::shared_ptr<PeerConnection> conn, std::span<const std::byte> payload) override;

private:
    asio::awaitable<bool> init();

    asio::awaitable<void> await_upload_tokens(size_t amount);
    asio::awaitable<void> await_download_tokens(size_t amount);

    asio::awaitable<void> load_session_state(const ResumeData& data);
    asio::awaitable<void> save_session_state();

    asio::awaitable<void> tracker_announce_loop();
    asio::awaitable<void> handle_new_connection(AsyncSocket socket, std::string peer_addr);
    asio::awaitable<void> periodically_save();

    asio::awaitable<void> send_cancel_for_block(uint32_t piece_index, uint32_t block_index, const PeerId& exclude_peer_id);

    void reset() noexcept;

    asio::io_context& io_context_;
    asio::strand<asio::any_io_executor> strand_;
    PeerId my_peer_id_;
    uint16_t peer_port_;
    Mode mode_;
    std::vector<std::vector<std::shared_ptr<ITrackerClient>>> tracker_clients_by_tier_;

    std::shared_ptr<SessionState> state_;
    std::unique_ptr<PieceManager> piece_manager_;
    std::unique_ptr<PeerManager> peer_manager_;
    std::unique_ptr<FileManager> file_manager_;
    AsyncRateLimiter<> upload_limiter_;
    AsyncRateLimiter<> download_limiter_;
    std::atomic<bool> shutting_down_{false};
    asio::steady_timer completion_timer_;
};