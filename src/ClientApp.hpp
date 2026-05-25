#pragma once

#include "ClientConfig.hpp"
#include "TorrentSession.hpp"
#include "Utils.hpp"

#include <boost/asio/signal_set.hpp>
#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

class ClientApp {
public:
    ClientApp();
    explicit ClientApp(ClientConfig config);
    ~ClientApp();

    // Add a torrent session (seed or download)
    void add_torrent(Mode mode, const std::filesystem::path& torrent_path,
                     const std::filesystem::path& save_path, uint16_t port);

    // Add a torrent with selective file download
    // file_selection[i] = true means the i-th file should be downloaded
    void add_torrent(Mode mode, const std::filesystem::path& torrent_path,
                     const std::filesystem::path& save_path, uint16_t port,
                     const std::vector<bool>& file_selection);

    // Start all torrents (runs io_context)
    int run();

    // Stop all torrents
    void stop_all();

    // Setup signal handlers (called automatically by run())
    void setup_signals();

    // Access config
    ClientConfig& config() { return config_; }
    const ClientConfig& config() const { return config_; }

    // Access torrent sessions for UI rendering (returns a copy for thread safety)
    auto torrents() const {
        std::lock_guard lock(torrents_mutex_);
        return torrents_;
    }
    size_t torrent_count() const {
        std::lock_guard lock(torrents_mutex_);
        return torrents_.size();
    }

    // Peer cache: cross-torrent known peer sharing
    void add_to_peer_cache(const InfoHash& hash, const EndPoint& ep);
    void add_peers_to_cache(const InfoHash& hash, const std::unordered_set<EndPoint>& peers);
    void seed_session_from_cache(const InfoHash& hash, std::shared_ptr<TorrentSession> session) const;

    // Expose io_context for cross-thread command dispatch
    asio::io_context& get_io_context() noexcept { return io_context_; }

private:
    asio::io_context io_context_;
    asio::signal_set signals_;
    mutable std::mutex torrents_mutex_;
    std::map<InfoHash, std::shared_ptr<TorrentSession>> torrents_;
    std::map<InfoHash, std::vector<EndPoint>> peer_cache_;
    ClientConfig config_;
    std::atomic<bool> stopping_{false};

    void spawn_session(const std::shared_ptr<TorrentSession>& session);
};
