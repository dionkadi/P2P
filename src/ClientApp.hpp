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

    // Stop a single torrent by display index (0-based iteration order)
    void stop_torrent(size_t index);

    // Resume a previously stopped torrent
    void resume_torrent(size_t index);

    // Remove a single torrent by display index (stop + remove from torrents_)
    void remove_torrent(size_t index);

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

    // Return the i-th torrent (0-based iteration order of the map), or nullptr
    std::shared_ptr<TorrentSession> torrent_by_index(size_t index) const;

    // Add a magnet link (BEP-9) while the io_context is running
    void add_torrent_magnet(const std::string& magnet_uri,
                            const std::filesystem::path& save_path,
                            uint16_t port);
    void add_torrent_magnet(const std::string& magnet_uri,
                            const std::filesystem::path& save_path,
                            uint16_t port,
                            const std::vector<std::byte>& info_bencoded);

    // Add a tracker URL to every active session AND store globally for future torrents
    void add_tracker_to_all(const std::string& url);
    // Add multiple tracker URLs to every active session AND store globally for future torrents
    void add_trackers_to_all(const std::vector<std::string>& urls);
    // Return the global tracker list (applied to every new torrent session)
    std::vector<std::string> global_trackers() const {
        std::lock_guard lock(torrents_mutex_);
        return global_trackers_;
    }

    // Peer cache: cross-torrent known peer sharing
    void add_to_peer_cache(const InfoHash& hash, const EndPoint& ep);
    void add_peers_to_cache(const InfoHash& hash, const std::unordered_set<EndPoint>& peers);
    void seed_session_from_cache(const InfoHash& hash, std::shared_ptr<TorrentSession> session) const;

    // Expose io_context for cross-thread command dispatch
    asio::io_context& get_io_context() noexcept { return io_context_; }

    bool is_stopping() const noexcept { return stopping_.load(std::memory_order_acquire); }

    // Check persisted stopped flag (set by explicit user commands, survives restart).
    bool is_torrent_stopped(const InfoHash& hash) const {
        std::lock_guard lock(torrents_mutex_);
        auto it = torrent_meta_.find(hash);
        return it != torrent_meta_.end() && it->second.stopped;
    }

    // Persist/restore active torrent metadata for restart
    void save_state(const std::filesystem::path& path) const;
    void load_state(const std::filesystem::path& path, uint16_t port);

private:
    struct TorrentEntry {
        std::string source;
        std::string save_path;
        bool is_magnet;
        bool stopped{false};
    };
    std::map<InfoHash, TorrentEntry> torrent_meta_;
    asio::io_context io_context_;
    asio::signal_set signals_;
    mutable std::mutex torrents_mutex_;
    std::map<InfoHash, std::shared_ptr<TorrentSession>> torrents_;
    std::map<InfoHash, std::vector<EndPoint>> peer_cache_;
    ClientConfig config_;
    std::vector<std::string> global_trackers_;
    std::atomic<bool> stopping_{false};

    void spawn_session(const std::shared_ptr<TorrentSession>& session);
    void apply_global_trackers(std::shared_ptr<TorrentSession> session);
};
