#include "ClientApp.hpp"

#include <algorithm>
#include <cstddef>
#include <ranges>

ClientApp::ClientApp()
    : signals_(io_context_, SIGINT, SIGTERM)
{
}

ClientApp::ClientApp(ClientConfig config)
    : signals_(io_context_, SIGINT, SIGTERM)
    , config_(std::move(config))
{
}

ClientApp::~ClientApp() {
    stop_all();
}

void ClientApp::add_torrent(Mode mode, const std::filesystem::path& torrent_path,
                            const std::filesystem::path& save_path, uint16_t port) {
    PeerId peer_id = generate_id(PEER_ID_PREFIX);

    auto session = std::make_shared<TorrentSession>(
        io_context_, peer_id, torrent_path, save_path, port, mode,
        config_.upload_rate_limit, config_.download_rate_limit
    );

    // Convert info_hash (vector) to InfoHash (array) for map key
    const auto& hash_vec = session->get_info_hash();
    InfoHash info_hash{};
    std::copy_n(hash_vec.begin(), std::min(hash_vec.size(), info_hash.size()), info_hash.begin());

    // Inject cached peers for this info_hash (cross-torrent bootstrapping)
    seed_session_from_cache(info_hash, session);

    auto [it, inserted] = torrents_.emplace(info_hash, session);
    if (!inserted) {
        LOGWARN("Torrent with info_hash already exists, replacing existing session");
        asio::co_spawn(io_context_, it->second->stop(), asio::detached);
        it->second = session;
    }

    LOGINFO("Added {} torrent: {} on port {}",
            (mode == Mode::Seed ? "seed" : "download"),
            session->get_display_name(), port);
}

void ClientApp::setup_signals() {
    signals_.async_wait([this](boost::system::error_code ec, int signal_number) {
        if (!ec) {
            LOGINFO("Signal {} received, initiating graceful shutdown...", signal_number);
            stop_all();
        }
    });
}

void ClientApp::stop_all() {
    if (stopping_.exchange(true)) {
        LOGDBG("Shutdown already in progress.");
        return;
    }

    LOGINFO("ClientApp stopping all {} torrent(s)...", torrents_.size());

    // Harvest peers from active sessions into cache before stopping
    for (auto& [hash, session] : torrents_) {
        if (!session) continue;
        auto discovered = session->peer_manager()->get_discovered_peers();
        if (!discovered.empty()) {
            add_peers_to_cache(hash, discovered);
        }
    }

    for (auto& [hash, session] : torrents_) {
        if (session) {
            asio::co_spawn(io_context_, session->stop(), asio::detached);
        }
    }
}

void ClientApp::spawn_session(const std::shared_ptr<TorrentSession>& session) {
    asio::co_spawn(io_context_,
        [session]() -> asio::awaitable<void> {
            try {
                co_await session->run();
            } catch (const boost::system::system_error& ex) {
                if (ex.code() != asio::error::operation_aborted) {
                    LOGCRITICAL("TorrentSession run() threw: {}", ex.what());
                }
            } catch (const std::exception& ex) {
                LOGCRITICAL("TorrentSession run() threw: {}", ex.what());
            }
            LOGINFO("TorrentSession run() finished.");
        },
        asio::detached
    );
}

int ClientApp::run() {
    setup_signals();

    if (torrents_.empty()) {
        LOGWARN("No torrents added, nothing to do.");
        return 0;
    }

    // Spawn all session coroutines before running io_context
    for (auto& [hash, session] : torrents_) {
        spawn_session(session);
    }

    LOGINFO("ClientApp running {} torrent(s) on io_context...", torrents_.size());

    // Run io_context on hardware concurrency threads
    unsigned int num_threads = std::thread::hardware_concurrency();
    if (num_threads == 0) num_threads = 1;

    std::vector<std::jthread> io_threads;
    io_threads.reserve(num_threads);

    for (unsigned int i = 0; i < num_threads; ++i) {
        io_threads.emplace_back([this] {
            try {
                io_context_.run();
            } catch (const std::exception& e) {
                LOGCRITICAL("IO thread error: {}", e.what());
            }
        });
    }

    LOGINFO("ClientApp running on {} thread(s)", num_threads);

    for (auto& t : io_threads) {
        if (t.joinable()) t.join();
    }

    LOGINFO("ClientApp finished.");
    return 0;
}

void ClientApp::add_to_peer_cache(const InfoHash& hash, const EndPoint& ep) {
    auto& peers = peer_cache_[hash];
    if (std::find(peers.begin(), peers.end(), ep) == peers.end()) {
        peers.push_back(ep);
    }
}

void ClientApp::add_peers_to_cache(const InfoHash& hash, const std::unordered_set<EndPoint>& peers) {
    auto& cached = peer_cache_[hash];
    for (const auto& ep : peers) {
        if (std::find(cached.begin(), cached.end(), ep) == cached.end()) {
            cached.push_back(ep);
        }
    }
    LOGDBG("Peer cache for torrent now has {} peers", cached.size());
}

void ClientApp::seed_session_from_cache(const InfoHash& hash, std::shared_ptr<TorrentSession> session) const {
    auto it = peer_cache_.find(hash);
    if (it != peer_cache_.end() && !it->second.empty()) {
        LOGINFO("Injecting {} cached peer(s) into new torrent session", it->second.size());
        for (const auto& ep : it->second) {
            session->peer_manager()->add_discovered_peer(ep);
        }
    }
}
