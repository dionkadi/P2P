#include "ClientApp.hpp"
#include "MagnetUri.hpp"

#include <algorithm>
#include <cstddef>
#include <filesystem>
#include <fstream>
#include <ranges>
#include <thread>

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
    apply_global_trackers(session);

    {
        std::lock_guard lock(torrents_mutex_);
        auto [it, inserted] = torrents_.emplace(info_hash, session);
        if (!inserted) {
            LOGWARN("Torrent with info_hash already exists, replacing existing session");
            asio::co_spawn(io_context_, it->second->stop(), asio::detached);
            it->second = session;
        }
    }

    // Spawn the session coroutine immediately (safe even before run())
    spawn_session(session);

    {
        std::lock_guard lock(torrents_mutex_);
        torrent_meta_[info_hash] = TorrentEntry{
            .source = torrent_path.string(),
            .save_path = save_path.string(),
            .is_magnet = false
        };
    }

    LOGINFO("Added {} torrent: {} on port {}",
            (mode == Mode::Seed ? "seed" : (mode == Mode::Hybrid ? "hybrid" : "download")),
            session->get_display_name(), port);
}

// Apply file selection to a session's state, marking pieces belonging to
// deselected files as Skipped so the downloader avoids them.
static void apply_file_selection(const std::shared_ptr<SessionState>& state,
                                  const std::vector<bool>& file_selection) {
    const auto& info = state->torrent_info();
    if (info.files.size() != file_selection.size()) return;
    size_t piece_size = info.piece_size;
    uint64_t file_offset = 0;
    for (size_t fi = 0; fi < info.files.size(); ++fi) {
        uint64_t file_start = file_offset;
        uint64_t file_end = file_offset + info.files[fi].size;
        size_t start_piece = file_start / piece_size;
        size_t end_piece = (file_end + piece_size - 1) / piece_size;
        if (!file_selection[fi]) {
            state->update_file_stat(fi, false);
            for (size_t pi = start_piece; pi < end_piece && pi < state->num_pieces(); ++pi) {
                uint64_t piece_start = pi * piece_size;
                uint64_t piece_end = piece_start + piece_size;
                // Only skip pieces fully within deselected files to avoid
                // corrupting data shared with selected files.
                if (piece_start >= file_start && piece_end <= file_end) {
                    state->piece_status(pi, PieceStatus::Skipped);
                }
            }
        }
        file_offset += info.files[fi].size;
    }
}

void ClientApp::add_torrent(Mode mode, const std::filesystem::path& torrent_path,
                             const std::filesystem::path& save_path, uint16_t port,
                             const std::vector<bool>& file_selection) {
    PeerId peer_id = generate_id(PEER_ID_PREFIX);

    auto session = std::make_shared<TorrentSession>(
        io_context_, peer_id, torrent_path, save_path, port, mode,
        config_.upload_rate_limit, config_.download_rate_limit
    );

    // Apply file selection before registering the session
    if (session->get_state() && !file_selection.empty()) {
        apply_file_selection(session->get_state(), file_selection);
    }

    const auto& hash_vec = session->get_info_hash();
    InfoHash info_hash{};
    std::copy_n(hash_vec.begin(), std::min(hash_vec.size(), info_hash.size()), info_hash.begin());

    seed_session_from_cache(info_hash, session);
    apply_global_trackers(session);

    {
        std::lock_guard lock(torrents_mutex_);
        auto [it, inserted] = torrents_.emplace(info_hash, session);
        if (!inserted) {
            LOGWARN("Torrent with info_hash already exists, replacing existing session");
            asio::co_spawn(io_context_, it->second->stop(), asio::detached);
            it->second = session;
        }
    }

    spawn_session(session);

    {
        std::lock_guard lock(torrents_mutex_);
        torrent_meta_[info_hash] = TorrentEntry{
            .source = torrent_path.string(),
            .save_path = save_path.string(),
            .is_magnet = false
        };
    }

    LOGINFO("Added {} torrent: {} on port {}",
            (mode == Mode::Seed ? "seed" : (mode == Mode::Hybrid ? "hybrid" : "download")),
            session->get_display_name(), port);
}

void ClientApp::setup_signals() {
    signals_.async_wait([this](boost::system::error_code ec, int signal_number) {
        if (!ec) {
            LOGINFO("Signal {} received, initiating graceful shutdown...", signal_number);
            stop_all();
            // Re-arm for subsequent signals (signal_set is one-shot)
            if (!stopping_.load()) {
                setup_signals();
            }
        }
    });
}

void ClientApp::stop_all() {
    if (stopping_.exchange(true)) {
        LOGDBG("Shutdown already in progress.");
        return;
    }

    std::map<InfoHash, std::shared_ptr<TorrentSession>> snapshot;
    {
        std::lock_guard lock(torrents_mutex_);
        LOGINFO("ClientApp stopping all {} torrent(s)...", torrents_.size());
        snapshot = torrents_;
    }

    // Harvest peers from active sessions into cache before stopping
    for (auto& [hash, session] : snapshot) {
        if (!session) continue;
        auto discovered = session->peer_manager()->get_discovered_peers();
        if (!discovered.empty()) {
            add_peers_to_cache(hash, discovered);
        }
    }

    for (auto& [hash, session] : snapshot) {
        if (session) {
            asio::co_spawn(io_context_, session->stop(), asio::detached);
        }
    }

    // Force-stop the io_context after a timeout if graceful shutdown stalls
    // (e.g., pending HTTP tracker requests that never complete)
    std::thread force_stop_thread([this] {
        // Give shutdown enough time: stop() has a 2s announce timeout + 5s save timeout.
        std::this_thread::sleep_for(std::chrono::seconds(5));
        if (!io_context_.stopped()) {
            io_context_.stop();
        }
    });
    force_stop_thread.detach();
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

    // io_context runs even with 0 torrents so interactive commands can add them

    {
        std::lock_guard lock(torrents_mutex_);
        LOGINFO("ClientApp running {} torrent(s) on io_context...", torrents_.size());
    }

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

void ClientApp::add_torrent_magnet(const std::string& magnet_uri,
                                    const std::filesystem::path& save_path,
                                    uint16_t port) {
    PeerId peer_id = generate_id(PEER_ID_PREFIX);
    auto session = TorrentSession::create_from_magnet(
        io_context_, peer_id, magnet_uri, save_path, port, Mode::Hybrid,
        config_.upload_rate_limit, config_.download_rate_limit
    );

    const auto& hash_vec = session->get_info_hash();
    InfoHash info_hash{};
    std::copy_n(hash_vec.begin(), std::min(hash_vec.size(), info_hash.size()), info_hash.begin());

    seed_session_from_cache(info_hash, session);
    apply_global_trackers(session);

    {
        std::lock_guard lock(torrents_mutex_);
        auto [it, inserted] = torrents_.emplace(info_hash, session);
        if (!inserted) {
            LOGWARN("Torrent with info_hash already exists");
            return;
        }
    }

    {
        std::lock_guard lock(torrents_mutex_);
        torrent_meta_[info_hash] = TorrentEntry{
            .source = magnet_uri,
            .save_path = save_path.string(),
            .is_magnet = true
        };
    }

    spawn_session(session);
    LOGINFO("Added magnet torrent: {} on port {}", session->get_display_name(), port);
}

std::shared_ptr<TorrentSession> ClientApp::torrent_by_index(size_t index) const {
    std::lock_guard lock(torrents_mutex_);
    if (index >= torrents_.size()) return nullptr;
    auto it = torrents_.begin();
    std::advance(it, index);
    return it->second;
}

void ClientApp::stop_torrent(size_t index) {
    auto session = torrent_by_index(index);
    if (!session) {
        LOGWARN("No torrent at index {}", index);
        return;
    }
    LOGINFO("Stopping torrent at index {}: {}", index, session->get_display_name());
    asio::co_spawn(io_context_, session->stop(), asio::detached);
}

void ClientApp::remove_torrent(size_t index) {
    std::shared_ptr<TorrentSession> session;
    InfoHash hash{};
    {
        std::lock_guard lock(torrents_mutex_);
        if (index >= torrents_.size()) {
            LOGWARN("No torrent at index {}", index);
            return;
        }
        auto it = torrents_.begin();
        std::advance(it, index);
        hash = it->first;
        session = std::move(it->second);
        torrents_.erase(it);
        torrent_meta_.erase(hash);
    }
    LOGINFO("Removed torrent: {} (will stop async)", session->get_display_name());
    asio::co_spawn(io_context_, session->stop(), asio::detached);
}

void ClientApp::apply_global_trackers(std::shared_ptr<TorrentSession> session) {
    std::lock_guard lock(torrents_mutex_);
    for (const auto& url : global_trackers_) {
        session->add_tracker_url(url);
    }
}

void ClientApp::add_tracker_to_all(const std::string& url) {
    std::lock_guard lock(torrents_mutex_);
    for (auto& [hash, session] : torrents_) {
        session->add_tracker_url(url);
    }
    if (std::find(global_trackers_.begin(), global_trackers_.end(), url) == global_trackers_.end()) {
        global_trackers_.push_back(url);
    }
    LOGINFO("Added tracker to {} active session(s): {}", torrents_.size(), url);
}

void ClientApp::add_trackers_to_all(const std::vector<std::string>& urls) {
    std::lock_guard lock(torrents_mutex_);
    for (auto& [hash, session] : torrents_) {
        for (const auto& url : urls) {
            session->add_tracker_url(url);
        }
    }
    for (const auto& url : urls) {
        if (std::find(global_trackers_.begin(), global_trackers_.end(), url) == global_trackers_.end()) {
            global_trackers_.push_back(url);
        }
    }
    LOGINFO("Added {} tracker(s) to {} active session(s)", urls.size(), torrents_.size());
}

void ClientApp::save_state(const std::filesystem::path& path) const {
    std::lock_guard lock(torrents_mutex_);

    Dict root;
    root["download_dir"] = Value{config_.download_dir};

    List torrents_list;
    for (const auto& [hash, entry] : torrent_meta_) {
        Dict entry_dict;
        entry_dict["source"] = Value{entry.source};
        entry_dict["dest"] = Value{entry.save_path};
        entry_dict["type"] = Value{entry.is_magnet ? String("magnet") : String("file")};
        torrents_list.push_back(Value{std::move(entry_dict)});
    }
    root["torrents"] = Value{std::move(torrents_list)};

    List trackers_list;
    for (const auto& url : global_trackers_) {
        trackers_list.push_back(Value{url});
    }
    root["global_trackers"] = Value{std::move(trackers_list)};

    auto encoded = encode(Value{std::move(root)});

    std::filesystem::create_directories(path.parent_path());
    std::ofstream ofs(path, std::ios::binary);
    ofs.write(reinterpret_cast<const char*>(encoded.data()), static_cast<std::streamsize>(encoded.size()));
    LOGINFO("Saved client state to {} ({} torrent(s))", path.string(), torrent_meta_.size());
}

void ClientApp::load_state(const std::filesystem::path& path, uint16_t port) {
    std::ifstream ifs(path, std::ios::binary | std::ios::ate);
    if (!ifs) {
        LOGWARN("State file not found: {}", path.string());
        return;
    }
    auto size = ifs.tellg();
    ifs.seekg(0);

    std::vector<std::byte> data(static_cast<size_t>(size));
    ifs.read(reinterpret_cast<char*>(data.data()), static_cast<std::streamsize>(size));

    Value decoded = decode(data);
    const auto* dict_ptr = std::get_if<std::unique_ptr<Dict>>(&decoded.get_variant());
    if (!dict_ptr) {
        LOGWARN("Invalid state file format (not a dict)");
        return;
    }
    const auto& root = **dict_ptr;

    auto dd_it = root.find("download_dir");
    if (dd_it != root.end()) {
        const auto* dd = std::get_if<String>(&dd_it->second.get_variant());
        if (dd) {
            config_.download_dir = *dd;
        }
    }

    auto gt_it = root.find("global_trackers");
    if (gt_it != root.end()) {
        const auto* list_ptr = std::get_if<std::unique_ptr<List>>(&gt_it->second.get_variant());
        if (list_ptr) {
            for (const auto& entry_val : **list_ptr) {
                const auto* url = std::get_if<String>(&entry_val.get_variant());
                if (url) {
                    global_trackers_.push_back(*url);
                }
            }
            LOGINFO("Restored {} global tracker(s) from state", global_trackers_.size());
        }
    }

    auto tr_it = root.find("torrents");
    if (tr_it == root.end()) return;

    const auto* list_ptr = std::get_if<std::unique_ptr<List>>(&tr_it->second.get_variant());
    if (!list_ptr) return;
    const auto& torrents_list = **list_ptr;

    for (const auto& entry_val : torrents_list) {
        const auto* entry_dict_ptr = std::get_if<std::unique_ptr<Dict>>(&entry_val.get_variant());
        if (!entry_dict_ptr) continue;
        const auto& entry = **entry_dict_ptr;

        auto src_it = entry.find("source");
        auto dest_it = entry.find("dest");
        auto type_it = entry.find("type");
        if (src_it == entry.end() || dest_it == entry.end() || type_it == entry.end()) continue;

        const auto* source = std::get_if<String>(&src_it->second.get_variant());
        const auto* dest = std::get_if<String>(&dest_it->second.get_variant());
        const auto* type = std::get_if<String>(&type_it->second.get_variant());
        if (!source || !dest || !type) continue;

        try {
            if (*type == "magnet") {
                add_torrent_magnet(*source, *dest, port);
            } else {
                add_torrent(Mode::Hybrid, *source, *dest, port);
            }
        } catch (const std::exception& e) {
            LOGWARN("Failed to restore torrent '{}': {}", *source, e.what());
        }
    }
    LOGINFO("Loaded client state from {} ({} torrent(s))", path.string(), torrent_meta_.size());
}
