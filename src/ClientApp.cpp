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
        } else {
            order_.push_back(info_hash);
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
        } else {
            order_.push_back(info_hash);
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
        // LOGDBG("Shutdown already in progress.");
        return;
    }

    // Fallback: if run() hasn't finished within 2s of shutdown (an op still
    // keeping the io_context alive — e.g. uncancellable DNS resolves from the
    // tracker fan-out storm, which can take several seconds to drain
    // naturally), force-stop so the process exits promptly. Woken early when
    // the drain completes; joined in ~ClientApp before the io_context is
    // destroyed. Created for every path, including the empty branch.
    force_stop_thread_ = std::jthread([this](std::stop_token st) {
        std::unique_lock lock(shutdown_mutex_);
        if (!shutdown_cv_.wait_for(lock, st, std::chrono::seconds(2),
                                   [this] { return run_finished_.load(); })) {
            if (!io_context_.stopped()) {
                io_context_.stop();
            }
        }
    });

    std::map<InfoHash, std::shared_ptr<TorrentSession>> snapshot;
    {
        std::lock_guard lock(torrents_mutex_);
        LOGINFO("ClientApp stopping all {} torrent(s)...", torrents_.size());
        snapshot = torrents_;
    }

    if (snapshot.empty()) {
        // No sessions to stop, but the shared DHT node may still be running
        // (e.g. after 'r' removed the last torrent): it must be stopped or
        // its loops keep the io_context alive forever. Cancel the
        // permanently-armed signal wait too, then let run() drain naturally.
        // io_context_.stop() here would abandon queued completions, leaking
        // suspended coroutine frames (LSan).
        if (dht_node_) {
            dht_node_->stop();
            dht_node_.reset();
        }
        signals_.cancel();
        return;
    }

    // Harvest peers from active sessions into cache before stopping
    for (auto& [hash, session] : snapshot) {
        if (!session) continue;
        auto discovered = session->peer_manager()->get_discovered_peers();
        if (!discovered.empty()) {
            add_peers_to_cache(hash, discovered);
        }
    }

    auto remaining = std::make_shared<std::atomic<size_t>>(snapshot.size());
    for (auto& [hash, session] : snapshot) {
        if (session) {
            asio::co_spawn(io_context_, session->stop(), [this, remaining](std::exception_ptr e) {
                if (e) {
                    try {
                        std::rethrow_exception(e);
                    } catch (const std::exception& ex) {
                        LOGERR("ClientApp: session stop failed: {}", ex.what());
                    }
                }

                if (remaining->fetch_sub(1, std::memory_order_acq_rel) == 1) {
                    // Stop the shared DHT node after all sessions have stopped.
                    if (dht_node_) {
                        dht_node_->stop();
                        dht_node_.reset();
                    }
                    // Graceful drain: cancel the permanently-armed signal
                    // wait, then let run() return once every queued
                    // completion (cancelled timers/ops, loop unwinds) is
                    // processed. io_context_.stop() here would abandon those
                    // completions, leaking the suspended coroutine frames
                    // they reference (LSan: awaitable_frame allocations).
                    signals_.cancel();
                }
            });
        }
    }
}

void ClientApp::init_dht(uint16_t port) {
    if (dht_node_ || !config_.enable_dht) return;
    dht_node_ = std::make_shared<DHTNode>(io_context_, port, DHTNode::load_or_generate_node_id(), true);
    dht_node_->start();
    auto bootstrap_nodes = config_.dht_bootstrap_nodes; // copy for async bootstrap
    asio::co_spawn(io_context_, [dht = dht_node_, bootstrap_nodes = std::move(bootstrap_nodes)]() -> asio::awaitable<void> {
        co_await dht->bootstrap(bootstrap_nodes);
    }, asio::detached);
}

void ClientApp::spawn_session(const std::shared_ptr<TorrentSession>& session) {
    // Share the single DHT node across all sessions to avoid SO_REUSEPORT breakage.
    init_dht(config_.peer_port);
    if (dht_node_) {
        session->set_shared_dht_node(dht_node_);
    }
    // Apply connection limits from config (otherwise PeerManager defaults are used
    // and --max-connections / --max-half-open / --max-connections-per-ip are no-ops).
    session->set_connection_limits(config_.max_connections, config_.max_connections_per_ip, config_.max_half_open);
    session->set_enable_mse(config_.enable_encryption);
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

    // Signal the shutdown fallback thread that the drain finished (it wakes
    // immediately instead of waiting out its 5s timeout).
    {
        std::lock_guard lock(shutdown_mutex_);
        run_finished_ = true;
    }
    shutdown_cv_.notify_all();

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
    // LOGDBG("Peer cache for torrent now has {} peers", cached.size());
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
        order_.push_back(info_hash);
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

void ClientApp::add_torrent_magnet(const std::string& magnet_uri,
                                    const std::filesystem::path& save_path,
                                    uint16_t port,
                                    const std::vector<std::byte>& info_bencoded) {
    PeerId peer_id = generate_id(PEER_ID_PREFIX);
    auto session = TorrentSession::create_from_magnet_with_metadata(
        io_context_, peer_id, magnet_uri, save_path, port, Mode::Hybrid,
        info_bencoded, config_.upload_rate_limit, config_.download_rate_limit
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
        order_.push_back(info_hash);
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
    LOGINFO("Restored magnet torrent with metadata: {} on port {}", session->get_display_name(), port);
}

std::shared_ptr<TorrentSession> ClientApp::torrent_by_index(size_t index) const {
    std::lock_guard lock(torrents_mutex_);
    if (index >= order_.size()) return nullptr;
    auto it = torrents_.find(order_[index]);
    if (it == torrents_.end()) return nullptr;
    return it->second;
}

void ClientApp::stop_torrent(size_t index) {
    auto session = torrent_by_index(index);
    if (!session) {
        LOGWARN("No torrent at index {}", index);
        return;
    }
    LOGINFO("Stopping torrent at index {}: {}", index, session->get_display_name());
    // Mark stopped in metadata so it persists across restarts
    {
        std::lock_guard lock(torrents_mutex_);
        for (auto& [hash, entry] : torrent_meta_) {
            if (torrents_.find(hash) != torrents_.end() &&
                torrents_.at(hash) == session) {
                entry.stopped = true;
                break;
            }
        }
    }
    asio::co_spawn(io_context_, session->stop(), asio::detached);
}

void ClientApp::resume_torrent(size_t index) {
    InfoHash hash{};
    TorrentEntry entry_copy;
    std::vector<std::byte> old_info;
    uint64_t old_downloaded = 0, old_uploaded = 0;
    {
        std::lock_guard lock(torrents_mutex_);
        if (index >= order_.size()) {
            LOGWARN("No torrent at index {}", index);
            return;
        }
        hash = order_[index];
        auto meta_it = torrent_meta_.find(hash);
        if (meta_it == torrent_meta_.end()) {
            LOGWARN("No torrent metadata at index {}", index);
            return;
        }
        entry_copy = meta_it->second;
        // If not actually stopped (e.g. already running), nothing to do
        auto session_it = torrents_.find(hash);
        if (session_it != torrents_.end()) {
            if (!session_it->second->is_stopped()) {
                LOGINFO("Torrent {} is already running.", entry_copy.source);
                return;
            }
            // Capture metadata and stats from the old stopped session
            // before erasing it.
            auto st = session_it->second->get_state();
            if (st) {
                old_downloaded = st->total_bytes_downloaded();
                old_uploaded = st->total_bytes_uploaded();
                if (st->has_metadata()) {
                    old_info = st->info().get_info_bencoded();
                }
            }
        }
    }
    LOGINFO("Resuming torrent at index {}: {}", index, entry_copy.source);
    // Remove old stopped session and re-create
    {
        std::lock_guard lock(torrents_mutex_);
        torrents_.erase(hash);
        std::erase(order_, hash);
    }
    try {
        if (entry_copy.is_magnet) {
            // If we have cached metadata from the stopped session, pass it
            // so the new session has the name and piece layout immediately.
            if (!old_info.empty()) {
                add_torrent_magnet(entry_copy.source, entry_copy.save_path,
                                   config_.peer_port, old_info);
            } else {
                add_torrent_magnet(entry_copy.source, entry_copy.save_path,
                                   config_.peer_port);
            }
        } else {
            add_torrent(Mode::Hybrid, entry_copy.source, entry_copy.save_path,
                        config_.peer_port);
        }
        // Seed the new session with the old DL/UL byte counts so the TUI
        // shows accumulated totals immediately (not 0).
        if (old_downloaded > 0 || old_uploaded > 0) {
            std::lock_guard lock(torrents_mutex_);
            auto session_it = torrents_.find(hash);
            if (session_it != torrents_.end()) {
                auto st = session_it->second->get_state();
                if (st) {
                    st->add_total_bytes_downloaded(old_downloaded);
                    st->add_total_bytes_uploaded(old_uploaded);
                }
            }
        }
        // Clear the stopped flag
        {
            std::lock_guard lock(torrents_mutex_);
            auto meta_it = torrent_meta_.find(hash);
            if (meta_it != torrent_meta_.end()) {
                meta_it->second.stopped = false;
            }
        }
    } catch (const std::exception& e) {
        LOGERR("Failed to resume torrent '{}': {}", entry_copy.source, e.what());
        // Restore stopped entry state
        std::lock_guard lock(torrents_mutex_);
        auto meta_it = torrent_meta_.find(hash);
        if (meta_it != torrent_meta_.end()) {
            meta_it->second.stopped = true;
        }
    }
}

void ClientApp::remove_torrent(size_t index) {
    std::shared_ptr<TorrentSession> session;
    InfoHash hash{};
    {
        std::lock_guard lock(torrents_mutex_);
        if (index >= order_.size()) {
            LOGWARN("No torrent at index {}", index);
            return;
        }
        hash = order_[index];
        auto it = torrents_.find(hash);
        if (it == torrents_.end()) {
            LOGWARN("No torrent at index {}", index);
            return;
        }
        session = std::move(it->second);
        torrents_.erase(it);
        torrent_meta_.erase(hash);
        std::erase(order_, hash);
    }
    LOGINFO("Removed torrent: {} (will stop async)", session->get_display_name());
    // Keep the session alive until stop() completes by chaining a
    // completion handler that holds the shared_ptr.  This avoids a
    // use-after-free when the lazy awaitable (initial_suspend = suspend_always)
    // hasn't yet been pumped by the io_context.
    asio::co_spawn(
        io_context_,
        session->stop(),
        [session](std::exception_ptr) mutable {
            session.reset();
        });
}

void ClientApp::apply_global_trackers(std::shared_ptr<TorrentSession> session) {
    std::lock_guard lock(torrents_mutex_);
    for (const auto& url : global_trackers_) {
        session->add_tracker_url_direct(url);
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
    for (const auto& hash : order_) {
        auto meta_it = torrent_meta_.find(hash);
        if (meta_it == torrent_meta_.end()) continue;
        const auto& entry = meta_it->second;
        Dict entry_dict;
        entry_dict["source"] = Value{entry.source};
        entry_dict["dest"] = Value{entry.save_path};
        entry_dict["type"] = Value{entry.is_magnet ? String("magnet") : String("file")};

        // Persist stopped/paused status from the metadata entry
        // (set by explicit user stop/resume commands, not shutdown).
        entry_dict["stopped"] = Value{static_cast<Integer>(entry.stopped ? 1 : 0)};

        // Persist info_hash as hex so load_state can look up the session
        // without re-parsing source.
        std::string hash_hex;
        hash_hex.reserve(hash.size() * 2);
        for (auto b : hash) {
            hash_hex += std::format("{:02x}", static_cast<unsigned>(b));
        }
        entry_dict["hash"] = Value{hash_hex};

        if (entry.is_magnet) {
            auto it = torrents_.find(hash);
            if (it == torrents_.end()) {
                continue;
            }
            auto state = it->second->get_state();
            // Always save the magnet entry, even without metadata yet.
            // On restore we'll re-initiate metadata download from peers.
            if (state && state->has_metadata()) {
                const auto& info_bencoded = state->info().get_info_bencoded();
                entry_dict["info"] = Value(String(
                    reinterpret_cast<const char*>(info_bencoded.data()),
                    info_bencoded.size()
                ));
            }
        }

        // Persist DL/UL byte counters so they survive restart
        {
            auto sit = torrents_.find(hash);
            if (sit != torrents_.end()) {
                auto st = sit->second->get_state();
                if (st) {
                    entry_dict["downloaded"] =
                        Value{static_cast<Integer>(st->total_bytes_downloaded())};
                    entry_dict["uploaded"] =
                        Value{static_cast<Integer>(st->total_bytes_uploaded())};
                }
            }
        }

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

        // Read persisted info_hash (optional — absent in legacy state files)
        InfoHash hash{};
        bool have_hash = false;
        auto hash_it = entry.find("hash");
        if (hash_it != entry.end()) {
            const auto* hash_str = std::get_if<String>(&hash_it->second.get_variant());
            if (hash_str && hash_str->size() == HASH_SIZE * 2) {
                hash = decode_hex_info_hash(*hash_str);
                have_hash = true;
            }
        }

        // Read persisted DL/UL byte counters (optional — absent in legacy)
        uint64_t saved_dl = 0, saved_ul = 0;
        auto dl_it = entry.find("downloaded");
        if (dl_it != entry.end()) {
            try { saved_dl = static_cast<uint64_t>(std::get<Integer>(dl_it->second.get_variant())); } catch (...) {}
        }
        auto ul_it = entry.find("uploaded");
        if (ul_it != entry.end()) {
            try { saved_ul = static_cast<uint64_t>(std::get<Integer>(ul_it->second.get_variant())); } catch (...) {}
        }

        // Read persisted stopped status (default to false for legacy state)
        bool was_stopped = false;
        auto stopped_it = entry.find("stopped");
        if (stopped_it != entry.end()) {
            was_stopped = std::get<Integer>(stopped_it->second.get_variant()) != 0;
        }

        try {
            if (*type == "magnet") {
                auto info_it = entry.find("info");
                if (info_it != entry.end()) {
                    const auto* info = std::get_if<String>(&info_it->second.get_variant());
                    if (info && !info->empty()) {
                        std::vector<std::byte> info_bencoded(info->size());
                        std::transform(info->begin(), info->end(), info_bencoded.begin(),
                                       [](char c) { return static_cast<std::byte>(c); });
                        add_torrent_magnet(*source, *dest, port, info_bencoded);
                    } else {
                        // Metadata not yet downloaded; re-initiate from magnet URI
                        add_torrent_magnet(*source, *dest, port);
                    }
                } else {
                    // No metadata stored yet; re-initiate metadata download
                    add_torrent_magnet(*source, *dest, port);
                }
            } else {
                add_torrent(Mode::Hybrid, *source, *dest, port);
            }

            // Restore DL/UL byte counters into the new session so the TUI
            // shows accumulated totals from the start.
            if ((saved_dl > 0 || saved_ul > 0) && have_hash) {
                std::lock_guard lock(torrents_mutex_);
                auto sit = torrents_.find(hash);
                if (sit != torrents_.end()) {
                    auto st = sit->second->get_state();
                    if (st) {
                        st->add_total_bytes_downloaded(saved_dl);
                        st->add_total_bytes_uploaded(saved_ul);
                    }
                }
            }

            // If this torrent was stopped before shutdown, spawn a deferred
            // stop after the io_context starts.  Use the saved hash when
            // available (robust) and fall back to the last map element for
            // legacy state files.
            if (was_stopped) {
                asio::post(io_context_, [this, hash, have_hash] {
                    std::lock_guard lock(torrents_mutex_);
                    InfoHash target = hash;
                    if (!have_hash) {
                        if (torrents_.empty()) return;
                        target = std::prev(torrents_.end())->first;
                    }
                    auto it = torrents_.find(target);
                    if (it == torrents_.end()) return;
                    asio::co_spawn(io_context_, it->second->stop(), asio::detached);
                    auto meta = torrent_meta_.find(target);
                    if (meta != torrent_meta_.end()) {
                        meta->second.stopped = true;
                    }
                });
            }
        } catch (const std::exception& e) {
            LOGWARN("Failed to restore torrent '{}': {}", *source, e.what());
        }
    }
    LOGINFO("Loaded client state from {} ({} torrent(s))", path.string(), torrent_meta_.size());
}
