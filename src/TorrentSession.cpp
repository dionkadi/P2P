#include "TorrentSession.hpp"
#include "MagnetUri.hpp"
#include "Utils.hpp"

#include <algorithm>
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <ifaddrs.h>
#include <limits>

// Returns true if `ip` is an address of one of this host's network
// interfaces (lo, eth, docker bridge, ...). Used to skip tracker-echoed
// self-announcements: our own address + listening port is us, and
// connecting to ourselves wastes half-open slots on the handshake ->
// self-drop path.
static bool is_local_interface_address(const std::string& ip) {
    ifaddrs* ifaddr = nullptr;
    if (::getifaddrs(&ifaddr) != 0) {
        return false;
    }
    bool found = false;
    for (ifaddrs* ifa = ifaddr; ifa != nullptr; ifa = ifa->ifa_next) {
        if (ifa->ifa_addr == nullptr || ifa->ifa_addr->sa_family != AF_INET) {
            continue;
        }
        auto* sin = reinterpret_cast<sockaddr_in*>(ifa->ifa_addr);
        char buf[INET_ADDRSTRLEN] = {};
        if (::inet_ntop(AF_INET, &sin->sin_addr, buf, sizeof(buf)) != nullptr &&
            ip == buf) {
            found = true;
            break;
        }
    }
    ::freeifaddrs(ifaddr);
    return found;
}

TorrentSession::TorrentSession(
    asio::io_context& io_context, 
    PeerId my_peer_id, 
    const std::filesystem::path& torrent_path, 
    const std::filesystem::path& save_path, 
    int peer_port, 
    Mode mode,
    uint64_t upload_rate_bps, 
    uint64_t download_rate_bps 
) : io_context_(io_context), strand_(asio::make_strand(io_context)), 
    save_timer_(io_context_), tracker_announce_timer_(io_context_),
    discovered_peers_timer_(io_context_),
    my_peer_id_(std::move(my_peer_id)), peer_port_(peer_port), mode_(mode),
    state_(std::make_shared<SessionState>(torrent_path, save_path)),
    piece_manager_(std::make_shared<PieceManager>(io_context, state_)),
    peer_manager_(std::make_shared<PeerManager>(io_context, state_)),
    file_manager_(std::make_unique<FileManager>(state_)),
    dht_node_(nullptr),
    lsd_discovery_(std::make_shared<LsdDiscovery>(io_context, peer_port, peer_manager_, state_)),
    dht_announce_timer_(io_context),
    dht_bootstrap_nodes_({
        "router.bittorrent.com:6881",
        "dht.libtorrent.org:25401",
        "dht.transmissionbt.com:6881"
    }),
    metadata_retry_timer_(io_context),
    upload_limiter_(io_context, upload_rate_bps),
    download_limiter_(io_context, download_rate_bps),
    completion_timer_(io_context),
    tracker_announce_interval_(std::chrono::seconds(1800))
{
    for (const auto& tier : state_->tracker_tiers()) {
        // Plain loop, not a view pipeline: std::views::transform+filter can
        // evaluate the transform more than once per element (find_if in
        // begin(), then the vector ctor re-derefs), which doubles the
        // is_duplicate_tracker_url side effect and pushed nullptr clients.
        std::vector<std::shared_ptr<ITrackerClient>> client_tier;
        for (const auto& url : tier) {
            if (is_duplicate_tracker_url(url)) {
                LOGDBG("Duplicate tracker URL '{}' skipped.", url);
                continue;
            }
            try {
                auto client = create_tracker_client(io_context, url);
                if (client) {
                    client_tier.push_back(std::move(client));
                }
            } catch (const std::exception& e) {
                LOGWARN("Failed to create tracker client for URL '{}': {}", url, e.what());
            }
        }
        if (!client_tier.empty()) {
            tracker_clients_by_tier_.push_back(std::move(client_tier));
        }
    }

    if (tracker_clients_by_tier_.empty()) {
        throw std::runtime_error("No valid tracker clients could be created from the torrent file.");
    }

    completion_timer_.expires_at(asio::steady_timer::time_point::max());
}

// Internal constructor: create from a pre-built SessionState (magnet links)
TorrentSession::TorrentSession(
    asio::io_context& io_context,
    PeerId my_peer_id,
    std::shared_ptr<SessionState> state,
    int peer_port,
    Mode mode,
    uint64_t upload_rate_bps,
    uint64_t download_rate_bps
) : io_context_(io_context), strand_(asio::make_strand(io_context)),
    save_timer_(io_context_), tracker_announce_timer_(io_context_),
    discovered_peers_timer_(io_context_),
    my_peer_id_(std::move(my_peer_id)), peer_port_(peer_port), mode_(mode),
    state_(std::move(state)),
    piece_manager_(std::make_shared<PieceManager>(io_context, state_)),
    peer_manager_(std::make_shared<PeerManager>(io_context, state_)),
    file_manager_(std::make_unique<FileManager>(state_)),
    dht_node_(nullptr),
    lsd_discovery_(std::make_shared<LsdDiscovery>(io_context, peer_port, peer_manager_, state_)),
    dht_announce_timer_(io_context),
    dht_bootstrap_nodes_({
        "router.bittorrent.com:6881",
        "dht.libtorrent.org:25401",
        "dht.transmissionbt.com:6881"
    }),
    metadata_retry_timer_(io_context),
    upload_limiter_(io_context, upload_rate_bps),
    download_limiter_(io_context, download_rate_bps),
    completion_timer_(io_context),
    tracker_announce_interval_(std::chrono::seconds(1800))
{
    for (const auto& tier : state_->tracker_tiers()) {
        // Plain loop: view pipelines can evaluate the transform twice per
        // element, doubling tracker client creation.
        std::vector<std::shared_ptr<ITrackerClient>> client_tier;
        for (const auto& url : tier) {
            if (is_duplicate_tracker_url(url)) {
                LOGDBG("Duplicate tracker URL '{}' skipped.", url);
                continue;
            }
            try {
                auto client = create_tracker_client(io_context, url);
                if (client) {
                    client_tier.push_back(std::move(client));
                }
            } catch (const std::exception& e) {
                LOGWARN("Failed to create tracker client for URL '{}': {}", url, e.what());
            }
        }
        if (!client_tier.empty()) {
            tracker_clients_by_tier_.push_back(std::move(client_tier));
        }
    }
    completion_timer_.expires_at(asio::steady_timer::time_point::max());
}

std::shared_ptr<TorrentSession> TorrentSession::create_from_magnet(
    asio::io_context& io_context,
    PeerId my_peer_id,
    const std::string& magnet_uri,
    const std::filesystem::path& save_path,
    int peer_port,
    Mode mode,
    uint64_t upload_rate_bps,
    uint64_t download_rate_bps
) {
    MagnetLink link = parse_magnet_uri(magnet_uri);
    std::vector<std::vector<std::string>> tracker_tiers;
    if (!link.tracker_urls.empty()) {
        tracker_tiers.push_back(link.tracker_urls);
    }

    InfoHash info_hash_arr = link.info_hash;
    auto state = std::make_shared<SessionState>(info_hash_arr, std::move(tracker_tiers), save_path);
    auto session = std::shared_ptr<TorrentSession>(new TorrentSession(
        io_context, std::move(my_peer_id), state, peer_port, mode,
        upload_rate_bps, download_rate_bps
    ));
    session->metadata_download_active_ = true;
    return session;
}

std::shared_ptr<TorrentSession> TorrentSession::create_from_magnet_with_metadata(
    asio::io_context& io_context,
    PeerId my_peer_id,
    const std::string& magnet_uri,
    const std::filesystem::path& save_path,
    int peer_port,
    Mode mode,
    const std::vector<std::byte>& info_bencoded,
    uint64_t upload_rate_bps,
    uint64_t download_rate_bps
) {
    MagnetLink link = parse_magnet_uri(magnet_uri);
    std::vector<std::vector<std::string>> tracker_tiers;
    if (!link.tracker_urls.empty()) {
        tracker_tiers.push_back(link.tracker_urls);
    }

    auto state = std::make_shared<SessionState>(link.info_hash, std::move(tracker_tiers), save_path);
    state->load_metadata(info_bencoded);
    return std::shared_ptr<TorrentSession>(new TorrentSession(
        io_context, std::move(my_peer_id), state, peer_port, mode,
        upload_rate_bps, download_rate_bps
    ));
}

asio::awaitable<bool> TorrentSession::init() {
    CTRACK_ASYNC("TorrentSession::init");
    // Magnet link mode: no metadata yet, skip file operations
    if (state_->num_pieces() == 0) {
        LOGINFO("Session started in metadata-download mode (magnet link). Waiting for metadata from peers...");
        co_return true;
    }

    if (mode_ == Mode::Seed) {
        bool success = co_await file_manager_->verify_seed_data();
        if (!success) {
            co_return false;
        }
        std::ranges::for_each(std::views::iota(0UL, state_->num_pieces()), 
                              [this](size_t i) { state_->piece_status(i, PieceStatus::Have); });
        state_->completed_pieces(state_->num_pieces());
        state_->is_download_complete(true);
        piece_manager_->build_piece_rarity();
    } else {
        if (!co_await file_manager_->preallocate_files()) {
            co_return false;
        }

        auto resume_opt = co_await file_manager_->load_resume_data();
        if (resume_opt) {
            ResumeData resume = ResumeData::deserialize(*resume_opt);
            co_await load_session_state(resume);
            if (state_->completed_pieces() == state_->num_pieces()) {
                state_->is_download_complete(true);
                LOGINFO("File is already complete and verified. Nothing to download.");
                completion_timer_.cancel();
            }
        } else {
            piece_manager_->build_piece_rarity();
        }

        // Hybrid mode: after loading resume data, verify any pieces
        // we already have on disk (marked Have by resume data).
        // Only checks pieces that were already Have — newly pre-allocated
        // files (zeros) are never verified, avoiding false CORRUPTION logs.
        if (mode_ == Mode::Hybrid && !state_->is_download_complete()) {
            co_await file_manager_->verify_pieces();
            if (state_->completed_pieces() == state_->num_pieces()) {
                state_->is_download_complete(true);
                completion_timer_.cancel();
                LOGINFO("All pieces verified on disk, nothing to download.");
            }
        }
    }

    co_return true;
}

asio::awaitable<void> TorrentSession::run() {
    CTRACK_ASYNC("TorrentSession::run");
    if (!co_await init()) {
        LOGERR("Failed to initialize");
        co_return ;
    }

    if (state_->is_download_complete() && mode_ == Mode::Leech) { 
        LOGINFO("Torrent download already complete. Exiting run.");
        co_return;
    }

    auto self = shared_from_this();
    {
        auto server = std::make_unique<AsyncServerSocket>(io_context_, peer_port_);
        if (server->is_listening()) {
            peer_server_ = std::move(server);
        } else {
            LOGWARN("Could not bind to port {} (already in use?). "
                    "This torrent will not accept incoming connections.", peer_port_);
        }
    }

    if (peer_server_) {
        auto weak_server = weak_from_this();
        asio::co_spawn(self->io_context_, [weak_server]() -> asio::awaitable<void> {
            auto self = weak_server.lock();
            if (!self) co_return;
            LOGINFO("Listening for incoming connections on port {}", self->peer_port_);
            while (!self->shutting_down_) {
                try {
                    AsyncSocket new_socket = co_await self->peer_server_->accept();
                    auto endpoint = new_socket.remote_endpoint();
                    std::string addr = endpoint.address().to_string() + ":" + std::to_string(endpoint.port());
                    asio::co_spawn(self->io_context_, self->handle_new_connection(std::move(new_socket), addr), asio::detached);
                } catch (const std::exception& e) {
                    LOGERR("Error accepting new peer connection: {}", e.what());
                }
            }
        }, asio::detached);
    }

    asio::co_spawn(strand_, tracker_announce_loop(), asio::detached);

    if (enable_dht_) {
        // Lazily create a per-session DHT node if no shared node was provided.
        if (!dht_node_) {
            dht_node_ = std::make_shared<DHTNode>(io_context_, peer_port_);
        }
        // Only start/bootstrap if we own this DHT node (not externally managed).
        if (!external_dht_node_) {
            dht_node_->start();
            LOGINFO("DHT node started on UDP port {}", peer_port_);
            auto dht = dht_node_;
            auto bootstrap_nodes = dht_bootstrap_nodes_;
            dht_bootstrap_in_progress_.store(true, std::memory_order_release);
            asio::co_spawn(io_context_, [dht, bootstrap_nodes = std::move(bootstrap_nodes)]() -> asio::awaitable<void> {
                co_await dht->bootstrap(bootstrap_nodes);
            }, [weak_self = weak_from_this()](std::exception_ptr e) {
                if (e) {
                    try { std::rethrow_exception(e); }
                    catch (const std::exception& ex) { LOGWARN("DHT bootstrap failed: {}", ex.what()); }
                }
                if (auto self = weak_self.lock()) {
                    self->dht_bootstrap_in_progress_.store(false, std::memory_order_release);
                }
            });
        }
        asio::co_spawn(strand_, dht_announce_loop(), asio::detached);
    }
    if (enable_lsd_) {
        lsd_discovery_->start();
    }
    asio::co_spawn(io_context_, peer_manager_->choke_loop(), asio::detached);
    if (mode_ != Mode::Seed) {
        auto weak_cb = weak_from_this();
        piece_manager_->set_callback([weak_cb] (size_t piece_index) 
                                             -> asio::awaitable<std::vector<std::shared_ptr<PeerConnection>>> {
                                         auto self = weak_cb.lock();
                                         if (!self) co_return std::vector<std::shared_ptr<PeerConnection>>{};
                                         co_return co_await self->peer_manager_->available_peers(piece_index);
                                     });
        piece_manager_->set_unchoked_count_callback([weak_cb]() -> size_t {
            auto self = weak_cb.lock();
            if (!self) return 0;
            return self->peer_manager_->unchoked_by_peer_count();
        });
        piece_manager_->set_block_timeout_callback([weak_cb](uint32_t piece_index, uint32_t block_index)
                                                         -> asio::awaitable<void> {
                                                     auto self = weak_cb.lock();
                                                     if (!self) co_return;
                                                     co_await self->send_cancel_for_block(piece_index, block_index, PeerId{});
                                                 });
        piece_manager_->set_peer_activity_check([weak_cb](const PeerId& pid) -> std::optional<TimePoint> {
            auto self = weak_cb.lock();
            if (!self) return std::nullopt;
            auto conn = self->peer_manager_->get_connection(pid);
            if (!conn) return std::nullopt;
            auto tp = conn->last_data_received();
            return (tp == std::chrono::steady_clock::time_point{}) ? std::nullopt : std::optional<TimePoint>(tp);
        });
        asio::co_spawn(io_context_, piece_manager_->downloader(), asio::detached);
        asio::co_spawn(strand_, periodically_save(), asio::detached);
        asio::co_spawn(strand_, peer_manager_->pex_loop(), asio::detached);
    }
    if (metadata_download_active_) {
        asio::co_spawn(strand_, metadata_retry_loop(), asio::detached);
    }

    // DEBUG: inject a peer from environment (P2P_DEBUG_PEER="ip:port") for testing.
    if (const char* dbg_peer = std::getenv("P2P_DEBUG_PEER")) {
        try {
            std::string s(dbg_peer);
            auto colon = s.rfind(':');
            auto ip = s.substr(0, colon);
            uint16_t port = static_cast<uint16_t>(std::stoi(s.substr(colon + 1)));
            peer_manager_->add_discovered_peer(EndPoint(asio::ip::make_address(ip), port));
            LOGINFO("DEBUG: injected peer {}:{}", ip, port);
        } catch (const std::exception& e) {
            LOGWARN("DEBUG: bad P2P_DEBUG_PEER value: {} ({})", dbg_peer, e.what());
        }
    }

    if (enable_dht_ || enable_lsd_) {
        asio::co_spawn(strand_, discovered_peers_loop(), asio::detached);
    }

    asio::co_spawn(io_context_, peer_manager_->ban_cleanup_loop(), asio::detached);

    try {
        co_await completion_timer_.async_wait(asio::use_awaitable);
    } catch (const boost::system::system_error& e) {
        if (e.code() == asio::error::operation_aborted) {
            LOGDBG("TorrentSession run() completion timer aborted, likely due to shutdown.");
        } else {
            LOGCRITICAL("TorrentSession run() encountered an unexpected timer error: {}", e.what());
            throw; // Re-throw other errors
        }
    }
}

asio::awaitable<void> TorrentSession::stop() {
    CTRACK_ASYNC("TorrentSession::stop");
    using namespace boost::asio::experimental::awaitable_operators;
    auto self = shared_from_this();
    (void)self;

    if (shutting_down_.exchange(true)) { // Use exchange to set flag and check if already set
        LOGDBG("Shutdown already in progress.");
        co_return; // Already shutting down
    }

    LOGINFO("Torrent session shutdown initiated.");
    if (peer_server_) {
        peer_server_->close();
    }

    if (dht_node_ && !external_dht_node_) {
        dht_node_->stop();
    }
    lsd_discovery_->stop();
    dht_announce_timer_.cancel();
    metadata_retry_timer_.cancel();

    completion_timer_.cancel();
    save_timer_.cancel();
    tracker_announce_timer_.cancel();
    discovered_peers_timer_.cancel();
    peer_manager_->cancel();
    piece_manager_->signal_shutdown();
    piece_manager_->notify_one();

    // Abort any coroutine suspended in the session rate limiters (e.g. a
    // connection's message_loop serving a block request and waiting for
    // upload/download tokens). If the io_context stops before the refill
    // timer fires — common at process teardown — such a waiter never
    // completes, pinning the connection (and through events_ the whole
    // session graph) forever: LSan reports a pure shared_ptr cycle with no
    // direct root. stop() unblocks queued awaiters with operation_aborted.
    upload_limiter_.stop();
    download_limiter_.stop();

    // Cancel any in-flight tracker announce requests so they don't keep
    // shared_from_this() alive for up to 30s (the HTTP timeout).
    // The tracker_announce_loop will then exit promptly when it checks
    // shutting_down_.
    // Snapshot the client list under the strand first: add_tracker_url()
    // mutates tracker_clients_by_tier_ via strand dispatch from other
    // threads, so iterating it directly here races vector reallocation.
    std::vector<std::shared_ptr<ITrackerClient>> clients_to_cancel;
    co_await asio::dispatch(strand_, asio::use_awaitable);
    for (auto& tier : tracker_clients_by_tier_) {
        for (auto& client : tier) {
            if (client) {
                clients_to_cancel.push_back(client);
            }
        }
    }
    for (auto& client : clients_to_cancel) {
        client->cancel();
    }

    if (mode_ != Mode::Seed) {
        LOGINFO("Shutdown initiated, saving final progress...");
        asio::steady_timer timer(io_context_);
        timer.expires_after(std::chrono::seconds(5));
        
        // Wait for either save to complete or timeout
        auto result = co_await ( save_session_state() || timer.async_wait(asio::use_awaitable));
        
        if (result.index() == 1) {
            LOGWARN("Save operation timed out, forcing shutdown");
        } else {
            LOGINFO("Final save complete.");
        }
    }
    
    LOGINFO("Closing all peer connections...");
    peer_manager_->close_all();
    peer_manager_->remove_all_connections();

    // Signal FileManager to stop its periodic flush loop so it doesn't keep
    // the io_context alive. Do this before sync flush so periodic_flush()
    // exits and releases any captured state.
    file_manager_->signal_shutdown();

    // Sync-flush remaining dirty cache blocks (safe during shutdown
    // because it does not dispatch through the io_context).
    file_manager_->sync_flush_all_dirty();

    LOGINFO("Torrent session shutdown complete.");
}

void TorrentSession::record_request_sent(size_t piece_index, uint32_t begin, uint32_t, const PeerId& peer_id) {
    auto progress = piece_manager_->in_progress_piece(piece_index);
    if (!progress) {
        return;
    }

    uint32_t block_index = begin / BLOCK_SIZE;
    std::lock_guard lock(progress->piece_mutex_);
    if (block_index >= progress->total_blocks || progress->blocks_received[block_index]) {
        return;
    }

    auto& outstanding = progress->outstanding_requests[block_index];
    if (std::find(outstanding.begin(), outstanding.end(), peer_id) == outstanding.end()) {
        outstanding.push_back(peer_id);
    }
    progress->request_times[block_index] = std::chrono::steady_clock::now();
}

asio::awaitable<void> TorrentSession::handle_new_connection(AsyncSocket socket, std::string peer_addr) {
    // LOGDBG("Incomming {}", peer_addr);
    std::shared_ptr<PeerConnection> conn = nullptr;
    try {
        conn = co_await PeerConnection::create(io_context_, std::move(socket), peer_addr, my_peer_id_, state_, shared_from_this());
    } catch (const boost::system::system_error& e) {
        LOGERR("Network error during PeerConnection::create for {}: {}", peer_addr, e.what());
        peer_manager_->release_half_open();
        peer_manager_->report_connection_failure(peer_addr);
        co_return;
    } catch (const std::exception& e) {
        LOGERR("General exception during PeerConnection::create for {}: {}", peer_addr, e.what());
        peer_manager_->release_half_open();
        peer_manager_->report_connection_failure(peer_addr);
        co_return;
    }
    if (!conn) {
        // This catches cases where PeerConnection::create explicitly returns nullptr (e.g., self-connection, info hash mismatch)
        LOGERR("PeerConnection::create returned nullptr for {}. Handshake likely failed or was dropped.", peer_addr);
        peer_manager_->release_half_open();
        peer_manager_->report_connection_failure(peer_addr);
        co_return;
    }

    conn->set_request_sent_hook([weak_self = weak_from_this()](uint32_t piece_index, uint32_t begin, uint32_t length, const PeerId& peer_id) {
        if (auto self = weak_self.lock()) {
            self->record_request_sent(piece_index, begin, length, peer_id);
        }
    });

    // Re-evaluate upload slots as soon as the peer tells us it is (not)
    // interested, instead of waiting for the next choke-loop tick. The choke
    // loop remains the single authority on who gets unchoked.
    conn->set_interest_change_hook([weak_self = weak_from_this()]() {
        if (auto self = weak_self.lock()) {
            self->peer_manager_->poke_choke_loop();
        }
    });

    // New peers start CHOKED. Unchoking every connection on arrival produced
    // a choke/unchoke oscillation: the peer got one round of unchoke here,
    // then the choke loop (4 slots, tit-for-tat) choked it 10s later, and
    // when it got re-unchoked the loop choked it again — repeatedly. The
    // choke loop is the single authority on upload slots.

    if (peer_manager_->contains_peer(conn->peer_id())) {
        if (my_peer_id_ < conn->peer_id()) {
            LOGWARN("Duplicate connection to {}. Dropping this one.", conn->peer_id());
            peer_manager_->release_half_open();
            co_return;
        } else {
            LOGWARN("Duplicate connection to {}. Closing the other one.", conn->peer_id());
            if (auto other_conn = peer_manager_->get_connection(conn->peer_id())) {
                other_conn->close();
    peer_manager_->remove_connection(conn->peer_id(), conn.get());
            }
        }
    }
    if (!peer_manager_->add_connection(conn->peer_id(), conn)) {
        LOGWARN("handle_new_connection: connection rejected by PeerManager for {} (limits)", conn->peer_addr());
        co_return;
    }

    // Fast start: when upload slots are abundant (fewer connections than
    // slots), unchoke immediately instead of waiting for the poked choke-loop
    // pass (~250ms later). The choke loop remains the authority — it would
    // unchoke this peer anyway in this situation, and re-evaluates on its
    // next pass, so there is no choke/unchoke oscillation while slots are
    // free.
    if (peer_manager_->connection_count() <= PeerManager::kUnchokeSlots && conn->am_choking()) {
        conn->am_choking(false);
        co_await conn->send_simple_message(MessageType::Unchoke);
    }

    try {
        auto [ip, port] = decode_address(peer_addr);
        auto peer_ip = asio::ip::make_address_v4(ip);
        EndPoint ep(peer_ip, port);
        peer_manager_->add_active_peer(ep);
    } catch (const std::exception& e) {
        LOGWARN("Failed to add know peer {}: {}", peer_addr, e.what());
    }

    LOGDBG("PeerConnection created and handshake finished for {}", conn->peer_addr());

    // Give the new peer an upload slot quickly: it starts choked, so without
    // this poke it would wait up to a full choke interval for its first
    // Unchoke.
    peer_manager_->poke_choke_loop();
}

asio::awaitable<void> TorrentSession::tracker_announce_loop() {
    auto weak_this = weak_from_this();
    std::string event = "started";
    bool completed_event_sent = false;

    while (true) {
        auto self = weak_this.lock();
        if (!self) co_return;

        bool is_completed = self->state_->is_download_complete();

        if (is_completed && self->mode_ == Mode::Leech) {
            LOGINFO("Download complete. Stopping tracker announcements.");
            co_return;
        }

        if (is_completed && !completed_event_sent && self->mode_ != Mode::Seed) {
            event = "completed";
            completed_event_sent = true;
        }

        co_await self->announce_tracker_for(event);
        event = "";

        if (!self->state_->is_download_complete() && self->peerless_announces_ < 10) {
            // Peers can vanish en masse (seedbox rotation, tracker churn),
            // and waiting the full tracker interval (~30 min) would leave the
            // session idle.  If we have nobody to download from, re-announce
            // quickly (bounded) so replacement peers are found in a minute
            // instead of half an hour.
            size_t unchoked_peers = 0;
            for (const auto& conn : self->peer_manager_->get_all_connections()) {
                if (!conn->peer_is_choking()) {
                    ++unchoked_peers;
                }
            }
            if (self->peer_manager_->connection_count() == 0 || unchoked_peers == 0) {
                ++self->peerless_announces_;
                self->tracker_announce_timer_.expires_after(std::chrono::seconds(60));
            } else {
                self->peerless_announces_ = 0;
                self->tracker_announce_timer_.expires_after(self->tracker_announce_interval_);
            }
        } else {
            self->peerless_announces_ = 0;
            self->tracker_announce_timer_.expires_after(self->tracker_announce_interval_);
        }
        boost::system::error_code ec;
        co_await self->tracker_announce_timer_.async_wait(asio::redirect_error(asio::use_awaitable, ec));
        if (ec == asio::error::operation_aborted) {
            if (self->shutting_down_) {
                LOGDBG("Tracker announce timer aborted during shutdown.");
                co_return;
            }
            LOGDBG("Tracker announce timer cancelled (new trackers added), restarting.");
            continue;
        }
    }
}

asio::awaitable<void> TorrentSession::announce_tracker_for(std::string event) {
    auto weak_this = weak_from_this();

    auto self = weak_this.lock();
    if (!self) co_return;

    uint64_t left = 0;
    if (self->state_->is_download_complete()) {
        left = 0; // Seeder or completed download
    } else {
        uint64_t total_size = self->state_->torrent_info().total_size;
        uint64_t downloaded = static_cast<uint64_t>(self->state_->completed_pieces()) * self->state_->torrent_info().piece_size;
        left = (downloaded >= total_size) ? 0 : (total_size - downloaded);
    }

    AnnounceRequestParams params {
        .info_hash_bytes = self->state_->info_hash(),
        .peer_id = self->my_peer_id_,
        .event = event,
        .port = self->peer_port_,
        .uploaded = self->state_->total_bytes_uploaded(),
        .downloaded = self->state_->total_bytes_downloaded(),
        .left = left,
    };

    // Snapshot all tracker clients under the strand (add_tracker_url mutates
    // tracker_clients_by_tier_ from other threads via dispatch).
    std::vector<std::shared_ptr<ITrackerClient>> trackers;
    co_await asio::dispatch(self->strand_, asio::use_awaitable);
    for (auto& tier : self->tracker_clients_by_tier_) {
        for (const auto& tracker_client : tier) {
            if (tracker_client) {
                trackers.push_back(tracker_client);
            }
        }
    }

    if (trackers.empty()) {
        LOGERR("Failed to announce '{}': no trackers configured.", event);
        co_return;
    }

    // Announce to ALL trackers in parallel instead of stopping at the first
    // success. With ~90 configured trackers the old sequential loop only ever
    // used the 4 fastest, and a single dead UDP tracker (15s+30s+60s+120s
    // backoff retries) could monopolize the whole cycle. This is what
    // qBittorrent does: fan out, merge, dedupe.
    struct AnnounceOutcome {
        bool success = false;
        int interval_seconds = 0;
        std::vector<std::string> peers;
    };
    auto outcomes = std::make_shared<std::vector<AnnounceOutcome>>(trackers.size());

    // Shared dedup state so concurrent announce responses don't spawn
    // duplicate connections for peers returned by several trackers.
    auto peers_dedup = std::make_shared<std::unordered_set<std::string>>();
    auto peers_dedup_mutex = std::make_shared<std::mutex>();

    auto announce_one = [self, weak_this, params, event, outcomes, trackers, peers_dedup, peers_dedup_mutex](size_t i) -> asio::awaitable<void> {
        const auto& url = trackers[i]->get_url();
        auto& outcome = (*outcomes)[i];

        // Check backoff for this tracker URL
        {
            std::lock_guard lock(self->tracker_backoff_mutex_);
            auto it = self->tracker_backoff_states_.find(url);
            if (it != self->tracker_backoff_states_.end()) {
                it->second.check_and_reset_if_idle();
                if (it->second.is_in_backoff()) {
                    LOGDBG("announce_tracker_for: skipping tracker {} (in backoff, attempt {})",
                           url, it->second.attempt_count_);
                    co_return;
                }
            }
        }

        try {
            LOGINFO("Announcing to tracker {} (event: '{}')...", url, event);
            auto result = co_await trackers[i]->announce(params);
            {
                std::lock_guard lock(self->tracker_backoff_mutex_);
                self->tracker_backoff_states_[url].on_success();
            }
            outcome.success = true;
            outcome.interval_seconds = result.interval_seconds;
            outcome.peers = std::move(result.peers);

            // Connect to THIS tracker's peers immediately, without waiting
            // for the rest of the fan-out. A dead UDP tracker retries with
            // 15s/30s/60s/120s backoff, and gating peer connects behind
            // wait_for_all() starved the swarm for minutes — the first peer
            // connection happened ~4min after startup while qBittorrent
            // connected within seconds of the first tracker response.
            if (event != "stopped") {
                auto peer_manager = self->peer_manager_;
                for (const auto& peer_addr : outcome.peers) {
                    std::string ip = PeerManager::extract_ip_from_addr(peer_addr);
                    // Trackers echo our own announce back as a peer (our
                    // address + listening port). Connecting to ourselves
                    // wastes half-open slots and cycles the handshake ->
                    // self-drop path. qBittorrent filters these.
                    if (is_local_interface_address(ip) &&
                        peer_addr.substr(ip.size() + 1) == std::to_string(self->peer_port_)) {
                        LOGDBG("Skipping self-echoed peer {}", peer_addr);
                        continue;
                    }
                    {
                        std::lock_guard lock(*peers_dedup_mutex);
                        if (!peers_dedup->insert(peer_addr).second) {
                            continue;  // another tracker already queued this peer
                        }
                    }
                    bool already_connected = peer_manager->contains_peer_addr(peer_addr)
                                             || peer_manager->contains_peer_ip(ip);
                    if (already_connected) {
                        continue;
                    }
                    asio::co_spawn(self->io_context_,
                        [peer_addr, weak_this, peer_manager] () -> asio::awaitable<void> {
                            auto socket = co_await peer_manager->connect_to_peer(peer_addr);
                            if (socket) {
                                if (auto self = weak_this.lock()) {
                                    co_await self->handle_new_connection(std::move(*socket), peer_addr);
                                }
                            }
                        },
                        asio::detached
                    );
                }
            }
        } catch (const std::exception& e) {
            LOGERR("Failed to announce to tracker: {}.", e.what());
            {
                std::lock_guard lock(self->tracker_backoff_mutex_);
                self->tracker_backoff_states_[url].on_failure(self->tracker_announce_interval_);
            }
        }
    };

    using deferred_t = decltype(asio::co_spawn(
        std::declval<asio::io_context&>(), announce_one(0), asio::deferred));
    std::vector<deferred_t> queries;
    queries.reserve(trackers.size());
    for (size_t i = 0; i < trackers.size(); ++i) {
        queries.push_back(asio::co_spawn(self->io_context_, announce_one(i), asio::deferred));
    }
    auto group = asio::experimental::make_parallel_group(std::move(queries));
    co_await group.async_wait(asio::experimental::wait_for_all(), asio::use_awaitable);

    // Merge successful announces: refresh the re-announce interval. Peer
    // connection spawning happens per-response above.
    int min_interval = std::numeric_limits<int>::max();
    bool announce_successful = false;
    for (const auto& outcome : *outcomes) {
        if (!outcome.success) continue;
        announce_successful = true;
        if (outcome.interval_seconds > 0) {
            min_interval = std::min(min_interval, outcome.interval_seconds);
        }
    }

    if (announce_successful) {
        if (min_interval != std::numeric_limits<int>::max()) {
            self->tracker_announce_interval_ = std::chrono::seconds(min_interval);
        }
    } else {
        LOGERR("Failed to announce '{}' to any tracker.", event);
    }
}

bool TorrentSession::is_duplicate_tracker_url(const std::string& url) {
    std::lock_guard lock(tracker_urls_mutex_);
    return !tracker_urls_.insert(url).second;
}

void TorrentSession::add_tracker_url(const std::string& url) {
    if (shutting_down_) {
        return;
    }
    std::shared_ptr<ITrackerClient> client;
    try {
        client = create_tracker_client(io_context_, url);
    } catch (const std::exception& e) {
        LOGWARN("Failed to create tracker client for URL '{}': {}", url, e.what());
        return;
    }
    if (!client) {
        LOGERR("Failed to create tracker client for URL: {}", url);
        return;
    }
    asio::dispatch(strand_, [weak_self = weak_from_this(), client = std::move(client), url]() mutable {
        auto self = weak_self.lock();
        if (!self || self->shutting_down_) {
            return;
        }
        if (self->is_duplicate_tracker_url(url)) {
            LOGDBG("Duplicate tracker URL '{}' skipped.", url);
            return;
        }
        if (self->tracker_clients_by_tier_.empty()) {
            self->tracker_clients_by_tier_.push_back({});
        }
        self->tracker_clients_by_tier_[0].push_back(std::move(client));
        self->tracker_announce_timer_.cancel();
        LOGINFO("Tracker URL added: {}", url);
    });
}

void TorrentSession::add_tracker_url_direct(const std::string& url) {
    if (shutting_down_) {
        return;
    }

    if (is_duplicate_tracker_url(url)) {
        LOGDBG("Duplicate tracker URL '{}' skipped.", url);
        return;
    }

    std::shared_ptr<ITrackerClient> client;
    try {
        client = create_tracker_client(io_context_, url);
    } catch (const std::exception& e) {
        LOGWARN("Failed to create tracker client for URL '{}': {}", url, e.what());
        return;
    }
    if (!client) {
        LOGERR("Failed to create tracker client for URL: {}", url);
        return;
    }

    if (tracker_clients_by_tier_.empty()) {
        tracker_clients_by_tier_.push_back({});
    }
    tracker_clients_by_tier_[0].push_back(std::move(client));
    LOGINFO("Tracker URL added: {}", url);
}

asio::awaitable<void> TorrentSession::discovered_peers_loop() {
    auto weak_self = weak_from_this();
    auto peer_manager = peer_manager_;

    while (!shutting_down_) {
        for (const auto& ep : peer_manager_->get_discovered_peers()) {
            std::string ip = ep.address().to_string();
            std::string addr = std::format("{}:{}", ip, ep.port());
            // Also check by IP to avoid duplicating an incoming connection that
            // has a different ephemeral source port.
            bool already_connected = peer_manager_->contains_peer_addr(addr) ||
                                     peer_manager_->contains_peer_ip(ip);
            if (!already_connected) {
                asio::co_spawn(io_context_, 
                    [addr, weak_self, peer_manager] () -> asio::awaitable<void> {
                        auto socket = co_await peer_manager->connect_to_peer(addr);
                        if (socket) {
                            if (auto self = weak_self.lock()) {
                                co_await self->handle_new_connection(std::move(*socket), addr);
                            }
                        }
                    }, 
                    asio::detached
                );
            }
        }

        discovered_peers_timer_.expires_after(1min);
        boost::system::error_code ec;
        co_await discovered_peers_timer_.async_wait(asio::redirect_error(asio::use_awaitable, ec));
        if (ec == asio::error::operation_aborted) {
            if (shutting_down_) {
                LOGDBG("Discovered peers periodic loop aborted.");
                co_return;
            }
            LOGDBG("Discovered peers timer cancelled, restarting.");
            continue;
        }
    }
}

asio::awaitable<void> TorrentSession::dht_announce_loop() {
    while (!shutting_down_) {
        const auto& info_hash_vec = state_->info_hash();
        InfoHash info_hash{};
        std::ranges::copy(info_hash_vec, info_hash.begin());

        if (mode_ == Mode::Seed || state_->is_download_complete()) {
            auto announce_start = std::chrono::steady_clock::now();
            co_await dht_node_->announce_peer(info_hash, peer_port_);
            auto announce_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - announce_start);
            LOGDBG("DHT announce_peer took {} ms", announce_ms.count());
        }

        auto dht_start = std::chrono::steady_clock::now();
        auto dht_peers = co_await dht_node_->get_peers(info_hash, 50);
        auto dht_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - dht_start);
        LOGINFO("DHT get_peers returned {} peers in {} ms", dht_peers.size(), dht_ms.count());
        for (const auto& ep : dht_peers) {
            if (ep.port() != peer_port_ || ep.address().to_string() != "127.0.0.1") {
                peer_manager_->add_discovered_peer(ep);
            }
        }

        if (dht_peers.empty()) {
            // The first lookup usually runs before the DHT routing table is
            // bootstrapped (it starts empty), so it finds nothing.  Retry
            // quickly a few times instead of sleeping 30 minutes on a useless
            // query — metadata-download (magnet) sessions depend on DHT for
            // peer discovery when trackers know nothing about the infohash.
            ++empty_dht_lookups_;
            if (dht_node_->routing_table_size() < 16 &&
                !dht_bootstrap_in_progress_.load(std::memory_order_acquire)) {
                // Routing table too small to answer anything: the initial
                // bootstrap (one ping round at startup) either timed out or
                // only reached 2 of 3 bootstrap nodes, leaving the table with
                // a handful of stale entries that get_peers keeps querying
                // into silence. Re-bootstrap to repopulate it; this is what
                // makes a dead-start DHT recover instead of going dormant.
                // Never run while another bootstrap is still in flight: a
                // duplicate round would double the ping traffic on the shared
                // io_context and slow every other session on it.
                LOGWARN("DHT routing table small ({} nodes) and no peers found; re-bootstrapping.",
                        dht_node_->routing_table_size());
                auto dht = dht_node_;
                auto bootstrap_nodes = dht_bootstrap_nodes_;
                dht_bootstrap_in_progress_.store(true, std::memory_order_release);
                asio::co_spawn(io_context_, [dht, bootstrap_nodes = std::move(bootstrap_nodes)]() -> asio::awaitable<void> {
                    co_await dht->bootstrap(bootstrap_nodes);
                }, [weak_self = weak_from_this()](std::exception_ptr e) {
                    if (e) {
                        try { std::rethrow_exception(e); }
                        catch (const std::exception& ex) { LOGWARN("DHT re-bootstrap failed: {}", ex.what()); }
                    }
                    if (auto self = weak_self.lock()) {
                        self->dht_bootstrap_in_progress_.store(false, std::memory_order_release);
                    }
                });
            }
            dht_announce_timer_.expires_after(std::chrono::seconds(30));
        } else {
            empty_dht_lookups_ = 0;
            dht_announce_timer_.expires_after(std::chrono::minutes(30));
        }
        boost::system::error_code ec;
        co_await dht_announce_timer_.async_wait(asio::redirect_error(asio::use_awaitable, ec));
        if (ec == asio::error::operation_aborted) {
            if (shutting_down_) {
                LOGDBG("DHT announce timer aborted during shutdown.");
                co_return;
            }
            LOGDBG("DHT announce timer cancelled, restarting.");
            continue;
        }
    }
}

asio::awaitable<void> TorrentSession::periodically_save() {
    while (!shutting_down_) {
        save_timer_.expires_after(std::chrono::minutes(1));
        boost::system::error_code ec;
        co_await save_timer_.async_wait(asio::redirect_error(asio::use_awaitable, ec));
        if (ec == asio::error::operation_aborted) {
            LOGDBG("Periodically save timer aborted.");
            co_return; // Likely during shutdown
        }
        co_await save_session_state();
    }
}

asio::awaitable<void> TorrentSession::save_session_state() {
    ResumeData resume_data;
    resume_data.total_uploaded = state_->total_bytes_uploaded();
    resume_data.total_downloaded = state_->total_bytes_downloaded();
    resume_data.have_bitfield = state_->get_have_bitfield_str();
    resume_data.in_progress_pieces = piece_manager_->get_in_progress_for_resume();
    resume_data.file_mtimes = co_await file_manager_->async_get_file_mtimes();

    auto resume_bytes = resume_data.serialize();
    co_await file_manager_->save_resume_data(resume_bytes);
}

asio::awaitable<void> TorrentSession::await_upload_tokens(size_t amount) {
    co_await upload_limiter_.await_tokens(amount);
}

asio::awaitable<void> TorrentSession::await_download_tokens(size_t amount) {
    co_await download_limiter_.await_tokens(amount);
}

asio::awaitable<void> TorrentSession::on_piece_block(std::shared_ptr<PeerConnection> conn, size_t piece_index, uint32_t begin, std::span<const std::byte> block_data) {
    CTRACK_ASYNC("TorrentSession::on_piece_block");
    co_await await_download_tokens(block_data.size());

    conn->last_data_received(std::chrono::steady_clock::now());

    if (state_->piece_status(piece_index) == PieceStatus::Have) {
        LOGINFO("Received block for piece {} which is already complete. Sending CANCEL.", piece_index);
        asio::co_spawn(io_context_, conn->send_cancel(piece_index, begin, block_data.size()), asio::detached);
        co_return;
    }

    auto progress = piece_manager_->in_progress_piece(piece_index);
    if (!progress) {
        LOGDBG("Received block for piece {} (offset {}) which is not currently in progress or was removed. Sending CANCEL to {}.", piece_index, begin, conn->peer_id());
        asio::co_spawn(io_context_, conn->send_cancel(piece_index, begin, block_data.size()), asio::detached);
        co_return;
    }
    uint32_t block_index = begin / BLOCK_SIZE;

    // Fast path: check duplicate and write block data under the piece mutex.
    // Hold is brief — no co_await while locked.
    bool in_endgame = state_->is_in_endgame_mode();
    bool piece_complete = false;
    {
        std::lock_guard lock(progress->piece_mutex_);
        if (block_index >= progress->blocks_received.size() || progress->blocks_received[block_index]) {
            co_return; // Invalid or Duplicate
        }

        std::ranges::copy(block_data, progress->data.begin() + begin);
        progress->blocks_received[block_index] = true;
        ++progress->received_count;

        // Block is done: clear rejection history so it never blocks a re-request.
        if (block_index < progress->rejected_by.size()) {
            progress->rejected_by[block_index].clear();
        }

        piece_complete = (progress->received_count == progress->total_blocks);
    }

    if (in_endgame) {
        co_await send_cancel_for_block(piece_index, block_index, conn->peer_id());
    }

    state_->add_total_bytes_downloaded(block_data.size());
    conn->add_bytes_downloaded(block_data.size());

    // A block landed and possibly freed a slot in the downloader window; wake
    // it so it can refill immediately rather than waiting for piece completion.
    piece_manager_->notify_one();

    // Refill THIS piece's pipeline on every delivery. resume_piece_download is
    // single-pass (it co_returns after placing requests) and is otherwise only
    // re-triggered by failures (REJECT/choke/timeout/10s idle poll), so without
    // this a piece drains its seeded blocks and then idles at 0.3 blocks/s/peer
    // until a failure event rescues it. Each delivery runs a resume pass: for
    // fully-placed pieces it's a cheap no-op (missing_indices empty); for
    // pieces with gaps (rejected/choked-out blocks) it retries placement at
    // delivery rate instead of waiting out a 4s timeout / 15s reject window.
    // The resume_task_active guard prevents pile-up.
    piece_manager_->ensure_resume_piece_download(piece_index);

    if (piece_complete) {
        auto expected_hash = std::vector<std::byte>(state_->torrent_info().pieces.begin() + piece_index * 20, state_->torrent_info().pieces.begin() + (piece_index * 20 + 20));
        // Lock just for reading progress->data during hash calculation (co_await-free)
        std::vector<std::byte> piece_data;
        {
            std::lock_guard lock(progress->piece_mutex_);
            piece_data = progress->data;
        }
        auto actual_hash = Crypto::calculate_sha1_hash_data(piece_data);
        if (actual_hash != expected_hash) {
            LOGERR("Hash mismatch for piece {}. Returning to queue.", piece_index);
            peer_manager_->report_corrupt_piece(conn->peer_addr());
            co_await piece_manager_->return_piece_to_queue(piece_index);
            co_return;
        }

        co_await file_manager_->write_piece(piece_index, piece_data);
        state_->piece_status(piece_index, PieceStatus::Have);
        state_->add_completed_pieces(1);
        piece_manager_->notify_one();

        LOGINFO("Piece {} downloaded and verified. Progress: {}", piece_index, state_->progress());
        
        co_await peer_manager_->send_have_message_to_all(piece_index);
        
        piece_manager_->remove_in_progress_piece(piece_index);

        if (state_->completed_pieces() == state_->num_pieces()) {
            state_->is_download_complete(true);
            LOGINFO("🎉 Download complete! File saved to {}", state_->save_path().string());
            co_await file_manager_->flush();
            // stop() may have begun while the flush was in flight: the pool
            // tasks complete with operation_canceled and flush_all_dirty
            // swallows that error, so flush() returns normally. The session
            // is shutting down — never run the completion path (or call
            // on_complete_) against a torn-down owner (ASan:
            // stack-use-after-return on SessionHandle).
            if (shutting_down_.load(std::memory_order_acquire)) {
                co_return;
            }
            LOGINFO("Closing all peer connections...");
            peer_manager_->close_all();
            peer_manager_->remove_all_connections();
            completion_timer_.cancel();
            if (on_complete_) {
                on_complete_();
            }
        }
    }
}

asio::awaitable<void> TorrentSession::on_block_request(std::shared_ptr<PeerConnection> conn, size_t piece_index, uint32_t begin, uint32_t length) {
    bool should_respond = false;
    if (!conn->am_choking() && conn->peer_is_interested()) {
        should_respond = true;
    } else if (!conn->am_choking() && !conn->peer_is_interested()) {
        LOGWARN("Peer {} sent REQUEST for piece {} begin {} length {} without previously signaling interest. Assuming interested for this request and updating state.", 
                conn->peer_id(), piece_index, begin, length);
        conn->peer_is_interested(true); // Update state to reflect their implied interest
        should_respond = true;
    }
 
    if (!should_respond) {
        if (conn->fast_extension_supported()) {
            LOGDBG("Sending REJECT to peer {} for piece {} begin {} length {} (choked).",
                    conn->peer_id(), piece_index, begin, length);
            co_await conn->send_reject(piece_index, begin, length);
        } else {
            LOGWARN("Ignoring REQUEST from peer {} because state is not met (am_choking: {}, peer_is_interested: {})",
                    conn->peer_id(), conn->am_choking(), conn->peer_is_interested()
            );
        }
        co_return;
    }

    co_await await_upload_tokens(length);
    // LOGDBG("TorrentSession: Peer {} requested piece_idx={}, begin={}, length={}", 
    //         conn->peer_id(), piece_index, begin, length);

    try {
        auto block = co_await file_manager_->read_block(piece_index, begin, length);
        co_await conn->send_piece(piece_index, begin, block);
        state_->add_total_bytes_uploaded(length);
        conn->add_bytes_uploaded(length);
    } catch (const std::exception& e) {
        LOGERR("TorrentSession: Error serving block request from peer {}: {}", conn->peer_id(), e.what());
        // Optionally, close the connection here if the error is critical
        // conn->close(); 
        throw; // Re-throw to propagate the error up to the message_loop's catch block
    }
}

asio::awaitable<void> TorrentSession::on_piece_rejected(std::shared_ptr<PeerConnection> conn, size_t piece_index, uint32_t begin, uint32_t length) {
    LOGDBG("Peer {} ({}) rejected our request for piece {} begin {} length {}. Re-requesting from another peer.",
            conn->peer_id(), conn->peer_addr(), piece_index, begin, length);

    auto progress = piece_manager_->in_progress_piece(piece_index);
    if (!progress) {
        co_return;
    }

    uint32_t block_index = begin / BLOCK_SIZE;
    if (block_index >= progress->total_blocks) {
        co_return;
    }

    {
        std::lock_guard lock(progress->piece_mutex_);
        auto& outstanding = progress->outstanding_requests[block_index];
        std::erase(outstanding, conn->peer_id());
        if (outstanding.empty()) {
            progress->request_times[block_index] = TimePoint{};
        }
    }

    // Blacklist this peer for this block (TTL-bounded): without this, a peer
    // that rejects once gets re-targeted on every retry — the reject ->
    // re-request -> reject loop that produced thousands of REJECTs.
    //
    // All PieceManager state (in_progress_pieces_, rng_) must be touched on
    // its own strand — pick_block_peer/record_block_rejection run there,
    // concurrently with check_block_timeouts on the same strand.
    co_await asio::dispatch(piece_manager_->strand(), asio::use_awaitable);
    piece_manager_->record_block_rejection(piece_index, block_index, conn->peer_id());

    // DEMOTE the primary peer the moment it rejects ANY block of this piece.
    // pick_block_peer_preferring_primary returns the primary for every block
    // it hasn't individually rejected, so a single reject would otherwise let
    // the resume pass re-send ALL sibling blocks back to the same peer — the
    // reject -> re-hammer loop (observed: 517 REJECTs from one peer, 476 in a
    // single second, after a 480-block whole-piece flood). Clearing primary_peer
    // makes every remaining block fall through to the random non-rejecting
    // selection, so the flood self-limits to one flush per peer.
    if (progress->primary_peer && *progress->primary_peer == conn->peer_id()) {
        std::lock_guard lock(progress->piece_mutex_);
        progress->primary_peer.reset();
        LOGINFO("Demoting primary peer {} for piece {} after reject.", conn->peer_id(), piece_index);
    }

    // Defer the replacement to the resume loop instead of re-sending here:
    // on a choke-wave, one peer REJECTs many blocks at once and the old
    // inline pick+send fired a detached re-request per block into the same
    // still-congested set — re-flooding peers that had just choked. The
    // resume loop selects one peer per missing block, prunes rejected peers,
    // runs on the strand, and paces through the pipeline gate.
    piece_manager_->ensure_resume_piece_download(piece_index);

    piece_manager_->notify_one();
}

asio::awaitable<void> TorrentSession::on_peer_has_piece(std::shared_ptr<PeerConnection> conn, size_t piece_index) {
    if (piece_index >= state_->num_pieces()) {
        co_return;
    }

    if (conn->bitfield_size() == 0) {
        conn->bitfield(std::vector<uint8_t>((state_->num_pieces() + 7) / 8, 0));
    }

    if (piece_index / 8 < conn->bitfield_size()) {
        conn->set_has_piece(piece_index);
        if (state_->piece_status(piece_index) == PieceStatus::Have) {
            piece_manager_->add_piece_availability(piece_index, 1);
            co_return ;
        }
    
        uint32_t old_rarity = piece_manager_->piece_availability(piece_index);
        piece_manager_->add_piece_availability(piece_index, 1);
        uint32_t new_rarity = piece_manager_->piece_availability(piece_index);
        piece_manager_->update_piece_rarity(piece_index, old_rarity, new_rarity);
    }

    if (state_->piece_status(piece_index) == PieceStatus::Needed && !conn->am_interested()) {
        conn->am_interested(true);
        co_await conn->send_simple_message(MessageType::Interested);
    }

    piece_manager_->notify_one();
}

asio::awaitable<void> TorrentSession::on_peer_has_all(std::shared_ptr<PeerConnection> conn) {
    size_t num_pieces = state_->num_pieces();
    if (num_pieces == 0) {
        conn->peer_has_all_hint(true);
        conn->inventory_pending_metadata(true);
        co_return;
    }

    // Initialize bitfield if not yet set (HaveAll may arrive before a bitfield message)
    if (conn->bitfield_size() == 0) {
        conn->bitfield(std::vector<uint8_t>((num_pieces + 7) / 8, 0xFF));
    }
    for (size_t i = 0; i < num_pieces; ++i) {
        conn->set_has_piece(i);
    }

    bool should_be_interested = false;
    for (size_t i = 0; i < num_pieces; ++i) {
        if (state_->piece_status(i) == PieceStatus::Have) {
            piece_manager_->add_piece_availability(i, 1);
            continue;
        }

        uint32_t old_rarity = piece_manager_->piece_availability(i);
        piece_manager_->add_piece_availability(i, 1);
        uint32_t new_rarity = piece_manager_->piece_availability(i);
        piece_manager_->update_piece_rarity(i, old_rarity, new_rarity);
        if (state_->piece_status(i) == PieceStatus::Needed) should_be_interested = true;
    }

    if (should_be_interested && !conn->am_interested()) {
        conn->am_interested(true);
        co_await conn->send_simple_message(MessageType::Interested);
    }

    conn->inventory_pending_metadata(false);
    piece_manager_->notify_one();
}

asio::awaitable<void> TorrentSession::on_peer_has_none(std::shared_ptr<PeerConnection> conn) {
    co_return;
}

asio::awaitable<void> TorrentSession::on_peer_bitfield(std::shared_ptr<PeerConnection> conn, std::span<const std::byte> bitfield) {
    size_t num_pieces = state_->num_pieces();

    // If metadata hasn't been fetched yet (magnet link), just store the bitfield
    // and defer processing. The bitfield will be reconciled when metadata arrives.
    if (num_pieces == 0) {
        conn->bitfield(std::vector<uint8_t>(
            reinterpret_cast<const uint8_t*>(bitfield.data()),
            reinterpret_cast<const uint8_t*>(bitfield.data()) + bitfield.size()
        ));
        conn->peer_has_all_hint(false);
        conn->inventory_pending_metadata(true);
        co_return;
    }

    size_t expected_bitfield_size = (num_pieces + 7) / 8;
    if (expected_bitfield_size != bitfield.size()) {
        LOGWARN("Received bitfield of incorrect size. Expected {}, got {}. Dropping connection.",
            expected_bitfield_size, bitfield.size()
        );
        peer_manager_->report_protocol_violation(conn->peer_addr());
        conn->close();
        co_return ;
    }

    conn->bitfield(std::vector<uint8_t>(reinterpret_cast<const uint8_t *>(bitfield.data()), reinterpret_cast<const uint8_t *>(bitfield.data()) + bitfield.size()));

    bool should_be_interested = false;
    for (size_t i = 0; i < num_pieces; ++i) {
        if (conn->has_piece(i)) {
            if (state_->piece_status(i) == PieceStatus::Have) {
                piece_manager_->add_piece_availability(i, 1);
                continue ;
            }
            
            uint32_t old_rarity = piece_manager_->piece_availability(i);
            piece_manager_->add_piece_availability(i, 1);
            uint32_t new_rarity = piece_manager_->piece_availability(i);
            piece_manager_->update_piece_rarity(i, old_rarity, new_rarity);
            if (state_->piece_status(i) == PieceStatus::Needed) should_be_interested = true;
        }
    }

    if (should_be_interested && !conn->am_interested()) {
        conn->am_interested(true);
        co_await conn->send_simple_message(MessageType::Interested);
    }

    conn->inventory_pending_metadata(false);
    piece_manager_->notify_one();
}

asio::awaitable<void> TorrentSession::on_choke_status_changed(std::shared_ptr<PeerConnection> conn, bool is_choking) {
    if (!is_choking) {
        if (!conn->am_interested()) {
            bool has_needed_pieces = false;
            for (size_t i = 0; i < state_->num_pieces(); ++i) {
                if ((state_->piece_status(i) == PieceStatus::Needed || state_->piece_status(i) == PieceStatus::InProgress) &&
                    conn->has_piece(i)) 
                {
                    has_needed_pieces = true;
                    break;
                }
            }
            
            if (has_needed_pieces) {
                conn->am_interested(true);
                co_await conn->send_simple_message(MessageType::Interested);
            }
        }

        piece_manager_->notify_one();
        co_return;
    }

    // Peer choked us: it will not serve our outstanding requests. Free their
    // slots immediately (instead of waiting out BLOCK_REQUEST_TIMEOUT) and
    // let the piece resume path re-request from peers that still serve us.
    // Without this, blocks sat on a refusing peer for 30s+ while the rest of
    // the pipeline stalled behind them.
    auto pieces_snapshot = piece_manager_->in_progress_pieces();
    if (!pieces_snapshot) co_return;

    // If this peer held unsent queued requests, those blocks become orphaned
    // (empty outstanding-request lists) once the queue is dropped, so every
    // in-progress piece may need a resume pass.
    bool peer_had_queued_requests = conn->has_pending_requests();

    for (const auto& [piece_idx, progress] : *pieces_snapshot) {
        bool needs_resume = peer_had_queued_requests;
        {
            std::lock_guard lock(progress->piece_mutex_);
            for (size_t block_index = 0; block_index < progress->outstanding_requests.size(); ++block_index) {
                auto& requests = progress->outstanding_requests[block_index];
                std::erase(requests, conn->peer_id());
                if (requests.empty() && !progress->blocks_received[block_index]) {
                    progress->request_times[block_index] = TimePoint{};
                    needs_resume = true;
                }
            }
        }
        if (needs_resume) {
            piece_manager_->ensure_resume_piece_download(piece_idx);
        }
    }
    co_return;
}

asio::awaitable<void> TorrentSession::on_disconnect(std::shared_ptr<PeerConnection> conn) {
    // Keep TorrentSession alive across suspension points — events_ on the
    // PeerConnection is the usual anchor, but an unhandled exception from
    // send_request (on a closed-socket peer) can destroy the message_loop
    // frame and break that chain while this coroutine is still live.
    auto self = shared_from_this();

    // During shutdown signal_shutdown() clears piece_availability_ and other
    // manager data.  Skip cleanup to avoid UAF / noexcept violations.
    if (shutting_down_.load()) co_return;
    for (size_t i = 0; i < state_->num_pieces(); ++i) {
        if (conn->has_piece(i)) {
            uint32_t old_rarity = piece_manager_->piece_availability(i);
            piece_manager_->add_piece_availability(i, -1);
            uint32_t new_rarity = piece_manager_->piece_availability(i);
            piece_manager_->update_piece_rarity(i, old_rarity, new_rarity);
        }
    }

    // Clean outstanding requests
    for (auto& [piece_index, progress] : *piece_manager_->in_progress_pieces()) {
        bool needs_resume = false;
        {
            std::lock_guard lock(progress->piece_mutex_);
            for (size_t block_index = 0; block_index < progress->outstanding_requests.size(); ++block_index) {
                auto& requests = progress->outstanding_requests[block_index];
                requests.erase(std::remove(requests.begin(), requests.end(), conn->peer_id()), requests.end());
                if (requests.empty() && !progress->blocks_received[block_index]) {
                    progress->request_times[block_index] = TimePoint{};
                    needs_resume = true;
                }
            }
            // Drop this peer from rejection history too (stale entries would
            // otherwise keep excluding a peer that merely disconnected).
            for (auto& rejected : progress->rejected_by) {
                std::erase_if(rejected, [&](const auto& entry) { return entry.first == conn->peer_id(); });
            }
        }
        if (needs_resume) {
            piece_manager_->ensure_resume_piece_download(piece_index);
        }
    }

    // Re-queue any pending (unsent) requests for other peers.
    // If a replacement peer also disconnects during send_request, catch the
    // exception and resume the piece through the normal path instead of letting
    // the exception destroy this coroutine (and the caller's message_loop).
    // Iterate a snapshot: the live queue is mutated concurrently under
    // pipeline_mutex_ by other coroutines (resumer send_request push_back,
    // Choke drop_pending_requests clear, flush pop_front, send_cancel erase),
    // so iterating the live deque across the co_awaits below is a
    // use-after-free (dangling req reference → SEGV in on_disconnect.resume).
    const auto pending = conn->pending_requests_snapshot();
    for (const auto& req : pending) {
        auto progress = piece_manager_->in_progress_piece(req.index);
        if (!progress) {
            continue;
        }
        uint32_t block_index = req.begin / BLOCK_SIZE;
        if (block_index >= progress->total_blocks || progress->blocks_received[block_index]) {
            continue;
        }
        auto available_peers = co_await peer_manager_->available_peers(req.index);
        bool replacement_found = false;
        for (const auto& peer : available_peers) {
            // Skip the disconnecting peer — its socket is already closed.
            if (peer == conn) continue;
            if (peer->peer_is_choking()) continue;
            try {
                co_await peer->send_request(req.index, req.begin, req.length);
                replacement_found = true;
                break;
            } catch (const std::exception& e) {
                LOGWARN("Failed to re-queue request to peer {}: {}",
                        peer->peer_addr(), e.what());
            }
        }
        if (!replacement_found) {
            piece_manager_->ensure_resume_piece_download(req.index);
        }
    }

    try {
        auto [ip, port] = decode_address(conn->peer_addr());
        auto peer_ip = asio::ip::make_address_v4(ip);
        EndPoint ep(peer_ip, port);
        peer_manager_->add_dropped_peer(ep);
        peer_manager_->remove_active_peer(ep);
    } catch (const std::exception& e) {
        LOGWARN("Failed to add dropped peer: {}", e.what());
    }

    peer_manager_->remove_connection(conn->peer_id());
    peer_manager_->poke_choke_loop();
    piece_manager_->notify_one();
}

asio::awaitable<void> TorrentSession::on_extended_message(std::shared_ptr<PeerConnection> conn, std::span<const std::byte> payload) {
    if (payload.empty()) {
        LOGWARN("Received empty extended message payload from peer {}. Disconnecting.", conn->peer_id());
        peer_manager_->report_protocol_violation(conn->peer_addr());
        conn->close(); // Disconnect on malformed message
        co_return;
    }

    conn->update_extension_type(0, ExtendedMessageType::Handshake);
    auto remote_id = static_cast<uint8_t>(payload[0]);
    auto message_type = conn->extension_type(remote_id);

    // LOGDBG("Received extended message type {} (ID: {}) from peer {}",
    //        static_cast<int>(message_type), remote_id, conn->peer_id());

    std::span<const std::byte> extended_payload(payload.data() + 1, payload.size() - 1);
    switch (message_type) {
        case ExtendedMessageType::Handshake: {
            LOGDBG("Received extended handshake message");

            auto decoded_payload = decode(extended_payload);
            const auto *ehs_dict = std::get_if<std::unique_ptr<Dict>>(&decoded_payload.get_variant());
            if (!ehs_dict || !ehs_dict->get()->count("m")) {
                LOGWARN("Peer {} sent extended handshake without \"m\" dictionary, skipping extension negotiation.", conn->peer_id());
                co_return;
            }

            const auto *m_dict = std::get_if<std::unique_ptr<Dict>>(&(ehs_dict->get()->at("m").get_variant()));
            if (m_dict && !m_dict->get()->empty()) {
                LOGDBG("Peer {} supports:", conn->peer_id());
                for (auto &[k, v] : **m_dict) {
                    LOGDBG("\t{}", k);
                    uint8_t index = std::get<Integer>(v.get_variant());
                    auto ext_type = to_extended_type(k);
                    if (ext_type != ExtendedMessageType::UNKNOWN) {
                        conn->update_extension_type(index, ext_type);
                    }
                }
            }

            if (ehs_dict->get()->count("v")) {
                auto version = std::get<String>(ehs_dict->get()->at("v").get_variant());
                LOGDBG("Version: {}", version);
            }

            if (m_dict) {
                for (auto &[k, v] : **m_dict) {
                    uint8_t index = std::get<Integer>(v.get_variant());
                    if (k == "ut_metadata") {
                        conn->metadata_ext_id(index);
                        LOGDBG("Peer {} supports ut_metadata at extension ID {}", conn->peer_id(), index);
                    }
                }
            }
            if (ehs_dict->get()->count("metadata_size")) {
                int32_t size = static_cast<int32_t>(std::get<Integer>(ehs_dict->get()->at("metadata_size").get_variant()));
                conn->metadata_size(size);

                bool need_metadata = metadata_download_active_ && state_->torrent_info().pieces.empty();
                if (need_metadata && conn->metadata_ext_id() != 0 && size > 0) {
                    asio::co_spawn(io_context_, request_metadata_from_peer(conn), asio::detached);
                }
            }
            break;
        }
        case ExtendedMessageType::ut_pex: {
            LOGDBG("Received PEX message");

            auto decoded_payload = decode(extended_payload);
            const auto *pex_dict = std::get_if<std::unique_ptr<Dict>>(&decoded_payload.get_variant());
            if (!pex_dict) {
                throw std::runtime_error("Invalid PEX message");
            }

            if (pex_dict->get()->count("added")) {
                std::string added = std::get<std::string>(pex_dict->get()->at("added").get_variant());
                for (size_t i = 0; i < added.size(); i += 6) {
                    if (i + 6 > added.size()) { // Bounds check for IP:Port pair
                        LOGERR("Malformed PEX 'added' payload from peer {}. Not enough bytes for an IP:Port pair. Disconnecting.", conn->peer_id());
                        peer_manager_->report_protocol_violation(conn->peer_addr());
                        conn->close(); 
                        co_return;
                    }
                    auto ip_bytes = asio::ip::address_v4::bytes_type();
                    std::copy_n(added.begin() + i, 4, ip_bytes.begin());
                    auto ip = asio::ip::make_address_v4(std::move(ip_bytes));
                    uint16_t port_net = 0;
                    std::copy_n(added.begin() + i + 4, 2, reinterpret_cast<unsigned char *>(&port_net));
                    uint16_t port = asio::detail::socket_ops::network_to_host_short(port_net);
                    EndPoint ep(ip, port);
                    // Filter out self-connection from PEX
                    if (ip == asio::ip::make_address_v4("127.0.0.1") && port == peer_port_) { // Assuming peer_port_ is this session's listening port
                        LOGDBG("PEX: Ignoring self-peer {}:{} received from {}", ip.to_string(), port, conn->peer_id());
                    } else {
                        peer_manager_->add_discovered_peer(ep);
                        LOGDBG("PEX: Discovered peer {}:{} from {}", ip.to_string(), port, conn->peer_id());
                    }
                }
            }

            if (pex_dict->get()->count("dropped")) {
                std::string dropped = std::get<std::string>(pex_dict->get()->at("dropped").get_variant());
                for (size_t i = 0; i < dropped.size(); i += 6) {
                    if (i + 6 > dropped.size()) { // Bounds check
                        LOGERR("Malformed PEX 'dropped' payload from peer {}. Not enough bytes for an IP:Port pair. Disconnecting.", conn->peer_id());
                        peer_manager_->report_protocol_violation(conn->peer_addr());
                        conn->close(); 
                        co_return;
                    }
                    auto ip_bytes = asio::ip::address_v4::bytes_type();
                    std::copy_n(dropped.begin() + i, 4, ip_bytes.begin());
                    auto ip = asio::ip::make_address_v4(std::move(ip_bytes));
                    uint16_t port_net = 0;
                    std::copy_n(dropped.begin() + i + 4, 2, reinterpret_cast<unsigned char *>(&port_net));
                    uint16_t port = asio::detail::socket_ops::network_to_host_short(port_net);
                    EndPoint ep(ip, port);
                    peer_manager_->add_dropped_peer(ep);
                    peer_manager_->remove_discovered_peer(ep);
                    LOGDBG("PEX: Dropped peer {}:{} from {}", ip.to_string(), port, conn->peer_id());
                }
            }

            break;
        }
        case ExtendedMessageType::ut_metadata: {
            if (extended_payload.empty()) {
                LOGWARN("Empty ut_metadata payload from peer {}", conn->peer_id());
                co_return;
            }

            size_t dict_end = 0;
            auto decoded_val = decode_prefix(extended_payload, dict_end);
            const auto* msg_dict = std::get_if<std::unique_ptr<Dict>>(&decoded_val.get_variant());
            if (!msg_dict) {
                LOGWARN("Invalid ut_metadata message from peer {}", conn->peer_id());
                co_return;
            }

            const Dict& msg = **msg_dict;
            Integer msg_type = 0;
            Integer piece_idx = 0;
            if (msg.count("msg_type")) msg_type = std::get<Integer>(msg.at("msg_type").get_variant());
            if (msg.count("piece")) piece_idx = std::get<Integer>(msg.at("piece").get_variant());

            if (msg_type == 0) {
                // Request from peer for a metadata piece
                if (mode_ != Mode::Seed && mode_ != Mode::Hybrid) {
                    co_return;
                }
                const auto& info_bencoded = state_->info().get_info_bencoded();
                if (info_bencoded.empty()) {
                    LOGWARN("Metadata requested but we don't have it yet");
                    co_return;
                }
                int num_pieces = (static_cast<int>(info_bencoded.size()) + METADATA_PIECE_SIZE - 1) / METADATA_PIECE_SIZE;
                if (piece_idx < 0 || piece_idx >= num_pieces) {
                    // Reject
                    Dict reject_dict;
                    reject_dict["msg_type"] = Value(static_cast<Integer>(2));
                    reject_dict["piece"] = Value(piece_idx);
                    auto reject_payload = encode(Value(std::move(reject_dict)));
                    co_await conn->send_extended_message(conn->metadata_ext_id(), reject_payload);
                    co_return;
                }

                size_t offset = static_cast<size_t>(piece_idx) * METADATA_PIECE_SIZE;
                size_t length = std::min<size_t>(METADATA_PIECE_SIZE, info_bencoded.size() - offset);
                std::span<const std::byte> piece_data(info_bencoded.data() + offset, length);

                Dict data_dict;
                data_dict["msg_type"] = Value(static_cast<Integer>(1));
                data_dict["piece"] = Value(piece_idx);
                data_dict["total_size"] = Value(static_cast<Integer>(info_bencoded.size()));
                auto dict_encoded = encode(Value(std::move(data_dict)));
                // Append raw piece data after the bencoded dict
                dict_encoded.insert(dict_encoded.end(), piece_data.begin(), piece_data.end());
                co_await conn->send_extended_message(conn->metadata_ext_id(), dict_encoded);
                LOGDBG("Sent metadata piece {} to peer {}", piece_idx, conn->peer_id());
            } else if (msg_type == 1) {
                // Data: piece of metadata received from peer
                size_t total_size = 0;
                if (msg.count("total_size")) total_size = std::get<Integer>(msg.at("total_size").get_variant());
                if (total_size == 0) {
                    LOGWARN("ut_metadata data message missing total_size from peer {}", conn->peer_id());
                    co_return;
                }

                if (dict_end == 0 || dict_end >= extended_payload.size()) {
                    LOGWARN("ut_metadata data message has no payload data from peer {}", conn->peer_id());
                    co_return;
                }
                std::span<const std::byte> raw_piece = extended_payload.subspan(dict_end);

                if (metadata_download_active_) {
                    // Thread-safe one-time buffer initialization.
                    std::call_once(metadata_buffer_init_flag_, [&] {
                        metadata_buffer_.resize(total_size);
                    });
                    size_t offset = static_cast<size_t>(piece_idx) * METADATA_PIECE_SIZE;
                    if (offset + raw_piece.size() <= metadata_buffer_.size()) {
                        std::ranges::copy(raw_piece, metadata_buffer_.begin() + offset);
                        size_t received = metadata_pieces_received_.fetch_add(1, std::memory_order_relaxed) + 1;
                        LOGDBG("Received metadata piece {}/{} from peer {}", piece_idx,
                               (total_size + METADATA_PIECE_SIZE - 1) / METADATA_PIECE_SIZE,
                               conn->peer_id());

                        size_t total_pieces = (total_size + METADATA_PIECE_SIZE - 1) / METADATA_PIECE_SIZE;
                        if (received >= total_pieces) {
                            LOGINFO("Full metadata received ({} bytes) from peer {}", total_size, conn->peer_id());
                            co_await on_metadata_complete();
                        }
                    }
                }
            } else if (msg_type == 2) {
                // Reject
                LOGDBG("Peer {} rejected metadata piece {}", conn->peer_id(), piece_idx);
            }
            break;
        }
        default: {
            LOGDBG("Ignoring unhandled extended message type {} (ID: {}) from peer {}",
                   static_cast<int>(message_type), remote_id, conn->peer_id());
            break;
        }
    }
    co_return ;
}

asio::awaitable<void> TorrentSession::load_session_state(const ResumeData& data) {
    const auto &info = state_->torrent_info();
    const size_t num_pieces = state_->num_pieces();
    auto p = file_manager_->get_resume_file_path();

    if (!std::filesystem::exists(p)) {
        LOGINFO("No downloaded data");
        co_await asio::dispatch(strand_, asio::use_awaitable);
        reset();
        co_return;
    }

    auto file_size = std::filesystem::file_size(p);
    if (file_size == 0) {
        LOGWARN("Resume file is empty, starting fresh");
        std::filesystem::remove(p);
        co_await asio::dispatch(strand_, asio::use_awaitable);
        reset();
        co_return;
    }

    LOGINFO("Loading history data...");

    try {
        std::string have_bitfield_str = data.have_bitfield;
        
        // Validate bitfield length
        size_t expected_bitfield_size = (num_pieces + 7) / 8;
        if (have_bitfield_str.size() != expected_bitfield_size) {
            throw std::runtime_error(std::format("Invalid bitfield size: expected {}, got {}", 
                expected_bitfield_size, have_bitfield_str.size()));
        }
        
        size_t pieces_done_count = 0;
        for (size_t i = 0; i < num_pieces; ++i) {
            if (i / 8 >= have_bitfield_str.size()) {
                state_->piece_status(i, PieceStatus::Needed);
                continue;
            }
            
            uint8_t byte = static_cast<uint8_t>(have_bitfield_str[i / 8]);
            uint8_t bit_position = 7 - (i % 8);
            bool has_piece = (byte & (1 << bit_position)) != 0;
            
            if (has_piece) {
                state_->piece_status(i, PieceStatus::Have);
                ++pieces_done_count;

                piece_manager_->remove_piece_rarity(i, 0);
            } else {
                // This piece is not in the 'have' bitfield.
                // Only mark it as 'Needed' if it wasn't already marked 'Skipped'
                // by the selective download logic which runs before this.
                if (state_->piece_status(i) != PieceStatus::Skipped) {
                    state_->piece_status(i, PieceStatus::Needed);
                }
            }
        }

        // Check for corrupted resume files
        if (pieces_done_count > num_pieces) {
            throw std::runtime_error(std::format("Corrupted resume file: {} pieces marked as done, but torrent only has {}", 
                pieces_done_count, num_pieces));
        }

        // Check file metadata
        if (!data.file_mtimes.empty()) {
            for (size_t file_idx = 0; file_idx < info.files.size(); ++file_idx) {
                const auto &file_info = info.files[file_idx];
                std::filesystem::path full_path = file_manager_->get_full_path_for_file(file_info);
                const std::string file_key = file_info.path.string();

                if (!data.file_mtimes.count(file_key)) {
                    LOGWARN("File metadata missing for: {}", file_key);
                    continue;
                }

                if (!std::filesystem::exists(full_path)) {
                    // Mark all pieces for this file as needed
                    const auto& file_to_piece_map = *file_manager_->get_file_to_pieces_map(file_idx);
                    for (size_t piece_idx : file_to_piece_map) {
                        if (state_->piece_status(piece_idx) == PieceStatus::Have) {
                            state_->piece_status(piece_idx, PieceStatus::Needed);
                            --pieces_done_count;
                        }
                    }
                    continue;
                }

                // Check modification time
                try {
                    auto saved_mtime = data.file_mtimes.at(file_key);
                    auto current_mtime = std::filesystem::last_write_time(full_path).time_since_epoch().count();
                    if (current_mtime != saved_mtime) {
                        LOGWARN("File modification time changed for: {}", file_info.path.string());
                        // Mark pieces as needed
                        const auto& file_to_piece_map = *file_manager_->get_file_to_pieces_map(file_idx);
                        for (size_t piece_idx : file_to_piece_map) {
                            if (state_->piece_status(piece_idx) == PieceStatus::Have) {
                                state_->piece_status(piece_idx, PieceStatus::Needed);
                                --pieces_done_count;
                            }
                        }
                    }
                } catch (const std::exception& e) {
                    LOGWARN("Could not check mtime for {}: {}", file_info.path.string(), e.what());
                }
            }
        }

        state_->completed_pieces(pieces_done_count);

        // Rebuild the rarity map
        piece_manager_->build_piece_rarity();

        // Load in-progress pieces
        if (!data.in_progress_pieces.empty()) {
            for (const auto& [piece_idx_str, block_bitfield] : data.in_progress_pieces) {
                try {
                    size_t piece_idx = std::stoul(piece_idx_str);
                    if (piece_idx >= num_pieces || state_->piece_status(piece_idx) != PieceStatus::Needed) {
                        continue; // Skip if already have or invalid index
                    }

                    if (block_bitfield.empty()) continue;

                    uint64_t piece_size;
                    if (static_cast<uint64_t>(piece_idx) == num_pieces - 1) {
                        uint64_t last_piece_size = info.total_size % info.piece_size;
                        piece_size = (last_piece_size == 0) ? info.piece_size : last_piece_size;
                    } else {
                        piece_size = info.piece_size;
                    }

                    // Restore the state
                    auto progress = std::make_shared<InProgressPiece>(piece_size);
                    for (size_t i = 0; i < progress->blocks_received.size(); ++i) {
                        if (i / 8 >= block_bitfield.size()) break;
                        
                        uint8_t byte = static_cast<uint8_t>(block_bitfield[i / 8]);
                        uint8_t bit_position = 7 - (i % 8);
                        if ((byte & (1 << bit_position)) != 0) {
                            progress->blocks_received[i] = true;
                            progress->received_count++;
                        }
                    }
                    // Don't resume if it was actually complete
                    if (progress->received_count == progress->total_blocks) continue;

                    // Update status and rarity maps
                    state_->piece_status(piece_idx, PieceStatus::InProgress);
                    uint32_t current_rarity = piece_manager_->piece_availability(piece_idx);
                    piece_manager_->update_piece_rarity(piece_idx, current_rarity, IN_PROGRESS_RARITY_GROUP_ID);
                    piece_manager_->emplace_in_progress_pieces(piece_idx, std::move(progress));

                    // Re-initiate the action by spawning a guarded resume task
                    piece_manager_->ensure_resume_piece_download(piece_idx);
                } catch (const std::exception& e) {
                    LOGWARN("Could not parse in-progress piece '{}': {}", piece_idx_str, e.what());
                }
            }
        }

        // Check if we should be in endgame mode (all pieces are either Have or InProgress)
        bool all_pieces_accounted_for = state_->needed_pieces() == state_->num_pieces();

        if (all_pieces_accounted_for && !piece_manager_->in_progress_pieces()->empty()) {
            LOGINFO("Resuming in ENDGAME MODE");
            state_->is_in_endgame_mode(true);
            // Re-broadcast outstanding requests
            asio::co_spawn(io_context_, piece_manager_->broadcast_outstanding_requests(), asio::detached);
        }

    } catch (const std::exception& e) {
        LOGERR("Failed to load resume file: {}. Starting fresh.", e.what());
        
        // Delete the corrupted resume file
        try {
            std::filesystem::remove(p);
            LOGINFO("Deleted corrupted resume file: {}", p.string());
        } catch (const std::exception& remove_e) {
            LOGWARN("Failed to delete corrupted resume file: {}", remove_e.what());
        }
        
        // Reset to initial state
        reset();
    }
    
    size_t pieces_done_count = state_->completed_pieces();
    if (pieces_done_count > 0) {
        float progress = (static_cast<float>(pieces_done_count) / num_pieces) * 100.0f;
        LOGINFO("Loading progress complete. Found {}/{} valid pieces ({:.2f}% progress).", 
                pieces_done_count, num_pieces, progress);
    } else {
        LOGINFO("No valid progress found in resume file. Starting fresh download.");
    }
}

void TorrentSession::reset() noexcept {
    std::ranges::for_each(std::views::iota(0UL, state_->num_pieces()), 
                          [this](size_t i) { state_->piece_status(i, PieceStatus::Needed); });
    state_->completed_pieces(0);
    piece_manager_->remove_all_in_progress_pieces();
}

asio::awaitable<void> TorrentSession::send_cancel_for_block(uint32_t piece_index, uint32_t block_index, const PeerId& exclude_peer_id) {
    auto in_progress_pieces = piece_manager_->in_progress_pieces();

    auto it = in_progress_pieces->find(piece_index);
    if (it == in_progress_pieces->end() || block_index >= it->second->outstanding_requests.size()) {
        co_return ;
    }

    auto progress = it->second;
    std::vector<PeerId> peer_ids_to_cancel;
    {
        std::lock_guard lock(progress->piece_mutex_);
        if (block_index >= progress->outstanding_requests.size()) {
            co_return;
        }
        peer_ids_to_cancel = progress->outstanding_requests[block_index];
        progress->outstanding_requests[block_index].clear();
        progress->request_times[block_index] = TimePoint{};
    }

    const auto& info = state_->torrent_info();
    const size_t num_pieces = state_->num_pieces();

    uint32_t offset = block_index * BLOCK_SIZE;
    uint64_t current_piece_size;
    if (static_cast<uint64_t>(piece_index) == num_pieces - 1) {
        current_piece_size = info.total_size - (static_cast<uint64_t>(piece_index) * info.piece_size);
    } else {
        current_piece_size = info.piece_size;
    }
 
    uint32_t length = (offset + BLOCK_SIZE > current_piece_size)
                    ? (current_piece_size - offset)
                    : BLOCK_SIZE;
    
    std::ranges::for_each(peer_ids_to_cancel 
                            | std::views::filter([&](const PeerId& peer_id) { return peer_id != exclude_peer_id; }),
                            [&](const PeerId& peer_id) {
                                auto conn = peer_manager_->get_connection(peer_id);
                                if (conn) {
                                    asio::co_spawn(io_context_, conn->send_cancel(piece_index, offset, length), asio::detached);
                                }
                            });
}

asio::awaitable<void> TorrentSession::request_metadata_from_peer(std::shared_ptr<PeerConnection> conn) {
    if (state_->num_pieces() > 0) {
        co_return;
    }
    uint8_t ext_id = conn->metadata_ext_id();
    if (ext_id == 0) {
        co_return;
    }

    int32_t total_size = conn->metadata_size();
    if (total_size <= 0) {
        co_return;
    }

    if (conn->metadata_requested()) {
        co_return; // A request coroutine is already running for this peer.
    }
    conn->metadata_requested(true);

    // Thread-safe one-time initialization of the metadata buffer.
    std::call_once(metadata_buffer_init_flag_, [&] {
        metadata_buffer_.resize(static_cast<size_t>(total_size));
        metadata_pieces_received_.store(0, std::memory_order_relaxed);
    });

    int total_pieces = (total_size + static_cast<int>(METADATA_PIECE_SIZE) - 1) / static_cast<int>(METADATA_PIECE_SIZE);
    for (int piece = 0; piece < total_pieces; ++piece) {
        co_await conn->send_metadata_request(ext_id, piece);
        LOGDBG("Requested metadata piece {}/{} from peer {}", piece + 1, total_pieces, conn->peer_id());
    }
}

asio::awaitable<void> TorrentSession::metadata_retry_loop() {
    while (!shutting_down_ && state_->torrent_info().pieces.empty()) {
        // The initial request is spawned when a peer's extended handshake
        // advertises metadata_size; if that coroutine dies with its peer
        // (disconnect mid-transfer), no one ever asks again.  Also catch
        // peers whose metadata_size arrived after our EHS handling.  The
        // per-connection flag prevents duplicate concurrent requests.
        for (auto& conn : peer_manager_->get_all_connections()) {
            if (conn->metadata_ext_id() != 0 && conn->metadata_size() > 0 && !conn->metadata_requested()) {
                asio::co_spawn(io_context_, request_metadata_from_peer(conn), asio::detached);
            }
        }
        metadata_retry_timer_.expires_after(std::chrono::seconds(30));
        boost::system::error_code ec;
        co_await metadata_retry_timer_.async_wait(asio::redirect_error(asio::use_awaitable, ec));
        if (ec == asio::error::operation_aborted) {
            co_return;
        }
    }
}

asio::awaitable<void> TorrentSession::on_metadata_complete() {
    try {
        state_->load_metadata(metadata_buffer_);
        const auto& info = state_->torrent_info();
        size_t num_pieces = state_->num_pieces();

        LOGINFO("Metadata received and parsed: {} ({} pieces, {} bytes)",
                info.name, num_pieces, info.total_size);

        // File manager, choker, downloader were skipped in init()
        // because metadata wasn't available yet. Start them now.
        if (mode_ == Mode::Seed) {
            co_await file_manager_->verify_seed_data();
            std::ranges::for_each(std::views::iota(0UL, num_pieces),
                                  [this](size_t i) { state_->piece_status(i, PieceStatus::Have); });
            state_->completed_pieces(num_pieces);
            state_->is_download_complete(true);
            piece_manager_->build_piece_rarity();
        } else if (mode_ == Mode::Hybrid) {
            // Combined approach: preallocate files, then verify any pieces
            // that were already on disk (marked Have during init()).
            // Pre-allocated (zeroed) files are not verified, avoiding false
            // CORRUPTION logs for new downloads.
            if (!co_await file_manager_->preallocate_files()) {
                LOGERR("Failed to preallocate files after metadata download");
                co_return;
            }
            co_await file_manager_->verify_pieces();
            if (state_->completed_pieces() == num_pieces) {
                state_->is_download_complete(true);
                LOGINFO("Metadata received: all pieces verified on disk.");
            } else {
                piece_manager_->build_piece_rarity();
                LOGINFO("Metadata received: partial data on disk, downloading remaining pieces.");
            }
        } else {
            if (!co_await file_manager_->preallocate_files()) {
                LOGERR("Failed to preallocate files after metadata download");
                co_return;
            }
            piece_manager_->build_piece_rarity();
        }

        if (mode_ != Mode::Seed) {
            size_t expected_bitfield_size = (num_pieces + 7) / 8;
            for (const auto& conn : peer_manager_->get_all_connections()) {
                if (!conn->inventory_pending_metadata()) {
                    continue;
                }

                if (conn->peer_has_all_hint()) {
                    co_await on_peer_has_all(conn);
                    continue;
                }

                auto peer_bitfield = conn->bitfield_copy();
                if (peer_bitfield.empty()) {
                    conn->inventory_pending_metadata(false);
                    continue;
                }

                if (peer_bitfield.size() != expected_bitfield_size) {
                    LOGWARN("Stored peer bitfield has incorrect size after metadata. Expected {}, got {}. Dropping connection.",
                            expected_bitfield_size, peer_bitfield.size());
                    peer_manager_->report_protocol_violation(conn->peer_addr());
                    conn->close();
                    continue;
                }

                auto peer_bitfield_span = std::span<const uint8_t>(peer_bitfield.data(), peer_bitfield.size());
                co_await on_peer_bitfield(conn, std::as_bytes(peer_bitfield_span));
            }
        }

        // All loops and callbacks are already set up in run() — no duplicate spawns needed.
        // Notably, a duplicate discovered_peers_loop would share the same timer with the
        // running instance, causing both to cancel each other in an infinite ping-pong → OOM.

        metadata_download_active_ = false;
        metadata_buffer_.clear();
        metadata_pieces_received_.store(0, std::memory_order_relaxed);

    } catch (const std::exception& e) {
        LOGERR("Failed to parse received metadata: {}", e.what());
        metadata_download_active_ = false;
        metadata_buffer_.clear();
        metadata_pieces_received_.store(0, std::memory_order_relaxed);
    }
}
