#include "TorrentSession.hpp"
#include "MagnetUri.hpp"
#include "Utils.hpp"

#include <algorithm>
#include <boost/asio/experimental/awaitable_operators.hpp>

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
    dht_node_(std::make_shared<DHTNode>(io_context, peer_port)),
    dht_announce_timer_(io_context),
    dht_bootstrap_nodes_({
        "router.bittorrent.com:6881",
        "dht.libtorrent.org:25401",
        "dht.transmissionbt.com:6881"
    }),
    upload_limiter_(io_context, upload_rate_bps),
    download_limiter_(io_context, download_rate_bps),
    completion_timer_(io_context) 
{
    for (const auto& tier : state_->tracker_tiers()) {
        auto client_tier_view = tier
                                | std::views::transform([&](const std::string& url) -> std::shared_ptr<ITrackerClient> {
                                    try {
                                        return create_tracker_client(io_context, url);
                                    } catch (const std::exception& e) {
                                        LOGWARN("Failed to create tracker client for URL '{}': {}", url, e.what());
                                        return nullptr; // Return nullptr on failure
                                    }
                                })
                                | std::views::filter([](const std::shared_ptr<ITrackerClient>& client) {
                                    return client != nullptr; // Filter out failed clients
                                });
        std::vector<std::shared_ptr<ITrackerClient>> client_tier(client_tier_view.begin(), client_tier_view.end());
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
    dht_node_(std::make_shared<DHTNode>(io_context, peer_port)),
    dht_announce_timer_(io_context),
    dht_bootstrap_nodes_({
        "router.bittorrent.com:6881",
        "dht.libtorrent.org:25401",
        "dht.transmissionbt.com:6881"
    }),
    upload_limiter_(io_context, upload_rate_bps),
    download_limiter_(io_context, download_rate_bps),
    completion_timer_(io_context)
{
    for (const auto& tier : state_->tracker_tiers()) {
        auto client_tier_view = tier
                                | std::views::transform([&](const std::string& url) -> std::shared_ptr<ITrackerClient> {
                                    try {
                                        return create_tracker_client(io_context, url);
                                    } catch (const std::exception& e) {
                                        LOGWARN("Failed to create tracker client for URL '{}': {}", url, e.what());
                                        return nullptr;
                                    }
                                })
                                | std::views::filter([](const std::shared_ptr<ITrackerClient>& client) {
                                    return client != nullptr;
                                });
        std::vector<std::shared_ptr<ITrackerClient>> client_tier(client_tier_view.begin(), client_tier_view.end());
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

asio::awaitable<bool> TorrentSession::init() {
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
        co_await piece_manager_->build_piece_rarity();
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
            co_await piece_manager_->build_piece_rarity();
        }
    }

    co_return true;
}

asio::awaitable<void> TorrentSession::run() {
    if (!co_await init()) {
        LOGERR("Failed to initialize");
        co_return ;
    }

    if (state_->is_download_complete() && mode_ == Mode::Leech) { 
        LOGINFO("Torrent download already complete. Exiting run.");
        co_return;
    }

    peer_server_ = std::make_unique<AsyncServerSocket>(io_context_, peer_port_);
    asio::co_spawn(io_context_, [this]() -> asio::awaitable<void> {
        LOGINFO("Listening for incoming connections on port {}", peer_port_);
        while (!shutting_down_) {
            try {
                AsyncSocket new_socket = co_await peer_server_->accept();
                auto endpoint = new_socket.remote_endpoint();
                std::string addr = endpoint.address().to_string() + ":" + std::to_string(endpoint.port());
                asio::co_spawn(io_context_, handle_new_connection(std::move(new_socket), addr), asio::detached);
            } catch (const std::exception& e) {
                LOGERR("Error accepting new peer connection: {}", e.what());
            }
        }
    }, asio::detached);

    asio::co_spawn(strand_, tracker_announce_loop(), asio::detached);

    dht_node_->start();
    LOGINFO("DHT node started on UDP port {}", peer_port_);
    asio::co_spawn(io_context_, [self = shared_from_this()]() -> asio::awaitable<void> {
        co_await self->dht_node_->bootstrap(self->dht_bootstrap_nodes_);
    }, asio::detached);
    asio::co_spawn(strand_, dht_announce_loop(), asio::detached);
    
    if (mode_ == Mode::Leech) {
        asio::co_spawn(io_context_, peer_manager_->choke_loop(), asio::detached);
        piece_manager_->set_callback([this] (size_t piece_index) 
                                            -> asio::awaitable<std::vector<std::shared_ptr<PeerConnection>>> 
                                        { co_return co_await peer_manager_->available_peers(piece_index); }
                                    );
        asio::co_spawn(io_context_, piece_manager_->downloader(), asio::detached);
        asio::co_spawn(strand_, periodically_save(), asio::detached);
        asio::co_spawn(strand_, peer_manager_->pex_loop(), asio::detached);
        asio::co_spawn(strand_, discovered_peers_loop(), asio::detached);
    }

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
    using namespace boost::asio::experimental::awaitable_operators;

    if (shutting_down_.exchange(true)) { // Use exchange to set flag and check if already set
        LOGDBG("Shutdown already in progress.");
        co_return; // Already shutting down
    }

    LOGINFO("Torrent session shutdown initiated.");
    if (peer_server_) {
        peer_server_->close();
    }

    dht_node_->stop();
    dht_announce_timer_.cancel();

    completion_timer_.cancel();
    save_timer_.cancel();
    tracker_announce_timer_.cancel();
    discovered_peers_timer_.cancel();
    peer_manager_->cancel();
    piece_manager_->notify_one();

    asio::steady_timer announce_timeout_timer(io_context_);
    announce_timeout_timer.expires_after(std::chrono::seconds(2));
    auto result_announce = co_await (asio::co_spawn(strand_, announce_tracker_for("stopped"), asio::use_awaitable) || announce_timeout_timer.async_wait(asio::use_awaitable));
    
    if (result_announce.index() == 1) { // timeout
        LOGWARN("Sending 'stopped' event to tracker timed out during shutdown.");
    }

    if (mode_ == Mode::Leech) {
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

    // if (!io_context_.stopped()) {
    //     io_context_.stop();
    //     LOGINFO("io_context stopped by TorrentSession::stop().");
    // }
}

asio::awaitable<void> TorrentSession::handle_new_connection(AsyncSocket socket, std::string peer_addr) {
    // LOGDBG("Incomming {}", peer_addr);
    std::shared_ptr<PeerConnection> conn = nullptr;
    try {
        conn = co_await PeerConnection::create(io_context_, std::move(socket), peer_addr, my_peer_id_, state_, shared_from_this());
    } catch (const boost::system::system_error& e) {
        LOGERR("Network error during PeerConnection::create for {}: {}", peer_addr, e.what());
        co_return;
    } catch (const std::exception& e) {
        LOGERR("General exception during PeerConnection::create for {}: {}", peer_addr, e.what());
        co_return;
    }
    if (!conn) {
        // This catches cases where PeerConnection::create explicitly returns nullptr (e.g., self-connection, info hash mismatch)
        LOGERR("PeerConnection::create returned nullptr for {}. Handshake likely failed or was dropped.", peer_addr);
        co_return;
    }

    asio::co_spawn(io_context_, [conn] () -> asio::awaitable<void> {
        co_await conn->send_simple_message(MessageType::Unchoke);
        conn->am_choking(false);
    }, asio::detached);

    if (peer_manager_->contains_peer(conn->peer_id())) {
        if (my_peer_id_ < conn->peer_id()) {
            LOGWARN("Duplicate connection to {}. Dropping this one.", conn->peer_id());
            co_return;
        } else {
            LOGWARN("Duplicate connection to {}. Closing the other one.", conn->peer_id());
            if (auto other_conn = peer_manager_->get_connection(conn->peer_id())) {
                other_conn->close();
                peer_manager_->remove_connection(conn->peer_id());
            }
        }
    }
    peer_manager_->add_connection(conn->peer_id(), conn);

    try {
        auto [ip, port] = decode_address(peer_addr);
        auto peer_ip = asio::ip::make_address_v4(ip);
        EndPoint ep(peer_ip, port);
        peer_manager_->add_active_peer(ep);
    } catch (const std::exception& e) {
        LOGWARN("Failed to add know peer {}: {}", peer_addr, e.what());
    }

    std::vector<uint8_t> bitfield((state_->num_pieces() + 7) / 8, 0);
    std::string have_bitfield_str = state_->get_have_bitfield_str();
    std::ranges::copy(have_bitfield_str, bitfield.begin());
    asio::co_spawn(io_context_, conn->send_bitfield(bitfield), asio::detached);

    LOGDBG("PeerConnection created and handshake finished for {}", conn->peer_addr());
}

asio::awaitable<void> TorrentSession::tracker_announce_loop() {
    std::string event = "started";
    bool completed_event_sent = false;
    std::random_device rd;
    std::mt19937 g(rd());

    while (true) {
        bool is_completed = state_->is_download_complete();

        if (is_completed && mode_ != Mode::Seed) {
            LOGINFO("Download complete. Stopping tracker announcements.");
            co_return;
        }

        if (is_completed && !completed_event_sent && mode_ != Mode::Seed) {
            event = "completed";
            completed_event_sent = true;
        }

        co_await announce_tracker_for(event);
        event = "";
 
        tracker_announce_timer_.expires_after(tracker_announce_interval_);
        boost::system::error_code ec;
        co_await tracker_announce_timer_.async_wait(asio::redirect_error(asio::use_awaitable, ec)); // Wait on timer
        if (ec == asio::error::operation_aborted) {
            LOGDBG("Tracker announce timer aborted.");
            co_return; // Likely during shutdown
        }
    }
}

asio::awaitable<void> TorrentSession::announce_tracker_for(std::string event) {
    AnnounceRequestParams params {
        .info_hash_bytes = state_->info_hash(),
        .peer_id = my_peer_id_,
        .event = event,
        .port = peer_port_,
        .uploaded = state_->total_bytes_uploaded(),
        .downloaded = state_->total_bytes_downloaded(),
        .left = state_->torrent_info().total_size - (state_->completed_pieces() * state_->torrent_info().piece_size), // Adjust for last piece
    };

    bool announce_successful = false;
    co_await asio::dispatch(strand_, asio::use_awaitable);
    for (auto& tier : tracker_clients_by_tier_) {
        for (const auto& tracker_client : tier) {
            try {
                LOGINFO("Announcing to tracker {} (event: '{}')...", tracker_client->get_url(), event);
                auto result = co_await tracker_client->announce(params);
                announce_successful = true;
                
                if (event == "started") {
                    tracker_announce_interval_ = std::chrono::seconds(result.interval_seconds);
                    for (const auto& peer_addr : result.peers) {
                        bool already_connected = peer_manager_->contains_peer_addr(peer_addr);
                        if (!already_connected) {
                            asio::co_spawn(io_context_, 
                                [peer_addr, this] () -> asio::awaitable<void> {
                                    auto socket = co_await peer_manager_->connect_to_peer(peer_addr);
                                    if (socket) {
                                        co_await handle_new_connection(std::move(*socket), peer_addr);
                                    }
                                }, 
                                asio::detached
                            );
                        }
                    }
                }

                break;
            } catch (const std::exception& e) {
                LOGERR("Failed to announce to tracker: {}.", e.what());
            }
        }

        if (announce_successful) {
            break ;
        }
    }

    if (!announce_successful) {
        LOGERR("Failed to announce '{}' to any tracker.", event);
    }
}

asio::awaitable<void> TorrentSession::discovered_peers_loop() {
    auto self = shared_from_this();

    while (!shutting_down_) {
        for (const auto& ep : peer_manager_->get_discovered_peers()) {
            std::string addr = std::format("{}:{}", ep.address().to_string(), ep.port());
            bool already_connected = peer_manager_->contains_peer_addr(addr);
            if (!already_connected) {
                asio::co_spawn(io_context_, 
                    [addr, this] () -> asio::awaitable<void> {
                        auto socket = co_await peer_manager_->connect_to_peer(addr);
                        if (socket) {
                            co_await handle_new_connection(std::move(*socket), addr);
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
            LOGDBG("Discovered peers periodic loop aborted.");
            co_return;
        }
    }
}

asio::awaitable<void> TorrentSession::dht_announce_loop() {
    while (!shutting_down_) {
        const auto& info_hash_vec = state_->info_hash();
        InfoHash info_hash{};
        std::ranges::copy(info_hash_vec, info_hash.begin());

        if (mode_ == Mode::Seed || state_->is_download_complete()) {
            co_await dht_node_->announce_peer(info_hash, peer_port_);
        }

        auto dht_peers = co_await dht_node_->get_peers(info_hash, 50);
        for (const auto& ep : dht_peers) {
            if (ep.port() != peer_port_ || ep.address().to_string() != "127.0.0.1") {
                peer_manager_->add_discovered_peer(ep);
            }
        }

        dht_announce_timer_.expires_after(std::chrono::minutes(30));
        boost::system::error_code ec;
        co_await dht_announce_timer_.async_wait(asio::redirect_error(asio::use_awaitable, ec));
        if (ec == asio::error::operation_aborted) {
            LOGDBG("DHT announce timer aborted.");
            co_return;
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
    auto self = shared_from_this();
    
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
    co_await await_download_tokens(block_data.size());

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
    if (block_index >= progress->blocks_received.size() || progress->blocks_received[block_index]) {
        co_return; // Invalid or Duplicate
    }

    if (state_->is_in_endgame_mode()) {
        co_await send_cancel_for_block(piece_index, block_index, conn->peer_id());
    }

    std::ranges::copy(block_data, progress->data.begin() + begin);
    progress->blocks_received[block_index] = true;
    ++progress->received_count;

    state_->add_total_bytes_downloaded(block_data.size());
    conn->add_bytes_downloaded(block_data.size());

    if (progress->received_count == progress->total_blocks) {
        auto expected_hash = std::vector<std::byte>(state_->torrent_info().pieces.begin() + piece_index * 20, state_->torrent_info().pieces.begin() + (piece_index * 20 + 20));
        auto actual_hash = Crypto::calculate_sha1_hash_data(progress->data);
        if (actual_hash != expected_hash) {
            LOGERR("Hash mismatch for piece {}. Returning to queue.", piece_index);
            co_await piece_manager_->return_piece_to_queue(piece_index);
            co_return;
        }

        co_await file_manager_->write_piece(piece_index, progress->data);
        state_->piece_status(piece_index, PieceStatus::Have);
        state_->add_completed_pieces(1);
        piece_manager_->notify_one();

        LOGINFO("Piece {} downloaded and verified. Progress: {}", piece_index, state_->progress());
        
        co_await peer_manager_->send_have_message_to_all(piece_index);
        
        piece_manager_->remove_in_progress_piece(piece_index);

        if (state_->completed_pieces() == state_->num_pieces()) {
            state_->is_download_complete(true);
            LOGINFO("🎉 Download complete! File saved to {}", state_->save_path().string());
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
        LOGWARN("Ignoring REQUEST from peer {} because state is not met (am_choking: {}, peer_is_interested: {})",
                conn->peer_id(), conn->am_choking(), conn->peer_is_interested()
        );
        co_return;
    }

    // LOGDBG("TorrentSession: Peer {} requested piece_idx={}, begin={}, length={}", 
    //         conn->peer_id(), piece_index, begin, length);

    co_await await_upload_tokens(length);

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

asio::awaitable<void> TorrentSession::on_peer_has_piece(std::shared_ptr<PeerConnection> conn, size_t piece_index) {
    if (piece_index < conn->bitfield_size()) {
        conn->set_has_piece(piece_index);
        if (state_->piece_status(piece_index) == PieceStatus::Have) {
            piece_manager_->add_piece_availability(piece_index, 1);
            co_return ;
        }
    
        uint32_t old_rarity = piece_manager_->piece_availability(piece_index);
        piece_manager_->add_piece_availability(piece_index, 1);
        uint32_t new_rarity = piece_manager_->piece_availability(piece_index);
        co_await piece_manager_->update_piece_rarity(piece_index, old_rarity, new_rarity);
    }

    if (state_->piece_status(piece_index) == PieceStatus::Needed && !conn->am_interested()) {
        conn->am_interested(true);
        co_await conn->send_simple_message(MessageType::Interested);
    }

    piece_manager_->notify_one();
}

asio::awaitable<void> TorrentSession::on_peer_bitfield(std::shared_ptr<PeerConnection> conn, std::span<const std::byte> bitfield) {
    size_t expected_bitfield_size = (state_->num_pieces() + 7) / 8;
    if (expected_bitfield_size != bitfield.size()) {
        LOGWARN("Received bitfield of incorrect size. Expected {}, got {}. Dropping connection.",
            expected_bitfield_size, bitfield.size()
        );
        conn->close();
        co_return ;
    }

    conn->bitfield(std::vector<uint8_t>(reinterpret_cast<const uint8_t *>(bitfield.data()), reinterpret_cast<const uint8_t *>(bitfield.data()) + bitfield.size()));

    bool should_be_interested = false;
    for (size_t i = 0; i < state_->num_pieces(); ++i) {
        if (conn->has_piece(i)) {
            if (state_->piece_status(i) == PieceStatus::Have) {
                piece_manager_->add_piece_availability(i, 1);
                continue ;
            }
            
            uint32_t old_rarity = piece_manager_->piece_availability(i);
            piece_manager_->add_piece_availability(i, 1);
            uint32_t new_rarity = piece_manager_->piece_availability(i);
            co_await piece_manager_->update_piece_rarity(i, old_rarity, new_rarity);
            if (state_->piece_status(i) == PieceStatus::Needed) should_be_interested = true;
        }
    }

    if (should_be_interested && !conn->am_interested()) {
        conn->am_interested(true);
        co_await conn->send_simple_message(MessageType::Interested);
    }

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
    }
    co_return ;
}

asio::awaitable<void> TorrentSession::on_disconnect(std::shared_ptr<PeerConnection> conn) {
    for (size_t i = 0; i < state_->num_pieces(); ++i) {
        if (conn->has_piece(i)) {
            uint32_t old_rarity = piece_manager_->piece_availability(i);
            piece_manager_->add_piece_availability(i, -1);
            uint32_t new_rarity = piece_manager_->piece_availability(i);
            co_await piece_manager_->update_piece_rarity(i, old_rarity, new_rarity);
        }
    }

    // Clean outstanding requests
    for (auto& [piece_index, progress] : *piece_manager_->in_progress_pieces()) {
        for (auto& requests : progress->outstanding_requests) {
            requests.erase(std::remove(requests.begin(), requests.end(), conn->peer_id()), requests.end());
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
    piece_manager_->notify_one();
}

asio::awaitable<void> TorrentSession::on_extended_message(std::shared_ptr<PeerConnection> conn, std::span<const std::byte> payload) {
    if (payload.empty()) {
        LOGWARN("Received empty extended message payload from peer {}. Disconnecting.", conn->peer_id());
        conn->close(); // Disconnect on malformed message
        co_return;
    }

    conn->update_extension_type(0, ExtendedMessageType::Handshake);
    auto remote_id = static_cast<uint8_t>(payload[0]);
    auto message_type = conn->extension_type(remote_id);

    LOGDBG("Received extended message type {} (ID: {}) from peer {}",
           static_cast<int>(message_type), remote_id, conn->peer_id());

    std::span<const std::byte> extended_payload(payload.data() + 1, payload.size() - 1);
    switch (message_type) {
        case ExtendedMessageType::Handshake: {
            LOGDBG("Received extended handshake message");

            auto decoded_payload = decode(extended_payload);
            const auto *ehs_dict = std::get_if<std::unique_ptr<Dict>>(&decoded_payload.get_variant());
            if (!ehs_dict || !ehs_dict->get()->count("m")) {
                throw std::runtime_error("Invalid extended handshake message");
            }

            const auto *m_dict = std::get_if<std::unique_ptr<Dict>>(&(ehs_dict->get()->at("m").get_variant()));
            if (m_dict && !m_dict->get()->empty()) {
                LOGDBG("Peer {} supports:", conn->peer_id());
                for (auto &[k, v] : **m_dict) {
                    LOGDBG("\t{}", k);
                    uint8_t index = std::get<Integer>(v.get_variant());
                    conn->update_extension_type(index, to_extended_type(k));
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

            auto decoded_val = decode(extended_payload);
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
                if (mode_ != Mode::Seed) {
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

                // The raw metadata piece data follows the bencoded dict
                size_t dict_end = 0;
                // Find where the bencoded dict ends by finding the 'e' that closes the top-level dict
                int depth = 0;
                for (size_t i = 0; i < extended_payload.size(); ++i) {
                    char c = static_cast<char>(extended_payload[i]);
                    if (c == 'd' || c == 'l' || c == 'i') {
                        if (depth == 0 && (c == 'd' || c == 'l')) {
                            // OK, start of dict/list
                        }
                        if (c == 'd' || c == 'l') depth++;
                    } else if (c == 'e') {
                        depth--;
                        if (depth == 0) {
                            dict_end = i + 1;
                            break;
                        }
                    }
                }
                if (dict_end == 0 || dict_end >= extended_payload.size()) {
                    LOGWARN("ut_metadata data message has no payload data from peer {}", conn->peer_id());
                    co_return;
                }
                std::span<const std::byte> raw_piece = extended_payload.subspan(dict_end);

                if (metadata_download_active_) {
                    metadata_buffer_.resize(total_size);
                    size_t offset = static_cast<size_t>(piece_idx) * METADATA_PIECE_SIZE;
                    if (offset + raw_piece.size() <= total_size) {
                        std::ranges::copy(raw_piece, metadata_buffer_.begin() + offset);
                        metadata_pieces_received_++;
                        LOGDBG("Received metadata piece {}/{} from peer {}", piece_idx,
                               (total_size + METADATA_PIECE_SIZE - 1) / METADATA_PIECE_SIZE,
                               conn->peer_id());

                        size_t total_pieces = (total_size + METADATA_PIECE_SIZE - 1) / METADATA_PIECE_SIZE;
                        if (metadata_pieces_received_ >= total_pieces) {
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
            LOGWARN("Received unhandled extended message type {} (ID: {}) from peer {}. Disconnecting.", 
                    static_cast<int>(message_type), remote_id, conn->peer_id());
            conn->close(); // Disconnect on unhandled extended message
            co_return;
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

                co_await piece_manager_->remove_piece_rarity(i, 0);
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
        co_await piece_manager_->build_piece_rarity();

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
                    co_await piece_manager_->update_piece_rarity(piece_idx, current_rarity, IN_PROGRESS_RARITY_GROUP_ID);
                    piece_manager_->emplace_in_progress_pieces(piece_idx, std::move(progress));

                    // Re-initiate the action by spawning a resume task
                    asio::co_spawn(io_context_, piece_manager_->resume_piece_download(piece_idx), asio::detached);
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

    auto& requests_for_block = it->second->outstanding_requests[block_index];
    std::vector<PeerId> peer_ids_to_cancel = requests_for_block;
    requests_for_block.clear();

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

    if (metadata_buffer_.empty()) {
        metadata_buffer_.resize(static_cast<size_t>(total_size));
        metadata_pieces_received_ = 0;
    }

    int total_pieces = (total_size + static_cast<int>(METADATA_PIECE_SIZE) - 1) / static_cast<int>(METADATA_PIECE_SIZE);
    for (int piece = 0; piece < total_pieces; ++piece) {
        co_await conn->send_metadata_request(ext_id, piece);
        LOGDBG("Requested metadata piece {}/{} from peer {}", piece + 1, total_pieces, conn->peer_id());
    }
}

asio::awaitable<void> TorrentSession::on_metadata_complete() {
    try {
        Value info_val = decode(metadata_buffer_);
        const auto* info_dict_ptr = std::get_if<std::unique_ptr<Dict>>(&info_val.get_variant());
        if (!info_dict_ptr) {
            LOGERR("Received metadata is not a dictionary");
            metadata_download_active_ = false;
            co_return;
        }

        state_->info_mut().set_info_bencoded(metadata_buffer_);
        const Dict& info_dict = **info_dict_ptr;
        auto& info = state_->torrent_info();
        info.name = std::get<String>(info_dict.at("name").get_variant());
        info.piece_size = std::get<Integer>(info_dict.at("piece length").get_variant());
        const auto& pieces_str = std::get<String>(info_dict.at("pieces").get_variant());
        info.pieces.assign(reinterpret_cast<const std::byte*>(pieces_str.data()),
                           reinterpret_cast<const std::byte*>(pieces_str.data()) + pieces_str.size());

        if (info_dict.count("length")) {
            info.total_size = std::get<Integer>(info_dict.at("length").get_variant());
            info.files.push_back({std::filesystem::path(info.name), info.total_size, true});
        } else {
            const List* file_list = std::get_if<std::unique_ptr<List>>(&info_dict.at("files").get_variant())->get();
            for (const auto& file_val : *file_list) {
                const Dict* file_dict = std::get_if<std::unique_ptr<Dict>>(&file_val.get_variant())->get();
                uint64_t length = std::get<Integer>(file_dict->at("length").get_variant());
                const List* path_list = std::get_if<std::unique_ptr<List>>(&file_dict->at("path").get_variant())->get();
                std::filesystem::path file_path;
                for (const std::string& part : *path_list | std::views::transform(
                        [](const Value& pv) { return std::get<String>(pv.get_variant()); })) {
                    file_path /= part;
                }
                info.files.push_back({file_path, length, true});
                info.total_size += length;
            }
        }

        size_t num_pieces = info.pieces.size() / 20;
        state_->init_pieces(num_pieces);

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
            co_await piece_manager_->build_piece_rarity();
        } else {
            if (!co_await file_manager_->preallocate_files()) {
                LOGERR("Failed to preallocate files after metadata download");
                co_return;
            }
            co_await piece_manager_->build_piece_rarity();
        }

        // Start download loops that were skipped in init()
        if (mode_ == Mode::Leech) {
            asio::co_spawn(io_context_, peer_manager_->choke_loop(), asio::detached);
            piece_manager_->set_callback([this](size_t piece_index)
                -> asio::awaitable<std::vector<std::shared_ptr<PeerConnection>>> {
                    co_return co_await peer_manager_->available_peers(piece_index);
                });
            asio::co_spawn(io_context_, piece_manager_->downloader(), asio::detached);
            asio::co_spawn(strand_, periodically_save(), asio::detached);
            asio::co_spawn(strand_, discovered_peers_loop(), asio::detached);
        }

        metadata_download_active_ = false;
        metadata_buffer_.clear();
        metadata_pieces_received_ = 0;

    } catch (const std::exception& e) {
        LOGERR("Failed to parse received metadata: {}", e.what());
        metadata_download_active_ = false;
        metadata_buffer_.clear();
        metadata_pieces_received_ = 0;
    }
}