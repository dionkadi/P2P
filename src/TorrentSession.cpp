#include "TorrentSession.hpp"
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

asio::awaitable<bool> TorrentSession::init() {
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

            // ...
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
            LOGWARN("UT_METADATA not implemented yet. Received from peer {}. Disconnecting.", conn->peer_id());
            conn->close(); // Disconnect if unimplemented essential feature is received
            co_return;
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