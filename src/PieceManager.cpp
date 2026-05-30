#include "PieceManager.hpp"

#include <random>

PieceManager::PieceManager(asio::io_context& io_context, std::shared_ptr<SessionState> state)
    : io_context_(io_context), strand_(asio::make_strand(io_context)),
      piece_request_trigger_(io_context), block_timeout_timer_(io_context), state_(state) 
{
    const size_t num_pieces = state_->num_pieces();
    piece_availability_ = std::make_shared<std::vector<size_t>>();
    piece_availability_->resize(num_pieces, 0);
    in_progress_pieces_ = std::make_shared<std::map<size_t, std::shared_ptr<InProgressPiece>>>();
    pieces_by_rarity_ = std::make_shared<std::map<size_t, std::shared_ptr<std::unordered_set<int>>>>();

    piece_request_trigger_.expires_at(asio::steady_timer::time_point::max());
}

std::map<std::string, std::string> PieceManager::get_in_progress_for_resume() const {
    std::map<std::string, std::string> result;
    for (const auto& [piece_index, progress] : *in_progress_pieces()) {
        std::string block_bitfield((progress->total_blocks + 7) / 8, 0);
        for (uint32_t i = 0; i < progress->total_blocks; ++i) {
            if (progress->blocks_received[i]) {
                block_bitfield[i / 8] |= (1 << (7 - (i % 8)));
            }
        }
        result[std::to_string(piece_index)] = block_bitfield;
    }
    return result;
}

asio::awaitable<void> PieceManager::downloader() {
    auto self = shared_from_this();
    const int max_in_progress_pieces = 5;

    // Start the periodic block timeout checker
    asio::co_spawn(io_context_, self->block_timeout_loop(), asio::detached);

    while (!state_->is_download_complete() && !shutting_down_) {
        int slots_to_fill = 0;

        co_await asio::dispatch(strand_, asio::use_awaitable);

        if (shutting_down_) break;

        size_t current_in_progress = in_progress_pieces_->size();
        if (current_in_progress < max_in_progress_pieces) {
            slots_to_fill = std::min(max_in_progress_pieces - current_in_progress, state_->needed_pieces());
        }
        
        if (slots_to_fill > 0) {
            for (int i = 0; i < slots_to_fill; ++i) {
                asio::co_spawn(io_context_, self->request_one_piece(), asio::detached);
            }
        }

        piece_request_trigger_.expires_at(asio::steady_timer::time_point::max());
        try {
            co_await piece_request_trigger_.async_wait(asio::use_awaitable);
        } catch (const boost::system::system_error& e) {
            if (e.code() != asio::error::operation_aborted) {
                throw ;
            }
        }
    }
}

asio::awaitable<void> PieceManager::request_one_piece() {
    if (state_->is_download_complete()) {
        co_return;
    }

    for (const auto& [rarity, piece_set] : *pieces_by_rarity()) {
        if (rarity == 0) continue;  // We only care about pieces that are actually available (rarity > 0)

        // Create a shuffled list of candidates at this rarity level
        // Shuffling prevents multiple clients from requesting the same piece simultaneously
        std::vector<int> candidates(piece_set->begin(), piece_set->end());
        std::shuffle(
            candidates.begin(), candidates.end(), 
            std::mt19937{std::random_device{}()}
        );

        for (size_t piece_index : candidates) {
            if (co_await try_piece_download(piece_index)) {
                co_return ;
            }
        }
    }

    LOGDBG("No available pieces to download, trying any needed pieces...");

    std::vector<size_t> all_needed_pieces;
    std::ranges::copy(
        std::views::iota(0UL, state_->num_pieces()) 
            | std::views::filter([this](size_t i) {
                return state_->piece_status(i) == PieceStatus::Needed;
            }),
        std::back_inserter(all_needed_pieces)
    );
    
    if (!all_needed_pieces.empty()) {
        std::shuffle(
            all_needed_pieces.begin(), all_needed_pieces.end(),
            std::mt19937{std::random_device{}()}
        );
        
        for (size_t piece_index : all_needed_pieces) {
            if (co_await try_piece_download(piece_index)) {
                co_return;
            }
        }
    }

    LOGDBG("No available and needed pieces to download from any unchoked peer right now.");
}

asio::awaitable<bool> PieceManager::try_piece_download(size_t piece_index) {
    auto self = shared_from_this();

    if (state_->piece_status(piece_index) != PieceStatus::Needed) {
        co_return false;
    }
    
    // Find an unchoked peer that has this piece
    auto available_peers = co_await get_available_peers_(piece_index);
    if (available_peers.empty()) {
        co_return false;
    }
    
    // Start downloading this piece
    const auto& t_info = state_->torrent_info();
    uint64_t piece_size = (piece_index == state_->num_pieces() - 1) 
        ? (t_info.total_size % t_info.piece_size ?: t_info.piece_size)
        : t_info.piece_size;
    
    state_->piece_status(piece_index, PieceStatus::InProgress);
    
    // Update rarity map - remove from current rarity
    uint32_t current_rarity = piece_availability(piece_index);
    co_await update_piece_rarity(piece_index, current_rarity, IN_PROGRESS_RARITY_GROUP_ID);
    
    emplace_in_progress_pieces(piece_index, std::make_shared<InProgressPiece>(piece_size));
    auto piece_progress = in_progress_piece(piece_index);
    uint32_t num_blocks = piece_progress->total_blocks;
    
    // Request all blocks for this piece
    for (uint32_t block_idx = 0; block_idx < num_blocks; ++block_idx) {
        uint32_t offset = block_idx * BLOCK_SIZE;
        uint32_t length = (block_idx == num_blocks - 1) 
            ? (piece_progress->data.size() - offset)
            : BLOCK_SIZE;
        
        auto& peer_conn = available_peers[block_idx % available_peers.size()];
        asio::co_spawn(io_context_, 
            [peer_conn, piece_index, offset, length]() -> asio::awaitable<void> {
                co_await peer_conn->send_request(piece_index, offset, length);
            }, 
            asio::detached
        );
        
        piece_progress->outstanding_requests[block_idx].push_back(peer_conn->peer_id());
        piece_progress->request_times[block_idx] = std::chrono::steady_clock::now();
    }
    
    asio::co_spawn(io_context_, self->check_and_enter_endgame(), asio::detached);
    co_return true;
}

asio::awaitable<void> PieceManager::check_and_enter_endgame() {
    auto self = shared_from_this();
    
    if (state_->is_in_endgame_mode()) {
        co_return ;
    }

    size_t needed_count = state_->status_count(
        [](PieceStatus status) {
            return status == PieceStatus::Needed || status == PieceStatus::InProgress;
    });

    if (needed_count > 0 && needed_count < state_->num_pieces() * 0.1) {
        LOGINFO("🎉 All pieces requested. Entering ENDGAME MODE. 🎉");
        state_->is_in_endgame_mode(true);
        asio::co_spawn(io_context_, self->broadcast_outstanding_requests(), asio::detached);
    }
}

asio::awaitable<void> PieceManager::broadcast_outstanding_requests() {
    if (state_->is_download_complete()) {
        co_return;
    }
    
    LOGINFO("Endgame: Re-requesting all outstanding blocks from all unchoked peers.");

    const auto& info = state_->torrent_info();
    const size_t num_pieces = info.pieces.size() / 20;

    // Snapshot shared_ptrs to avoid use-after-free: co_await inside the loop
    // allows concurrent map mutation (remove_in_progress_piece, etc.), which
    // would dangle references into the map. Local shared_ptr copies keep
    // each InProgressPiece alive regardless of map state.
    auto pieces_snapshot = in_progress_pieces();
    if (!pieces_snapshot) co_return;
    std::vector<std::pair<size_t, std::shared_ptr<InProgressPiece>>> pieces;
    pieces.reserve(pieces_snapshot->size());
    for (const auto& [idx, prog] : *pieces_snapshot) {
        pieces.emplace_back(idx, prog);
    }

    for (auto& [piece_idx, piece_progress] : pieces) {
        for (uint32_t block_idx = 0; block_idx < piece_progress->total_blocks; ++block_idx) {
            uint32_t offset = block_idx * BLOCK_SIZE;
            uint64_t current_piece_size;
            if (static_cast<uint64_t>(piece_idx) == num_pieces - 1) {
                current_piece_size = info.total_size - (static_cast<uint64_t>(piece_idx) * info.piece_size);
            } else {
                current_piece_size = info.piece_size;
            }

            uint32_t length = (offset + BLOCK_SIZE > current_piece_size)
                            ? (current_piece_size - offset)
                            : BLOCK_SIZE;

            // Get available peers BEFORE the lock — no co_await inside lock_guard.
            auto unchoked_peers_with_piece = co_await get_available_peers_(piece_idx);

            // Snapshot which peers we need to re-request from under the piece lock,
            // but do the actual co_await sends outside the lock.
            std::vector<std::shared_ptr<PeerConnection>> target_peers;
            {
                std::lock_guard lock(piece_progress->piece_mutex_);
                if (piece_progress->blocks_received[block_idx]) {
                    continue;
                }
                for (const auto& peer_conn : unchoked_peers_with_piece) {
                    piece_progress->outstanding_requests[block_idx].push_back(peer_conn->peer_id());
                }
                target_peers = unchoked_peers_with_piece;
                piece_progress->request_times[block_idx] = std::chrono::steady_clock::now();
            }

            for (const auto& peer_conn : target_peers) {
                co_await peer_conn->send_request(piece_idx, offset, length);
            }
        }
    }
    LOGDBG("Endgame broadcast complete.");
}

asio::awaitable<void> PieceManager::return_piece_to_queue(size_t piece_index) {
    auto self = shared_from_this();

    co_await asio::dispatch(strand_, asio::use_awaitable);

    auto it = in_progress_pieces_->find(piece_index);
    if (it != in_progress_pieces_->end()) {
        in_progress_pieces_->erase(it);
    }
    
    state_->piece_status(piece_index, PieceStatus::Needed);
    co_await update_piece_rarity(piece_index, IN_PROGRESS_RARITY_GROUP_ID, piece_availability_->at(piece_index));
    piece_request_trigger_.cancel_one();
    co_return;
}

asio::awaitable<void> PieceManager::update_piece_rarity(size_t piece_index, uint32_t old_rarity, uint32_t new_rarity) {
    // Remove from the old rarity set (post to strand ensures we're on strand upon return)
    co_await remove_piece_rarity(piece_index, old_rarity);
    // Add to the new rarity set
    auto& set_ptr = (*pieces_by_rarity_)[new_rarity];
    if (!set_ptr) {
        set_ptr = std::make_shared<std::unordered_set<int>>();
    }
    set_ptr->insert(static_cast<int>(piece_index));
}

asio::awaitable<void> PieceManager::remove_piece_rarity(size_t piece_index, uint32_t rarity) {
    co_await asio::post(strand_, asio::use_awaitable);
    if (auto it = pieces_by_rarity_->find(rarity); it != pieces_by_rarity_->end()) {
        it->second->erase(piece_index);
        // If the set for the old rarity is now empty, remove the map entry
        if (it->second->empty()) {
            pieces_by_rarity_->erase(it);
        }
    }
}

asio::awaitable<void> PieceManager::build_piece_rarity() {
    co_await asio::dispatch(strand_, asio::use_awaitable);
    pieces_by_rarity_->clear();
    for (size_t i : std::views::iota(0UL, state_->num_pieces())) {
        PieceStatus status = state_->piece_status(i);
        uint32_t target_rarity;
        if (status == PieceStatus::Have) {
            target_rarity = 0; // 'Have' pieces typically go into rarity 0
        } else if (status == PieceStatus::Needed) {
            target_rarity = piece_availability_->at(i); // Use actual count of peers
        } else if (status == PieceStatus::InProgress) {
            target_rarity = IN_PROGRESS_RARITY_GROUP_ID; // Special group for in-progress
        } else if (status == PieceStatus::Skipped) {
            continue; // Skipped pieces are not tracked for downloading
        } else {
            LOGWARN("Unknown piece status for piece {}: {}. Skipping.", i, static_cast<int>(status));
            continue;
        }
        auto& set_ptr = (*pieces_by_rarity_)[target_rarity]; // Create entry if it doesn't exist
        if (!set_ptr) { // If the shared_ptr itself is null (first access for this rarity)
            set_ptr = std::make_shared<std::unordered_set<int>>();
        }
        set_ptr->insert(static_cast<int>(i));
    }
}

asio::awaitable<void> PieceManager::resume_piece_download(size_t piece_index) {
    asio::steady_timer timer(io_context_);

    while (true) {
        if (shutting_down_.load()) {
            LOGDBG("Resumer for piece {}: shutting down, exiting.", piece_index);
            co_return;
        }

        timer.expires_after(std::chrono::seconds(1));
        try {
            co_await timer.async_wait(asio::use_awaitable);
        } catch (const boost::system::system_error& e) {
            if (e.code() == asio::error::operation_aborted) {
                co_return;
            }
            throw;
        }

        co_await asio::dispatch(strand_, asio::use_awaitable);
        auto piece_it = in_progress_pieces_->find(piece_index);
        if (piece_it == in_progress_pieces_->end()) {
            co_return;
        }

        auto piece_progress = piece_it->second;

        auto available_peers = co_await get_available_peers_(piece_index);
        if (available_peers.empty()) {
            LOGDBG("Resumer for piece {}: No available peers yet, will retry...", piece_index);
            continue;
        }

        LOGINFO("Resuming download for piece {}. Requesting missing blocks.", piece_index);

        // Collect missing block info under lock, spawn sends outside lock
        struct MissingBlock {
            uint32_t offset;
            uint32_t length;
            size_t peer_idx;
        };
        std::vector<MissingBlock> missing;
        {
            std::lock_guard lock(piece_progress->piece_mutex_);
            for (uint32_t block_idx = 0; block_idx < piece_progress->total_blocks; ++block_idx) {
                if (piece_progress->blocks_received[block_idx]) continue;
                uint32_t offset = block_idx * BLOCK_SIZE;
                uint32_t length = (block_idx == piece_progress->total_blocks - 1)
                    ? (piece_progress->data.size() - offset)
                    : BLOCK_SIZE;

                if (state_->is_in_endgame_mode()) {
                    // In endgame mode, request from all unchoked peers
                    for (const auto& peer_conn : available_peers) {
                        if (peer_conn->peer_is_choking()) continue;
                        piece_progress->outstanding_requests[block_idx].push_back(peer_conn->peer_id());
                        asio::co_spawn(io_context_,
                            [peer_conn, piece_index, offset, length]() -> asio::awaitable<void> {
                                co_await peer_conn->send_request(piece_index, offset, length);
                            },
                            asio::detached
                        );
                    }
                } else {
                    size_t peer_idx = block_idx % available_peers.size();
                    auto& peer_conn = available_peers[peer_idx];
                    piece_progress->outstanding_requests[block_idx].push_back(peer_conn->peer_id());
                    asio::co_spawn(io_context_,
                        [peer_conn, piece_index, offset, length]() -> asio::awaitable<void> {
                            co_await peer_conn->send_request(piece_index, offset, length);
                        },
                        asio::detached
                    );
                }
                piece_progress->request_times[block_idx] = std::chrono::steady_clock::now();
            }
        }

        co_return;
    }
}

asio::awaitable<void> PieceManager::check_block_timeouts() {
    auto self = shared_from_this();
    co_await asio::dispatch(strand_, asio::use_awaitable);

    if (state_->is_download_complete()) {
        co_return;
    }

    // Snapshot shared_ptrs to avoid use-after-free from concurrent map mutation
    // while suspended at co_await (see broadcast_outstanding_requests for details).
    auto pieces_snapshot = in_progress_pieces();
    if (!pieces_snapshot) co_return;
    std::vector<std::pair<size_t, std::shared_ptr<InProgressPiece>>> pieces;
    pieces.reserve(pieces_snapshot->size());
    for (const auto& [idx, prog] : *pieces_snapshot) {
        pieces.emplace_back(idx, prog);
    }

    auto now = std::chrono::steady_clock::now();

    for (const auto& [piece_idx, piece_progress] : pieces) {
        for (uint32_t block_idx = 0; block_idx < piece_progress->total_blocks; ++block_idx) {
            // Read request_time under lock to check for timeout
            TimePoint request_time;
            bool already_received = false;
            bool timed_out = false;
            {
                std::lock_guard lock(piece_progress->piece_mutex_);
                already_received = piece_progress->blocks_received[block_idx];
                request_time = piece_progress->request_times[block_idx];
            }

            if (already_received) continue;
            if (request_time == TimePoint{}) continue;

            if (now - request_time > BLOCK_REQUEST_TIMEOUT) {
                timed_out = true;
            }

            if (!timed_out) continue;

            LOGWARN("Block {}/{} timed out after {}s. Cancelling and re-requesting.",
                    piece_idx, block_idx, BLOCK_REQUEST_TIMEOUT.count());

            if (block_timeout_callback_) {
                co_await block_timeout_callback_(static_cast<uint32_t>(piece_idx), block_idx);
            }

            auto available_peers = co_await get_available_peers_(piece_idx);
            if (!available_peers.empty()) {
                uint32_t offset = block_idx * BLOCK_SIZE;
                uint32_t length = (block_idx == piece_progress->total_blocks - 1)
                    ? (piece_progress->data.size() - offset)
                    : BLOCK_SIZE;

                auto& new_peer = available_peers[block_idx % available_peers.size()];
                asio::co_spawn(io_context_,
                    [new_peer, piece_idx, offset, length]() -> asio::awaitable<void> {
                        co_await new_peer->send_request(piece_idx, offset, length);
                    },
                    asio::detached
                );

                // Re-lock to update outstanding requests
                {
                    std::lock_guard lock(piece_progress->piece_mutex_);
                    piece_progress->outstanding_requests[block_idx].push_back(new_peer->peer_id());
                    piece_progress->request_times[block_idx] = now;
                }
            }
        }
    }
}

asio::awaitable<void> PieceManager::block_timeout_loop() {
    auto self = shared_from_this();

    while (!state_->is_download_complete() && !shutting_down_.load()) {
        co_await self->check_block_timeouts();

        block_timeout_timer_.expires_after(std::chrono::seconds(1));
        try {
            co_await block_timeout_timer_.async_wait(asio::use_awaitable);
        } catch (const boost::system::system_error& e) {
            if (e.code() == asio::error::operation_aborted) {
                break;
            }
            throw;
        }
    }
}