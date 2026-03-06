#include "PieceManager.hpp"

#include <random>

PieceManager::PieceManager(asio::io_context& io_context, std::shared_ptr<SessionState> state)
    : io_context_(io_context), strand_(asio::make_strand(io_context)),
      piece_request_trigger_(io_context), state_(state) 
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

    while (!state_->is_download_complete()) {
        int slots_to_fill = 0;

        co_await asio::dispatch(strand_, asio::use_awaitable);

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

    for (auto& [piece_idx, piece_progress] : *in_progress_pieces()) {
        for (uint32_t block_idx = 0; block_idx < piece_progress->total_blocks; ++block_idx) {
            if (!piece_progress->blocks_received[block_idx]) {
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
                

                auto unchoked_peers_with_piece = co_await get_available_peers_(piece_idx);
                for (const auto& peer_conn : unchoked_peers_with_piece) {
                    co_await peer_conn->send_request(piece_idx, offset, length);
                    piece_progress->outstanding_requests[block_idx].push_back(peer_conn->peer_id());
                }
            }
        }
    }
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
    // Remove from the old rarity set
    co_await remove_piece_rarity(piece_index, old_rarity);
    co_await asio::dispatch(strand_, asio::use_awaitable);
    // Add to the new rarity set
    auto& set_ptr = (*pieces_by_rarity_)[new_rarity];
    if (!set_ptr) {
        set_ptr = std::make_shared<std::unordered_set<int>>();
    }
    set_ptr->insert(static_cast<int>(piece_index));
}

asio::awaitable<void> PieceManager::remove_piece_rarity(size_t piece_index, uint32_t rarity) {
    co_await asio::dispatch(strand_, asio::use_awaitable);
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

    // Loop until successfully re-request all missing blocks
    while (true) {
        // Wait for a short period to allow peer connections to be established
        timer.expires_after(std::chrono::seconds(1));
        co_await timer.async_wait(asio::use_awaitable);

        co_await asio::dispatch(strand_, asio::use_awaitable);
        // Check if the piece is still in progress (it might have been completed or cancelled)
        auto piece_it = in_progress_pieces_->find(piece_index);
        if (piece_it == in_progress_pieces_->end()) {
            // The piece is no longer in progress, our job here is done.
            co_return;
        }

        auto piece_progress = piece_it->second;
        // Find peers that have this piece and are not choking us

        auto available_peers = co_await get_available_peers_(piece_index);
        if (available_peers.empty()) {
            LOGDBG("Resumer for piece {}: No available peers yet, will retry...", piece_index);
            continue; // Go back to the start of the loop and wait again
        }

        LOGINFO("Resuming download for piece {}. Requesting missing blocks.", piece_index);
        
        if (state_->is_in_endgame_mode()) {
            // In endgame mode, request from all available peers
            for (uint32_t block_idx = 0; block_idx < piece_progress->total_blocks; ++block_idx) {
                if (!piece_progress->blocks_received[block_idx]) {
                    uint32_t offset = block_idx * BLOCK_SIZE;
                    uint32_t length = (block_idx == piece_progress->total_blocks - 1) 
                        ? (piece_progress->data.size() - offset)
                        : BLOCK_SIZE;
                    
                    // Request from all available peers for this block
                    std::vector<std::shared_ptr<PeerConnection>> unchoked_peers;
                    std::ranges::copy(
                        available_peers
                            | std::views::filter([](const std::shared_ptr<PeerConnection>& peer_conn){ 
                                return !peer_conn->peer_is_choking(); 
                            }),
                        std::back_inserter(unchoked_peers)
                    );
                    for (const auto& peer_conn : unchoked_peers) {
                        asio::co_spawn(io_context_,
                            [peer_conn, piece_index, offset, length]() -> asio::awaitable<void> {
                                co_await peer_conn->send_request(piece_index, offset, length);
                            },
                            asio::detached
                        );
                        piece_progress->outstanding_requests[block_idx].push_back(peer_conn->peer_id());
                    }
                }
            }
        } else {
            // Request all missing blocks for this piece
            for (uint32_t block_idx = 0; block_idx < piece_progress->total_blocks; ++block_idx) {
                if (!piece_progress->blocks_received[block_idx]) {
                    uint32_t offset = block_idx * BLOCK_SIZE;
                    uint32_t length = (block_idx == piece_progress->total_blocks - 1) 
                        ? (piece_progress->data.size() - offset)
                        : BLOCK_SIZE;
                    // Pick a peer to request from (round-robin)
                    auto& peer_conn = available_peers[block_idx % available_peers.size()];
                    
                    asio::co_spawn(io_context_,
                        [peer_conn, piece_index, offset, length]() -> asio::awaitable<void> {
                            co_await peer_conn->send_request(piece_index, offset, length);
                        },
                        asio::detached
                    );
                    
                    // Track that we made a request for this block
                    piece_progress->outstanding_requests[block_idx].push_back(peer_conn->peer_id());
                }
            }
        }
        co_return;
    }
}