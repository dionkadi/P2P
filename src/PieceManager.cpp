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
    CTRACK_ASYNC("PieceManager::downloader");
    auto self = shared_from_this();
    // Aggregate in-flight window must span the connected swarm, not a fixed
    // handful of pieces. 5 pieces (~1.28 MiB at 256 KiB pieces) serialized
    // downloads on slow tail blocks: with only 5 pieces in flight a single
    // snubbing peer stalls the whole pipe for BLOCK_REQUEST_TIMEOUT (12s),
    // producing the observed sawtooth (~200 KB/s ceiling despite 50+ peers).
    // 32 pieces keeps a healthy multi-MiB window; qBittorrent holds 30-60.
    size_t max_in_progress_pieces = std::clamp<size_t>(state_->needed_pieces(), 5, 32);

    // Start the periodic block timeout checker
    asio::co_spawn(io_context_, self->block_timeout_loop(), asio::detached);

    while (!state_->is_download_complete() && !shutting_down_) {
        int slots_to_fill = 0;

        co_await asio::dispatch(strand_, asio::use_awaitable);

        if (shutting_down_) break;

        size_t current_in_progress = in_progress_pieces()->size();
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
    CTRACK_ASYNC("PieceManager::request_one_piece");
    if (state_->is_download_complete()) {
        co_return;
    }

    // libtorrent initial_picker_threshold=4: the first few pieces are picked
    // RANDOMLY rather than rarest-first, so a fresh leecher quickly gains
    // tradeable pieces. Rarest-first at 0% picks pieces almost nobody needs,
    // leaving us nothing to upload in return -> tit-for-tat seeders keep us
    // choked -> no unchoke slots -> slow ramp. Random early pieces become
    // instantly tradeable, earning regular (non-optimistic) unchoke slots.
    size_t started = state_->completed_pieces() + in_progress_pieces()->size();
    if (started < kInitialPickerThreshold) {
        std::vector<size_t> random_candidates;
        std::ranges::copy(
            std::views::iota(0UL, state_->num_pieces())
                | std::views::filter([this](size_t i) {
                    return state_->piece_status(i) == PieceStatus::Needed;
                }),
            std::back_inserter(random_candidates)
        );
        std::shuffle(
            random_candidates.begin(), random_candidates.end(),
            std::mt19937{std::random_device{}()}
        );
        for (size_t piece_index : random_candidates) {
            if (co_await try_piece_download(piece_index)) {
                co_return;
            }
        }
        // No unchoked peer had any needed piece — fall through to the
        // rarest-first scan below (it may find a rarer piece a peer has).
    }

    for (const auto& [rarity, piece_set] : snapshot_pieces_by_rarity()) {
        if (rarity == 0) continue;  // We only care about pieces that are actually available (rarity > 0)

        // Create a shuffled list of candidates at this rarity level
        // Shuffling prevents multiple clients from requesting the same piece simultaneously
        std::vector<int> candidates(piece_set.begin(), piece_set.end());
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
    CTRACK_ASYNC("PieceManager::try_piece_download");
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
    update_piece_rarity(piece_index, current_rarity, IN_PROGRESS_RARITY_GROUP_ID);
    
    emplace_in_progress_pieces(piece_index, std::make_shared<InProgressPiece>(piece_size));
    auto piece_progress = in_progress_piece(piece_index);
    uint32_t num_blocks = piece_progress->total_blocks;

    // Seed a small bounded window at piece start instead of committing every
    // block to the peers unchoked THIS instant. A fast-extension peer that is
    // momentarily choking us REJECTs the whole committed batch at once (seen:
    // all 16 blocks of a piece rejected in one timestamp), and the synchronous
    // re-flood on each REJECT cascades it across peers. Requesting only a few
    // blocks per peer lets the resume loop (one peer per missing block, runs
    // on the strand, prunes rejected peers) fill the rest as pipeline slots
    // free up — a momentary choke then costs a handful of blocks, not a piece.
    size_t seed_per_peer = 2;
    size_t seed_count = std::min<size_t>(num_blocks, available_peers.size() * seed_per_peer);
    for (uint32_t block_idx = 0; block_idx < seed_count; ++block_idx) {
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
    }

    // The resume loop now drives the rest of this piece's blocks incrementally.
    if (seed_count < num_blocks) {
        ensure_resume_piece_download(piece_index);
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
    CTRACK_ASYNC("PieceManager::broadcast_outstanding_requests");
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
                target_peers = unchoked_peers_with_piece;
            }

            for (const auto& peer_conn : target_peers) {
                co_await peer_conn->send_request(piece_idx, offset, length);
            }
        }
    }
    LOGDBG("Endgame broadcast complete.");
}

asio::awaitable<void> PieceManager::return_piece_to_queue(size_t piece_index) {
    CTRACK_ASYNC("PieceManager::return_piece_to_queue");
    auto self = shared_from_this();

    co_await asio::dispatch(strand_, asio::use_awaitable);

    remove_in_progress_piece(piece_index);

    state_->piece_status(piece_index, PieceStatus::Needed);
    update_piece_rarity(piece_index, IN_PROGRESS_RARITY_GROUP_ID, piece_availability(piece_index));
    piece_request_trigger_.cancel_one();
    co_return;
}

void PieceManager::update_piece_rarity(size_t piece_index, uint32_t old_rarity, uint32_t new_rarity) {
    std::lock_guard lock(mutex_);

    if (auto it = pieces_by_rarity_->find(old_rarity); it != pieces_by_rarity_->end()) {
        it->second->erase(static_cast<int>(piece_index));
        if (it->second->empty()) {
            pieces_by_rarity_->erase(it);
        }
    }

    auto& set_ptr = (*pieces_by_rarity_)[new_rarity];
    if (!set_ptr) {
        set_ptr = std::make_shared<std::unordered_set<int>>();
    }
    set_ptr->insert(static_cast<int>(piece_index));
}

void PieceManager::remove_piece_rarity(size_t piece_index, uint32_t rarity) {
    std::lock_guard lock(mutex_);
    if (auto it = pieces_by_rarity_->find(rarity); it != pieces_by_rarity_->end()) {
        it->second->erase(piece_index);
        // If the set for the old rarity is now empty, remove the map entry
        if (it->second->empty()) {
            pieces_by_rarity_->erase(it);
        }
    }
}

void PieceManager::build_piece_rarity() {
    std::lock_guard lock(mutex_);

    // Resize if metadata was loaded after construction (magnet link case).
    size_t num_pieces = state_->num_pieces();
    if (piece_availability_->size() < num_pieces) {
        piece_availability_->resize(num_pieces, 0);
    }

    pieces_by_rarity_->clear();
    for (size_t i : std::views::iota(0UL, num_pieces)) {
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

void PieceManager::ensure_resume_piece_download(size_t piece_index) {
    auto progress = in_progress_piece(piece_index);
    if (!progress || shutting_down_.load()) {
        return;
    }

    {
        std::lock_guard lock(progress->piece_mutex_);
        if (progress->resume_task_active) {
            return;
        }
        progress->resume_task_active = true;
    }

    auto self = shared_from_this();
    asio::co_spawn(io_context_,
        [self, piece_index]() -> asio::awaitable<void> {
            try {
                co_await self->resume_piece_download(piece_index);
            } catch (const std::exception& e) {
                LOGWARN("Resumer for piece {} failed: {}", piece_index, e.what());
            }

            auto progress = self->in_progress_piece(piece_index);
            if (progress) {
                std::lock_guard lock(progress->piece_mutex_);
                progress->resume_task_active = false;
            }
        },
        asio::detached
    );
}

asio::awaitable<void> PieceManager::resume_piece_download(size_t piece_index) {
    CTRACK_ASYNC("PieceManager::resume_piece_download");
    asio::steady_timer timer(io_context_);

    while (true) {
        if (shutting_down_.load()) {
            LOGDBG("Resumer for piece {}: shutting down, exiting.", piece_index);
            co_return;
        }

        co_await asio::dispatch(strand_, asio::use_awaitable);
        auto pieces_snapshot = in_progress_pieces();
        auto piece_it = pieces_snapshot->find(piece_index);
        if (piece_it == pieces_snapshot->end()) {
            co_return;
        }

        auto piece_progress = piece_it->second;

        // Collect missing block indices under lock, spawn sends outside lock.
        // Only blocks with no outstanding request are considered; the resume
        // task itself is single-pass so it can never pile up requests.
        std::vector<uint32_t> missing_indices;
        {
            std::lock_guard lock(piece_progress->piece_mutex_);
            for (uint32_t block_idx = 0; block_idx < piece_progress->total_blocks; ++block_idx) {
                if (piece_progress->blocks_received[block_idx]) continue;
                if (!piece_progress->outstanding_requests[block_idx].empty()) continue;
                missing_indices.push_back(block_idx);
            }
        }

        if (missing_indices.empty()) {
            co_return;  // nothing left for this task to do
        }

        bool endgame = state_->is_in_endgame_mode();
        bool spawned_any = false;
        for (uint32_t block_idx : missing_indices) {
            uint32_t offset = block_idx * BLOCK_SIZE;
            uint32_t length = (block_idx == piece_progress->total_blocks - 1)
                ? (piece_progress->data.size() - offset)
                : BLOCK_SIZE;

            if (endgame) {
                // Endgame: request from all unchoked peers that have not
                // recently rejected this block (redundancy is the point).
                auto rejected = pruned_rejected_peers(piece_progress, block_idx);
                auto available_peers = co_await get_available_peers_(piece_index);
                for (const auto& peer_conn : available_peers) {
                    if (peer_conn->peer_is_choking()) continue;
                    if (std::ranges::find(rejected, peer_conn->peer_id()) != rejected.end()) continue;
                    asio::co_spawn(io_context_,
                        [peer_conn, piece_index, offset, length]() -> asio::awaitable<void> {
                            co_await peer_conn->send_request(piece_index, offset, length);
                        },
                        asio::detached
                    );
                    spawned_any = true;
                }
            } else {
                // Normal mode: one peer per block, skipping peers that already
                // rejected it or are already serving it.
                auto replacement = co_await pick_block_peer(piece_index, block_idx, nullptr, true);
                if (!replacement) continue;
                // Register the block as outstanding BEFORE spawning the send.
                // The notify hook (record_request_sent) only fires after the
                // async write completes, so without eager registration a burst
                // of on_disconnect cleanups re-enters this resumer and each
                // pass re-spawns the same 64 requests (observed 27x in ~3ms,
                // flooding the peer's pipeline queue and widening the race
                // window on pending_requests_). Stale entries are cleaned by
                // on_disconnect / block timeouts / piece completion.
                {
                    std::lock_guard lock(piece_progress->piece_mutex_);
                    if (block_idx < piece_progress->outstanding_requests.size()) {
                        auto& outstanding = piece_progress->outstanding_requests[block_idx];
                        if (std::find(outstanding.begin(), outstanding.end(), replacement->peer_id()) == outstanding.end()) {
                            outstanding.push_back(replacement->peer_id());
                        }
                    }
                }
                asio::co_spawn(io_context_,
                    [replacement, piece_index, offset, length]() -> asio::awaitable<void> {
                        co_await replacement->send_request(piece_index, offset, length);
                    },
                    asio::detached
                );
                spawned_any = true;
            }
        }

        if (spawned_any) {
            LOGINFO("Resuming download for piece {}. Requesting {} missing blocks.", piece_index, missing_indices.size());
            co_return;
        }

        // Nothing could be placed: no unchoked peer has the piece, or every
        // eligible peer has recently rejected these blocks. The old 1s poll
        // made a piece whose only peer keeps rejecting re-hammer it every
        // second; the rejection window is 60s (kRejectedBlockTTL), so a 10s
        // idle poll costs nothing in recovery latency. (The first pass runs
        // immediately — the wait is only for idle retries.)
        timer.expires_after(std::chrono::seconds(10));
        try {
            co_await timer.async_wait(asio::use_awaitable);
        } catch (const boost::system::system_error& e) {
            if (e.code() == asio::error::operation_aborted) {
                co_return;
            }
            throw;
        }
    }
}

std::vector<PeerId> PieceManager::pruned_rejected_peers(const std::shared_ptr<InProgressPiece>& progress, uint32_t block_idx) {
    std::vector<PeerId> result;
    if (!progress || block_idx >= progress->rejected_by.size()) {
        return result;
    }
    auto now = std::chrono::steady_clock::now();
    std::lock_guard lock(progress->piece_mutex_);
    auto& rejected = progress->rejected_by[block_idx];
    std::erase_if(rejected, [&](const auto& entry) {
        return now - entry.second > InProgressPiece::kRejectedBlockTTL;
    });
    for (const auto& [pid, when] : rejected) {
        result.push_back(pid);
    }
    return result;
}

void PieceManager::record_block_rejection(size_t piece_index, uint32_t block_idx, const PeerId& peer_id) {
    auto progress = in_progress_piece(piece_index);
    if (!progress || block_idx >= progress->rejected_by.size()) {
        return;
    }
    auto now = std::chrono::steady_clock::now();
    std::lock_guard lock(progress->piece_mutex_);
    auto& rejected = progress->rejected_by[block_idx];
    for (auto& [pid, when] : rejected) {
        if (pid == peer_id) {
            when = now;  // refresh the rejection window
            return;
        }
    }
    rejected.emplace_back(peer_id, now);
}

asio::awaitable<std::shared_ptr<PeerConnection>> PieceManager::pick_block_peer(
    size_t piece_index, uint32_t block_idx, const PeerId* exclude_peer, bool exclude_outstanding)
{
    auto progress = in_progress_piece(piece_index);
    if (!progress || block_idx >= progress->total_blocks) {
        co_return nullptr;
    }

    auto rejected = pruned_rejected_peers(progress, block_idx);

    std::vector<PeerId> outstanding;
    if (exclude_outstanding) {
        std::lock_guard lock(progress->piece_mutex_);
        if (block_idx < progress->outstanding_requests.size()) {
            outstanding = progress->outstanding_requests[block_idx];
        }
    }

    auto available_peers = co_await get_available_peers_(piece_index);

    std::vector<std::shared_ptr<PeerConnection>> candidates;
    for (const auto& peer : available_peers) {
        if (exclude_peer && peer->peer_id() == *exclude_peer) continue;
        if (std::ranges::find(rejected, peer->peer_id()) != rejected.end()) continue;
        if (std::ranges::find(outstanding, peer->peer_id()) != outstanding.end()) continue;
        candidates.push_back(peer);
    }

    if (candidates.empty()) {
        co_return nullptr;
    }

    std::uniform_int_distribution<size_t> dist(0, candidates.size() - 1);
    co_return candidates[dist(rng_)];
}

asio::awaitable<void> PieceManager::check_block_timeouts() {
    CTRACK_ASYNC("PieceManager::check_block_timeouts");
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

            // Record the peers that failed us so the re-request goes elsewhere.
            // (pick_block_peer would otherwise re-target the same peer, since
            // the timeout callback clears the outstanding-request bookkeeping.)
            {
                std::vector<PeerId> timed_out_peers;
                {
                    std::lock_guard lock(piece_progress->piece_mutex_);
                    if (block_idx < piece_progress->outstanding_requests.size()) {
                        timed_out_peers = piece_progress->outstanding_requests[block_idx];
                    }
                }
                for (const auto& pid : timed_out_peers) {
                    record_block_rejection(piece_idx, block_idx, pid);
                }
            }

            if (block_timeout_callback_) {
                co_await block_timeout_callback_(static_cast<uint32_t>(piece_idx), block_idx);
            }

            auto replacement = co_await pick_block_peer(piece_idx, block_idx, nullptr, /*exclude_outstanding=*/false);
            if (replacement) {
                uint32_t offset = block_idx * BLOCK_SIZE;
                uint32_t length = (block_idx == piece_progress->total_blocks - 1)
                    ? (piece_progress->data.size() - offset)
                    : BLOCK_SIZE;

                // Eager outstanding registration (same reasoning as the
                // resumer): the timeout pass can race a concurrent resumer on
                // the same block; registering up front keeps both from
                // double-spawning the request.
                {
                    std::lock_guard lock(piece_progress->piece_mutex_);
                    if (block_idx < piece_progress->outstanding_requests.size()) {
                        auto& outstanding = piece_progress->outstanding_requests[block_idx];
                        if (std::find(outstanding.begin(), outstanding.end(), replacement->peer_id()) == outstanding.end()) {
                            outstanding.push_back(replacement->peer_id());
                        }
                    }
                }
                asio::co_spawn(io_context_,
                    [replacement, piece_idx, offset, length]() -> asio::awaitable<void> {
                        co_await replacement->send_request(piece_idx, offset, length);
                    },
                    asio::detached
                );
            } else {
                ensure_resume_piece_download(piece_idx);
            }
        }
    }
}

asio::awaitable<void> PieceManager::block_timeout_loop() {
    CTRACK_ASYNC("PieceManager::block_timeout_loop");
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
