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
    // Scale the in-flight window to the currently-unchoked peer set. A fixed
    // 5..32 window commits the whole window when only ~1 peer has unchoked us
    // (the swarm unchokes us staggered over the next ~12s, too late to absorb
    // an already-locked window). Observed: 32 pieces x 16 blocks = 512 blocks
    // flooded onto the single first-unchoked peer -> 446-block timeout burst
    // exactly 12s later. Measure the window against live unchoked peers so it
    // stays small at cold-start and grows as peers stagger in (each UNCHOKE
    // fires notify_one, waking the downloader to refill).
    size_t max_in_progress_pieces = 1;

    // Start the periodic block timeout checker
    asio::co_spawn(io_context_, self->block_timeout_loop(), asio::detached);

    while (!state_->is_download_complete() && !shutting_down_) {
        int slots_to_fill = 0;

        co_await asio::dispatch(strand_, asio::use_awaitable);

        if (shutting_down_) break;

        // Refit the in-flight window to the live unchoked set each wake. Each
        // UNCHOKE fires notify_one, so the window grows as the swarm staggers
        // in instead of having been locked too small (or flooded onto 1 peer).
        size_t unchoked = get_unchoked_count_ ? get_unchoked_count_() : 1;
        if (unchoked < 1) unchoked = 1;
        // kPiecesPerPeer whole pieces per unchoked peer: the old 1:1 formula
        // capped in-flight at the instant-unchoked set, so each primary held
        // ONE piece and idled ~7s between pieces (queue-empty -> snub-choke ->
        // churn). Depth 4 = ~29s of continuous queue per server: unchokes
        // hold and the serving set grows. Clamped to kMaxInFlightPieces; the
        // per-peer pipeline gate (500 requests / 8 MiB) carries 128*16 blocks
        // easily (~82 blocks/server over 25 servers). Cold start: 1 unchoked
        // peer -> 4 pieces = 64 blocks (8x smaller than the old 512-block
        // flood); the window scales up as the swarm staggers in.
        max_in_progress_pieces = std::clamp<size_t>(unchoked * kPiecesPerPeer, 8, kMaxInFlightPieces);

        size_t current_in_progress = in_progress_pieces()->size();
        if (current_in_progress < max_in_progress_pieces) {
            slots_to_fill = std::min(max_in_progress_pieces - current_in_progress, state_->needed_pieces());
        }

        if (slots_to_fill > 0) {
            // Bound spawns per wake: a drained window could otherwise spawn
            // (128 - 0) fills per wake ~ 640/s (each ~50-200us of rarity-map
            // copy + shuffle). 32 x ~5 wakes/s = 160 attempts/s is ~80x the
            // ~2 completions/s and costs nothing.
            slots_to_fill = std::min(slots_to_fill, static_cast<int>(kMaxSpawnsPerWake));
            for (int i = 0; i < slots_to_fill; ++i) {
                asio::co_spawn(io_context_, self->request_one_piece(), asio::detached);
            }
        }

        // Watchdog wake: bounded expiry so the loop always re-evaluates at
        // least once per second even if every notify_one is lost. The original
        // expires_at(max) slept until the next cancel_one — a notify firing
        // between expires_at and async_wait (or a quiet swarm) left the loop
        // asleep forever with an EMPTY window while needed pieces sat
        // undownloaded (observed: 3 good minutes then permanent 0 B/s; the
        // loop's wake logs stopped mid-run while the window drained via
        // completions and unchokes kept firing notify_one).
        piece_request_trigger_.expires_after(std::chrono::seconds(1));
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

    // Chain-refill: a primary whose piece just completed is queued; seed its
    // next piece to the SAME peer so its queue never drains (seeders choke
    // rate-zero peers — the ~4.9s avg unchoke window == drain+idle cycle).
    // Folded into the slot-fill path so the chain consumes exactly the window
    // slot freed by the completion instead of racing the rotor.
    PeerId chain_primary;
    bool have_chain = false;
    {
        std::lock_guard lock(mutex_);
        if (!pending_chains_.empty()) {
            chain_primary = pending_chains_.front();
            pending_chains_.pop_front();
            have_chain = true;
        }
    }
    if (have_chain) {
        LOGDBG("Chain: popped completing primary {}", chain_primary);
        if (co_await try_seed_to_primary(chain_primary)) {
            co_return;
        }
        // No needed piece available on the chained primary (it may lack the
        // remaining rare pieces or got choked) — fall through to the normal
        // rotor-based selection below.
        LOGDBG("Chain: no piece available on primary {}; rotor fallback", chain_primary);
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

asio::awaitable<bool> PieceManager::try_seed_to_primary(const PeerId& primary) {
    for (const auto& [rarity, piece_set] : snapshot_pieces_by_rarity()) {
        if (rarity == 0) continue;  // Only actually-available pieces (rarity > 0)
        std::vector<int> candidates(piece_set.begin(), piece_set.end());
        std::shuffle(candidates.begin(), candidates.end(), std::mt19937{std::random_device{}()});
        for (size_t piece_index : candidates) {
            if (co_await try_piece_download(piece_index, &primary)) {
                co_return true;
            }
        }
    }
    co_return false;
}

asio::awaitable<void> PieceManager::engage_unchoked_peer_impl(PeerId peer) {
    CTRACK_ASYNC("PieceManager::engage_unchoked_peer_impl");
    if (state_->is_download_complete() || state_->is_in_endgame_mode()) {
        co_return;
    }
    if (co_await try_seed_to_primary(peer)) {
        LOGDBG("Engage: seeded a piece to freshly-unchoked peer {}", peer);
    }
}

void PieceManager::engage_unchoked_peer(const PeerId& peer_id) noexcept {
    if (state_->is_download_complete() || state_->is_in_endgame_mode()) {
        return;
    }
    auto self = shared_from_this();
    asio::co_spawn(io_context_, self->engage_unchoked_peer_impl(peer_id), asio::detached);
}

asio::awaitable<bool> PieceManager::try_piece_download(size_t piece_index, const PeerId* preferred) {
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

    // Whole-piece assignment to a PRIMARY peer (qBittorrent model): give one
    // serving peer a deep queue (all blocks of the piece) instead of spreading
    // 2 blocks across every unchoked peer. Shallow 2-block queues made every
    // request a 4s tail-latency gamble (~9 req/peer, 53% timeout rate), because
    // the peer's FIFO had our block buried behind the rest of the swarm's
    // requests. A whole piece on one peer completes blocks back-to-back
    // (p -> 1 for that peer), and the resumer refills the SAME peer, keeping
    // the deep queue continuous.
    //
    // PREFER primaries with DELIVERY EVIDENCE: only ~16 of the ~222 unchoked
    // peers actually send blocks, and blind rotation parked ~48 of the 64
    // window pieces on silent leecher primaries whose 16 blocks all
    // batch-timeout at 4s (piece latencies 50-70s, ~100 KB/s instead of MB/s).
    // Concentrate the window on the proven servers — they get deep queues
    // (~4 pieces each = 64 blocks) spread across the actual workhorses. Silent
    // peers receive no primary assignment until they prove themselves; cold
    // start falls back to the full rotor for the first pieces. Eligibility
    // uses kPrimaryEvidenceWindow (5 min), NOT the 30s kPeerActivityWindow
    // used for timeout deferral — a peer between 40-80s bursts must stay
    // eligible or the pool collapses onto the mid-burst subset.
    //
    // ADMIT freshly-unchoked peers too (evidenced ∪ fresh): the swarm unchokes
    // us in ~5s-average windows, and a fresh seeder's window is only
    // harvestable with a whole-piece deep queue — block-level exploration
    // (1-in-8 single blocks) wastes it. Fresh-primary contamination is
    // bounded by the proven-only preference in
    // pick_block_peer_preferring_primary: once a fresh primary's batch times
    // out or it chokes, the preference is skipped and blocks re-spread to
    // proven/fresh peers — a silent leecher primary costs one 4s
    // batch-timeout, never a permanently stuck piece.
    //
    auto now_ev = std::chrono::steady_clock::now();
    std::vector<std::shared_ptr<PeerConnection>> primary_pool;
    if (peer_activity_check_) {
        for (const auto& p : available_peers) {
            bool is_proven = false;
            auto last = peer_activity_check_(p->peer_id());
            if (last && now_ev - *last < kPrimaryEvidenceWindow) {
                primary_pool.push_back(p);
                is_proven = true;
            }
            if (!is_proven) {
                auto unchoked = p->last_unchoke_time();
                if (unchoked != std::chrono::steady_clock::time_point{} && now_ev - unchoked < kFreshUnchokeWindow) {
                    primary_pool.push_back(p);
                }
            }
        }
    }
    // Chain-refill: a preferred primary (its piece just completed) keeps the
    // next piece too, so its queue never drains. It is present in
    // available_peers iff still unchoked, and having just delivered a
    // verified piece it is trivially proven — the preference is safe without
    // re-checking the pool.
    std::shared_ptr<PeerConnection> primary;
    if (preferred) {
        for (const auto& p : available_peers) {
            if (p->peer_id() == *preferred) {
                primary = p;
                LOGDBG("Chain: piece {} seeded to completing primary {}", piece_index, *preferred);
                break;
            }
        }
    }
    if (!primary) {
        auto& pool = primary_pool.empty() ? available_peers : primary_pool;
        primary = pool[primary_rotor_.fetch_add(1, std::memory_order_relaxed) % pool.size()];
    }
    {
        std::lock_guard lock(piece_progress->piece_mutex_);
        piece_progress->primary_peer = primary->peer_id();
    }
    size_t seed_count = num_blocks;
    for (uint32_t block_idx = 0; block_idx < seed_count; ++block_idx) {
        uint32_t offset = block_idx * BLOCK_SIZE;
        uint32_t length = (block_idx == num_blocks - 1) 
            ? (piece_progress->data.size() - offset)
            : BLOCK_SIZE;
        
        auto& peer_conn = primary;

        // Eager outstanding registration: the notify hook (record_request_sent)
        // only fires AFTER the async write completes, so without registering up
        // front the resume loop (spawned right below for the remaining blocks)
        // sees every seeded block as "missing" and re-requests ALL 16 - an
        // immediate double-commit on the same peer. Mirror the resumer's
        // registration so seeded blocks are treated as in-flight immediately.
        {
            std::lock_guard lock(piece_progress->piece_mutex_);
            if (block_idx < piece_progress->outstanding_requests.size()) {
                auto& outstanding = piece_progress->outstanding_requests[block_idx];
                if (std::find(outstanding.begin(), outstanding.end(), peer_conn->peer_id()) == outstanding.end()) {
                    outstanding.push_back(peer_conn->peer_id());
                }
            }
        }
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

    // Endgame redundancy only for the true tail: the 10% threshold fired at
    // 92.4% completion (840 of 11134 pieces = 215 MB still to go), flooding
    // every unchoked peer with duplicate requests for all 840 pieces —
    // observed 13,151 REJECTs and the last 11 pieces stuck missing their
    // final blocks while the flood kept them blacklisted at 99.9%. Cap the
    // trigger at 64 pieces (a bounded redundancy window for the real tail).
    size_t endgame_threshold = std::min<size_t>(state_->num_pieces() / 10, kMaxInFlightPieces);
    if (needed_count > 0 && needed_count < endgame_threshold) {
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
                // Normal mode: prefer the piece's PRIMARY peer (deep-queue
                // model). The seed assigned the whole piece to one peer and
                // recorded it in primary_peer; refilling the SAME peer keeps
                // its request queue continuously deep so blocks complete
                // back-to-back instead of each being a 4s tail-latency gamble
                // buried behind the swarm's other requests. Fall back to a
                // random peer only when the primary is choking, has rejected
                // this block, or is already serving it.
                auto replacement = co_await pick_block_peer_preferring_primary(
                    piece_index, block_idx, piece_progress, true);
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
        timer.expires_after(std::chrono::seconds(2));
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
    std::vector<std::shared_ptr<PeerConnection>> proven;
    std::vector<std::shared_ptr<PeerConnection>> fresh;
    auto now_ev = std::chrono::steady_clock::now();
    for (const auto& peer : available_peers) {
        if (exclude_peer && peer->peer_id() == *exclude_peer) continue;
        if (std::ranges::find(rejected, peer->peer_id()) != rejected.end()) continue;
        if (std::ranges::find(outstanding, peer->peer_id()) != outstanding.end()) continue;
        candidates.push_back(peer);
        bool is_proven = false;
        if (peer_activity_check_) {
            auto last = peer_activity_check_(peer->peer_id());
            if (last && now_ev - *last < kPrimaryEvidenceWindow) {
                proven.push_back(peer);
                is_proven = true;
            }
        }
        if (!is_proven) {
            auto unchoked = peer->last_unchoke_time();
            if (unchoked != std::chrono::steady_clock::time_point{} && now_ev - unchoked < kFreshUnchokeWindow) {
                fresh.push_back(peer);
            }
        }
    }

    if (candidates.empty()) {
        co_return nullptr;
    }

    // Prefer delivery-proven peers: most unchoked peers have the piece but
    // never send blocks (leechers), and a random pick lands on one ~197/200
    // times — the recycling loop (choke -> resume -> random leecher -> 4s
    // timeout -> re-pick, ~4/s) that floors throughput between seeder unchoke
    // bursts. Only fall back to unproven candidates when no proven peer can
    // serve this block (cold start, rare pieces).
    //
    // A fraction of picks explores the fresh pool (recently unchoked, not yet
    // proven) instead: without it the proven set can only shrink (choke,
    // disconnect, 5-min expiry) while unproven peers get no requests, so
    // newly-unchoked seeders can never deliver and join the set — a run locks
    // into leecher recycling (observed: 820 KB/s peak, never MB/s; a run 3
    // minutes later hit 3.3 MB/s only because it caught seeders at cold
    // start). Exploration is block-level only — a silent peer costs one 4s
    // timeout per pick, never a deep queue.
    std::uniform_int_distribution<size_t> explore_dist(0, kDiscoveryExplorationDivisor - 1);
    const auto& pick_from = (!fresh.empty() && explore_dist(rng_) == 0)
        ? fresh
        : (!proven.empty() ? proven : (!fresh.empty() ? fresh : candidates));
    std::uniform_int_distribution<size_t> dist(0, pick_from.size() - 1);
    co_return pick_from[dist(rng_)];
}

asio::awaitable<std::shared_ptr<PeerConnection>> PieceManager::pick_block_peer_preferring_primary(
    size_t piece_index, uint32_t block_idx, const std::shared_ptr<InProgressPiece>& progress, bool exclude_outstanding)
{
    if (!progress || block_idx >= progress->total_blocks) {
        co_return nullptr;
    }

    auto rejected = pruned_rejected_peers(progress, block_idx);

    std::vector<PeerId> outstanding;
    PeerId primary;
    bool has_primary = false;
    {
        std::lock_guard lock(progress->piece_mutex_);
        if (progress->primary_peer) {
            primary = *progress->primary_peer;
            has_primary = true;
        }
        if (exclude_outstanding && block_idx < progress->outstanding_requests.size()) {
            outstanding = progress->outstanding_requests[block_idx];
        }
    }

    auto now = std::chrono::steady_clock::now();

    // The primary preference applies only while the primary is PROVEN
    // (delivered within kPrimaryEvidenceWindow). Fresh primaries (seeded via
    // the evidenced ∪ fresh pool) must not keep capturing re-requests if they
    // never deliver: once their batch times out or they choke, skip the
    // preference and clear primary_peer so blocks re-spread to proven/fresh
    // peers (mirrors the REJECT demote at TorrentSession.cpp:1250-1253). This
    // bounds silent-leecher contamination from fresh primaries to one 4s
    // batch-timeout per piece instead of a permanently stuck piece. A working
    // fresh primary delivers its first block in <1s and flips proven before
    // any resume pass needs the preference, so pick-time is a sufficient
    // grace — no deadline state to manage.
    if (has_primary && peer_activity_check_) {
        auto last = peer_activity_check_(primary);
        if (!last || now - *last >= kPrimaryEvidenceWindow) {
            std::lock_guard lock(progress->piece_mutex_);
            if (progress->primary_peer && *progress->primary_peer == primary) {
                progress->primary_peer.reset();
            }
            has_primary = false;
        }
    }

    auto available_peers = co_await get_available_peers_(piece_index);

    // Prefer the piece's primary peer: it was chosen at seed time precisely to
    // carry a deep, continuous queue for this piece. Only fall back to a random
    // peer if the primary is not available (choked/disconnected), has rejected
    // this block, or is already serving it.
    if (has_primary) {
        for (const auto& peer : available_peers) {
            if (peer->peer_id() != primary) continue;
            if (std::ranges::find(rejected, peer->peer_id()) != rejected.end()) continue;
            if (std::ranges::find(outstanding, peer->peer_id()) != outstanding.end()) continue;
            co_return peer;
        }
    }

    // Fallback: random non-rejecting, non-outstanding peer, preferring
    // delivery-proven peers (see pick_block_peer) to keep re-requests off the
    // silent-leecher recycling path; a fraction of picks explores the fresh
    // pool so newly-unchoked seeders can deliver and join the proven set.
    std::vector<std::shared_ptr<PeerConnection>> candidates;
    std::vector<std::shared_ptr<PeerConnection>> proven;
    std::vector<std::shared_ptr<PeerConnection>> fresh;
    auto now_ev = std::chrono::steady_clock::now();
    for (const auto& peer : available_peers) {
        if (std::ranges::find(rejected, peer->peer_id()) != rejected.end()) continue;
        if (std::ranges::find(outstanding, peer->peer_id()) != outstanding.end()) continue;
        candidates.push_back(peer);
        bool is_proven = false;
        if (peer_activity_check_) {
            auto last = peer_activity_check_(peer->peer_id());
            if (last && now_ev - *last < kPrimaryEvidenceWindow) {
                proven.push_back(peer);
                is_proven = true;
            }
        }
        if (!is_proven) {
            auto unchoked = peer->last_unchoke_time();
            if (unchoked != std::chrono::steady_clock::time_point{} && now_ev - unchoked < kFreshUnchokeWindow) {
                fresh.push_back(peer);
            }
        }
    }

    if (candidates.empty()) {
        co_return nullptr;
    }

    std::uniform_int_distribution<size_t> explore_dist(0, kDiscoveryExplorationDivisor - 1);
    const auto& pick_from = (!fresh.empty() && explore_dist(rng_) == 0)
        ? fresh
        : (!proven.empty() ? proven : (!fresh.empty() ? fresh : candidates));
    std::uniform_int_distribution<size_t> dist(0, pick_from.size() - 1);
    co_return pick_from[dist(rng_)];
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
            std::vector<PeerId> outstanding_ids;
            {
                std::lock_guard lock(piece_progress->piece_mutex_);
                already_received = piece_progress->blocks_received[block_idx];
                request_time = piece_progress->request_times[block_idx];
                if (block_idx < piece_progress->outstanding_requests.size()) {
                    outstanding_ids = piece_progress->outstanding_requests[block_idx];
                }
            }

            if (already_received) continue;
            if (request_time == TimePoint{}) continue;

            if (now - request_time > BLOCK_REQUEST_TIMEOUT) {
                // Activity-aware deferral (libtorrent request_queue_time): a
                // block whose owning peer is STILL delivering data is not lost
                // — it is queued behind that peer's backlog (whole-piece
                // primaries sit 16-deep behind the swarm's other clients). The
                // flat 4s budget fires on healthy tail latency, cancelling
                // in-flight deliveries and spawning the late-duplicate CANCEL
                // storm (621 CANCELs, 2088 REJECTs, 705 resume passes all
                // downstream of fake timeouts). Only re-request when the owner
                // has gone silent; kHardRequestCap bounds trickle peers.
                bool still_active = false;
                if (peer_activity_check_ && !outstanding_ids.empty()) {
                    for (const auto& pid : outstanding_ids) {
                        auto last = peer_activity_check_(pid);
                        if (last && now - *last < kPeerActivityWindow) {
                            still_active = true;
                            break;
                        }
                    }
                }
                if (!still_active || now - request_time > kHardRequestCap) {
                    timed_out = true;
                }
            }

            if (!timed_out) continue;

            LOGWARN("Block {}/{} timed out after {}s. Cancelling and re-requesting.",
                    piece_idx, block_idx, std::chrono::duration_cast<std::chrono::seconds>(now - request_time).count());

            // Do NOT record_block_rejection here: a timeout is CONGESTION, not
            // hostility. The peer may simply be momentarily busy (the swarm
            // staggers its unchokes); blacklisting it for 15s would freeze a
            // healthy peer through the exact window it would recover in, shrinking
            // the serving set during a spike->decay. Explicit REJECT (a peer
            // decision to not serve) still blacklists via on_piece_rejected. The
            // re-request below already avoids re-hammering the same peer by
            // picking a replacement.

            if (block_timeout_callback_) {
                co_await block_timeout_callback_(static_cast<uint32_t>(piece_idx), block_idx);
            }

            // Exclude the stalled peer(s) from the re-request: the timeout
            // callback (send_cancel_for_block) clears the block's outstanding
            // entries, which would otherwise make the just-stalled primary
            // eligible again — the next pick would re-target the stall source
            // and burn a 4s+REJECT+demote cycle per dead primary (observed:
            // 12-16 REJECT batches right after each whole-piece batch timeout).
            const PeerId* exclude = outstanding_ids.empty() ? nullptr : &outstanding_ids.front();
            auto replacement = co_await pick_block_peer(piece_idx, block_idx, exclude, /*exclude_outstanding=*/true);
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
