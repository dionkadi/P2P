#pragma once

#include <atomic>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <unordered_set>
#include <utility>

#include "Utils.hpp"
#include "SessionState.hpp"
#include "PeerConnection.hpp"

static constexpr uint32_t IN_PROGRESS_RARITY_GROUP_ID = std::numeric_limits<uint32_t>::max();
// How long a written block request may go unanswered before it is re-requested
// from another peer. libtorrent re-requests stalled blocks after
// request_queue_time=3s. 12s was 4x too slow: with peers unchoking us in
// bursts, a wave of requests that a peer stops serving all timed out together
// 12s later (observed: 446 blocks stalling at once, right after the unchoke
// burst), blacklisting busy-but-healthy peers and decaying the pipeline. 4s
// recycles stalled requests fast enough to keep the window full while
// tolerating normal peer latency and burst scheduling.
static constexpr auto BLOCK_REQUEST_TIMEOUT = std::chrono::seconds(4);

// Activity-aware timeout deferral (libtorrent request_queue_time semantics):
// a peer that sent ANY block within this window is making progress — its
// queue is just deep (whole-piece primaries sit 16-deep behind the swarm's
// other clients), so the flat 4s budget would fire on healthy tail latency,
// cancelling in-flight deliveries (the late-duplicate CANCEL storm). Blocks
// whose owning peer is still delivering are NOT timed out. kHardRequestCap
// bounds trickle peers that send something every <10s but never reach ours.
//
// 10s was too short: on an oversubscribed shared seeder, whole-piece
// primaries deliver in 10-80s bursts (their per-connection share collapses
// ~10x as the swarm grows), so a 10s window fired false batch timeouts,
// cleared outstanding_requests, made the resumer re-request the same primary
// -> duplicate REJECT -> demote -> 40-84s piece stalls (observed: 724
// timeouts, 244 REJECTs, 20 demotes, all downstream of the timeout decision).
// 30s matches the measured inter-burst cadence; kHardRequestCap (60s) still
// bounds genuinely dead primaries.
static constexpr auto kPeerActivityWindow = std::chrono::seconds(30);
static constexpr auto kHardRequestCap = std::chrono::seconds(60);

// kPrimaryEvidenceWindow is the ELIGIBILITY window for the primary filter:
// a peer that delivered a block within this window may own primary pieces.
// Deliberately much longer than kPeerActivityWindow: the timeout deferral
// must stay tight (30s bounds silent queues), but the filter should admit
// every proven server, not just the currently-bursting subset. On an
// oversubscribed shared seeder, peers deliver in 10-80s bursts; a 30s
// eligibility window parked the 64-piece window on only the mid-burst
// handful and the pool self-locked (evicted peers cannot re-evidence
// without outstanding requests, and new pieces only go to evidenced peers)
// -> 600-700 KB/s floor instead of 3-4 MB/s. 5 minutes admits all proven
// servers while still excluding never-delivered leechers (whose contamination
// caused the ~100 KB/s blind-rotation collapse).
static constexpr auto kPrimaryEvidenceWindow = std::chrono::minutes(5);

// Fresh-unchoke discovery: freshly-unchoked peers (last UNCHOKE within
// kFreshUnchokeWindow, not yet delivery-proven) are admitted to BOTH the
// primary pool (try_piece_download: evidenced ∪ fresh, whole-piece deep
// queues) and a 1/kDiscoveryExplorationDivisor share of block-level picks.
// The swarm unchokes us in ~5s-average windows; a fresh seeder's window is
// only harvestable with a deep queue, and without it the proven set can only
// SHRINK (choke, disconnect, 5-min expiry) — newly-unchoked seeders never
// deliver and a run locks into leecher recycling (observed: 820 KB/s peak,
// never MB/s; a run 3 minutes later hit 3.3 MB/s only because it caught
// seeders at cold start). Fresh-primary contamination is bounded by the
// proven-only primary preference (pick_block_peer_preferring_primary): a
// silent fresh primary costs one 4s batch-timeout per piece. Exploration
// stays block-level — a silent peer costs one 4s timeout per pick, never a
// deep queue.
static constexpr auto kFreshUnchokeWindow = std::chrono::seconds(15);
static constexpr size_t kDiscoveryExplorationDivisor = 8;

// libtorrent initial_picker_threshold: pick this many pieces RANDOMLY at the
// start of a download instead of rarest-first, so a fresh leecher quickly
// gains pieces it can upload — earning regular (non-optimistic) tit-for-tat
// unchoke slots from seeders instead of only the rotating optimistic slot.
static constexpr size_t kInitialPickerThreshold = 4;

class PieceManager : public std::enable_shared_from_this<PieceManager> {
public:
    using GetAvailableCallback = std::function<asio::awaitable<std::vector<std::shared_ptr<PeerConnection>>>(size_t)>;
    using BlockTimeoutCallback = std::function<asio::awaitable<void>(uint32_t piece_index, uint32_t block_index)>;
    // Returns the time this peer last delivered us a block (nullopt if never
    // or unknown). Used by the timeout checker to defer re-requesting blocks
    // whose owning peer is still actively serving (deep-queue tail latency).
    using PeerActivityCheck = std::function<std::optional<TimePoint>(const PeerId&)>;
    using InProgressType = std::shared_ptr<const std::map<size_t, std::shared_ptr<InProgressPiece>>>;
    using AvailType = std::shared_ptr<const std::vector<size_t>>;
    using RarityType = std::shared_ptr<const std::map<size_t, std::shared_ptr<std::unordered_set<int>>>>;

    PieceManager(asio::io_context& io_context, std::shared_ptr<SessionState> state);

    InProgressType in_progress_pieces() const noexcept { 
        std::lock_guard lock(mutex_);
        return in_progress_pieces_;
    }
    std::shared_ptr<const InProgressPiece> in_progress_piece(size_t piece_index) const noexcept { 
        assert(piece_index < state_->num_pieces());
        std::lock_guard lock(mutex_);
        if (in_progress_pieces_->count(piece_index)) {
            return in_progress_pieces_->at(piece_index); 
        }
        return nullptr;
    }
    std::shared_ptr<InProgressPiece> in_progress_piece(size_t piece_index) noexcept { 
        assert(piece_index < state_->num_pieces());
        std::lock_guard lock(mutex_);
        if (in_progress_pieces_->count(piece_index)) {
            return in_progress_pieces_->at(piece_index); 
        }
        return nullptr;
    }
    AvailType piece_availability() const noexcept { 
        std::lock_guard lock(mutex_);
        return piece_availability_; 
    }
    size_t piece_availability(size_t piece_index) const {
        assert(piece_index < state_->num_pieces());
        std::lock_guard lock(mutex_);
        if (piece_index >= piece_availability_->size()) return 0;
        return piece_availability_->at(piece_index); 
    }
    RarityType pieces_by_rarity() const { 
        std::lock_guard lock(mutex_);
        return pieces_by_rarity_; 
    };
    RarityType pieces_by_rarity() { 
        std::lock_guard lock(mutex_);
        return pieces_by_rarity_; 
    };
    // Locked deep copy of the rarity map for iteration. The underlying sets
    // are mutated in place under mutex_ (update_piece_rarity), so iterating
    // the shared snapshot without the lock races concurrent updates (same
    // crash class as the in_progress_pieces_ iteration race).
    std::map<size_t, std::vector<int>> snapshot_pieces_by_rarity() const {
        std::lock_guard lock(mutex_);
        std::map<size_t, std::vector<int>> out;
        for (const auto& [rarity, set] : *pieces_by_rarity_) {
            out.emplace(rarity, std::vector<int>(set->begin(), set->end()));
        }
        return out;
    }
    void add_piece_availability(size_t piece_index, int32_t val) { 
        std::lock_guard lock(mutex_);
        // The vector may have been cleared by signal_shutdown() while a late
        // HAVE/disconnect handler is still running — skip rather than assert
        // on an out-of-bounds operator[] (the piece index itself is valid).
        if (piece_index >= piece_availability_->size()) return;
        (*piece_availability_)[piece_index] += val; 
    }

    void update_piece_rarity(size_t piece_index, uint32_t old_rarity, uint32_t new_rarity);
    void remove_piece_rarity(size_t piece_index, uint32_t rarity);
    void build_piece_rarity();

    asio::awaitable<void> resume_piece_download(size_t piece_index);
    void ensure_resume_piece_download(size_t piece_index);
    asio::awaitable<void> broadcast_outstanding_requests();
    
    asio::awaitable<void> downloader();  
    asio::awaitable<void> request_one_piece();
    asio::awaitable<void> check_and_enter_endgame();
    asio::awaitable<void> return_piece_to_queue(size_t piece_index);

    // All three shared structures (in_progress_pieces_, pieces_by_rarity_,
    // piece_availability_) are published as shared_ptr<const T> snapshots
    // (e.g. TorrentSession::on_disconnect iterates *in_progress_pieces()).
    // Mutations must therefore be copy-on-write: replace the shared_ptr with
    // a new copy instead of mutating the shared object in place. In-place
    // mutation raced concurrent snapshot iteration (std::map rebalancing
    // during insert/erase) and crashed on_disconnect with a dangling
    // iterator (SEGV at resume). The maps are tiny (<= max_in_progress_pieces
    // entries), so the copies are cheap.
    template<typename... Args>
    void emplace_in_progress_pieces(Args... args) { 
        std::lock_guard lock(mutex_);
        auto new_map = std::make_shared<std::map<size_t, std::shared_ptr<InProgressPiece>>>(*in_progress_pieces_);
        new_map->emplace(std::forward<Args>(args)...);
        in_progress_pieces_ = std::move(new_map);
    }
    void remove_in_progress_piece(size_t piece_index) {
        assert(piece_index < state_->num_pieces());
        std::lock_guard lock(mutex_);
        auto new_map = std::make_shared<std::map<size_t, std::shared_ptr<InProgressPiece>>>(*in_progress_pieces_);
        new_map->erase(piece_index);
        in_progress_pieces_ = std::move(new_map);
    }
    void remove_all_in_progress_pieces() {
        std::lock_guard lock(mutex_);
        in_progress_pieces_ = std::make_shared<std::map<size_t, std::shared_ptr<InProgressPiece>>>();
    }

    std::map<std::string, std::string> get_in_progress_for_resume() const;
    void notify_one() noexcept { piece_request_trigger_.cancel_one(); }
    void set_callback(GetAvailableCallback cb) { 
        std::lock_guard lock(mutex_);
        get_available_peers_ = std::move(cb); 
    }
    // Source for the count of peers that have unchoked us; the downloader
    // scales the in-flight piece window to this so it never commits a fixed
    // 32-piece window to a single newly-unchoked peer.
    void set_unchoked_count_callback(std::function<size_t()> cb) {
        std::lock_guard lock(mutex_);
        get_unchoked_count_ = std::move(cb);
    }
    void set_block_timeout_callback(BlockTimeoutCallback cb) { 
        std::lock_guard lock(mutex_);
        block_timeout_callback_ = std::move(cb); 
    }
    void set_peer_activity_check(PeerActivityCheck cb) {
        std::lock_guard lock(mutex_);
        peer_activity_check_ = std::move(cb);
    }

    // Scans all in-progress pieces for blocks whose request has exceeded BLOCK_REQUEST_TIMEOUT.
    // On timeout: calls the timeout callback (cancel) and re-requests from another peer.
    asio::awaitable<void> check_block_timeouts();

    // Picks a peer to request `block_idx` of `piece_index` from.
    // Excludes peers that recently REJECTED this block, peers already
    // carrying an outstanding request for it (when exclude_outstanding), and
    // the optional exclude_peer. Returns nullptr when no suitable peer exists.
    // Random selection (instead of deterministic round-robin) prevents the
    // same peer being re-targeted on every retry.
    asio::awaitable<std::shared_ptr<PeerConnection>> pick_block_peer(
        size_t piece_index, uint32_t block_idx, const PeerId* exclude_peer = nullptr,
        bool exclude_outstanding = true);
    // Picks a peer for a missing block, PREFERRING the piece's primary_peer
    // (the deep-queue peer chosen at seed time) so its request queue stays
    // continuously deep; falls back to a random peer only when the primary is
    // choking, rejected this block, or already serving it.
    asio::awaitable<std::shared_ptr<PeerConnection>> pick_block_peer_preferring_primary(
        size_t piece_index, uint32_t block_idx, const std::shared_ptr<InProgressPiece>& progress,
        bool exclude_outstanding = true);

    // Records that `peer_id` rejected (or failed) block `block_idx` of
    // `piece_index`. The peer is then excluded from re-requests for that block
    // until the entry expires (InProgressPiece::kRejectedBlockTTL).
    void record_block_rejection(size_t piece_index, uint32_t block_idx, const PeerId& peer_id);

    // Signal that shutdown is in progress (called by TorrentSession::stop())
    void signal_shutdown() noexcept {
        shutting_down_ = true;
        block_timeout_timer_.cancel();
        piece_request_trigger_.cancel();
        std::lock_guard lock(mutex_);
        // Break shared_ptr cycle: callbacks capture shared_from_this() of TorrentSession;
        // without clearing them TorrentSession never destructs → FileManager cache_ leaks.
        get_available_peers_ = nullptr;
        block_timeout_callback_ = nullptr;
        peer_activity_check_ = nullptr;
        // Free heap-allocated data structures immediately so memory is released
        // even if the PieceManager itself is not destroyed (e.g. due to reference
        // cycles keeping TorrentSession alive past shutdown). Copy-on-write:
        // replace, don't mutate in place (see emplace_in_progress_pieces).
        pieces_by_rarity_ = std::make_shared<std::map<size_t, std::shared_ptr<std::unordered_set<int>>>>();
        piece_availability_ = std::make_shared<std::vector<size_t>>();
        in_progress_pieces_ = std::make_shared<std::map<size_t, std::shared_ptr<InProgressPiece>>>();
    }

    // Runs all PieceManager state access (in_progress_pieces_, rng_, ...).
    const auto& strand() const noexcept { return strand_; }

private:

    asio::awaitable<bool> try_piece_download(size_t piece_index);

    asio::awaitable<void> block_timeout_loop();

    // Expired entries are pruned; returns the surviving rejecting peer ids.
    std::vector<PeerId> pruned_rejected_peers(const std::shared_ptr<InProgressPiece>& progress, uint32_t block_idx);

    std::mt19937 rng_{std::random_device{}()};

    // Rotating counter for whole-piece primary assignment. available_peers
    // preserves get_all_connections() map order (lexicographic by peer id), so
    // picking .front() deterministically gave EVERY window piece to the same
    // peer (e.g. -BI4100/-DE211s/-DE220s sort before all -qB/-TR peers). When
    // that peer is a persistent rejector, its pipeline overflows (per-peer cap
    // 512 blocks vs a 64-piece window) and it flushes hundreds of REJECTs at
    // once, collapsing throughput (observed: 494 rejects in 40ms, 512/min ->
    // 37/min). Rotating the index across the unchoked set spreads the window
    // over distinct primaries (each keeps its 16-deep queue), bounding any
    // single rejector's blast radius to ~1-2 pieces. Atomic so concurrent
    // request_one_piece coroutines pick distinct indices.
    std::atomic<size_t> primary_rotor_{0};

    asio::io_context& io_context_;
    asio::strand<asio::io_context::executor_type> strand_;
    asio::steady_timer piece_request_trigger_;
    asio::steady_timer block_timeout_timer_;
    std::shared_ptr<std::map<size_t, std::shared_ptr<InProgressPiece>>> in_progress_pieces_;
    std::shared_ptr<std::vector<size_t>> piece_availability_;
    std::shared_ptr<std::map<size_t, std::shared_ptr<std::unordered_set<int>>>> pieces_by_rarity_;
    std::shared_ptr<SessionState> state_;
    mutable std::mutex mutex_;
    GetAvailableCallback get_available_peers_;
    BlockTimeoutCallback block_timeout_callback_;
    PeerActivityCheck peer_activity_check_;
    std::function<size_t()> get_unchoked_count_;
    std::atomic<bool> shutting_down_{false};
};