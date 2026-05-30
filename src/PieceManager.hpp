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
#include <unordered_set>
#include <utility>

#include "Utils.hpp"
#include "SessionState.hpp"
#include "PeerConnection.hpp"

static constexpr uint32_t IN_PROGRESS_RARITY_GROUP_ID = std::numeric_limits<uint32_t>::max();
static constexpr auto BLOCK_REQUEST_TIMEOUT = std::chrono::seconds(30);

class PieceManager : public std::enable_shared_from_this<PieceManager> {
public:
    using GetAvailableCallback = std::function<asio::awaitable<std::vector<std::shared_ptr<PeerConnection>>>(size_t)>;
    using BlockTimeoutCallback = std::function<asio::awaitable<void>(uint32_t piece_index, uint32_t block_index)>;
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
    size_t piece_availability(size_t piece_index) const noexcept { 
        assert(piece_index < state_->num_pieces());
        std::lock_guard lock(mutex_);
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
    void add_piece_availability(size_t piece_index, int32_t val) { 
        assert(piece_index < state_->num_pieces());
        std::lock_guard lock(mutex_);
        (*piece_availability_)[piece_index] += val; 
    }

    asio::awaitable<void> update_piece_rarity(size_t piece_index, uint32_t old_rarity, uint32_t new_rarity);
    asio::awaitable<void> remove_piece_rarity(size_t piece_index, uint32_t rarity);
    asio::awaitable<void> build_piece_rarity();

    asio::awaitable<void> resume_piece_download(size_t piece_index);
    asio::awaitable<void> broadcast_outstanding_requests();
    
    asio::awaitable<void> downloader();  
    asio::awaitable<void> request_one_piece();
    asio::awaitable<void> check_and_enter_endgame();
    asio::awaitable<void> return_piece_to_queue(size_t piece_index);

    template<typename... Args>
    void emplace_in_progress_pieces(Args... args) { 
        std::lock_guard lock(mutex_);
        in_progress_pieces_->emplace(std::forward<Args>(args)...); 
    }
    void remove_in_progress_piece(size_t piece_index) {
        assert(piece_index < state_->num_pieces());
        std::lock_guard lock(mutex_);
        in_progress_pieces_->erase(piece_index);
    }
    void remove_all_in_progress_pieces() {
        std::lock_guard lock(mutex_);
        in_progress_pieces_->clear();
    }

    std::map<std::string, std::string> get_in_progress_for_resume() const;
    void notify_one() noexcept { piece_request_trigger_.cancel_one(); }
    void set_callback(GetAvailableCallback cb) { 
        std::lock_guard lock(mutex_);
        get_available_peers_ = std::move(cb); 
    }
    void set_block_timeout_callback(BlockTimeoutCallback cb) { 
        std::lock_guard lock(mutex_);
        block_timeout_callback_ = std::move(cb); 
    }

    // Scans all in-progress pieces for blocks whose request has exceeded BLOCK_REQUEST_TIMEOUT.
    // On timeout: calls the timeout callback (cancel) and re-requests from another peer.
    asio::awaitable<void> check_block_timeouts();

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
    }

private:

    asio::awaitable<bool> try_piece_download(size_t piece_index);

    asio::awaitable<void> block_timeout_loop();

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
    std::atomic<bool> shutting_down_{false};
};