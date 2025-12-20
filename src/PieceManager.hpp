#pragma once

#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/strand.hpp>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <unordered_set>
#include <utility>

#include "Types.hpp"
#include "SessionState.hpp"
#include "PeerConnection.hpp"

class PieceManager {
public:
    using GetActiveConnectionsCallback = std::function<const std::map<PeerId, std::shared_ptr<PeerConnection>>&()>;

    PieceManager(asio::io_context& io_context, std::shared_ptr<SessionState> state);

    const std::map<size_t, InProgressPiece>& in_progress_pieces() const { return in_progress_pieces_; }
    std::map<size_t, InProgressPiece>& in_progress_pieces() { return in_progress_pieces_; }
    const InProgressPiece& in_progress_piece(size_t piece_index) const { return in_progress_pieces_.at(piece_index); }
    InProgressPiece& in_progress_piece(size_t piece_index) { return in_progress_pieces_.at(piece_index); }
    const std::vector<size_t>& piece_availability() const { return piece_availability_; }
    std::vector<size_t>& piece_availability() { return piece_availability_; }
    size_t piece_availability(size_t piece_index) const { return piece_availability_[piece_index]; }
    const std::map<size_t, std::unordered_set<int>>& pieces_by_rarity() const { return pieces_by_rarity_; };
    std::map<size_t, std::unordered_set<int>>& pieces_by_rarity() { return pieces_by_rarity_; };

    void add_piece_availability(size_t piece_index, int32_t val) { piece_availability_[piece_index] += val; }

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
    void emplace_in_progress_pieces(Args... args) { in_progress_pieces_.emplace(std::forward<Args>(args)...); }

    asio::awaitable<std::map<std::string, std::string>> get_in_progress_for_resume() const;

    void notify_one() { piece_request_trigger_.cancel_one(); }

    void set_callback(GetActiveConnectionsCallback cb) { get_connections_ = std::move(cb); }

private:

    asio::awaitable<bool> try_piece_download(size_t piece_index);

    asio::io_context& io_context_;
    asio::strand<asio::io_context::executor_type> strand_;
    asio::steady_timer piece_request_trigger_;

    std::map<size_t, InProgressPiece> in_progress_pieces_;
    std::vector<size_t> piece_availability_;
    std::map<size_t, std::unordered_set<int>> pieces_by_rarity_;
    std::shared_ptr<SessionState> state_;

    GetActiveConnectionsCallback get_connections_;
};