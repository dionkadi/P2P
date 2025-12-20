#include "PeerManager.hpp"

#include <cstddef>
#include <optional>
#include <random>
#include "Logger.hpp"
#include "PeerConnection.hpp"

PeerManager::PeerManager(asio::io_context& io_context, std::shared_ptr<SessionState> state)
    : io_context_(io_context), state_(state) 
{}

asio::awaitable<std::optional<AsyncSocket>> PeerManager::connect_to_peer(const std::string& peer_addr) {
    try {
        size_t colon_pos = peer_addr.find(':');
        if (colon_pos == std::string::npos) co_return std::nullopt;

        std::string ip = peer_addr.substr(0, colon_pos);
        int port = std::stoi(peer_addr.substr(colon_pos + 1));

        AsyncSocket socket(asio::ip::tcp::socket{io_context_});
        co_await socket.connect(ip, port);
        LOGINFO("Successfully connected to peer {}", peer_addr);

        co_return socket;
    } catch (const boost::system::system_error& e) {
        if (e.code() == asio::error::eof ||
            e.code() == asio::error::connection_reset ||
            e.code() == asio::error::broken_pipe)
        {

        } else {
            LOGERR("Failed to connect to peer {}: {}", peer_addr, e.what());
        }
    } catch (const std::exception& e) {
        LOGERR("Failed to connect to peer {}: {}", peer_addr, e.what());
    }

    co_return std::nullopt;
}

asio::awaitable<void> PeerManager::choke_loop() {
    asio::steady_timer timer(io_context_);
    std::mt19937 rng(std::chrono::high_resolution_clock::now().time_since_epoch().count());

    std::shared_ptr<PeerConnection> optimistically_unchoked_peer = nullptr;

    while (!state_->is_download_complete()) {
        timer.expires_after(std::chrono::seconds(10));
        co_await timer.async_wait(asio::use_awaitable);

        // co_await asio::dispatch(strand_, asio::use_awaitable);
        ++choke_loop_counter_;

        std::vector<std::shared_ptr<PeerConnection>> interested_peers;
        for (auto const& [id, conn] : active_connections_) {
            if (conn->peer_is_interested()) {
                interested_peers.push_back(conn);
            }
        }

        std::sort(interested_peers.begin(), interested_peers.end(), 
            [] (const auto& a, const auto& b) {
                return a->bytes_uploaded() > b->bytes_uploaded();
            }
        );

        const int unchoke_slots = 4;
        std::vector<std::shared_ptr<PeerConnection>> unchoked_this_round;

        for (size_t i = 0; i < interested_peers.size() && unchoked_this_round.size() < unchoke_slots - 1; ++i) {
            auto& conn = interested_peers[i];
            if (conn->am_choking()) {
                LOGDBG("Unchoking fast peer {} (uploaded {} bytes)", conn->peer_id(), conn->bytes_uploaded());
                co_await conn->send_simple_message(MessageType::Unchoke);
                conn->am_choking(false);
            }
            unchoked_this_round.push_back(conn);
        }

        if (choke_loop_counter_ % 3 == 0) {
            if (optimistically_unchoked_peer && optimistically_unchoked_peer->am_choking() == false) {
                bool is_top_peer = false;
                for (const auto& top_peer : unchoked_this_round) {
                    if (top_peer->peer_id() == optimistically_unchoked_peer->peer_id()) {
                        is_top_peer = true;
                        break ;
                    }
                }
                if (!is_top_peer) {
                    LOGINFO("Re-choking previous optimistic peer {}", optimistically_unchoked_peer->peer_id());
                    co_await optimistically_unchoked_peer->send_simple_message(MessageType::Choke);
                    optimistically_unchoked_peer->am_choking(true);
                }
            }

            std::vector<std::shared_ptr<PeerConnection>> candidates;
            for (const auto& peer: interested_peers) {
                if (peer->am_choking()) {
                    candidates.push_back(peer);
                }
            }

            if (!candidates.empty()) {
                std::uniform_int_distribution<size_t> dist(0, candidates.size() - 1);
                auto& new_optimistic_peer = candidates[dist(rng)];

                LOGINFO("Optimistically unchoking peer {}", new_optimistic_peer->peer_id());
                co_await new_optimistic_peer->send_simple_message(MessageType::Unchoke);
                new_optimistic_peer->am_choking(false);

                optimistically_unchoked_peer = new_optimistic_peer;
                unchoked_this_round.push_back(new_optimistic_peer);
            }
        } else if (optimistically_unchoked_peer) {
            unchoked_this_round.push_back(optimistically_unchoked_peer);
        }

        for (const auto& peer : interested_peers) {
            bool should_be_unchoked = false;
            for (const auto& unchoked_peer : unchoked_this_round) {
                if (unchoked_peer->peer_id() == peer->peer_id()) {
                    should_be_unchoked = true;
                    break ;
                }
            }
            if (!should_be_unchoked && !peer->am_choking()) {
                LOGDBG("Choking slow/non-optimistic peer {}", peer->peer_id());
                co_await peer->send_simple_message(MessageType::Choke);
                peer->am_choking(true);
            }
        }

        for (auto const& [id, conn] : active_connections_) {
            conn->bytes_uploaded(0);
        }
    }
}

asio::awaitable<void> PeerManager::send_have_message_to_all(size_t piece_index) {
    for (const auto& [id, conn] : active_connections_) {
        co_await conn->send_have(piece_index);
    }
}

std::vector<std::shared_ptr<PeerConnection>> PeerManager::available_peers(size_t piece_index) {
    std::vector<std::shared_ptr<PeerConnection>> available_peers;
    for (const auto& [id, conn] : active_connections_) {
        if (!conn->peer_is_choking() && conn->has_piece(piece_index)) {
            available_peers.push_back(conn);

            if (!conn->am_interested()) {
                conn->am_interested(true);
                // Send INTERESTED message asynchronously
                asio::co_spawn(io_context_, 
                    [conn]() -> asio::awaitable<void> {
                        co_await conn->send_simple_message(MessageType::Interested);
                    }, 
                    asio::detached
                );
            }
        }
    }
    return available_peers;
}
