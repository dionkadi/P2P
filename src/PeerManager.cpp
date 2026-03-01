#include "PeerManager.hpp"
#include "Utils.hpp"

#include <random>

PeerManager::PeerManager(asio::io_context& io_context, std::shared_ptr<SessionState> state) noexcept
    : io_context_(io_context), strand_(asio::make_strand(io_context)), state_(state) 
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

        co_await asio::dispatch(strand_, asio::use_awaitable);
        ++choke_loop_counter_;

        std::vector<std::shared_ptr<PeerConnection>> interested_peers;
        std::ranges::copy(
            get_all_connections()
                | std::views::filter([](const std::shared_ptr<PeerConnection>& conn) {
                    return conn->peer_is_interested();
                }),
            std::back_inserter(interested_peers)
        );
        
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
                asio::co_spawn(io_context_, conn->send_simple_message(MessageType::Unchoke), asio::detached);
                conn->am_choking(false);
            }
            unchoked_this_round.push_back(conn);
        }

        if (choke_loop_counter_ % 3 == 0) {
            if (optimistically_unchoked_peer && optimistically_unchoked_peer->am_choking() == false) {
                bool is_top_peer = std::ranges::any_of(unchoked_this_round, 
                                                       [&optimistically_unchoked_peer](const std::shared_ptr<PeerConnection>& top_peer) {
                                                           return top_peer->peer_id() == optimistically_unchoked_peer->peer_id();
                                                       });

                if (!is_top_peer) {
                    LOGINFO("Re-choking previous optimistic peer {}", optimistically_unchoked_peer->peer_id());
                    asio::co_spawn(io_context_, optimistically_unchoked_peer->send_simple_message(MessageType::Choke), asio::detached);
                    optimistically_unchoked_peer->am_choking(true);
                }
            }

            std::vector<std::shared_ptr<PeerConnection>> candidates;
            std::ranges::copy(
                interested_peers
                    | std::views::filter([](const std::shared_ptr<PeerConnection>& peer) {
                        return peer->am_choking();
                    }),
                std::back_inserter(candidates)
            );

            if (!candidates.empty()) {
                std::uniform_int_distribution<size_t> dist(0, candidates.size() - 1);
                auto& new_optimistic_peer = candidates[dist(rng)];

                LOGINFO("Optimistically unchoking peer {}", new_optimistic_peer->peer_id());
                asio::co_spawn(io_context_, new_optimistic_peer->send_simple_message(MessageType::Unchoke), asio::detached);
                new_optimistic_peer->am_choking(false);

                optimistically_unchoked_peer = new_optimistic_peer;
                unchoked_this_round.push_back(new_optimistic_peer);
            }
        } else if (optimistically_unchoked_peer) {
            unchoked_this_round.push_back(optimistically_unchoked_peer);
        }

        for (const auto& peer : interested_peers) {
            bool should_be_unchoked = std::ranges::any_of(unchoked_this_round, 
                                                         [&peer](const std::shared_ptr<PeerConnection>& unchoked_peer) {
                                                             return unchoked_peer->peer_id() == peer->peer_id();
                                                         });

            if (!should_be_unchoked && !peer->am_choking()) {
                LOGDBG("Choking slow/non-optimistic peer {}", peer->peer_id());
                asio::co_spawn(io_context_, peer->send_simple_message(MessageType::Choke), asio::detached);
                peer->am_choking(true);
            }
        }

        std::ranges::for_each(get_all_connections(), 
            [] (const auto& conn) {
                conn->bytes_uploaded(0);
        });
    }
}

asio::awaitable<void> PeerManager::send_have_message_to_all(size_t piece_index) {
    std::ranges::for_each(get_all_connections(), [this, piece_index] (const auto& conn) {
        asio::co_spawn(io_context_, conn->send_have(piece_index), asio::detached);
    });
    co_return ;
}

asio::awaitable<std::vector<std::shared_ptr<PeerConnection>>> PeerManager::available_peers(size_t piece_index) const {
    std::vector<std::shared_ptr<PeerConnection>> available_peers;
    std::ranges::copy(
        std::views::filter(get_all_connections(), 
                        [piece_index] (const auto& conn) {
                            return !conn->peer_is_choking() && conn->has_piece(piece_index);
                        }),
        std::back_inserter(available_peers)
    );
    
    // for (const auto& conn : available_peers) {
    //     if (!conn->am_interested()) {
    //         conn->am_interested(true);
    //         asio::co_spawn(io_context_, 
    //             conn->send_simple_message(MessageType::Interested),
    //             asio::detached
    //         );
    //     }
    // }
    co_return available_peers;
}

asio::awaitable<void> PeerManager::pex_loop() {
    asio::steady_timer timer(io_context_);

    while (true) {
        std::string added = populate_added();
        std::string dropped = populate_dropped();
        
        Dict pex_dict;
        if (!added.empty()) {
            pex_dict["added"] = Value(std::move(added));
        }
        if (!dropped.empty()) {
            pex_dict["dropped"] = Value(std::move(dropped));
        }

        if (!pex_dict.empty()) {
            auto encoded = encode(Value(std::move(pex_dict)));

            for (const auto& conn : get_all_connections()) {
                if (conn->supported_pex()) {
                    uint8_t pex_id = static_cast<uint8_t>(ExtendedMessageType::ut_pex);
                    co_await conn->send_extended_message(pex_id, encoded);
                }
            }
        }
        
        timer.expires_after(std::chrono::minutes(1));
        EC ec;
        co_await timer.async_wait(asio::redirect_error(asio::use_awaitable, ec));

        if (ec == asio::error::operation_aborted) {
            LOGWARN("PEX loop aborted");
            co_return;
        }
    }
}

std::string PeerManager::populate_added(size_t max_peers) {
    std::string peers;
    size_t added_peers = 0;
    for (const auto& ep : known_pex_peers_) {
        const auto ep_bytes = ep.address().to_v4().to_bytes();
        peers.append(ep_bytes.begin(), ep_bytes.end());
        uint16_t port = asio::detail::socket_ops::host_to_network_short(ep.port());
        peers.append(reinterpret_cast<char *>(&port), 2);
        ++added_peers;

        if (added_peers > max_peers) {
            break;
        }
    }
    return peers;
}

std::string PeerManager::populate_dropped() {
    std::string dropped;
    while (!dropped_peers_.empty()) {
        const auto& ep = dropped_peers_.front();
        dropped_peers_.pop_front();
        
        const auto ep_bytes = ep.address().to_v4().to_bytes();
        dropped.append(ep_bytes.begin(), ep_bytes.end());
        uint16_t port = asio::detail::socket_ops::host_to_network_short(ep.port());
        dropped.append(reinterpret_cast<char *>(&port), 2);
    }
    return dropped;
}