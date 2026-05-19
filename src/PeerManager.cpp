#include "PeerManager.hpp"
#include "Utils.hpp"

#include <algorithm>
#include <limits>
#include <random>

PeerManager::PeerManager(asio::io_context& io_context, std::shared_ptr<SessionState> state, std::chrono::seconds choke_interval) noexcept
    : io_context_(io_context), strand_(asio::make_strand(io_context)), pex_timer_(io_context_), choke_timer_(io_context_),
    state_(state), choke_interval_(choke_interval), backoff_retry_timer_(io_context_), ban_cleanup_timer_(io_context_)
{}

bool PeerManager::add_connection(const PeerId& id, std::shared_ptr<PeerConnection> conn) {
    std::lock_guard lock(mutex_);

    // Decrement half-open count since the handshake completed (for outgoing connections).
    // For incoming connections that bypassed connect_to_peer, this is a no-op.
    if (half_open_connections_.load() > 0) {
        --half_open_connections_;
    }

    // Reject banned peers
    std::string ip = extract_ip_from_addr(conn->peer_addr());
    if (is_banned(ip)) {
        LOGWARN("add_connection: rejecting {} ({}): IP {} is banned", id, conn->peer_addr(), ip);
        conn->close();
        return false;
    }

    // Enforce per-IP connection limit
    size_t ip_count = 0;
    for (const auto& [pid, existing] : active_connections_) {
        if (extract_ip_from_addr(existing->peer_addr()) == ip) {
            ++ip_count;
        }
    }
    if (ip_count >= max_connections_per_ip_) {
        LOGWARN("add_connection: rejecting {} ({}): max {} connections per IP reached",
                id, conn->peer_addr(), max_connections_per_ip_);
        conn->close();
        return false;
    }

    // Enforce total connection limit with replace-worst-peer strategy
    if (active_connections_.size() >= max_total_connections_) {
        auto worst = find_worst_peer_locked();
        if (worst) {
            LOGINFO("add_connection: replacing worst peer {} (rate={}) with new peer {}",
                    worst->peer_id(), worst->bytes_downloaded() + worst->bytes_uploaded(), id);
            worst->close();
            active_connections_.erase(worst->peer_id());
        } else {
            LOGWARN("add_connection: rejecting {}: max total connections ({}) reached and no peer to replace",
                    id, max_total_connections_);
            conn->close();
            return false;
        }
    }

    active_connections_[id] = std::move(conn);
    return true;
}

std::shared_ptr<PeerConnection> PeerManager::find_worst_peer_locked() {
    std::shared_ptr<PeerConnection> worst;
    uint64_t min_rate = std::numeric_limits<uint64_t>::max();
    for (const auto& [pid, conn] : active_connections_) {
        uint64_t rate = conn->bytes_downloaded() + conn->bytes_uploaded();
        if (rate < min_rate) {
            min_rate = rate;
            worst = conn;
        }
    }
    return worst;
}

std::string PeerManager::extract_ip_from_addr(const std::string& peer_addr) {
    auto colon_pos = peer_addr.rfind(':');
    if (colon_pos == std::string::npos) {
        return peer_addr;
    }
    return peer_addr.substr(0, colon_pos);
}

asio::awaitable<std::optional<AsyncSocket>> PeerManager::connect_to_peer(const std::string& peer_addr) {
    // Check backoff state before attempting
    {
        std::lock_guard lock(backoff_mutex_);
        auto it = backoff_states_.find(peer_addr);
        if (it != backoff_states_.end()) {
            it->second.check_and_reset_if_idle();
            if (it->second.is_in_backoff()) {
                auto remaining = std::chrono::duration_cast<std::chrono::seconds>(
                    it->second.next_retry_at_ - std::chrono::steady_clock::now()).count();
                LOGDBG("connect_to_peer: skipping {}: in backoff ({}s remaining, attempt {})",
                       peer_addr, remaining, it->second.attempt_count_);
                co_return std::nullopt;
            }
        }
    }

    // Check if peer is banned before attempting
    {
        std::string ip = extract_ip_from_addr(peer_addr);
        if (is_banned(ip)) {
            LOGDBG("connect_to_peer: skipping {}: IP {} is banned", peer_addr, ip);
            co_return std::nullopt;
        }
    }

    // Check half-open connection limit before attempting
    if (half_open_connections_.load() >= max_half_open_connections_) {
        LOGWARN("connect_to_peer: rejecting {}: max half-open connections ({}) reached",
                peer_addr, max_half_open_connections_);
        co_return std::nullopt;
    }

    ++half_open_connections_;

    try {
        size_t colon_pos = peer_addr.find(':');
        if (colon_pos == std::string::npos) {
            // Will fall through to decrement half-open below
        } else {
            std::string ip = peer_addr.substr(0, colon_pos);
            int port = std::stoi(peer_addr.substr(colon_pos + 1));

            AsyncSocket socket(asio::ip::tcp::socket{io_context_});
            co_await socket.connect(ip, port);
            LOGINFO("Successfully connected to peer {}", peer_addr);
            report_connection_success(peer_addr);
            // Success - return socket, half-open will be decremented in add_connection
            co_return socket;
        }
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

    // Decrement half-open on failure (success path returned socket above)
    if (half_open_connections_.load() > 0) {
        --half_open_connections_;
    }

    report_connection_failure(peer_addr);
    co_return std::nullopt;
}

void PeerManager::report_connection_success(const std::string& peer_addr) {
    std::lock_guard lock(backoff_mutex_);
    backoff_states_[peer_addr].on_success();
}

void PeerManager::report_connection_failure(const std::string& peer_addr) {
    std::lock_guard lock(backoff_mutex_);
    auto& state = backoff_states_[peer_addr];
    state.on_failure();

    // Track connection failures for ban purposes
    bool should_ban = false;
    std::string banned_ip;
    {
        std::lock_guard ban_lock(ban_mutex_);
        auto& m = peer_misbehavior_[extract_ip_from_addr(peer_addr)];
        m.connection_failures++;
        LOGDBG("PeerManager: peer {} reported {} connection failures", peer_addr, m.connection_failures);
        if (m.has_exceeded_thresholds()) {
            should_ban = true;
            banned_ip = extract_ip_from_addr(peer_addr);
        }
    }
    if (should_ban) {
        ban_peer_by_ip(banned_ip);
    }

    // Schedule the backoff retry timer for the earliest expiry
    TimePoint earliest = state.next_retry_at_;
    for (const auto& [addr, bs] : backoff_states_) {
        if (bs.attempt_count_ > 0 && bs.next_retry_at_ != TimePoint{} && bs.next_retry_at_ < earliest) {
            earliest = bs.next_retry_at_;
        }
    }
    auto now = std::chrono::steady_clock::now();
    if (earliest > now) {
        backoff_retry_timer_.expires_at(earliest);
        asio::co_spawn(io_context_, [this]() -> asio::awaitable<void> {
            boost::system::error_code ec;
            co_await backoff_retry_timer_.async_wait(asio::redirect_error(asio::use_awaitable, ec));
            if (!ec) {
                LOGDBG("Backoff retry timer fired, some peers may be eligible for retry.");
            }
        }, asio::detached);
    }
}

asio::awaitable<void> PeerManager::choke_loop() {
    auto self = shared_from_this();

    std::mt19937 rng(std::chrono::high_resolution_clock::now().time_since_epoch().count());

    std::shared_ptr<PeerConnection> optimistically_unchoked_peer = nullptr;
    while (!state_->is_download_complete()) {
        choke_timer_.expires_after(choke_interval_);
        co_await choke_timer_.async_wait(asio::use_awaitable);

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
    auto self = shared_from_this();
    
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
        
        pex_timer_.expires_after(std::chrono::minutes(1));
        EC ec;
        co_await pex_timer_.async_wait(asio::redirect_error(asio::use_awaitable, ec));

        if (ec == asio::error::operation_aborted) {
            LOGWARN("PEX loop aborted");
            co_return;
        }
    }
}

std::string PeerManager::populate_added(size_t max_peers) {
    std::string peers;
    size_t added_peers = 0;
    for (const auto& ep : active_peers_) {
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

// ---- Ban implementation ----

bool PeerManager::is_banned(const std::string& ip) const {
    std::lock_guard lock(ban_mutex_);
    auto it = banned_peers_.find(ip);
    if (it == banned_peers_.end()) {
        return false;
    }
    // Check if ban has expired
    auto now = std::chrono::steady_clock::now();
    if (now >= it->second.expiry_time) {
        banned_peers_.erase(it);
        return false;
    }
    return true;
}

void PeerManager::ban_peer_by_ip(const std::string& ip) {
    auto now = std::chrono::steady_clock::now();
    BannedPeer bp;
    bp.ip = ip;
    bp.ban_time = now;
    bp.expiry_time = now + 1h;

    {
        std::lock_guard lock(ban_mutex_);
        banned_peers_[ip] = bp;
        // Clear misbehavior counters for this IP
        peer_misbehavior_.erase(ip);
    }

    LOGINFO("PeerManager: Banned IP {} for 1 hour", ip);

    // Close any existing connections from this IP
    std::lock_guard lock(mutex_);
    std::vector<PeerId> to_remove;
    for (const auto& [pid, conn] : active_connections_) {
        if (extract_ip_from_addr(conn->peer_addr()) == ip) {
            conn->close();
            to_remove.push_back(pid);
        }
    }
    for (const auto& pid : to_remove) {
        active_connections_.erase(pid);
        LOGINFO("PeerManager: Closed connection to banned peer {} (IP {})", pid, ip);
    }
}

void PeerManager::report_corrupt_piece(const std::string& peer_addr) {
    std::string ip = extract_ip_from_addr(peer_addr);
    bool should_ban = false;
    {
        std::lock_guard lock(ban_mutex_);
        auto& m = peer_misbehavior_[ip];
        m.corrupt_pieces++;
        LOGDBG("PeerManager: peer {} reported {} corrupt pieces", peer_addr, m.corrupt_pieces);
        if (m.has_exceeded_thresholds()) {
            should_ban = true;
        }
    }
    if (should_ban) {
        ban_peer_by_ip(ip);
    }
}

void PeerManager::report_protocol_violation(const std::string& peer_addr) {
    std::string ip = extract_ip_from_addr(peer_addr);
    bool should_ban = false;
    {
        std::lock_guard lock(ban_mutex_);
        auto& m = peer_misbehavior_[ip];
        m.protocol_violations++;
        LOGDBG("PeerManager: peer {} reported {} protocol violations", peer_addr, m.protocol_violations);
        if (m.has_exceeded_thresholds()) {
            should_ban = true;
        }
    }
    if (should_ban) {
        ban_peer_by_ip(ip);
    }
}

void PeerManager::report_timeout(const std::string& peer_addr) {
    std::string ip = extract_ip_from_addr(peer_addr);
    bool should_ban = false;
    {
        std::lock_guard lock(ban_mutex_);
        auto& m = peer_misbehavior_[ip];
        m.timeouts++;
        LOGDBG("PeerManager: peer {} reported {} timeouts", peer_addr, m.timeouts);
        if (m.has_exceeded_thresholds()) {
            should_ban = true;
        }
    }
    if (should_ban) {
        ban_peer_by_ip(ip);
    }
}

asio::awaitable<void> PeerManager::ban_cleanup_loop() {
    auto self = shared_from_this();

    while (true) {
        ban_cleanup_timer_.expires_after(5min);
        boost::system::error_code ec;
        co_await ban_cleanup_timer_.async_wait(asio::redirect_error(asio::use_awaitable, ec));

        if (ec == asio::error::operation_aborted) {
            LOGDBG("Ban cleanup loop aborted");
            co_return;
        }

        auto now = std::chrono::steady_clock::now();
        std::lock_guard lock(ban_mutex_);
        std::erase_if(banned_peers_, [&](const auto& pair) {
            return now >= pair.second.expiry_time;
        });
        LOGDBG("Ban cleanup: {} active bans", banned_peers_.size());
    }
}