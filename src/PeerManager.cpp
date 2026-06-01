#include "PeerManager.hpp"
#include "Utils.hpp"

#include <algorithm>
#include <limits>
#include <random>

PeerManager::PeerManager(asio::io_context& io_context, std::shared_ptr<SessionState> state, std::chrono::milliseconds choke_interval) noexcept
    : io_context_(io_context), strand_(asio::make_strand(io_context)), pex_timer_(io_context_), choke_timer_(io_context_),
    state_(state), choke_interval_(choke_interval), backoff_retry_timer_(io_context_), ban_cleanup_timer_(io_context_)
{}

void PeerManager::close_all() {
    std::lock_guard lock(mutex_);
    std::ranges::for_each(active_connections_ | std::views::values,
        [] (std::shared_ptr<PeerConnection>& conn) { conn->close(); });
}

bool PeerManager::add_connection(const PeerId& id, std::shared_ptr<PeerConnection> conn) {
    // Reject banned peers (must check BEFORE locking mutex_ to avoid ABBA deadlock
    // with ban_peer_by_ip which locks ban_mutex_ then mutex_)
    std::string ip = extract_ip_from_addr(conn->peer_addr());
    if (is_banned(ip)) {
        conn->close();
        return false;
    }

    std::lock_guard lock(mutex_);

    // Decrement half-open count since the handshake completed (for outgoing connections).
    // For incoming connections that bypassed connect_to_peer, this is a no-op.
    if (half_open_connections_.load() > 0) {
        --half_open_connections_;
    }

    // Re-check banned status in case it changed between the unlocked check and now
    // (unlikely race window, but safe to double-check under lock)
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
    if (shutting_down_.load(std::memory_order_acquire)) {
        co_return std::nullopt;
    }

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

            auto raw_socket = std::make_shared<asio::ip::tcp::socket>(io_context_);
            {
                std::lock_guard lock(pending_connect_mutex_);
                pending_connect_sockets_.push_back(raw_socket);
            }

            auto remove_pending_socket = [this, &raw_socket] {
                std::lock_guard lock(pending_connect_mutex_);
                std::erase(pending_connect_sockets_, raw_socket);
            };

            asio::ip::tcp::resolver resolver(io_context_);
            boost::system::error_code ec;
            auto endpoints = co_await resolver.async_resolve(ip, std::to_string(port), asio::redirect_error(asio::use_awaitable, ec));
            if (ec || shutting_down_.load(std::memory_order_acquire)) {
                remove_pending_socket();
                if (raw_socket->is_open()) {
                    boost::system::error_code close_ec;
                    raw_socket->close(close_ec);
                }
                if (half_open_connections_.load() > 0) {
                    --half_open_connections_;
                }
                co_return std::nullopt;
            }

            co_await asio::async_connect(*raw_socket, endpoints, asio::redirect_error(asio::use_awaitable, ec));
            remove_pending_socket();

            if (ec || shutting_down_.load(std::memory_order_acquire)) {
                if (raw_socket->is_open()) {
                    boost::system::error_code close_ec;
                    raw_socket->close(close_ec);
                }
                if (half_open_connections_.load() > 0) {
                    --half_open_connections_;
                }
                if (!shutting_down_.load(std::memory_order_acquire)) {
                    report_connection_failure(peer_addr);
                }
                co_return std::nullopt;
            }

            AsyncSocket socket(std::move(*raw_socket));
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
    std::mt19937 rng(std::random_device{}());
    
    static constexpr size_t unchoke_slots = 4;
    std::shared_ptr<PeerConnection> optimistically_unchoked_peer = nullptr;

    auto send_async = [this](auto conn, MessageType type) {
        asio::co_spawn(io_context_, conn->send_simple_message(type),
            [type](std::exception_ptr e) {
                if (!e) return;
                try { std::rethrow_exception(e); }
                catch (const std::exception& ex) {
                    LOGDBG("Failed to send {} in choke loop: {}",
                           static_cast<int>(type), ex.what());
                }
            });
    };

    while (true) {
        if (shutting_down_.load(std::memory_order_acquire)) {
            break;
        }
        choke_timer_.expires_after(choke_interval_);
        if (shutting_down_.load(std::memory_order_acquire)) {
            choke_timer_.expires_at(decltype(choke_timer_)::clock_type::time_point::min());
        }
        boost::system::error_code ec;
        co_await choke_timer_.async_wait(asio::redirect_error(asio::use_awaitable, ec));
        if (ec || shutting_down_.load(std::memory_order_acquire)) {
            break;
        }

        co_await asio::dispatch(strand_, asio::use_awaitable);
        ++choke_loop_counter_;

        auto now = std::chrono::steady_clock::now();
        bool is_seed = state_->is_download_complete();

        auto all_peers = get_all_connections();

        std::vector<std::shared_ptr<PeerConnection>> interested;
        for (auto& conn : all_peers) {
            if (!conn->peer_is_interested()) {
                continue;
            }

            if (!conn->am_choking()) {
                auto last_data = conn->last_data_received();
                if (last_data != std::chrono::steady_clock::time_point{} &&
                    now - last_data > std::chrono::seconds(60)) {
                    LOGINFO("Anti-snub: choking snubbing peer {} ({}s since last data)",
                            conn->peer_id(),
                            static_cast<int>(std::chrono::duration_cast<std::chrono::seconds>(now - last_data).count()));
                    send_async(conn, MessageType::Choke);
                    conn->am_choking(true);
                    continue;
                }
            }

            interested.push_back(conn);
        }

        if (interested.empty()) {
            continue;
        }

        if (is_seed) {
            std::sort(interested.begin(), interested.end(),
                [](const auto& a, const auto& b) {
                    return a->bytes_downloaded() > b->bytes_downloaded();
                });
        } else {
            std::sort(interested.begin(), interested.end(),
                [](const auto& a, const auto& b) {
                    return a->bytes_uploaded() > b->bytes_uploaded();
                });
        }

        std::vector<std::shared_ptr<PeerConnection>> to_unchoke;
        size_t regular_slots = unchoke_slots - 1;
        for (size_t i = 0; i < interested.size() && to_unchoke.size() < regular_slots; ++i) {
            to_unchoke.push_back(interested[i]);
        }

        if (choke_loop_counter_ % 3 == 0) {
            if (optimistically_unchoked_peer) {
                bool still_in_top = std::ranges::any_of(to_unchoke,
                    [&](const auto& p) { return p->peer_id() == optimistically_unchoked_peer->peer_id(); });
                if (!still_in_top && !optimistically_unchoked_peer->am_choking()) {
                    LOGINFO("Rotating optimistic unchoke: choking previous peer {}",
                            optimistically_unchoked_peer->peer_id());
                    send_async(optimistically_unchoked_peer, MessageType::Choke);
                    optimistically_unchoked_peer->am_choking(true);
                }
            }

            std::vector<std::shared_ptr<PeerConnection>> candidates;
            std::ranges::copy_if(interested, std::back_inserter(candidates),
                [](const auto& p) { return p->am_choking(); });

            if (!candidates.empty()) {
                std::uniform_int_distribution<size_t> dist(0, candidates.size() - 1);
                optimistically_unchoked_peer = candidates[dist(rng)];
                LOGINFO("Optimistically unchoking peer {}", optimistically_unchoked_peer->peer_id());
                to_unchoke.push_back(optimistically_unchoked_peer);
            }
        } else if (optimistically_unchoked_peer) {
            bool still_interested = std::ranges::any_of(interested,
                [&](const auto& p) { return p->peer_id() == optimistically_unchoked_peer->peer_id(); });
            if (still_interested) {
                to_unchoke.push_back(optimistically_unchoked_peer);
            } else {
                optimistically_unchoked_peer = nullptr;
            }
        }

        for (auto& conn : interested) {
            bool should_unchoke = std::ranges::any_of(to_unchoke,
                [&](const auto& p) { return p->peer_id() == conn->peer_id(); });

            if (should_unchoke && conn->am_choking()) {
                LOGDBG("Unchoking peer {} (mode: {})", conn->peer_id(), is_seed ? "seed" : "leech");
                send_async(conn, MessageType::Unchoke);
                conn->am_choking(false);
            } else if (!should_unchoke && !conn->am_choking()) {
                LOGDBG("Choking peer {}", conn->peer_id());
                send_async(conn, MessageType::Choke);
                conn->am_choking(true);
            }
        }

        std::ranges::for_each(all_peers, [](auto& conn) {
            conn->bytes_uploaded(0);
            conn->bytes_downloaded(0);
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
        if (shutting_down_.load(std::memory_order_acquire)) {
            co_return;
        }
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
        if (shutting_down_.load(std::memory_order_acquire)) {
            pex_timer_.expires_at(decltype(pex_timer_)::clock_type::time_point::min());
        }
        EC ec;
        co_await pex_timer_.async_wait(asio::redirect_error(asio::use_awaitable, ec));

        if (ec == asio::error::operation_aborted || shutting_down_.load(std::memory_order_acquire)) {
            if (ec == asio::error::operation_aborted) {
                LOGWARN("PEX loop aborted");
            }
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
        auto ep = dropped_peers_.front();  // copy before pop (pop_front destroys the element)
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
        if (shutting_down_.load(std::memory_order_acquire)) {
            co_return;
        }
        ban_cleanup_timer_.expires_after(5min);
        if (shutting_down_.load(std::memory_order_acquire)) {
            ban_cleanup_timer_.expires_at(decltype(ban_cleanup_timer_)::clock_type::time_point::min());
        }
        boost::system::error_code ec;
        co_await ban_cleanup_timer_.async_wait(asio::redirect_error(asio::use_awaitable, ec));

        if (ec == asio::error::operation_aborted || shutting_down_.load(std::memory_order_acquire)) {
            if (ec == asio::error::operation_aborted) {
                LOGDBG("Ban cleanup loop aborted");
            }
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
