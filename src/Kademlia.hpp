#pragma once

#include "Utils.hpp"
#include "AsyncSocket.hpp"

#include <algorithm>
#include <bit>
#include <boost/asio/async_result.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/deferred.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/experimental/cancellation_condition.hpp>
#include <boost/asio/experimental/parallel_group.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <boost/system/detail/error_code.hpp>
#include <boost/system/system_error.hpp>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <functional>
#include <future>
#include <iterator>
#include <list>
#include <memory>
#include <mutex>
#include <numeric>
#include <openssl/evp.h>
#include <optional>
#include <random>
#include <sys/types.h>
#include <utility>
#include <vector>

static constexpr int K = 8; // Kademlia K-bucket size
static constexpr int ALPHA = 3; // Concurrency parameter for lookups
static constexpr std::chrono::minutes BUCKET_REFRESH_INTERVAL = std::chrono::minutes(15);
static constexpr std::chrono::minutes ANNOUNCE_TOKEN_LIFETIME = std::chrono::minutes(10); // How long a token from get_peers is valid (BEP5)
static constexpr std::chrono::minutes PEER_STORAGE_LIFETIME = std::chrono::minutes(30); // How long announced peers are stored (BEP5)
static constexpr std::chrono::seconds KRPC_QUERY_TIMEOUT = std::chrono::seconds(10); // Timeout for individual KRPC queries

using namespace boost::asio::experimental::awaitable_operators;

enum class NodeStatus {
    Good, Qustionable, Bad,
};

struct BucketEntry {
    NodeId id;
    udp::endpoint endpoint;
    NodeStatus status;
    TimePoint last_seen;    // last time we received a message from this node
    TimePoint last_replied; // last time this node replied to our query
};

inline Distance distance(const NodeId& node1, const NodeId& node2) {
    std::array<std::byte, 20> result{};

    auto zipped = std::views::zip(node1, node2)
        | std::views::transform([](auto pair) {
            auto [x, y] = pair;
            return x ^ y;
        });

    std::ranges::copy(zipped, result.begin());
    return result;
}

inline size_t get_bucket_index(const NodeId& my_id, const NodeId& other_id) {
    Distance d = distance(my_id, other_id);
    for (uint i = 0; i < HASH_SIZE; ++i) { // HASH_SIZE = 20 bytes
        if (d[i] != std::byte{0}) {
            // Found the first differing byte. Now find the MSB within this byte.
            // std::countl_zero counts leading zeros in an unsigned integer.
            // Need to cast std::byte to unsigned char for this.
            return i * 8 + std::countl_zero(static_cast<unsigned char>(d[i]));
        }
    }
    return HASH_SIZE * 8 - 1; // All bits are the same, same node, put in last bucket (or handle as special case)
}

struct KBucket {
    std::list<BucketEntry> bucket;
    TimePoint last_changed;

    KBucket() : last_changed(std::chrono::steady_clock::now()) {}

    bool add(const BucketEntry& entry);
    void touch(const NodeId& id); // move to back (MRU)
};

class RouteTable {
public:
    RouteTable(const NodeId& id)
        : my_node_id_(id)
    {}

    void insert(const BucketEntry& entry);
    void update_status(const NodeId& id, NodeStatus status);
    std::vector<BucketEntry> find_closest_nodes(const NodeId& target, size_t k) const;
    bool needs_refresh(size_t bucket_idx) const;
    void touch(size_t bucket_idx);
private:
    NodeId my_node_id_;
    std::array<KBucket, 160> buckets_;    
    mutable std::mutex mutex_;
};

struct PeerInfo {
    EndPoint endpoint;
    TimePoint expiry;
};

class DHTNode : public std::enable_shared_from_this<DHTNode> {
public:
    explicit DHTNode(asio::io_context& io_context, uint16_t port, const NodeId& nid = generate_id(NODE_ID_PREFIX))
        : io_context_(io_context)
        , my_node_id_(nid)
        , routing_table_(my_node_id_)
        , socket_(io_context_, port)
        , refresh_timer_(io_context_)
        , cleanup_timer_(io_context_)
    {
        LOGINFO("DHT Node started with ID: {}", Crypto::bytes_to_hex(my_node_id_));
        LOGINFO("DHT Node listening on UDP port {}", port);
    }

    void start();
    void stop();

    // KRPC Query methods (DHT Client functionality)
    asio::awaitable<void> send_ping(const udp::endpoint& target_endpoint); // Pings a node and updates routing table
    asio::awaitable<std::vector<BucketEntry>> find_nodes(const NodeId& target_id, uint count = K);
    asio::awaitable<std::vector<EndPoint>> get_peers(const InfoHash& info_hash, uint count = K);
    asio::awaitable<void> announce_peer(const InfoHash& info_hash, uint16_t client_port);
    const NodeId& get_node_id() const { return my_node_id_; }
    uint16_t get_port() const { return socket_.local_endpoint().port(); }
    // Bootstrap function to discover initial nodes
    asio::awaitable<void> bootstrap(const std::vector<std::string>& bootstrap_nodes_addrs);

private:
    asio::io_context& io_context_;
    NodeId my_node_id_;
    RouteTable routing_table_;
    AsyncUdpSocket socket_;
    asio::steady_timer refresh_timer_;
    asio::steady_timer cleanup_timer_;
    std::atomic_bool shuting_down_{false};

    std::mt19937_64 rng_{std::random_device{}()};
    String generate_transaction_id();
    String generate_token(const udp::endpoint& remote_endpoint) const;

    struct PendingQuery {
        std::move_only_function<void(boost::system::error_code, Value)> completion;
        udp::endpoint remote;
        TimePoint expiry;
    };
    std::map<String, std::shared_ptr<PendingQuery>> pending_queries_;  // transaction_id -> pending query
    mutable std::mutex queries_mutex_;

    asio::awaitable<void> udp_listen_loop();
    asio::awaitable<void> handle_krpc_message(std::vector<std::byte> data, udp::endpoint remote);
    asio::awaitable<void> handle_query(const Dict& query_dict, const String& transaction_id, udp::endpoint remote);
    asio::awaitable<void> handle_response(const Dict& response_dict, const String& transaction_id, udp::endpoint remote);
    asio::awaitable<void> handle_error(const Dict& error_dict, const String& transaction_id, udp::endpoint remote);

    std::map<InfoHash, std::vector<PeerInfo>> known_peers_;
    mutable std::mutex known_peers_mutex_;

    // Stored tokens for announce_peer: info_hash -> map (remote_endpoint -> {token, expiry_time})
    // The token is a short string, acts as a proof of a recent get_peers query
    struct AnnounceToken {
        String token_value;
        TimePoint expiry_time;
    };
    std::map<InfoHash, std::map<udp::endpoint, AnnounceToken>> tokens_issued_;
    mutable std::mutex tokens_mutex_;

    void routing_table_refresh_loop();
    void cleanup_loop();

    asio::awaitable<void> send_krpc_response(const Value& response, udp::endpoint remote);
    asio::awaitable<void> send_krpc_error(const String& transaction_id, Integer error_code, const String& error_message, udp::endpoint remote);
    std::string to_compact_node_info(const std::vector<BucketEntry>& nodes);
    std::string to_compact_peer_info(const EndPoint& peer_endpoint);
    List to_compact_peer_list(const std::vector<EndPoint>& peers);
    std::vector<BucketEntry> from_compact_node_info(std::string_view compact_nodes_str);
    std::vector<EndPoint> from_compact_peer_info(std::string_view compact_peers_str);

    using QueryHandler = asio::awaitable<void>(DHTNode::*)(const Dict&, const String&, udp::endpoint);
    std::map<String, QueryHandler> query_handlers_;
    asio::awaitable<void> on_ping_query(const Dict& args, const String& transaction_id, udp::endpoint remote_endpoint);
    asio::awaitable<void> on_find_node_query(const Dict& args, const String& transaction_id, udp::endpoint remote_endpoint);
    asio::awaitable<void> on_get_peers_query(const Dict& args, const String& transaction_id, udp::endpoint remote_endpoint);
    asio::awaitable<void> on_announce_peer_query(const Dict& args, const String& transaction_id, udp::endpoint remote_endpoint);

    template<typename CompletionToken> auto async_ping(String tid, udp::endpoint target, CompletionToken&& token);
    template<typename CompletionToken> auto async_find_nodes(String tid, BucketEntry node, NodeId target_id, CompletionToken&& token);
    template<typename CompletionToken> auto async_get_peers(String tid, BucketEntry node, InfoHash info_hash, CompletionToken&& token);
    template<typename CompletionToken> auto async_announce_peer(String tid, BucketEntry node, InfoHash info_hash, uint16_t port, String announce_token, CompletionToken&& token);
};


inline bool KBucket::add(const BucketEntry& entry) {
    auto it = std::find_if(bucket.begin(), bucket.end(), [&](const BucketEntry& e) {
        return e.id == entry.id;
    });

    if (it != bucket.end()) {
        // Node already in bucket, move to end (MRU) and update fields
        it->endpoint = entry.endpoint;
        it->status = entry.status;
        it->last_seen = std::chrono::steady_clock::now();
        it->last_replied = entry.last_replied;
        bucket.splice(bucket.end(), bucket, it); // move to back
        last_changed = std::chrono::steady_clock::now();
        return true;
    } else {
        if (bucket.size() < K) {
            bucket.push_back(entry);
            last_changed = std::chrono::steady_clock::now();
            return true;
        }

        // Bucket is full. Look for a bad node to replace.
        it = std::find_if(bucket.begin(), bucket.end(), [](const BucketEntry& e) {
            return e.status == NodeStatus::Bad;
        });
        if (it != bucket.end()) {
            // Replace bad node
            LOGDBG("Replacing bad node {} with new node {} in K-bucket", Crypto::bytes_to_hex(it->id), Crypto::bytes_to_hex(entry.id));
            *it = entry;
            bucket.splice(bucket.end(), bucket, it); // Move new node to back
            last_changed = std::chrono::steady_clock::now();
            return true;
        }

        // No bad nodes, all K nodes are good or questionable.
        // In this case, we need to ping the oldest node (front of the list).
        // The `DHTNode` class will handle the asynchronous ping and subsequent replacement.
        return false;
    }
}

inline void KBucket::touch(const NodeId& id) {
    auto it = std::find_if(bucket.begin(), bucket.end(), [&id] (const auto& e) {
        return e.id == id;
    });
    if (it != bucket.end()) {
        it->last_seen = std::chrono::steady_clock::now();
        bucket.splice(bucket.end(), bucket, it); // Move to back (MRU)
        last_changed = std::chrono::steady_clock::now();
    }
}

inline void RouteTable::insert(const BucketEntry& entry) {
    std::lock_guard lock(mutex_);

    if (entry.id == my_node_id_) {
        return;
    }

    size_t idx = get_bucket_index(my_node_id_, entry.id);
    if (idx >= buckets_.size()) {
        LOGERR("Invalid bucket index {} for node {}.", idx, Crypto::bytes_to_hex(entry.id));
        return;
    }

    if (!buckets_[idx].add(entry)) {
        // Bucket is full and all nodes are good/questionable.
        // A real DHT would ping the oldest node here and potentially replace it.
        // For simplicity, we just log and ignore the new entry for now,
        // but the refresh mechanism will eventually handle stale entries.
        LOGDBG("Bucket {} is full and all nodes are good. Cannot add node {}. Will ping oldest later.",
                idx, Crypto::bytes_to_hex(entry.id));
    }
}

inline void RouteTable::update_status(const NodeId& id, NodeStatus status) {
    std::lock_guard lock(mutex_);
    if (my_node_id_ == id) {
        return;
    }

    size_t idx = get_bucket_index(my_node_id_, id);
    if (idx >= buckets_.size()) {
        LOGWARN("Invalid bucket index");
        return;
    }

    auto& b = buckets_[idx];
    auto it = std::find_if(b.bucket.begin(), b.bucket.end(), [&id] (const auto& e) {
        return e.id == id;
    });
    if (it != b.bucket.end()) {
        it->status = status;
        if (status == NodeStatus::Good) {
            it->last_replied = std::chrono::steady_clock::now(); // Update last_replied on good status
            b.bucket.splice(b.bucket.end(), b.bucket, it);
        }
        b.last_changed = std::chrono::steady_clock::now();
    }
}

inline std::vector<BucketEntry> RouteTable::find_closest_nodes(const NodeId& target, size_t k) const {
    std::lock_guard lock(mutex_);
    std::vector<BucketEntry> closest;
    std::set<NodeId> added_ids;

    // sort by XOR distance to target
    auto sorter = [&] (const BucketEntry& a, const BucketEntry& b) {
        return distance(a.id, target) < distance(b.id, target); 
    };

    // Add nodes from target bucket
    size_t target_idx = get_bucket_index(my_node_id_, target);
    if (target_idx < buckets_.size()) {
        for (const auto& e : buckets_[target_idx].bucket) {
            if (e.id != my_node_id_ && !added_ids.count(e.id)) {
                closest.push_back(e);
                added_ids.insert(e.id);
            }
        }
    }

    // Expand outwards from target bucket until k nodes are found
    for (uint i = 1; i < HASH_SIZE * 8; ++i) {
        int left_bucket = target_idx - i;
        uint right_bucket = target_idx + i;
        if (left_bucket >= 0) {
            for (const auto& e : buckets_[left_bucket].bucket) {
                if (e.id != my_node_id_ && !added_ids.count(e.id)) {
                    closest.push_back(e);
                    added_ids.insert(e.id);
                }
            }
        }
        if (right_bucket < buckets_.size()) {
            for (const auto& e : buckets_[right_bucket].bucket) {
                if (e.id != my_node_id_ && !added_ids.count(e.id)) {
                    closest.push_back(e);
                    added_ids.insert(e.id);
                }
            }
        }

        if (closest.size() >= k && left_bucket < 0 && right_bucket >= buckets_.size()) {
            break;
        }
    }

    std::sort(closest.begin(), closest.end(), sorter);
    if (closest.size() > k) {
        closest.resize(k);
    }

    return closest;
}

inline bool RouteTable::needs_refresh(size_t bucket_idx) const {
    std::lock_guard lock(mutex_);
    if (bucket_idx >= buckets_.size()) {
        return false;
    }
    return std::chrono::steady_clock::now() - buckets_[bucket_idx].last_changed > BUCKET_REFRESH_INTERVAL;
}

inline void RouteTable::touch(size_t bucket_idx) {
    std::lock_guard lock(mutex_);
    if (bucket_idx >= buckets_.size()) {
        return ;
    }
    buckets_[bucket_idx].last_changed = std::chrono::steady_clock::now();
}

inline void DHTNode::start() {
    auto self = shared_from_this();
    asio::co_spawn(io_context_, self->udp_listen_loop(), asio::detached);

    query_handlers_["ping"] = &DHTNode::on_ping_query;
    query_handlers_["find_node"] = &DHTNode::on_find_node_query;
    query_handlers_["get_peers"] = &DHTNode::on_get_peers_query;
    query_handlers_["announce_peer"] = &DHTNode::on_announce_peer_query;

    routing_table_refresh_loop();
    cleanup_loop();
}

inline void DHTNode::stop() {
    shuting_down_ = true;
    socket_.close();
    refresh_timer_.cancel();
    cleanup_timer_.cancel();

    // Extract queries under lock, then complete outside lock to avoid
    // reentrancy when completion handlers resume coroutines that access
    // pending_queries_ (which would deadlock on queries_mutex_).
    std::vector<std::shared_ptr<PendingQuery>> queries;
    {
        std::lock_guard lock(queries_mutex_);
        for (auto& [tid, query_ptr] : pending_queries_) {
            queries.push_back(std::move(query_ptr));
        }
        pending_queries_.clear();
    }
    for (auto& query_ptr : queries) {
        query_ptr->completion(asio::error::operation_aborted, {});
    }

    LOGINFO("DHT Node stopped.");
}

inline String DHTNode::generate_transaction_id() {
    // Generate a random 2-byte transaction ID.
    // In a production environment, you might want something more robust
    // like a counter or a cryptographically secure random number.
    uint16_t tid = static_cast<uint16_t>(rng_() & 0xFFFF);
    return std::string(reinterpret_cast<char*>(&tid), sizeof(tid));
}

inline asio::awaitable<void> DHTNode::send_krpc_response(const Value& response, udp::endpoint remote) {
    auto encoded = encode(response);
    co_await socket_.send_to(encoded, remote);
}

inline asio::awaitable<void> DHTNode::send_krpc_error(const String& transaction_id, Integer error_code, const String& error_message, udp::endpoint remote) {
    auto err = create_error_response(transaction_id, error_code, error_message);
    co_await send_krpc_response(err, remote);
}

template<typename CompletionToken> 
auto DHTNode::async_ping(String tid, udp::endpoint target, CompletionToken&& token) {
    return asio::async_initiate<CompletionToken, void(boost::system::error_code, Value)>(
        [target = std::move(target), tid = std::move(tid), this] (auto handler) mutable {
            auto ping_query = create_ping_query(tid, my_node_id_);
            auto encoded = encode(ping_query);

            auto query = std::make_shared<PendingQuery>();
            query->remote = target;
            query->expiry = std::chrono::steady_clock::now() + KRPC_QUERY_TIMEOUT;

            // Register cancellation slot: when the parallel_group cancels this op,
            // clean up the pending query and invoke completion with operation_aborted.
            // This is required because parallel_group waits for ALL operations to
            // complete before invoking the final handler (outstanding_ == 0).
            auto cancel_slot = asio::get_associated_cancellation_slot(handler);
            if (cancel_slot.is_connected()) {
                cancel_slot.assign([this, tid, query] (asio::cancellation_type_t type) mutable {
                    if (type != asio::cancellation_type::none) {
                        std::shared_ptr<PendingQuery> extracted;
                        {
                            std::lock_guard lock(queries_mutex_);
                            auto it = pending_queries_.find(tid);
                            if (it != pending_queries_.end()) {
                                extracted = std::move(it->second);
                                pending_queries_.erase(it);
                            }
                        }
                        if (extracted && extracted->completion) {
                            extracted->completion(asio::error::operation_aborted, Value{});
                        }
                    }
                });
            }

            query->completion = [handler = std::move(handler)] (auto ec, auto value) mutable {
                handler(ec, value);
            };

            {
                std::lock_guard lock(queries_mutex_);
                pending_queries_[tid] = query;
            }

            asio::co_spawn(io_context_, socket_.send_to(encoded, target), asio::detached);
        }
    , token);
}

inline asio::awaitable<void> DHTNode::send_ping(const udp::endpoint& target_endpoint) {
    CTRACK_ASYNC("DHTNode::send_ping");
    if (shuting_down_) {
        co_return;
    }

    auto transaction_id = generate_transaction_id();
    try {
        asio::steady_timer timer(io_context_);
        timer.expires_after(KRPC_QUERY_TIMEOUT);

        auto result = co_await (
            async_ping(transaction_id, target_endpoint, asio::use_awaitable) || 
            timer.async_wait(asio::use_awaitable)
        );

        if (result.index() == 0) {
            Value response = std::get<0>(result);
            // LOGDBG("Received PING response from {}", target_endpoint);
            
            // Update routing table if node responded
            const Dict& response_dict = *std::get<std::unique_ptr<Dict>>(response.get_variant());
            const Dict& r_values = *std::get<std::unique_ptr<Dict>>(response_dict.at("r").get_variant());
            NodeId responder_id = Crypto::from_string(std::get<String>(r_values.at("id").get_variant()));
            
            routing_table_.insert({responder_id, target_endpoint, NodeStatus::Good, std::chrono::steady_clock::now(), std::chrono::steady_clock::now()});
        } else { // timeout
            LOGWARN("PING to {} timed out after {}s.", target_endpoint.address().to_string(), KRPC_QUERY_TIMEOUT.count());
            // Mark node as questionable/bad if it fails to respond
            // This requires finding it in the routing table by endpoint, which isn't efficient with current KBucketEntry.
            // For a full implementation, would need to enhance KBucketEntry to store endpoint or map endpoints to NodeIds.
        }
    } catch (const std::exception& e) {
        LOGWARN("PING to {} failed: {}", target_endpoint.address().to_string(), e.what());
    }
    {
        std::lock_guard lock(queries_mutex_);
        pending_queries_.erase(transaction_id);
    }
}

template<typename CompletionToken> 
auto DHTNode::async_find_nodes(String tid, BucketEntry node, NodeId target_id, CompletionToken&& token) {
    return asio::async_initiate<CompletionToken, void(boost::system::error_code, Value)>(
        [this, tid = std::move(tid), node = std::move(node), target_id = std::move(target_id)] (auto handler) {
            auto find_node_query = create_find_node_query(tid, node.id, target_id);
            auto encoded = encode(find_node_query);

            auto query = std::make_shared<PendingQuery>();
            query->remote = node.endpoint;
            query->expiry = std::chrono::steady_clock::now() + KRPC_QUERY_TIMEOUT;

            {
                auto cancel_slot = asio::get_associated_cancellation_slot(handler);
                if (cancel_slot.is_connected()) {
                    cancel_slot.assign([this, tid, query] (asio::cancellation_type_t type) mutable {
                        if (type != asio::cancellation_type::none) {
                            std::shared_ptr<PendingQuery> extracted;
                            {
                                std::lock_guard lock(queries_mutex_);
                                auto it = pending_queries_.find(tid);
                                if (it != pending_queries_.end()) {
                                    extracted = std::move(it->second);
                                    pending_queries_.erase(it);
                                }
                            }
                            if (extracted && extracted->completion) {
                                extracted->completion(asio::error::operation_aborted, Value{});
                            }
                        }
                    });
                }
            }

            query->completion = [handler = std::move(handler)] (auto ec, auto value) mutable {
                handler(ec, value);
            };

            {
                std::lock_guard lock(queries_mutex_);
                pending_queries_[tid] = query;
            }

            asio::co_spawn(io_context_, socket_.send_to(encoded, node.endpoint), asio::detached);
        }    
    , token);
}

inline asio::awaitable<std::vector<BucketEntry>> DHTNode::find_nodes(const NodeId& target_id, uint count) {
    CTRACK_ASYNC("DHTNode::find_nodes");
    if (shuting_down_) {
        co_return std::vector<BucketEntry>{};
    }

    auto closest_nodes = routing_table_.find_closest_nodes(target_id, count);
    std::vector<BucketEntry> result;
    
    for (const auto& e : closest_nodes) {
        if (e.id != my_node_id_ && e.status != NodeStatus::Bad) {
            result.push_back(e);
        }
    }
    
    std::sort(result.begin(), result.end(), [&target_id] (const auto& a, const auto& b) {
        return distance(a.id, target_id) < distance(b.id, target_id);
    });
    
    std::vector<BucketEntry> nodes_to_query(result);
    if (nodes_to_query.size() > ALPHA) {
        nodes_to_query.resize(ALPHA);
    }
    std::set<NodeId> queried_nodes;
    for (const auto& n : nodes_to_query) {
        queried_nodes.insert(n.id);
    }
    
    for (int i = 0; i < 3; ++i) {
        if (shuting_down_) {
            break;
        }
        if (nodes_to_query.empty()) {
            break;
        }

        std::vector<BucketEntry> discovered;
        std::mutex mutex;
        auto find_nodes_coro = [this, &target_id, &discovered, &mutex, &queried_nodes] (const BucketEntry& node) -> asio::awaitable<void> {
            if (shuting_down_) {
                co_return;
            }
            auto transaction_id = generate_transaction_id();
            try {
                asio::steady_timer timer(io_context_);
                timer.expires_after(KRPC_QUERY_TIMEOUT);

                auto result = co_await (
                    async_find_nodes(transaction_id, node, target_id, asio::use_awaitable) ||
                    timer.async_wait(asio::use_awaitable)
                );

                if (result.index() == 0) {
                    Value response = std::get<0>(result);
                    const Dict& response_dict = *std::get<std::unique_ptr<Dict>>(response.get_variant());
                    const Dict& r_values = *std::get<std::unique_ptr<Dict>>(response_dict.at("r").get_variant());
                    NodeId responder_id = Crypto::from_string(std::get<String>(r_values.at("id").get_variant()));
                    std::string compact_nodes_str = std::get<String>(r_values.at("nodes").get_variant());

                    routing_table_.insert({responder_id, node.endpoint, NodeStatus::Good, std::chrono::steady_clock::now(), std::chrono::steady_clock::now()});
                    std::vector<BucketEntry> discovered_from_node = from_compact_node_info(compact_nodes_str);
                    std::lock_guard lock(mutex);
                    for (const auto& n : discovered_from_node) {
                        if (n.id != my_node_id_ && !queried_nodes.count(n.id)) {
                            discovered.push_back(n);
                        }
                    }
                } else {  // timeout
                    routing_table_.update_status(node.id, NodeStatus::Bad);
                }
            } catch (const std::exception& e) {
                LOGDBG("find_node query to {} failed: {}", node.endpoint.address().to_string(), e.what());
                routing_table_.update_status(node.id, NodeStatus::Bad);
            }
            {
                std::lock_guard lock(queries_mutex_);
                pending_queries_.erase(transaction_id);
            }
        };

        using defered_t = decltype(asio::co_spawn(std::declval<asio::io_context&>(), find_nodes_coro(std::declval<BucketEntry>()), asio::deferred));
        std::vector<defered_t> current_queries;
        
        for (const auto& node : nodes_to_query) {
            current_queries.push_back(asio::co_spawn(io_context_, find_nodes_coro(node), asio::deferred));
        }

        if (!current_queries.empty()) {
            auto group = asio::experimental::make_parallel_group(std::move(current_queries));
            co_await group.async_wait(asio::experimental::wait_for_all(), asio::use_awaitable);
        }

        nodes_to_query.clear();
        for (const auto& node : discovered) {
            auto it = std::find_if(result.begin(), result.end(), [&node] (const auto& n) {
                return node.id == n.id;
            });
            if (it == result.end()) {
                result.push_back(node);
            }
            if (!queried_nodes.count(node.id)) {
                nodes_to_query.push_back(node);
                queried_nodes.insert(node.id);
            }
        }

        auto sorter = [&] (const BucketEntry& a, const BucketEntry& b) {
            return distance(a.id, target_id) < distance(b.id, target_id); 
        };

        std::sort(result.begin(), result.end(), sorter);
        if (result.size() > count) {
            result.resize(count);
        }

        std::sort(nodes_to_query.begin(), nodes_to_query.end(), sorter);
        if (nodes_to_query.size() > ALPHA) {
            nodes_to_query.resize(ALPHA);
        }
    } 

    co_return result;
}

template<typename CompletionToken> 
auto DHTNode::async_get_peers(String tid, BucketEntry node, InfoHash info_hash, CompletionToken&& token) {
    return asio::async_initiate<CompletionToken, void(boost::system::error_code, Value)>(
        [this, tid = std::move(tid), node = std::move(node), info_hash = std::move(info_hash)] (auto handler) {
            auto get_peers_query = create_get_peers_query(tid, node.id, info_hash);
            auto encoded = encode(get_peers_query);

            auto query = std::make_shared<PendingQuery>();
            query->remote = node.endpoint;
            query->expiry = std::chrono::steady_clock::now() + KRPC_QUERY_TIMEOUT;

            {
                auto cancel_slot = asio::get_associated_cancellation_slot(handler);
                if (cancel_slot.is_connected()) {
                    cancel_slot.assign([this, tid, query] (asio::cancellation_type_t type) mutable {
                        if (type != asio::cancellation_type::none) {
                            std::shared_ptr<PendingQuery> extracted;
                            {
                                std::lock_guard lock(queries_mutex_);
                                auto it = pending_queries_.find(tid);
                                if (it != pending_queries_.end()) {
                                    extracted = std::move(it->second);
                                    pending_queries_.erase(it);
                                }
                            }
                            if (extracted && extracted->completion) {
                                extracted->completion(asio::error::operation_aborted, Value{});
                            }
                        }
                    });
                }
            }

            query->completion = [handler = std::move(handler)] (auto ec, auto value) mutable {
                handler(ec, value);
            };

            {
                std::lock_guard lock(queries_mutex_);
                pending_queries_[tid] = query;
            }

            asio::co_spawn(io_context_, socket_.send_to(encoded, node.endpoint), asio::detached);            
        }    
    , token);
}

inline asio::awaitable<std::vector<EndPoint>> DHTNode::get_peers(const InfoHash& info_hash, uint count) {
    CTRACK_ASYNC("DHTNode::get_peers");
    if (shuting_down_) {
        co_return std::vector<EndPoint>{};
    }

    std::vector<EndPoint> discovered;
    std::set<EndPoint> added;
    {
        std::lock_guard lock(known_peers_mutex_);
        if (known_peers_.count(info_hash)) {
            for (const auto& peer_info : known_peers_.at(info_hash)) {
                if (peer_info.expiry > std::chrono::steady_clock::now() && !added.count(peer_info.endpoint)) {
                    discovered.push_back(peer_info.endpoint);
                    added.insert(peer_info.endpoint);
                    if (discovered.size() > count) {
                        co_return discovered;
                    }
                }
            }
        }
    }

    auto nodes_to_query = routing_table_.find_closest_nodes(info_hash, count);
    std::set<NodeId> queried_nodes;
    for (const auto& node : nodes_to_query) {
        queried_nodes.insert(node.id);
    }

    for (int i = 0; i < 3; ++i) {
        if (shuting_down_) {
            break;
        }
        if (nodes_to_query.empty() || discovered.size() > count) {
            break;
        }

        std::mutex query_result_mutex;
        std::vector<EndPoint> current_peers;
        std::vector<BucketEntry> current_nodes;

        auto get_peers_coro = [this, &info_hash, &query_result_mutex, &current_peers, &current_nodes] (const BucketEntry& node) -> asio::awaitable<void> {
            if (shuting_down_) {
                co_return;
            }
            auto tid = generate_transaction_id();
            try {
                asio::steady_timer timer(io_context_);
                timer.expires_after(KRPC_QUERY_TIMEOUT);

                auto result = co_await (
                    async_get_peers(tid, node, info_hash, asio::use_awaitable) ||
                    timer.async_wait(asio::use_awaitable)
                );

                if (result.index() == 0) {
                    Value response = std::get<0>(result);
                    const Dict& response_dict = *std::get<std::unique_ptr<Dict>>(response.get_variant());
                    const Dict& r_values = *std::get<std::unique_ptr<Dict>>(response_dict.at("r").get_variant());

                    NodeId responder_id = Crypto::from_string(std::get<String>(r_values.at("id").get_variant()));
                    routing_table_.insert({responder_id, node.endpoint, NodeStatus::Good, std::chrono::steady_clock::now(), std::chrono::steady_clock::now()});
                    
                    if (r_values.count("values")) {
                        const List& compact_peers_list = *std::get<std::unique_ptr<List>>(r_values.at("values").get_variant());
                        std::lock_guard lock(query_result_mutex);
                        for (const auto& val : compact_peers_list) {
                            std::string compact_peer_str = std::get<String>(val.get_variant());
                            std::vector<EndPoint> peers_from_node = from_compact_peer_info(compact_peer_str);
                            for (const auto& peer_ep : peers_from_node) {
                                current_peers.push_back(peer_ep);
                            }
                        }
                    } else if (r_values.count("nodes")) {
                        std::string compact_nodes_str = std::get<String>(r_values.at("nodes").get_variant());
                        std::vector<BucketEntry> nodes_from_node = from_compact_node_info(compact_nodes_str);
                        std::lock_guard lock(query_result_mutex);
                        for (auto& n : nodes_from_node) {
                            current_nodes.push_back(n);
                        }
                    }

                    if (r_values.count("token")) {
                        String token_val = std::get<String>(r_values.at("token").get_variant());
                        std::lock_guard lock(tokens_mutex_);
                        tokens_issued_[info_hash][node.endpoint] = {token_val, std::chrono::steady_clock::now() + ANNOUNCE_TOKEN_LIFETIME};
                    }
                } else {  // timeout
                    routing_table_.update_status(node.id, NodeStatus::Bad);
                }
            } catch (const std::exception& e) {
                LOGDBG("get_peers query to {} failed: {}", node.endpoint.address().to_string(), e.what());
                routing_table_.update_status(node.id, NodeStatus::Bad);
            }
            {
                std::lock_guard lock(queries_mutex_);
                pending_queries_.erase(tid);
            }
        };

        using defered_t = decltype(asio::co_spawn(std::declval<asio::io_context&>(), get_peers_coro(std::declval<BucketEntry>()), asio::deferred));
        std::vector<defered_t> current_queries;
        
        for (int i = 0; i < ALPHA && !nodes_to_query.empty(); ++i) {
            auto node = nodes_to_query.back();
            nodes_to_query.pop_back();
            if (node.id == my_node_id_ || queried_nodes.count(node.id)) {
                continue;
            }
            queried_nodes.insert(node.id);
            current_queries.push_back(asio::co_spawn(io_context_, get_peers_coro(node), asio::deferred));
        }
        
        if (!current_queries.empty()) {
            auto group = asio::experimental::make_parallel_group(std::move(current_queries));
            co_await group.async_wait(asio::experimental::wait_for_all(), asio::use_awaitable);
        }

        nodes_to_query.clear();
        for (auto& n : current_nodes) {
            auto it = std::find_if(nodes_to_query.begin(), nodes_to_query.end(), [&n](const auto& existing) {
                return existing.id == n.id;
            });
            if (it == nodes_to_query.end() && !queried_nodes.count(n.id)) {
                nodes_to_query.push_back(n);
                queried_nodes.insert(n.id);
            }
        }
        for (const auto& peer_ep : current_peers) {
            if (!added.count(peer_ep)) {
                discovered.push_back(peer_ep);
                added.insert(peer_ep);
            }
        }
        
        std::sort(nodes_to_query.begin(), nodes_to_query.end(), [&info_hash](const auto& a, const auto& b) {
            return distance(a.id, info_hash) < distance(b.id, info_hash);
        });
        if (nodes_to_query.size() > ALPHA) {
            nodes_to_query.resize(ALPHA);
        }
        
        if (discovered.size() >= count) {
            break;
        }
    }
    co_return discovered;
}

template<typename CompletionToken>
auto DHTNode::async_announce_peer(String tid, BucketEntry node, InfoHash info_hash, uint16_t port, String announce_token, CompletionToken&& token) {
    return asio::async_initiate<CompletionToken, void(boost::system::error_code, Value)>(
        [this, tid = std::move(tid), node = std::move(node), info_hash = std::move(info_hash), port, announce_token = std::move(announce_token)] (auto handler) {
            auto announce_query = create_announce_peer_query(tid, node.id, info_hash, port, announce_token);
            auto encoded = encode(announce_query);

            auto query = std::make_shared<PendingQuery>();
            query->remote = node.endpoint;
            query->expiry = std::chrono::steady_clock::now() + KRPC_QUERY_TIMEOUT;

            {
                auto cancel_slot = asio::get_associated_cancellation_slot(handler);
                if (cancel_slot.is_connected()) {
                    cancel_slot.assign([this, tid, query] (asio::cancellation_type_t type) mutable {
                        if (type != asio::cancellation_type::none) {
                            std::shared_ptr<PendingQuery> extracted;
                            {
                                std::lock_guard lock(queries_mutex_);
                                auto it = pending_queries_.find(tid);
                                if (it != pending_queries_.end()) {
                                    extracted = std::move(it->second);
                                    pending_queries_.erase(it);
                                }
                            }
                            if (extracted && extracted->completion) {
                                extracted->completion(asio::error::operation_aborted, Value{});
                            }
                        }
                    });
                }
            }

            query->completion = [handler = std::move(handler)] (auto ec, auto value) mutable {
                handler(ec, value);
            };

            {
                std::lock_guard lock(queries_mutex_);
                pending_queries_[tid] = query;
            }

            asio::co_spawn(io_context_, socket_.send_to(encoded, node.endpoint), asio::detached);
        }
    , token);
}

inline asio::awaitable<void> DHTNode::announce_peer(const InfoHash& info_hash, uint16_t client_port) {
    CTRACK_ASYNC("DHTNode::announce_peer");
    if (shuting_down_) {
        co_return;
    }

    auto closest = routing_table_.find_closest_nodes(info_hash, K);

    for (const auto& node : closest) {
        if (shuting_down_) {
            co_return;
        }
        if (node.id == my_node_id_ || node.status == NodeStatus::Bad) {
            continue;
        }

        String token;
        {
            auto tid = generate_transaction_id();
            try {
                asio::steady_timer timer(io_context_);
                timer.expires_after(KRPC_QUERY_TIMEOUT);

                auto result = co_await (
                    async_get_peers(tid, node, info_hash, asio::use_awaitable) ||
                    timer.async_wait(asio::use_awaitable)
                );

                if (result.index() == 0) {
                    Value response = std::get<0>(result);
                    const Dict& response_dict = *std::get<std::unique_ptr<Dict>>(response.get_variant());
                    const Dict& r_values = *std::get<std::unique_ptr<Dict>>(response_dict.at("r").get_variant());

                    if (r_values.count("token")) {
                        token = std::get<String>(r_values.at("token").get_variant());
                    }

                    NodeId responder_id = Crypto::from_string(std::get<String>(r_values.at("id").get_variant()));
                    routing_table_.insert({responder_id, node.endpoint, NodeStatus::Good,
                        std::chrono::steady_clock::now(), std::chrono::steady_clock::now()});
                } else {
                    routing_table_.update_status(node.id, NodeStatus::Bad);
                }
            } catch (const std::exception& e) {
                LOGDBG("get_peers during announce failed for {}: {}", node.endpoint.address().to_string(), e.what());
                routing_table_.update_status(node.id, NodeStatus::Bad);
            }
            {
                std::lock_guard lock(queries_mutex_);
                pending_queries_.erase(tid);
            }
        }

        if (token.empty()) {
            continue;
        }

        {
            auto tid = generate_transaction_id();
            try {
                asio::steady_timer timer(io_context_);
                timer.expires_after(KRPC_QUERY_TIMEOUT);

                auto result = co_await (
                    async_announce_peer(tid, node, info_hash, client_port, token, asio::use_awaitable) ||
                    timer.async_wait(asio::use_awaitable)
                );

                if (result.index() == 0) {
                    LOGDBG("Announced to {}:{} for infohash {}",
                        node.endpoint.address().to_string(), node.endpoint.port(),
                        Crypto::bytes_to_hex(info_hash));
                } else {
                    routing_table_.update_status(node.id, NodeStatus::Bad);
                }
            } catch (const std::exception& e) {
                LOGDBG("announce_peer to {} failed: {}", node.endpoint.address().to_string(), e.what());
                routing_table_.update_status(node.id, NodeStatus::Bad);
            }
            {
                std::lock_guard lock(queries_mutex_);
                pending_queries_.erase(tid);
            }
        }
    }
}

inline std::string DHTNode::to_compact_node_info(const std::vector<BucketEntry>& nodes) {
    std::string compact;
    compact.reserve(nodes.size() * 26);
    for (const auto& node : nodes) {
        compact.append(reinterpret_cast<const char*>(node.id.data()), 20);
        auto addr_bytes = node.endpoint.address().to_v4().to_bytes();
        compact.append(reinterpret_cast<const char*>(addr_bytes.data()), 4);
        uint16_t net_port = htons(node.endpoint.port());
        compact.append(reinterpret_cast<const char*>(&net_port), 2);
    }
    return compact;
}

inline std::string DHTNode::to_compact_peer_info(const EndPoint& peer_endpoint) {
    std::string compact;
    compact.reserve(6);
    auto addr_bytes = peer_endpoint.address().to_v4().to_bytes();
    compact.append(reinterpret_cast<const char*>(addr_bytes.data()), 4);
    uint16_t net_port = htons(peer_endpoint.port());
    compact.append(reinterpret_cast<const char*>(&net_port), 2);
    return compact;
}

inline List DHTNode::to_compact_peer_list(const std::vector<EndPoint>& peers) {
    List peer_list;
    peer_list.reserve(peers.size());
    for (const auto& peer : peers) {
        peer_list.push_back(Value(to_compact_peer_info(peer)));
    }
    return peer_list;
}

inline std::vector<BucketEntry> DHTNode::from_compact_node_info(std::string_view compact_nodes_str) {
    std::vector<BucketEntry> nodes;
    if (compact_nodes_str.empty()) {
        return nodes;
    }
    if (compact_nodes_str.size() % 26 != 0) {
        LOGWARN("Invalid compact node info length: {}", compact_nodes_str.size());
        return nodes;
    }

    for (size_t i = 0; i < compact_nodes_str.size(); i += 26) {
        BucketEntry entry;
        std::memcpy(entry.id.data(), compact_nodes_str.data() + i, 20);
        asio::ip::address_v4::bytes_type ip_bytes;
        std::memcpy(ip_bytes.data(), compact_nodes_str.data() + i + 20, 4);
        uint16_t net_port;
        std::memcpy(&net_port, compact_nodes_str.data() + i + 24, 2);
        uint16_t host_port = ntohs(net_port);

        entry.endpoint = udp::endpoint(asio::ip::address_v4(ip_bytes), host_port);
        entry.status = NodeStatus::Qustionable;
        auto now = std::chrono::steady_clock::now();
        entry.last_seen = now;
        entry.last_replied = now;

        nodes.push_back(std::move(entry));
    }
    return nodes;
}

inline std::vector<EndPoint> DHTNode::from_compact_peer_info(std::string_view compact_peers_str) {
    std::vector<EndPoint> peers;
    if (compact_peers_str.empty()) {
        return peers;
    }
    if (compact_peers_str.size() % 6 != 0) {
        LOGWARN("Invalid compact peer info length: {}", compact_peers_str.size());
        return peers;
    }

    for (size_t i = 0; i < compact_peers_str.size(); i += 6) {
        asio::ip::address_v4::bytes_type ip_bytes;
        std::memcpy(ip_bytes.data(), compact_peers_str.data() + i, 4);
        uint16_t net_port;
        std::memcpy(&net_port, compact_peers_str.data() + i + 4, 2);
        uint16_t host_port = ntohs(net_port);
        peers.emplace_back(asio::ip::address_v4(ip_bytes), host_port);
    }
    return peers;
}

inline String DHTNode::generate_token(const udp::endpoint& remote_endpoint) const {
    auto now = std::chrono::steady_clock::now();
    uint64_t time_window = std::chrono::duration_cast<std::chrono::minutes>(
        now.time_since_epoch()).count() / ANNOUNCE_TOKEN_LIFETIME.count();
    std::string secret = "DHT-TOKEN-SECRET-" + std::to_string(time_window);
    std::string input = remote_endpoint.address().to_string() + secret;
    auto hash = Crypto::calculate_data_hash(
        {reinterpret_cast<const std::byte*>(input.data()), input.size()});
    return hash.substr(0, 8);
}

inline asio::awaitable<void> DHTNode::on_ping_query(const Dict& args, const String& transaction_id, udp::endpoint remote_endpoint) {
    auto id_it = args.find("id");
    if (id_it != args.end()) {
        NodeId sender_id = Crypto::from_string(std::get<String>(id_it->second.get_variant()));
        if (sender_id != my_node_id_) {
            routing_table_.insert({sender_id, remote_endpoint, NodeStatus::Good,
                std::chrono::steady_clock::now(), std::chrono::steady_clock::now()});
        }
    }

    auto response = create_ping_response(transaction_id, my_node_id_);
    co_await send_krpc_response(response, remote_endpoint);
}

inline asio::awaitable<void> DHTNode::on_find_node_query(const Dict& args, const String& transaction_id, udp::endpoint remote_endpoint) {
    auto id_it = args.find("id");
    if (id_it != args.end()) {
        NodeId sender_id = Crypto::from_string(std::get<String>(id_it->second.get_variant()));
        if (sender_id != my_node_id_) {
            routing_table_.insert({sender_id, remote_endpoint, NodeStatus::Good,
                std::chrono::steady_clock::now(), std::chrono::steady_clock::now()});
        }
    }

    auto target_it = args.find("target");
    if (target_it == args.end()) {
        co_await send_krpc_error(transaction_id, 203, "Missing target argument", remote_endpoint);
        co_return;
    }
    NodeId target_id = Crypto::from_string(std::get<String>(target_it->second.get_variant()));

    auto closest = routing_table_.find_closest_nodes(target_id, K);
    std::string compact_nodes = to_compact_node_info(closest);
    auto response = create_find_node_response(transaction_id, my_node_id_, std::move(compact_nodes));
    co_await send_krpc_response(response, remote_endpoint);
}

inline asio::awaitable<void> DHTNode::on_get_peers_query(const Dict& args, const String& transaction_id, udp::endpoint remote_endpoint) {
    auto id_it = args.find("id");
    if (id_it != args.end()) {
        NodeId sender_id = Crypto::from_string(std::get<String>(id_it->second.get_variant()));
        if (sender_id != my_node_id_) {
            routing_table_.insert({sender_id, remote_endpoint, NodeStatus::Good,
                std::chrono::steady_clock::now(), std::chrono::steady_clock::now()});
        }
    }

    auto hash_it = args.find("info_hash");
    if (hash_it == args.end()) {
        co_await send_krpc_error(transaction_id, 203, "Missing info_hash argument", remote_endpoint);
        co_return;
    }
    InfoHash info_hash = Crypto::from_string(std::get<String>(hash_it->second.get_variant()));

    String token = generate_token(remote_endpoint);

    std::vector<EndPoint> peers_for_hash;
    {
        std::lock_guard lock(known_peers_mutex_);
        auto it = known_peers_.find(info_hash);
        if (it != known_peers_.end()) {
            auto now = std::chrono::steady_clock::now();
            for (const auto& peer_info : it->second) {
                if (peer_info.expiry > now) {
                    peers_for_hash.push_back(peer_info.endpoint);
                }
            }
        }
    }

    if (!peers_for_hash.empty()) {
        auto peer_list = to_compact_peer_list(peers_for_hash);
        auto response = create_get_peers_response_with_peers(transaction_id, my_node_id_, token, peer_list);
        co_await send_krpc_response(response, remote_endpoint);
    } else {
        auto closest = routing_table_.find_closest_nodes(info_hash, K);
        std::string compact_nodes = to_compact_node_info(closest);
        auto response = create_get_peers_response_with_nodes(transaction_id, my_node_id_, token, std::move(compact_nodes));
        co_await send_krpc_response(response, remote_endpoint);
    }
}

inline asio::awaitable<void> DHTNode::on_announce_peer_query(const Dict& args, const String& transaction_id, udp::endpoint remote_endpoint) {
    auto id_it = args.find("id");
    auto hash_it = args.find("info_hash");
    auto port_it = args.find("port");
    auto token_it = args.find("token");

    if (id_it == args.end() || hash_it == args.end() || port_it == args.end() || token_it == args.end()) {
        co_await send_krpc_error(transaction_id, 203, "Missing required arguments", remote_endpoint);
        co_return;
    }

    InfoHash info_hash = Crypto::from_string(std::get<String>(hash_it->second.get_variant()));
    String received_token = std::get<String>(token_it->second.get_variant());
    uint16_t port = static_cast<uint16_t>(std::get<Integer>(port_it->second.get_variant()));

    auto implied_it = args.find("implied_port");
    if (implied_it != args.end()) {
        Integer implied_port = std::get<Integer>(implied_it->second.get_variant());
        if (implied_port != 0) {
            port = remote_endpoint.port();
        }
    }

    NodeId sender_id = Crypto::from_string(std::get<String>(id_it->second.get_variant()));
    if (sender_id != my_node_id_) {
        routing_table_.insert({sender_id, remote_endpoint, NodeStatus::Good,
            std::chrono::steady_clock::now(), std::chrono::steady_clock::now()});
    }

    String expected_token = generate_token(remote_endpoint);
    if (received_token != expected_token) {
        LOGWARN("Invalid token from {} for announce_peer", remote_endpoint.address().to_string());
        co_await send_krpc_error(transaction_id, 203, "Invalid or expired token", remote_endpoint);
        co_return;
    }

    EndPoint peer_endpoint(remote_endpoint.address(), port);
    {
        std::lock_guard lock(known_peers_mutex_);
        auto& peers = known_peers_[info_hash];
        auto it = std::remove_if(peers.begin(), peers.end(), [&](const PeerInfo& pi) {
            return pi.endpoint == peer_endpoint;
        });
        peers.erase(it, peers.end());
        peers.push_back({peer_endpoint, std::chrono::steady_clock::now() + PEER_STORAGE_LIFETIME});
        LOGDBG("Stored peer {}:{} for info_hash {}", peer_endpoint.address().to_string(), port, Crypto::bytes_to_hex(info_hash));
    }

    auto response = create_announce_peer_response(transaction_id, my_node_id_);
    co_await send_krpc_response(response, remote_endpoint);
}

inline asio::awaitable<void> DHTNode::handle_krpc_message(std::vector<std::byte> data, udp::endpoint remote) {
    Value decoded;
    try {
        decoded = decode(data);
    } catch (const std::exception& e) {
        LOGWARN("Failed to decode KRPC message from {}: {}", remote.address().to_string(), e.what());
        co_return;
    }

    const auto* msg_dict_ptr = std::get_if<std::unique_ptr<Dict>>(&decoded.get_variant());
    if (!msg_dict_ptr) {
        LOGWARN("KRPC message is not a dictionary from {}", remote.address().to_string());
        co_return;
    }
    const Dict& msg_dict = **msg_dict_ptr;

    auto t_it = msg_dict.find("t");
    if (t_it == msg_dict.end()) {
        LOGWARN("KRPC message missing transaction ID from {}", remote.address().to_string());
        co_return;
    }
    String transaction_id = std::get<String>(t_it->second.get_variant());

    auto y_it = msg_dict.find("y");
    if (y_it == msg_dict.end()) {
        LOGWARN("KRPC message missing type from {}", remote.address().to_string());
        co_return;
    }
    String msg_type = std::get<String>(y_it->second.get_variant());

    if (msg_type == "q") {
        co_await handle_query(msg_dict, transaction_id, std::move(remote));
    } else if (msg_type == "r") {
        co_await handle_response(msg_dict, transaction_id, std::move(remote));
    } else if (msg_type == "e") {
        co_await handle_error(msg_dict, transaction_id, std::move(remote));
    } else {
        LOGWARN("Unknown KRPC message type '{}' from {}", msg_type, remote.address().to_string());
    }
}

inline asio::awaitable<void> DHTNode::handle_query(const Dict& query_dict, const String& transaction_id, udp::endpoint remote) {
    auto q_it = query_dict.find("q");
    if (q_it == query_dict.end()) {
        co_await send_krpc_error(transaction_id, 203, "Missing query method", remote);
        co_return;
    }
    String method = std::get<String>(q_it->second.get_variant());

    auto a_it = query_dict.find("a");
    if (a_it == query_dict.end()) {
        co_await send_krpc_error(transaction_id, 203, "Missing query arguments", remote);
        co_return;
    }
    const auto& args_ptr = std::get<std::unique_ptr<Dict>>(a_it->second.get_variant());
    const Dict& args = *args_ptr;

    auto handler_it = query_handlers_.find(method);
    if (handler_it == query_handlers_.end()) {
        LOGWARN("Unknown query method '{}' from {}", method, remote.address().to_string());
        co_await send_krpc_error(transaction_id, 204, "Method unknown", remote);
        co_return;
    }

    co_await (this->*(handler_it->second))(args, transaction_id, std::move(remote));
}

inline asio::awaitable<void> DHTNode::handle_response(const Dict& response_dict, const String& transaction_id, udp::endpoint remote) {
    std::shared_ptr<PendingQuery> query;
    {
        std::lock_guard lock(queries_mutex_);
        auto it = pending_queries_.find(transaction_id);
        if (it != pending_queries_.end()) {
            query = it->second;
            pending_queries_.erase(it);
        }
    }

    if (query) {
        query->completion({}, Value(response_dict));
    } else {
        LOGDBG("Received response for unknown transaction from {}", remote.address().to_string());
    }
    co_return;
}

inline asio::awaitable<void> DHTNode::handle_error(const Dict& error_dict, const String& transaction_id, udp::endpoint /*remote*/) {
    std::shared_ptr<PendingQuery> query;
    {
        std::lock_guard lock(queries_mutex_);
        auto it = pending_queries_.find(transaction_id);
        if (it != pending_queries_.end()) {
            query = it->second;
            pending_queries_.erase(it);
        }
    }

    if (query) {
        query->completion(boost::system::errc::make_error_code(boost::system::errc::protocol_error), Value(error_dict));
    }
    co_return;
}

inline asio::awaitable<void> DHTNode::udp_listen_loop() {
    CTRACK_ASYNC("DHTNode::udp_listen_loop");
    auto self = shared_from_this();
    while (!shuting_down_) {
        try {
            auto [data, remote] = co_await socket_.receive_from(2048);
            co_await handle_krpc_message(std::move(data), std::move(remote));
        } catch (const boost::system::system_error& e) {
            if (e.code() == asio::error::operation_aborted) {
                break;
            }
            LOGERR("UDP receive error: {}", e.what());
        } catch (const std::exception& e) {
            LOGERR("Error in UDP listen loop: {}", e.what());
        }
    }
}

inline asio::awaitable<void> DHTNode::bootstrap(const std::vector<std::string>& bootstrap_nodes_addrs) {
    CTRACK_ASYNC("DHTNode::bootstrap");
    LOGINFO("Bootstrapping DHT with {} nodes", bootstrap_nodes_addrs.size());

    for (const auto& addr : bootstrap_nodes_addrs) {
        if (shuting_down_) {
            co_return;
        }
        try {
            auto [host, port] = decode_address(addr);
            // Resolve hostname to endpoint (handles both dotted-decimal IPs and DNS names)
            udp::resolver resolver(io_context_);
            auto results = co_await resolver.async_resolve(
                host, std::to_string(port),
                asio::ip::resolver_base::numeric_service,
                asio::use_awaitable
            );
            if (results.empty()) {
                LOGWARN("Failed to resolve bootstrap node: {}", addr);
                continue;
            }
            if (shuting_down_) {
                co_return;
            }
            udp::endpoint ep = results.begin()->endpoint();
            co_await send_ping(ep);
        } catch (const std::exception& e) {
            LOGWARN("Failed to bootstrap with {}: {}", addr, e.what());
        }
    }

    if (shuting_down_) {
        co_return;
    }

    auto closest = co_await find_nodes(my_node_id_, K);
    LOGINFO("Bootstrap complete. Routing table has {} closest entries.", closest.size());
}

inline void DHTNode::routing_table_refresh_loop() {
    if (shuting_down_) {
        return;
    }
    auto self = shared_from_this();
    refresh_timer_.expires_after(BUCKET_REFRESH_INTERVAL);
    refresh_timer_.async_wait([self](boost::system::error_code ec) {
        if (ec || self->shuting_down_) {
            return;
        }
        for (size_t i = 0; i < 160; ++i) {
            if (self->routing_table_.needs_refresh(i)) {
                NodeId random_id = generate_id("");
                asio::co_spawn(self->io_context_, self->find_nodes(random_id, K), asio::detached);
            }
        }
        self->routing_table_refresh_loop();
    });
}

inline void DHTNode::cleanup_loop() {
    if (shuting_down_) {
        return;
    }
    auto self = shared_from_this();
    cleanup_timer_.expires_after(std::chrono::minutes(5));
    cleanup_timer_.async_wait([self](boost::system::error_code ec) {
        if (ec || self->shuting_down_) {
            return;
        }

        auto now = std::chrono::steady_clock::now();

        {
            std::lock_guard lock(self->known_peers_mutex_);
            for (auto it = self->known_peers_.begin(); it != self->known_peers_.end(); ) {
                auto& peers = it->second;
                peers.erase(std::remove_if(peers.begin(), peers.end(), [&](const PeerInfo& pi) {
                    return pi.expiry <= now;
                }), peers.end());
                if (peers.empty()) {
                    it = self->known_peers_.erase(it);
                } else {
                    ++it;
                }
            }
        }

        {
            std::lock_guard lock(self->tokens_mutex_);
            for (auto it = self->tokens_issued_.begin(); it != self->tokens_issued_.end(); ) {
                auto& endpoints = it->second;
                for (auto ep_it = endpoints.begin(); ep_it != endpoints.end(); ) {
                    if (ep_it->second.expiry_time <= now) {
                        ep_it = endpoints.erase(ep_it);
                    } else {
                        ++ep_it;
                    }
                }
                if (endpoints.empty()) {
                    it = self->tokens_issued_.erase(it);
                } else {
                    ++it;
                }
            }
        }

        {
            std::lock_guard lock(self->queries_mutex_);
            for (auto it = self->pending_queries_.begin(); it != self->pending_queries_.end(); ) {
                if (it->second->expiry <= now) {
                    it = self->pending_queries_.erase(it);
                } else {
                    ++it;
                }
            }
        }

        self->cleanup_loop();
    });
}
