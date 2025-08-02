#pragma once

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <string>
#include <string_view>
#include <vector>
#include <stdexcept>

#include "asio.hpp"


class BufferWriter {
public:
    explicit BufferWriter(std::vector<char>& buffer): buffer_(buffer) {}

    template<typename T>
    void write(const T& value) {
        const char* begin = reinterpret_cast<const char*>(&value);
        buffer_.insert(buffer_.end(), begin, begin + sizeof(T));
    }

    void write_raw(std::string_view sv) {
        buffer_.insert(buffer_.end(), sv.begin(), sv.end());
    }

    void write_bytes(const void *data, size_t size) {
        buffer_.insert(buffer_.end(), static_cast<const char *>(data), static_cast<const char *>(data) + size);
    }

private:
    std::vector<char>& buffer_;
};

class BufferReader {
public:
    explicit BufferReader(std::span<const char> buffer): view_(buffer) {}

    template<typename T>
    T read() {
        if (view_.size() < sizeof(T)) {
            throw std::runtime_error("Not enough data in buffer to read value.");
        }

        T value;
        std::copy_n(view_.begin(), sizeof(T), reinterpret_cast<char*>(&value));
        view_ = view_.subspan(sizeof(T));
        return value;
    }

    std::span<const char> read_bytes(size_t size) {
        if (view_.size() < size) {
            throw std::runtime_error("Not enough data in buffer to read bytes.");
        }

        auto result = view_.subspan(0, size);
        view_ = view_.subspan(size);
        return result;
    }

    size_t remaining() const { return view_.size(); }

private:
    std::span<const char> view_;
};


constexpr std::string_view PROTOCOL_STRING = "MIT-P2P-V1.0";
constexpr size_t HASH_SIZE = 32;
constexpr size_t PEER_ID_SIZE = 20;
constexpr size_t HANDSHAKE_RESERVED_BYTES = 8;
constexpr size_t HANDSHAKE_BASE_LEN = 1 + PROTOCOL_STRING.size() + HANDSHAKE_RESERVED_BYTES + HASH_SIZE + PEER_ID_SIZE;
constexpr size_t BLOCK_SIZE = 16384;

constexpr size_t INFO_HASH_SIZE = 32;

using PeerId = std::string;
using InfoHash = std::vector<char>;

struct Handshake {
    std::string info_hash_bytes;
    std::string peer_id_bytes;

    std::vector<char> serialize() const {
        std::vector<char> buffer;
        buffer.reserve(HANDSHAKE_BASE_LEN);
        BufferWriter writer(buffer);

        writer.write<uint8_t>(PROTOCOL_STRING.length());
        writer.write_raw(PROTOCOL_STRING);
        writer.write_bytes(std::string(HANDSHAKE_RESERVED_BYTES, '\0').data(), HANDSHAKE_RESERVED_BYTES);
        writer.write_bytes(info_hash_bytes.data(), HASH_SIZE);
        writer.write_bytes(peer_id_bytes.data(), PEER_ID_SIZE);

        return buffer;
    }

    static Handshake deserialize(std::span<const char> buffer) {
        if (buffer.size() != HANDSHAKE_BASE_LEN) {
            throw std::runtime_error("Invalid handshake size.");
        }

        BufferReader reader(buffer);

        uint8_t pstrlen = reader.read<uint8_t>();
        if (pstrlen != PROTOCOL_STRING.length()) {
            throw std::runtime_error("Invalid protocol string length in handshake.");
        }

        auto pstr_span = reader.read_bytes(pstrlen);
        if (std::string_view(pstr_span.data(), pstr_span.size()) != PROTOCOL_STRING) {
            throw std::runtime_error("Protocol mismatch.");
        }

        reader.read_bytes(HANDSHAKE_RESERVED_BYTES);
        
        Handshake hs;
        auto info_hash_span = reader.read_bytes(HASH_SIZE);
        hs.info_hash_bytes.assign(info_hash_span.begin(), info_hash_span.end());

        auto peer_id_span = reader.read_bytes(PEER_ID_SIZE);
        hs.peer_id_bytes.assign(peer_id_span.begin(), peer_id_span.end());
        
        return hs;
    }
};

enum class MessageType: uint8_t {
    Choke = 0,
    Unchoke = 1,
    Interested = 2,
    NotInterested = 3,
    Have = 4,
    Bitfield = 5,
    Request = 6,
    Piece = 7,
    Cancel = 8,
};

struct RequestPayload {
    uint32_t index;
    uint32_t begin;
    uint32_t length;

    static std::vector<char> serialize(uint32_t index, uint32_t begin, uint32_t length) {
        std::vector<char> payload;
        payload.reserve(12);
        BufferWriter writer(payload);
        writer.write(asio::detail::socket_ops::host_to_network_long(index));
        writer.write(asio::detail::socket_ops::host_to_network_long(begin));
        writer.write(asio::detail::socket_ops::host_to_network_long(length));
        return payload;
    }

    static RequestPayload deserialize(std::span<const char> payload) {
        if (payload.size() < 12) {
            throw std::runtime_error("Invalid Request payload size");
        }
        BufferReader reader(payload);
        RequestPayload req;
        req.index = asio::detail::socket_ops::network_to_host_long(reader.read<uint32_t>());
        req.begin = asio::detail::socket_ops::network_to_host_long(reader.read<uint32_t>());
        req.length = asio::detail::socket_ops::network_to_host_long(reader.read<uint32_t>());
        return req;
    }
};

struct PiecePayload {
    uint32_t index;
    uint32_t begin;
    std::string block;
};


enum class TrackerMessageType: uint8_t {
    ErrorResponse = 0,
    AnnounceRequest = 1,
    AnnounceResponse = 2,
    QueryRequest = 3,
    QueryResponse = 4,
};


struct TrackerAnnouceReqeust {
    std::string info_hash_bytes;
    uint16_t port;

    static std::vector<char> serialize(const TrackerAnnouceReqeust& req) {
        std::vector<char> buffer;
        buffer.push_back(static_cast<char>(TrackerMessageType::AnnounceRequest));
        BufferWriter writer(buffer);
        writer.write_bytes(req.info_hash_bytes.data(), INFO_HASH_SIZE);
        writer.write(asio::detail::socket_ops::host_to_network_short(req.port));
        return buffer;
    }

    static TrackerAnnouceReqeust deserialize(std::span<const char> payload) {
        TrackerAnnouceReqeust req;
        BufferReader reader(payload);
        auto info_hash_span = reader.read_bytes(INFO_HASH_SIZE);
        req.info_hash_bytes.assign(info_hash_span.begin(), info_hash_span.end());
        req.port = asio::detail::socket_ops::network_to_host_short(reader.read<uint16_t>());
        return req;
    }
};

struct TrackerQueryRequest {
    std::string info_hash_bytes;

    static std::vector<char> serialize(const TrackerQueryRequest& req) {
        std::vector<char> buffer;
        buffer.push_back(static_cast<char>(TrackerMessageType::QueryRequest));
        BufferWriter writer(buffer);
        writer.write_bytes(req.info_hash_bytes.data(), INFO_HASH_SIZE);
        return buffer;
    }

    static TrackerQueryRequest deserialize(std::span<const char> payload) {
        TrackerQueryRequest req;
        BufferReader reader(payload);
        auto info_hash_span = reader.read_bytes(INFO_HASH_SIZE);
        req.info_hash_bytes.assign(info_hash_span.begin(), info_hash_span.end());
        return req;
    }
};

struct TrackerQueryResponse {
    std::vector<std::string> peer_addrs;

    static std::vector<char> serialize(const TrackerQueryResponse& res) {
        std::vector<char> buffer;
        buffer.push_back(static_cast<char>(TrackerMessageType::QueryResponse));
        BufferWriter writer(buffer);

        for (const auto& addr_str : res.peer_addrs) {
            size_t colon_pos = addr_str.find(':');
            if (colon_pos == std::string::npos) {
                throw std::runtime_error("Invalid peer address");
            }
            std::string ip_str = addr_str.substr(0, colon_pos);
            uint16_t port = std::stoul(addr_str.substr(colon_pos+1));

            asio::ip::address_v4 ip = asio::ip::make_address_v4(ip_str);
            auto ip_bytes = ip.to_bytes();

            writer.write_bytes(ip_bytes.data(), ip_bytes.size());
            writer.write(asio::detail::socket_ops::host_to_network_short((port)));
        }

        return buffer;
    }

    static TrackerQueryResponse deserialize(std::span<const char> payload) {
        TrackerQueryResponse res;
        BufferReader reader(payload);

        while (reader.remaining() >= 6) {
            auto ip_bytes_span = reader.read_bytes(4);
            std::array<unsigned char, 4> ip_bytes_array;
            std::copy(ip_bytes_span.begin(), ip_bytes_span.end(), ip_bytes_array.begin());
            asio::ip::address_v4 ip(ip_bytes_array);
            uint16_t port = asio::detail::socket_ops::network_to_host_short(reader.read<uint16_t>());
            res.peer_addrs.push_back(ip.to_string() + ":" + std::to_string(port));
        }

        return res;
    }
};