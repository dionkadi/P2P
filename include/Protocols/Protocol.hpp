#pragma once

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <endian.h>
#include <span>
#include <string>
#include <string_view>
#include <vector>
#include <stdexcept>

#include <boost/asio.hpp>
namespace asio = boost::asio;


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
constexpr size_t HASH_SIZE = 20;
constexpr size_t PEER_ID_SIZE = 20;
constexpr size_t HANDSHAKE_RESERVED_BYTES = 8;
constexpr size_t HANDSHAKE_BASE_LEN = 1 + PROTOCOL_STRING.size() + HANDSHAKE_RESERVED_BYTES + HASH_SIZE + PEER_ID_SIZE;
constexpr size_t BLOCK_SIZE = 16384;

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


#pragma pack(push, 1)

struct UdpConnectRequest {
    uint64_t protocol_id = htobe64(0x41727101980);
    uint32_t action = htobe32(0);
    uint32_t transaction_id;
};

struct UdpConnectResponse {
    uint32_t action;
    uint32_t transaction_id;
    uint64_t connection_id;
};

struct UdpAnnounceRequest {
    uint64_t connection_id;
    uint32_t action = htobe32(1); // 1 for announce
    uint32_t transaction_id;
    std::array<char, 20> info_hash;
    std::array<char, 20> peer_id;
    uint64_t downloaded;
    uint64_t left;
    uint64_t uploaded;
    uint32_t event = htobe32(0); // 0: none; 1: completed; 2: started; 3: stopped
    uint32_t ip_address = 0; // 0 for sender's IP
    uint32_t key = 0; // optional
    int32_t num_want = htobe32(-1); // default
    uint16_t port;
};

struct UdpAnnounceResponse {
    uint32_t action;
    uint32_t transaction_id;
    uint32_t interval;
    uint32_t leechers;
    uint32_t seeders;
};

struct UdpPeerInfo {
    uint32_t ip;
    uint16_t port;
};

#pragma pack(pop)