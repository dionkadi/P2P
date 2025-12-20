#pragma once

#include "Buffer.hpp"
#include "Types.hpp"

#include <boost/asio.hpp>
namespace asio = boost::asio;

struct Handshake {
    InfoHash info_hash_bytes;
    std::array<std::byte, 20> peer_id_bytes;
    bool extended{false};

    std::vector<std::byte> serialize() const {
        std::vector<std::byte> buffer;
        buffer.reserve(HANDSHAKE_BASE_LEN);
        BufferWriter writer(buffer);

        writer.write<uint8_t>(PROTOCOL_STRING.length());
        writer.write_raw(PROTOCOL_STRING);

        std::vector<std::byte> reserved(HANDSHAKE_RESERVED_BYTES, std::byte{0});
        if (extended) {
            reserved[5] |= static_cast<std::byte>(0x10);
        }
        writer.write_bytes(reserved);
        writer.write_bytes(info_hash_bytes);
        writer.write_bytes(peer_id_bytes);

        return buffer;
    }

    static Handshake deserialize(std::span<const std::byte> buffer) {
        if (buffer.size() != HANDSHAKE_BASE_LEN) {
            throw std::runtime_error("Invalid handshake size.");
        }

        BufferReader reader(buffer);

        uint8_t pstrlen = reader.read<uint8_t>();
        if (pstrlen != PROTOCOL_STRING.length()) {
            throw std::runtime_error("Invalid protocol string length in handshake.");
        }

        auto pstr_span = reader.read_bytes(pstrlen);
        if (std::string_view(reinterpret_cast<const char*>(pstr_span.data()), pstr_span.size()) != PROTOCOL_STRING) {
            throw std::runtime_error("Protocol mismatch.");
        }

        Handshake hs;

        auto reserved = reader.read_bytes(HANDSHAKE_RESERVED_BYTES);
        hs.extended = (reserved[5] & static_cast<std::byte>(0x10)) != static_cast<std::byte>(0);
        
        auto info_hash_span = reader.read_bytes(HASH_SIZE);
        hs.info_hash_bytes.assign(info_hash_span.begin(), info_hash_span.end());

        auto peer_id_span = reader.read_bytes(PEER_ID_SIZE);
        std::ranges::copy(peer_id_span, hs.peer_id_bytes.begin());

        return hs;
    }
};

struct RequestPayload {
    uint32_t index;
    uint32_t begin;
    uint32_t length;

    static std::vector<std::byte> serialize(uint32_t index, uint32_t begin, uint32_t length) {
        std::vector<std::byte> payload;
        payload.reserve(12);
        BufferWriter writer(payload);
        writer.write(asio::detail::socket_ops::host_to_network_long(index));
        writer.write(asio::detail::socket_ops::host_to_network_long(begin));
        writer.write(asio::detail::socket_ops::host_to_network_long(length));
        return payload;
    }

    static RequestPayload deserialize(std::span<const std::byte> payload) {
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

inline ExtendedMessageType to_extended_type(const std::string& s) {
    if (s == "ut_pex") {
        return ExtendedMessageType::ut_pex;
    }
    if (s == "ut_metadata") {
        return ExtendedMessageType::ut_metadata;
    }
    throw;
}