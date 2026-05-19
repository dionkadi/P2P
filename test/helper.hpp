#pragma once

#include <gtest/gtest.h>
#include <vector>
#include <string>
#include <cstddef>
#include <algorithm>
#include <sstream>
#include <iomanip>
#include <chrono>
#include <random>
#include <filesystem>

#include "Types.hpp"
#include "Tracker.hpp"
#include "TorrentFile.hpp"
#include "TorrentSession.hpp"

using namespace std::chrono_literals;

// Helper to convert std::string to std::vector<std::byte>
inline std::vector<std::byte> string_to_bytes(const std::string& s) {
    std::vector<std::byte> bytes(s.length());
    std::transform(s.begin(), s.end(), bytes.begin(),
                   [](char c){ return static_cast<std::byte>(c); });
    return bytes;
}

// Helper to convert std::vector<std::byte> to std::string
inline std::string bytes_to_string(const std::vector<std::byte>& b) {
    std::string s(b.size(), '\0');
    std::transform(b.begin(), b.end(), s.begin(),
                   [](std::byte b_val){ return static_cast<char>(b_val); });
    return s;
}

// Helper to convert std::span<const std::byte> to std::string for printing
inline std::string span_to_hex_string(std::span<const std::byte> data) {
    std::stringstream ss;
    ss << std::hex << std::setfill('0');
    for (const auto& byte : data) {
        ss << std::setw(2) << static_cast<unsigned>(byte);
    }
    return ss.str();
}

// Helper to create a PeerId from a string for convenience
inline PeerId string_to_peer_id(const std::string& s) {
    PeerId id{};
    if (s.length() != PEER_ID_SIZE) {
        throw std::runtime_error("Invalid PeerId string length for test");
    }
    std::transform(s.begin(), s.end(), id.begin(),
                   [](char c){ return static_cast<std::byte>(c); });
    return id;
}

inline PeerId generate_peer_id() {
    return generate_id(PEER_ID_PREFIX);
}

// Helper to convert hex string to InfoHash (std::array<std::byte, 20>)
inline InfoHash hex_string_to_info_hash(const std::string& hex) {
    auto bytes = Crypto::hex_to_bytes(hex);
    InfoHash hash{};
    std::copy_n(bytes.begin(), std::min(bytes.size(), hash.size()), hash.begin());
    return hash;
}

template <typename Awaitable>
void RunAsync(asio::io_context& io, Awaitable&& awaitable) {
    asio::co_spawn(io, std::forward<Awaitable>(awaitable),
                   [](std::exception_ptr e) {
                       if (e) std::rethrow_exception(e);
                   });
    io.run();
}

template <typename Awaitable>
void RunAsyncFor(asio::io_context& io, std::chrono::milliseconds duration, Awaitable&& awaitable) {
    asio::co_spawn(io, std::forward<Awaitable>(awaitable), [](std::exception_ptr e) {
                       if (e) std::rethrow_exception(e);
                   });
    io.run_for(duration);
}