#pragma once

#include <vector>
#include <string>
#include <cstddef>
#include <algorithm>
#include <sstream>
#include <iomanip>
#include "Types.hpp" // For PeerId, InfoHash
#include <chrono>

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

// Helper to create an InfoHash from a hex string (assuming Crypto::hex_to_bytes exists)
// You might need to include Crypto.hpp here or define hex_to_bytes within the test helpers.
#include "Utils.hpp"
inline InfoHash hex_to_info_hash(const std::string& hex) {
    return Crypto::hex_to_bytes(hex);
}


template <typename Awaitable>
void RunAsync(asio::io_context& io, Awaitable&& awaitable) {
    asio::co_spawn(io, std::forward<Awaitable>(awaitable),
                   [](std::exception_ptr e) {
                       if (e) std::rethrow_exception(e);
                   });
    io.run();
}

#include <random>
#include <filesystem>
#include "Tracker.hpp"
#include "TorrentFile.hpp"

namespace asio = boost::asio;
namespace fs = std::filesystem;
using namespace std::chrono_literals;

// Helper to create a random file of given size
inline void create_random_file(const fs::path& path, size_t size) {
    std::ofstream ofs(path, std::ios::binary);
    std::vector<char> buf(4096);
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> dist(0, 255);
    size_t written = 0;
    while (written < size) {
        size_t chunk = std::min(buf.size(), size - written);
        for (size_t i = 0; i < chunk; ++i)
            buf[i] = static_cast<char>(dist(gen));
        ofs.write(buf.data(), chunk);
        written += chunk;
    }
}

// Compute SHA1 hash of a file (same as Torrent creation)
inline std::vector<std::byte> hash_file(const fs::path& path) {
    return Crypto::calculate_sha1_hash_file(path.string());
}

// Create a torrent file from a single file
inline void create_torrent(const fs::path& source_file,
                           const fs::path& torrent_file,
                           const std::vector<std::string>& trackers,
                           uint32_t piece_size = 16384) {
    MetaInfo::create_from_file(source_file, torrent_file, trackers, piece_size);
}

// Simple tracker that can be run in a separate thread
class TestTracker {
public:
    TestTracker() : io_(), work_guard_(asio::make_work_guard(io_)) {
        thread_ = std::thread([this] { io_.run(); });
    }

    ~TestTracker() {
        work_guard_.reset();
        io_.stop();
        if (thread_.joinable())
            thread_.join();
    }

    void listen_http(int port) {
        tracker_.listen_http(port);
    }

    void listen_udp(int port) {
        tracker_.listen_udp(port);
    }

private:
    asio::io_context io_;
    asio::executor_work_guard<asio::io_context::executor_type> work_guard_;
    std::thread thread_;
    Tracker tracker_;
};

// Helper to wait for a future with timeout
template<typename T>
bool wait_for_future(std::future<T>& fut, std::chrono::milliseconds timeout) {
    return fut.wait_for(timeout) == std::future_status::ready;
}

// RAII temporary directory
struct TempDir {
    fs::path path;
    TempDir() {
        path = fs::temp_directory_path() / "bittorrent_integ_test";
        fs::remove_all(path);
        fs::create_directories(path);
    }
    ~TempDir() { fs::remove_all(path); }
    TempDir(const TempDir&) = delete;
    TempDir& operator=(const TempDir&) = delete;
};

// RAII file with random content
struct TestFile {
    fs::path path;
    size_t size;
    TestFile(const fs::path& dir, size_t sz) : size(sz) {
        path = dir / "test.dat";
        create_random_file(path, sz);
    }
};

// RAII torrent file
struct TestTorrent {
    fs::path path;
    TestTorrent(const fs::path& dir, const fs::path& source_file,
                const std::vector<std::string>& trackers,
                uint32_t piece_size = 16384) {
        path = dir / "test.torrent";
        create_torrent(source_file, path, trackers, piece_size);
    }
};