#pragma once

#include <boost/asio.hpp>
#include <boost/asio/experimental/channel.hpp>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <array>
#include <type_traits>
#include <vector>
#include <string_view>
#include <endian.h>
#include <filesystem>
#include <fstream>
#include <random>
#include <string>
#include <map>
#include <set>
#include <queue>
#include <deque>
#include <chrono>
#include <spdlog/spdlog.h>
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/sinks/rotating_file_sink.h"
#include <openssl/evp.h>
#include <openssl/sha.h>
#include "Bencode.hpp"

using namespace std::chrono_literals;

// constants
constexpr std::string_view PROTOCOL_STRING = "BitTorrent protocol";
constexpr size_t HASH_SIZE = 20;
constexpr size_t PEER_ID_SIZE = 20;
constexpr size_t HANDSHAKE_RESERVED_BYTES = 8;
constexpr size_t HANDSHAKE_BASE_LEN = 1 + PROTOCOL_STRING.size() + HANDSHAKE_RESERVED_BYTES + HASH_SIZE + PEER_ID_SIZE;
constexpr size_t BLOCK_SIZE = 16384;

// alias
namespace asio = boost::asio;

using PeerId = std::array<std::byte, 20>;
using NodeId = std::array<std::byte, 20>;
using Distance = std::array<std::byte, 20>;
using InfoHash = std::array<std::byte, 20>;
using EndPoint = asio::ip::tcp::endpoint;
using EC = boost::system::error_code;
using IPv4 = asio::ip::address_v4;
using TimePoint = std::chrono::time_point<std::chrono::steady_clock>;
using udp = asio::ip::udp;


// -------------- HELPERS ---------------
inline std::pair<std::string, uint16_t> decode_address(const std::string& addr) {
    auto colon = addr.find(":");
    if (colon == std::string::npos) {
        throw std::runtime_error("Invalid address: " + addr);
    }

    std::string ip = addr.substr(0, colon);
    std::string port_str = addr.substr(colon + 1);
    uint16_t port;
    try {
        port = std::stoul(port_str);
    } catch (const std::exception& e) {
        throw std::runtime_error("Failed to parse port");
    }

    return {ip, port};
}

static constexpr std::string_view PEER_ID_PREFIX = "-PU0001-";
static constexpr std::string_view NODE_ID_PREFIX = "-PU0001-";
static constexpr std::string_view ALPHANUM = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

constexpr std::array<std::byte, 20> generate_id(std::string_view prefix = "") {
    assert(prefix.size() <= 20);

    std::array<std::byte, 20> id{};
    std::transform(prefix.begin(), prefix.end(), id.begin(), 
        [](char c) { return static_cast<std::byte>(c); }
    );
    
    static std::mt19937 rng = []{
        std::random_device rd;
        return std::mt19937(rd());
    }();
    std::uniform_int_distribution<size_t> distrib(0, ALPHANUM.size() - 1);

    // Fill the remaining bytes with random characters
    for (size_t i = prefix.size(); i < id.size(); ++i) {
        id[i] = static_cast<std::byte>(ALPHANUM[distrib(rng)]);
    }
    return id;
}

// ----------- BUFFER --------------
template<typename T>
concept POD = std::is_standard_layout_v<T> && std::is_trivial_v<T>;

class BufferWriter {
public:
    explicit BufferWriter(std::vector<std::byte>& buffer) noexcept
        : buffer_(buffer) {}

    template<POD T>
    void write(const T& value) {
        const std::byte* begin = reinterpret_cast<const std::byte*>(&value);
        buffer_.insert(buffer_.end(), begin, begin + sizeof(T));
    }

    void write_raw(std::string_view sv) {
        buffer_.insert(buffer_.end(), reinterpret_cast<const std::byte*>(sv.data()), reinterpret_cast<const std::byte*>(sv.data()) + sv.size());
    }

    void write_bytes(std::span<const std::byte> data) {
        buffer_.insert(buffer_.end(), data.begin(), data.end());
    }

private:
    std::vector<std::byte>& buffer_;
};

class BufferReader {
public:
    explicit BufferReader(std::span<const std::byte> buffer) noexcept
        : view_(buffer) {}

    template<POD T>
    T read() {
        if (view_.size() < sizeof(T)) {
            throw std::runtime_error("Not enough data in buffer to read value.");
        }

        T value;
        std::memcpy(&value, view_.data(), sizeof(T));
        view_ = view_.subspan(sizeof(T));
        return value;
    }

    std::span<const std::byte> read_bytes(size_t size) {
        if (view_.size() < size) {
            throw std::runtime_error("Not enough data in buffer to read bytes.");
        }

        auto result = view_.subspan(0, size);
        view_ = view_.subspan(size);
        return result;
    }

    std::span<const std::byte> read_all() {
        return read_bytes(remaining());
    }

    size_t remaining() const noexcept { return view_.size(); }
    bool empty() const noexcept { return view_.empty(); }
    
    private:
    std::span<const std::byte> view_;
};

// ---------- CRYPTO -----------
namespace Crypto {
    
inline std::string bytes_to_hex(std::span<const std::byte> data) {
    std::stringstream ss;
    ss << std::hex << std::setfill('0');
    for (const auto& byte : data) {
        ss << std::setw(2) << static_cast<unsigned int>(byte);
    }
    return ss.str();
}

inline std::string calculate_file_hash(const std::string& file_path) {
    std::ifstream file(file_path, std::ios::binary);
    if (!file.is_open()) {
        throw std::runtime_error(std::format("Failed to open file for hashing: {}", file_path));
    }

    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    if (!ctx) {
        throw std::runtime_error("EVP_MD_CTX_new failed");
    }
    if (EVP_DigestInit_ex(ctx, EVP_sha256(), nullptr) != 1) {
        EVP_MD_CTX_free(ctx);
        throw std::runtime_error("EVP_DigestInit_ex failed");
    }

    std::array<std::byte, 4096> buffer;
    while (file) {
        file.read(reinterpret_cast<char*>(buffer.data()), buffer.size());
        std::streamsize bytes_read = file.gcount();
        if (bytes_read > 0) {
            if (EVP_DigestUpdate(ctx, buffer.data(), static_cast<size_t>(bytes_read)) != 1) {
                EVP_MD_CTX_free(ctx);
                throw std::runtime_error("EVP_DigestUpdate failed");
            }
        }
    }

    unsigned char hash[EVP_MAX_MD_SIZE];
    unsigned int hash_len = 0;

    if (EVP_DigestFinal_ex(ctx, hash, &hash_len) != 1) {
        EVP_MD_CTX_free(ctx);
        throw std::runtime_error("EVP_DigestFinal_ex failed");
    }

    EVP_MD_CTX_free(ctx);

    std::vector<std::byte> result(hash_len);
    std::memcpy(result.data(), hash, hash_len);
    return bytes_to_hex(result);
}

inline std::string calculate_data_hash(std::span<const std::byte> data) {
    unsigned char hash[EVP_MAX_MD_SIZE];
    unsigned int hash_len = 0;
    if (EVP_Digest(data.data(), data.size(), hash, &hash_len, EVP_sha256(), nullptr) != 1) {
        throw std::runtime_error("EVP_Digest failed");
    }

    std::vector<std::byte> result(hash_len);
    std::memcpy(result.data(), hash, hash_len);
    return bytes_to_hex(result);
}

inline std::string calculate_string_hash(const std::string& str) {
    return calculate_data_hash({reinterpret_cast<const std::byte*>(str.data()), str.size()});
}

inline std::vector<std::byte> hex_to_bytes(const std::string &hex) {
    if (hex.length() % 2 != 0) {
        throw std::invalid_argument("Hex string must have an even number of characters");
    }

    std::vector<std::byte> bytes;
    bytes.reserve(hex.length() / 2);
    for (size_t i = 0; i < hex.length(); i += 2) {
        std::string_view byte_string_view(hex.data() + i, 2);
        unsigned int value;
        auto [ptr, ec] = std::from_chars(
            hex.data() + i,
            hex.data() + i + 2,
            value,
            16
        );
        
        if (ec != std::errc() || ptr != hex.data() + i + 2) {
            throw std::invalid_argument("Invalid hex character sequence.");
        }
        bytes.push_back(static_cast<std::byte>(value));
    }
    return bytes;
}


inline std::vector<std::byte> calculate_sha1_hash_data(std::span<const std::byte> data) {
    unsigned char hash[SHA_DIGEST_LENGTH];
    if (!(SHA1(reinterpret_cast<const unsigned char*>(data.data()), data.size(), hash))) {
        throw std::runtime_error("SHA1 failed");
    }

    std::vector<std::byte> result(SHA_DIGEST_LENGTH);
    std::memcpy(result.data(), hash, SHA_DIGEST_LENGTH);
    return result;
}

inline std::vector<std::byte> calculate_sha1_hash_file(const std::string& file_path) {
    std::ifstream file(file_path, std::ios::binary);
    if (!file.is_open()) {
        throw std::runtime_error(std::format("Failed to open file for SHA1 hashing: {}", file_path));
    }
    
    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    if (!ctx) {
        throw std::runtime_error("EVP_MD_CTX_new failed");
    }
    if (EVP_DigestInit_ex(ctx, EVP_sha1(), nullptr) != 1) {
        EVP_MD_CTX_free(ctx);
        throw std::runtime_error("EVP_DigestInit_ex failed");
    }

    std::array<std::byte, 4096> buffer;
    while (file) {
        file.read(reinterpret_cast<char*>(buffer.data()), buffer.size());
        std::streamsize bytes_read = file.gcount();
        if (bytes_read > 0) {
            if (EVP_DigestUpdate(ctx, buffer.data(), static_cast<size_t>(bytes_read)) != 1) {
                EVP_MD_CTX_free(ctx);
                throw std::runtime_error("EVP_DigestUpdate failed");
            }
        }
    }

    unsigned char hash[EVP_MAX_MD_SIZE];
    unsigned int hash_len = 0;

    if (EVP_DigestFinal_ex(ctx, hash, &hash_len) != 1) {
        EVP_MD_CTX_free(ctx);
        throw std::runtime_error("EVP_DigestFinal_ex failed");
    }

    EVP_MD_CTX_free(ctx);

    std::vector<std::byte> result(hash_len);
    std::memcpy(result.data(), hash, hash_len);
    return result;
}

inline std::string to_string(const std::array<std::byte, 20>& arr) {
    return std::string(reinterpret_cast<const char*>(arr.data()), arr.size());
}

inline std::array<std::byte, 20> from_string(std::string_view sv) {
    if (sv.size() != 20) {
        throw std::runtime_error("Invalid ID length (expected 20 bytes)");
    }

    std::array<std::byte, 20> arr;
    std::memcpy(arr.data(), sv.data(), 20);
    return arr;
}

} // namespace Crypto

// ------------ LOGGER -------------

namespace fmt {

// Format peer id, which is std::array<std::byte, 20>
template <>
struct formatter<PeerId> {
    constexpr auto parse(format_parse_context& ctx) {
        return ctx.begin();
    }

    std::string to_string(PeerId id) const {
        std::string s;
        for (auto& b : id) {
            s.push_back(static_cast<char>(b));
        }
        return s;
    }

    template <typename FormatContext>
    auto format(const PeerId& id, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{}", to_string(id));
    }
};

}

static std::once_flag logger_init_flag;

namespace spdlog {
    class logger;
}
 
#define LOGINFO(...) Logger::get()->info(__VA_ARGS__)
#define LOGDBG(...) Logger::get()->debug(__VA_ARGS__)
#define LOGERR(...) Logger::get()->error(__VA_ARGS__)
#define LOGWARN(...) Logger::get()->warn(__VA_ARGS__)
#define LOGCRITICAL(...) Logger::get()->critical(__VA_ARGS__)

class Logger {
public:
    Logger(const Logger&) = delete;
    Logger& operator= (const Logger&) = delete;

    static std::shared_ptr<spdlog::logger> get() {
        std::call_once(logger_init_flag, &Logger::init);
        return logger_;
    }

private:
    Logger() {}

    static void init() {
        constexpr std::string_view name = "logs/server.log";
        try {
            std::filesystem::create_directory("logs");
            std::ofstream f(name.data(), std::ios::trunc | std::ios::in | std::ios::out);
            f.close();
        } catch (const std::exception& e) {
        }

        // auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
        // console_sink->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%^%l%$] [thread %t] %v");

        auto file_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(name.data(), 1024 * 1024 * 5, 3);
        file_sink->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%^%l%$] [thread %t] %v");

        std::vector<spdlog::sink_ptr> sinks {file_sink};
        logger_ = std::make_shared<spdlog::logger>("server_logger", sinks.begin(), sinks.end());

        logger_->set_level(spdlog::level::debug);
        logger_->flush_on(spdlog::level::info);
        spdlog::register_logger(logger_);
    }

    static std::shared_ptr<spdlog::logger> logger_;
};


inline std::shared_ptr<spdlog::logger> Logger::logger_;

// ----------- THREAD POOL -------------
class ThreadPool {
  public:
    explicit ThreadPool(std::size_t num_threads) {
        for (std::size_t i = 0; i < num_threads; ++i) {
            workers_.emplace_back([this] (std::stop_token stop_token) {
                while (!stop_token.stop_requested()) {            
                    std::unique_ptr<ITask> task;
                    {
                        std::unique_lock lock(mutex_);
                        condition_.wait(lock, [this, &stop_token] {
                            return stop_token.stop_requested() || !tasks_.empty();
                        });

                        if (stop_token.stop_requested() && tasks_.empty()) {
                            return ;
                        }

                        task = std::move(tasks_.front());
                        tasks_.pop();
                    }
                    if (task)
                        task->execute();
                }
            });
        }
    }

    ~ThreadPool() {
        for (auto& worker : workers_) {
            worker.request_stop();
        }

        condition_.notify_all();
    }

    template<typename F>
    void enqueue(F&& f) {
        auto task = std::make_unique<Task<F>>(std::forward<F>(f));
        {
            std::lock_guard lock(mutex_);
            tasks_.emplace(std::move(task));
        }
        condition_.notify_one();
    }

  private:
    struct ITask {
        virtual ~ITask() = default;
        virtual void execute() = 0;
    };

    template<class F>
    struct Task : ITask {
        F func;
        Task(F&& f) : func(std::move(f)) {}
        void execute() override { func(); }
    };

    std::vector<std::jthread> workers_;
    std::queue<std::unique_ptr<ITask>> tasks_;
    std::mutex mutex_;
    std::condition_variable condition_;
};

// ----------- TEMPDIR ------------
class TempDir {
public:
    TempDir(std::string name = std::to_string(std::hash<long>()(std::chrono::steady_clock::now().time_since_epoch().count()))) {
        temp_dir_ = std::filesystem::temp_directory_path();
        temp_dir_ /= name;
        std::error_code ec;
        std::filesystem::remove_all(temp_dir_, ec);
        std::filesystem::create_directories(temp_dir_, ec);
    }

    ~TempDir() {
        if (std::filesystem::exists(temp_dir_)) {
            std::error_code ec;
            std::filesystem::remove_all(temp_dir_, ec);
        }
    }

    std::filesystem::path operator*() {
        return temp_dir_;
    }

    TempDir(const TempDir&) = delete;
    TempDir& operator=(const TempDir&) = delete;

private:
    std::filesystem::path temp_dir_;
};

// ----------- Protocol -------------
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
    Port = 9,
    ExtendedMessage = 20,
};

enum class ExtendedMessageType: uint8_t {
    Handshake = 0,
    ut_pex = 1,
    ut_metadata = 3,
    UNKNOWN = 255,
};

inline ExtendedMessageType to_extended_type(const std::string& s) {
    if (s == "ut_pex") {
        return ExtendedMessageType::ut_pex;
    }
    if (s == "ut_metadata") {
        return ExtendedMessageType::ut_metadata;
    }
    throw std::invalid_argument("Unknown extended message type");
}

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
    std::array<std::byte, 20> info_hash;
    std::array<std::byte, 20> peer_id;
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

struct FileInfo {
    std::filesystem::path path;
    uint64_t size;
    bool download{true};
};

struct TorrentInfo {
    std::string name;
    uint64_t total_size = 0;
    uint32_t piece_size = 0;
    std::vector<std::byte> pieces;  // SHA1 value of each piece
    std::vector<FileInfo> files;
};


enum class PieceStatus { 
    Needed, 
    InProgress, 
    Have,
    Skipped,
};

struct PieceFileOverlap {
    size_t file_index;             // Index into meta_info_.get_torrent_info().files
    uint64_t offset_in_file;    // Where this piece's data starts writing in the file
    uint32_t offset_in_piece;   // Where in the piece data the file's content starts
    uint32_t length;            // How many bytes from this piece belong to this file
};


struct AnnounceRequestParams {
    std::vector<std::byte> info_hash_bytes;
    PeerId peer_id;
    std::string event;
    uint16_t port;
    uint64_t uploaded;
    uint64_t downloaded;
    uint64_t left;
};

struct TrackerAnnounceResult {
    int interval_seconds;
    std::vector<std::string> peers;
};

struct InProgressPiece {
    std::vector<std::byte> data;
    std::vector<bool> blocks_received;
    uint32_t received_count = 0;
    uint32_t total_blocks = 0;

    std::vector<std::vector<PeerId>> outstanding_requests;

    InProgressPiece(uint64_t piece_size): data(piece_size) {
        total_blocks = (piece_size + BLOCK_SIZE - 1) / BLOCK_SIZE;
        blocks_received.resize(total_blocks, false);
        outstanding_requests.resize(total_blocks);
    }
};

enum class Mode {
    Seed,
    Leech
};

struct ResumeData {
    std::string have_bitfield;  // Bitfield as string
    std::map<std::string, int64_t> file_mtimes;  // path -> mtime
    uint64_t total_uploaded = 0;
    uint64_t total_downloaded = 0;
    std::map<std::string, std::string> in_progress_pieces;  // piece_index_str -> block bitfield

    std::vector<std::byte> serialize() const {
        Dict files_metadata_dict;
        for (const auto& [path, mtime] : file_mtimes) {
            Dict this_file_dict;
            this_file_dict["mtime"] = Value(static_cast<Integer>(mtime));
            files_metadata_dict[path] = Value(std::move(this_file_dict));
        }
        
        Dict stats_dict;
        stats_dict["uploaded"] = Value(static_cast<Integer>(total_uploaded));
        stats_dict["downloaded"] = Value(static_cast<Integer>(total_downloaded));
        
        Dict in_progress_dict;
        for (const auto& [piece_idx_str, block_bitfield] : in_progress_pieces) {
            in_progress_dict[piece_idx_str] = Value(block_bitfield);
        }

        Dict resume_dict;
        resume_dict["have_bitfield"] = Value(have_bitfield);
        resume_dict["files_metadata"] = Value(std::move(files_metadata_dict));
        resume_dict["stats"] = Value(std::move(stats_dict));
        resume_dict["in_progress"] = Value(std::move(in_progress_dict));

        return encode(Value(std::move(resume_dict)));
    }

    static ResumeData deserialize(std::span<const std::byte> data) {
        auto decoded = decode(data);
        const auto *resume_dict_ptr = std::get_if<std::unique_ptr<Dict>>(&decoded.get_variant());
        if (!resume_dict_ptr) {
            throw std::runtime_error("Resume file is not a dictionary");
        }
        const Dict& resume_dict = **resume_dict_ptr;

        if (!resume_dict.count("have_bitfield")) {
            throw std::runtime_error("Resume file missing 'have_bitfield'");
        }

        ResumeData resume_data;
        resume_data.have_bitfield = std::get<String>(resume_dict.at("have_bitfield").get_variant());

        const Dict* files_metadata_dict = nullptr;
        if (resume_dict.count("files_metadata")) {
            const auto* files_metadata_ptr = std::get_if<std::unique_ptr<Dict>>(&resume_dict.at("files_metadata").get_variant());
            if (files_metadata_ptr) {
                files_metadata_dict = files_metadata_ptr->get();
            }
        }

        const Dict* stats_dict = nullptr;
        if (resume_dict.count("stats")) {
            const auto* stats_ptr = std::get_if<std::unique_ptr<Dict>>(&resume_dict.at("stats").get_variant());
            if (stats_ptr) {
                stats_dict = stats_ptr->get();
            }
        }

        const Dict* in_progress_dict = nullptr;
        if (resume_dict.count("in_progress")) {
            const auto* in_progress_ptr = std::get_if<std::unique_ptr<Dict>>(&resume_dict.at("in_progress").get_variant());
            if (in_progress_ptr) {
                in_progress_dict = in_progress_ptr->get();
            }
        }

        if (files_metadata_dict) {
            for (const auto& [k, v] : *files_metadata_dict) {
                const auto *this_file_dict_ptr = std::get_if<std::unique_ptr<Dict>>(&(files_metadata_dict->at(k).get_variant()));
                if (!this_file_dict_ptr) {
                    continue;
                }
                const Dict& this_file_dict = **this_file_dict_ptr;
                Integer saved_mtime = std::get<Integer>(this_file_dict.at("mtime").get_variant());
                resume_data.file_mtimes[k] = saved_mtime;
            }
        }

        if (stats_dict) {
            resume_data.total_uploaded = std::get<Integer>(stats_dict->at("uploaded").get_variant());
            resume_data.total_downloaded = std::get<Integer>(stats_dict->at("downloaded").get_variant());
        }

        if (in_progress_dict) {
            for (const auto& [piece_idx_str, block_bitfield] : *in_progress_dict) {
                resume_data.in_progress_pieces[piece_idx_str] = std::get<String>(block_bitfield.get_variant());
            }
        }

        return resume_data;
    } 
};

struct Handshake {
    InfoHash info_hash_bytes;
    PeerId peer_id_bytes;
    bool extended{false};

    std::vector<std::byte> serialize() const {
        std::vector<std::byte> buffer;
        buffer.reserve(HANDSHAKE_BASE_LEN);
        BufferWriter writer(buffer);

        writer.write<uint8_t>(PROTOCOL_STRING.length());
        writer.write_raw(PROTOCOL_STRING);

        std::vector<std::byte> reserved(HANDSHAKE_RESERVED_BYTES, std::byte{0});
        // BEP-10: extended messaging
        if (extended) {
            reserved[5] |= static_cast<std::byte>(0x10);
        }
        // BEP-5: DHT support
        reserved[7] |= static_cast<std::byte>(0x01);
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
        std::ranges::copy(info_hash_span, hs.info_hash_bytes.begin());

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

struct EndpointHash {
    std::size_t operator()(const EndPoint& ep) const noexcept {
        std::size_t h1 = std::hash<std::string>{}(ep.address().to_string());
        std::size_t h2 = std::hash<unsigned short>{}(ep.port());
        return h1 ^ (h2 << 1);
    }
};


// ------------ Kademlia ----------------
struct Message {
    String transaction_id; // "t"
    String type; // "y": "q" (query), "r" (response), "e" (error)
    std::optional<String> version; // "v"
};

struct Query : public Message {
    String query_method; // "q"
    Dict arguments; // "a"
};

struct Response : public Message {
    Dict return_values; // "r"
};

struct Error : public Message {
    List error_code_and_messages; // "e": [integer, string]
};

// {"t":"aa", "y":"q", "q":"ping", "a":{"id":"<nid>"}}
inline Value create_ping_query(const String& transaction_id, const NodeId& sender_id) {
    Dict args;
    args["id"] = Value(Crypto::to_string(sender_id));
    return Value(Dict{
        {"t", Value(transaction_id)},
        {"y", Value("q")},
        {"q", Value("ping")},
        {"a", Value(std::move(args))}
    });
}

// {"t":"aa", "y":"q", "q":"find_node", "a":{"id":"<nid>", "target":"<target_node_id>"}}
inline Value create_find_node_query(const String& transaction_id, const NodeId& sender_id, const NodeId& target_id) {
    Dict args;
    args["id"] = Value(Crypto::to_string(sender_id));
    args["target"] = Value(Crypto::to_string(target_id));
    return Value(Dict{
        {"t", Value(transaction_id)},
        {"y", Value("q")},
        {"q", Value("find_node")},
        {"a", Value(std::move(args))}
    });
}

// {"t":"aa", "y":"q", "q":"get_peers", "a":{"id":"<nid>", "info_hash":"<20_byte_infohash>"}}
inline Value create_get_peers_query(const String& transaction_id, const NodeId& sender_id, const InfoHash& info_hash) {
    Dict args;
    args["id"] = Value(Crypto::to_string(sender_id));
    args["info_hash"] = Value(Crypto::to_string(info_hash));
    return Value(Dict{
        {"t", Value(transaction_id)},
        {"y", Value("q")},
        {"q", Value("get_peers")},
        {"a", Value(std::move(args))}
    });
}

// {"t":"aa", "y":"q", "q":"announce_peer", "a":{"id":"<nid>", "info_hash":"<20_byte_infohash>", "port":<port>, "token":"<opaque_token>"}}
inline Value create_announce_peer_query(const String& transaction_id, const NodeId& sender_id, const InfoHash& info_hash, uint16_t port, const String& token) {
    Dict args;
    args["id"] = Value(Crypto::to_string(sender_id));
    args["info_hash"] = Value(Crypto::to_string(info_hash));
    args["port"] = Value(static_cast<Integer>(port));
    args["token"] = Value(token);
    return Value(Dict{
        {"t", Value(transaction_id)},
        {"y", Value("q")},
        {"q", Value("announce_peer")},
        {"a", Value(std::move(args))}
    });
}

// {"t":"aa", "y":"r", "r":{"id":"<nid>"}}
inline Value create_ping_response(const String& transaction_id, const NodeId& responder_id) {
    Dict r;
    r["id"] = Value(Crypto::to_string(responder_id));
    return Value(Dict{
        {"t", Value(transaction_id)},
        {"y", Value("r")},
        {"r", Value(std::move(r))}
    });
}

// {"t":"aa", "y":"r", "r":{"id":"<nid>", "nodes":"<compact_node_info>"}}
inline Value create_find_node_response(const String& transaction_id, const NodeId& responder_id, std::string compact_nodes) {
    Dict r;
    r["id"] = Value(Crypto::to_string(responder_id));
    r["nodes"] = Value(std::move(compact_nodes));
    return Value(Dict{
        {"t", Value(transaction_id)},
        {"y", Value("r")},
        {"r", Value(std::move(r))}
    });
}

// {"t":"aa", "y":"r", "r":{"id":"<nid>", "token":"<opaque_token>", "values":["<compact_peer_info>", ...]}}
// OR
// {"t":"aa", "y":"r", "r":{"id":"<nid>", "token":"<opaque_token>", "nodes":"<compact_node_info>"}}
inline Value create_get_peers_response_with_peers(const String& transaction_id, const NodeId& responder_id, const String& token, const List& compact_peers_list) {
    Dict r;
    r["id"] = Value(Crypto::to_string(responder_id));
    r["token"] = Value(token);
    r["values"] = Value(compact_peers_list);
    return Value(Dict{
        {"t", Value(transaction_id)},
        {"y", Value("r")},
        {"r", Value(std::move(r))}
    });
}
inline Value create_get_peers_response_with_nodes(const String& transaction_id, const NodeId& responder_id, const String& token, std::string compact_nodes) {
    Dict r;
    r["id"] = Value(Crypto::to_string(responder_id));
    r["token"] = Value(token);
    r["nodes"] = Value(std::move(compact_nodes));
    return Value(Dict{
        {"t", Value(transaction_id)},
        {"y", Value("r")},
        {"r", Value(std::move(r))}
    });
}

// {"t":"aa", "y":"r", "r":{"id":"<nid>"}} (Same as ping response)
inline Value create_announce_peer_response(const String& transaction_id, const NodeId& responder_id) {
    return create_ping_response(transaction_id, responder_id);
}

// Error Response
// {"t":"aa", "y":"e", "e":[201, "Server Error"]}
inline Value create_error_response(const String& transaction_id, Integer error_code, const String& error_message) {
    return Value(Dict{
        {"t", Value(transaction_id)},
        {"y", Value("e")},
        {"e", Value(List{Value(error_code), Value(error_message)})}
    });
}


template <typename T>
void print_size(const T&) {
    // no member size()
}

template <typename T, typename = std::void_t<decltype(std::declval<T>().size())>>
void print_size(const T&) {

}