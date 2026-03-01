#pragma once

#include <array>
#include <cstddef>
#include <stdexcept>
#include <string>
#include <span>
#include <string_view>
#include <memory>
#include <filesystem>
#include <mutex>
#include <queue>
#include <thread>
#include <condition_variable>

#include <spdlog/spdlog.h>
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/sinks/rotating_file_sink.h"

#include "picosha2.h"
#include "sha1.hpp"

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

inline std::string calculate_file_hash(const std::string& file_path) {
    std::ifstream file(file_path, std::ios::binary);
    if (!file.is_open()) {
        throw std::runtime_error(std::format("Failed to open file for hashing: {}", file_path));
    }

    picosha2::hash256_one_by_one hasher;
    std::vector<std::byte> buffer(4096);

    while (file) {
        file.read(reinterpret_cast<char*>(buffer.data()), buffer.size());
        std::streamsize bytes_read = file.gcount();
        if (bytes_read > 0) {
            hasher.process(reinterpret_cast<const unsigned char*>(buffer.data()), 
                            reinterpret_cast<const unsigned char*>(buffer.data()) + bytes_read);
        }
    }

    hasher.finish();
    return picosha2::get_hash_hex_string(hasher);
}

inline std::string calculate_data_hash(std::span<const std::byte> data) {
    auto char_data_begin = reinterpret_cast<const unsigned char*>(data.data());
    auto char_data_end = char_data_begin + data.size();
    return picosha2::hash256_hex_string(char_data_begin, char_data_end);
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
        int value;
        auto [ptr, ec] = std::from_chars(byte_string_view.data(), byte_string_view.data() + byte_string_view.size(), value, 16);
        
        if (ec != std::errc() || ptr != byte_string_view.data() + byte_string_view.size()) {
            throw std::invalid_argument("Invalid hex character sequence.");
        }
        bytes.push_back(static_cast<std::byte>(value));
    }
    return bytes;
}

inline std::string bytes_to_hex(std::span<const std::byte> data) {
    std::stringstream ss;
    ss << std::hex << std::setfill('0');
    for (const auto& byte : data) {
        ss << std::setw(2) << static_cast<unsigned int>(byte);
    }
    return ss.str();
}

inline std::vector<std::byte> calculate_sha1_hash_data(std::span<const std::byte> data) {
    SHA1 checksum;
    checksum.update(std::string(reinterpret_cast<const char*>(data.data()), data.size()));
    return hex_to_bytes(checksum.final());
}

inline std::vector<std::byte> calculate_sha1_hash_file(const std::string& file_path) {
    std::ifstream file(file_path, std::ios::binary);
    if (!file.is_open()) {
        throw std::runtime_error(std::format("Failed to open file for SHA1 hashing: {}", file_path));
    }
    
    SHA1 checksum;
    std::array<char, 4096> buffer;
    while (file) {
        file.read(buffer.data(), buffer.size());
        checksum.update(std::string(buffer.data(), file.gcount()));
    }
 
    return hex_to_bytes(checksum.final());
}

} // namespace Crypto

// ------------ LOGGER -------------
using PeerId = std::array<std::byte, 20>;

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
        try {
            std::filesystem::create_directory("logs");
        } catch (const std::exception& e) {
        }

        auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
        console_sink->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%^%l%$] [thread %t] %v");

        auto file_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>("logs/server.log", 1024 * 1024 * 5, 3);
        file_sink->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%^%l%$] [thread %t] %v");

        std::vector<spdlog::sink_ptr> sinks {console_sink, file_sink};
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