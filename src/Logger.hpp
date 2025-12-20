#pragma once

#include <array>
#include <cstddef>
#include <memory>
#include <spdlog/spdlog.h>
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/sinks/rotating_file_sink.h"
#include <mutex>
#include <vector>
#include <filesystem>

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