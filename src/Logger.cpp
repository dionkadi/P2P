#include "Logger.hpp"

#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/sinks/rotating_file_sink.h"
#include <mutex>
#include <vector>
#include <filesystem>

std::shared_ptr<spdlog::logger> Logger::logger_;
static std::once_flag logger_init_flag;

Logger::Logger() {}

void Logger::init() {

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

std::shared_ptr<spdlog::logger> Logger::get() {
    std::call_once(logger_init_flag, &Logger::init);
    return logger_;
}