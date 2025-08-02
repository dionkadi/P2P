#pragma once

#include <memory>
#include <spdlog/spdlog.h>

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

    static std::shared_ptr<spdlog::logger> get();

  private:
    Logger();
    static void init();
    static std::shared_ptr<spdlog::logger> logger_;
};
