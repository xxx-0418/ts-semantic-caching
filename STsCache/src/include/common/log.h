#ifndef LOG_H
#define LOG_H

#include <memory>
#include <string>
#include "third_party/spdlog/include/spdlog/sinks/basic_file_sink.h"
#include "third_party/spdlog/include/spdlog/spdlog.h"

namespace logger {

#include <cstdint>

enum LogLevel : uint8_t {
  trace,
  debug,
  info,
  warn,
  err,
};

class Logger {
 public:
  explicit Logger() = default;

  explicit Logger(const char *log_path, const std::string &prefix, const bool &trun = false,
                  const std::string &log_name = "my_logger");

  Logger(const Logger &other);

  void Deinit();

  template <typename... T>
  void LogMsg(const LogLevel &level, const char *format, T &&...args) {
    if (log_ == nullptr) {
      return;
    }

    switch (level) {
      case LogLevel::trace: {
        log_->trace(format, std::forward<T>(args)...);
        break;
      }
      case LogLevel::debug: {
        log_->debug(format, std::forward<T>(args)...);
        break;
      }
      case LogLevel::info: {
        log_->info(format, std::forward<T>(args)...);
        break;
      }
      case LogLevel::warn: {
        log_->warn(format, std::forward<T>(args)...);
        break;
      }
      case LogLevel::err: {
        log_->error(format, std::forward<T>(args)...);
        break;
      }
    }
    
    log_->flush();
  }

  auto operator=(const Logger &other) -> Logger &;

  void SetLogLevel(const LogLevel &level);

 private:
  std::shared_ptr<spdlog::logger> log_{nullptr};
};

extern Logger my_logger;

#define init_log(file, prefix, trun, log_name) \
  logger::my_logger = logger::Logger(file, prefix, trun, log_name)
#define set_log_level(level) logger::my_logger.SetLogLevel(level)
#define log_debug(...) logger::my_logger.LogMsg(logger::debug, __VA_ARGS__)
#define log_info(...) logger::my_logger.LogMsg(logger::info, __VA_ARGS__)
#define log_warn(...) logger::my_logger.LogMsg(logger::warn, __VA_ARGS__)
#define log_err(...) logger::my_logger.LogMsg(logger::err, __VA_ARGS__)
#define deinit_log() 

}

#endif