#include <cstdarg>
#include <cstdio>
#include <iostream>
#include <utility>

#include "common/log.h"
#include "third_party/spdlog/include/spdlog/sinks/basic_file_sink.h"

namespace logger {

Logger my_logger;

Logger::Logger(const char *log_path, const std::string &prefix, const bool &trun,
               const std::string &log_name) {
  if (log_path == nullptr) {
    return;
  }

  auto file_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(log_path, trun);
  log_ = std::make_shared<spdlog::logger>(log_name, file_sink);
  log_->set_pattern(prefix + " [%Y-%m-%d %H:%M:%S] [%n] [%l] %v");
  SetLogLevel(LogLevel::info);
}

Logger::Logger(const Logger &other) { log_ = other.log_; }

void Logger::Deinit() { log_ = nullptr; }

auto Logger::operator=(const Logger &other) -> Logger & {
  log_ = other.log_;
  return *this;
}

void Logger::SetLogLevel(const LogLevel &level) {
  if (log_ == nullptr) {
    return;
  }

  switch (level) {
    case LogLevel::trace: {
      log_->set_level(spdlog::level::trace);
      break;
    }
    case LogLevel::debug: {
      log_->set_level(spdlog::level::debug);
      break;
    }
    case LogLevel::info: {
      log_->set_level(spdlog::level::info);
      break;
    }
    case LogLevel::warn: {
      log_->set_level(spdlog::level::warn);
      break;
    }
    case LogLevel::err: {
      log_->set_level(spdlog::level::err);
      break;
    }
  }
}

}