#pragma once
#ifndef EXCEPTION_H
#define EXCEPTION_H

#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>

namespace SemanticCache {
class Exception : public std::runtime_error {
 public:
  explicit Exception(const std::string &message) : std::runtime_error(message) {
    std::string exception_message = "Message :: " + message + "\n";
    std::cerr << exception_message;
  }
};
}

#endif