
#pragma once

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <string>

#include "common/macros.h"
#include "type/value.h"
#include "common/type.h"

namespace SemanticCache {

using hash_t = std::size_t;

class HashUtil {
 private:
  static const hash_t PRIME_FACTOR = 10000019;

 public:
  static inline auto HashBytes(const char *bytes, size_t length) -> hash_t {
    hash_t hash = length;
    for (size_t i = 0; i < length; ++i) {
      hash = ((hash << 5) ^ (hash >> 27)) ^ bytes[i];
    }
    return hash;
  }

  static inline auto CombineHashes(hash_t l, hash_t r) -> hash_t {
    hash_t both[2] = {};
    both[0] = l;
    both[1] = r;
    return HashBytes(reinterpret_cast<char *>(both), sizeof(hash_t) * 2);
  }

  static inline auto SumHashes(hash_t l, hash_t r) -> hash_t {
    return (l % PRIME_FACTOR + r % PRIME_FACTOR) % PRIME_FACTOR;
  }

  template <typename T>
  static inline auto Hash(const T *ptr) -> hash_t {
    return HashBytes(reinterpret_cast<const char *>(ptr), sizeof(T));
  }

  template <typename T>
  static inline auto HashPtr(const T *ptr) -> hash_t {
    return HashBytes(reinterpret_cast<const char *>(&ptr), sizeof(void *));
  }

  static inline auto HashValue(const Value *val) -> hash_t {
    switch (val->GetDataType()) {
      case DATA_TYPE::TINYINT: {
        auto raw = static_cast<int64_t>(val->GetAs<int8_t>());
        return Hash<int64_t>(&raw);
      }
      case DATA_TYPE::SMALLINT: {
        auto raw = static_cast<int64_t>(val->GetAs<int16_t>());
        return Hash<int64_t>(&raw);
      }
      case DATA_TYPE::INTEGER: {
        auto raw = static_cast<int64_t>(val->GetAs<int32_t>());
        return Hash<int64_t>(&raw);
      }
      case DATA_TYPE::BIGINT: {
        auto raw = static_cast<int64_t>(val->GetAs<int64_t>());
        return Hash<int64_t>(&raw);
      }
      case DATA_TYPE::BOOLEAN: {
        auto raw = val->GetAs<bool>();
        return Hash<bool>(&raw);
      }
      case DATA_TYPE::DECIMAL: {
        auto raw = val->GetAs<double>();
        return Hash<double>(&raw);
      }
      case DATA_TYPE::VARCHAR: {
        auto raw = val->GetData();
        auto len = val->GetLength();
        return HashBytes(raw, len);
      }
      case DATA_TYPE::TIMESTAMP: {
        auto raw = val->GetAs<uint64_t>();
        return Hash<uint64_t>(&raw);
      }
      default: {
        UNIMPLEMENTED("Unsupported type.");
      }
    }
  }
};

}
