#pragma once
#ifndef TYPE_UTIL_H
#define TYPE_UTIL_H
#include <algorithm>
#include <cassert>
#include <cstring>

#include "type/compare_type.h"

namespace SemanticCache {

class TypeUtil {
 public:

  static inline auto CompareStrings(const char *str1, int len1, const char *str2, int len2) -> int {
    assert(str1 != nullptr);
    assert(len1 >= 0);
    assert(str2 != nullptr);
    assert(len2 >= 0);
    int ret = memcmp(str1, str2, static_cast<size_t>(std::min(len1, len2)));
    if (ret == 0 && len1 != len2) {
      ret = len1 - len2;
    }
    return ret;
  }
};
}  
#endif