#pragma once
#ifndef TUPLE_H
#define TUPLE_H
#include <string>
#include <vector>

#include "catalog/schema.h"
#include "common/type.h"
#include "type/value.h"

namespace SemanticCache {
class Tuple {
 public:
  Tuple() = default;

  Tuple(const std::vector<Value> &values, const Schema *schema);

  Tuple(const Tuple &other);

  auto operator=(const Tuple &other) -> Tuple &;

  ~Tuple() {
    if (allocated_) {
      delete[] data_;
    }
    allocated_ = false;
    data_ = nullptr;
  }
  void SerializeTo(char *storage) const;
  void DeserializeFrom(const char *storage);

  inline auto GetData() const -> char * { return data_; }

  inline auto GetLength() const -> uint32_t { return size_; }

  auto GetValue(const Schema *schema, uint32_t column_idx) const -> Value;

  auto KeyFromTuple(const Schema &schema, const Schema &key_schema,
                    const std::vector<uint32_t> &key_attrs) -> Tuple;

  inline auto IsNull(const Schema *schema, uint32_t column_idx) const -> bool {
    Value value = GetValue(schema, column_idx);
    return value.IsNull();
  }
  inline auto IsAllocated() -> bool { return allocated_; }

  auto ToString(const Schema *schema) const -> std::string;

 private:
  auto GetDataPtr(const Schema *schema, uint32_t column_idx) const -> const char *;

  bool allocated_{false};
  uint32_t size_{0};
  char *data_{nullptr};
};
}

#endif