#pragma once
#include "common/type.h"
#ifndef BOOLEAN_TYPE_H
#define BOOLEAN_TYPE_H
#include <string>
#include "common/exception.h"
#include "type/compare_type.h"
#include "type/value.h"

namespace SemanticCache {
class BooleanType : public CompareType {
 public:
  ~BooleanType() override = default;
  BooleanType();

  auto CompareEquals(const Value &left, const Value &right) const -> CmpBool override;
  auto CompareNotEquals(const Value &left, const Value &right) const -> CmpBool override;
  auto CompareLessThan(const Value &left, const Value &right) const -> CmpBool override;
  auto CompareLessThanEquals(const Value &left, const Value &right) const -> CmpBool override;
  auto CompareGreaterThan(const Value &left, const Value &right) const -> CmpBool override;
  auto CompareGreaterThanEquals(const Value &left, const Value &right) const -> CmpBool override;

  auto IsInlined(const Value &val) const -> bool override { return true; }

  auto ToString(const Value &val) const -> std::string override;

  void SerializeTo(const Value &val, char *storage) const override;

  auto DeserializeFrom(const char *storage) const -> Value override;

  auto Copy(const Value &val) const -> Value override;

  auto CastAs(const Value &val, DATA_TYPE type_id) const -> Value override;
};
} 

#endif