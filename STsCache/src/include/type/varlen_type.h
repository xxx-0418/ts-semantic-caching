#pragma once
#include "common/type.h"
#ifndef VARLEN_TYPE_H
#define VARLEN_TYPE_H
#include <string>
#include "type/value.h"

namespace SemanticCache {

class VarlenType : public CompareType {
 public:
  explicit VarlenType(DATA_TYPE type);
  ~VarlenType() override;

  auto GetData(const Value &val) const -> const char * override;

  auto GetLength(const Value &val) const -> uint32_t override;

  auto CompareEquals(const Value &left, const Value &right) const -> CmpBool override;
  auto CompareNotEquals(const Value &left, const Value &right) const -> CmpBool override;
  auto CompareLessThan(const Value &left, const Value &right) const -> CmpBool override;
  auto CompareLessThanEquals(const Value &left, const Value &right) const -> CmpBool override;
  auto CompareGreaterThan(const Value &left, const Value &right) const -> CmpBool override;
  auto CompareGreaterThanEquals(const Value &left, const Value &right) const -> CmpBool override;

  auto Min(const Value &left, const Value &right) const -> Value override;
  auto Max(const Value &left, const Value &right) const -> Value override;

  auto CastAs(const Value &value, DATA_TYPE type_id) const -> Value override;

  auto IsInlined(const Value & /*val*/) const -> bool override { return false; }

  auto ToString(const Value &val) const -> std::string override;

  void SerializeTo(const Value &val, char *storage) const override;

  auto DeserializeFrom(const char *storage) const -> Value override;

  auto Copy(const Value &val) const -> Value override;
};
}  
#endif