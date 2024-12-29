#pragma once
#ifndef DECIMAL_TYPE_H
#define DECIMAL_TYPE_H
#include <string>
#include "type/numeric_type.h"
#include "type/value.h"

namespace SemanticCache {
class DecimalType : public NumericType {
 public:
  DecimalType();
  auto Add(const Value &left, const Value &right) const -> Value override;
  auto Subtract(const Value &left, const Value &right) const -> Value override;
  auto Multiply(const Value &left, const Value &right) const -> Value override;
  auto Divide(const Value &left, const Value &right) const -> Value override;
  auto Modulo(const Value &left, const Value &right) const -> Value override;
  auto Min(const Value &left, const Value &right) const -> Value override;
  auto Max(const Value &left, const Value &right) const -> Value override;
  auto Sqrt(const Value &val) const -> Value override;
  auto IsZero(const Value &val) const -> bool override;

  auto CompareEquals(const Value &left, const Value &right) const -> CmpBool override;
  auto CompareNotEquals(const Value &left, const Value &right) const -> CmpBool override;
  auto CompareLessThan(const Value &left, const Value &right) const -> CmpBool override;
  auto CompareLessThanEquals(const Value &left, const Value &right) const -> CmpBool override;
  auto CompareGreaterThan(const Value &left, const Value &right) const -> CmpBool override;
  auto CompareGreaterThanEquals(const Value &left, const Value &right) const -> CmpBool override;

  auto CastAs(const Value &val, DATA_TYPE type_id) const -> Value override;

  auto IsInlined(const Value &val) const -> bool override { return true; }

  auto ToString(const Value &val) const -> std::string override;

  void SerializeTo(const Value &val, char *storage) const override;

  auto DeserializeFrom(const char *storage) const -> Value override;

  auto Copy(const Value &val) const -> Value override;

 private:
  auto OperateNull(const Value &left, const Value &right) const -> Value override;
};
}  


#endif