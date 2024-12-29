#pragma once
#ifndef NUMERIC_TYPE_H
#define NUMERIC_TYPE_H
#include <cmath>

#include "common/type.h"
#include "type/value.h"

namespace SemanticCache
{
  class NumericType : public CompareType
  {
  public:
    explicit NumericType(DATA_TYPE type) : CompareType(type) {}
    ~NumericType() override = default;

    auto Add(const Value &left, const Value &right) const -> Value override = 0;
    auto Subtract(const Value &left, const Value &right) const -> Value override = 0;
    auto Multiply(const Value &left, const Value &right) const -> Value override = 0;
    auto Divide(const Value &left, const Value &right) const -> Value override = 0;
    auto Modulo(const Value &left, const Value &right) const -> Value override = 0;
    auto Min(const Value &left, const Value &right) const -> Value override = 0;
    auto Max(const Value &left, const Value &right) const -> Value override = 0;
    auto Sqrt(const Value &val) const -> Value override = 0;
    auto OperateNull(const Value &left, const Value &right) const -> Value override = 0;
    auto IsZero(const Value &val) const -> bool override = 0;

  protected:
    static inline auto ValMod(double x, double y) -> double
    {
      return x - std::trunc(static_cast<double>(x) / static_cast<double>(y)) * y;
    }
  };
}

#endif
