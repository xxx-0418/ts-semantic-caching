#pragma once
#include "common/type.h"
#ifndef INTEGER_PARENT_TYPE_H
#define INTEGER_PARENT_TYPE_H

#include <string>
#include "common/exception.h"
#include "type/numeric_type.h"

namespace SemanticCache
{
  class IntegerParentType : public NumericType
  {
  public:
    ~IntegerParentType() override = default;

    explicit IntegerParentType(DATA_TYPE type);

    auto Add(const Value &left, const Value &right) const -> Value override = 0;
    auto Subtract(const Value &left, const Value &right) const -> Value override = 0;
    auto Multiply(const Value &left, const Value &right) const -> Value override = 0;
    auto Divide(const Value &left, const Value &right) const -> Value override = 0;
    auto Modulo(const Value &left, const Value &right) const -> Value override = 0;
    auto Min(const Value &left, const Value &right) const -> Value override;
    auto Max(const Value &left, const Value &right) const -> Value override;
    auto Sqrt(const Value &val) const -> Value override = 0;

    auto CompareEquals(const Value &left, const Value &right) const -> CmpBool override = 0;
    auto CompareNotEquals(const Value &left, const Value &right) const -> CmpBool override = 0;
    auto CompareLessThan(const Value &left, const Value &right) const -> CmpBool override = 0;
    auto CompareLessThanEquals(const Value &left, const Value &right) const -> CmpBool override = 0;
    auto CompareGreaterThan(const Value &left, const Value &right) const -> CmpBool override = 0;
    auto CompareGreaterThanEquals(const Value &left, const Value &right) const -> CmpBool override = 0;

    auto CastAs(const Value &val, DATA_TYPE type_id) const -> Value override = 0;

    auto IsInlined(const Value &val) const -> bool override { return true; }

    auto ToString(const Value &val) const -> std::string override = 0;

    void SerializeTo(const Value &val, char *storage) const override = 0;

    auto DeserializeFrom(const char *storage) const -> Value override = 0;

    auto Copy(const Value &val) const -> Value override = 0;

  protected:
    template <class T1, class T2>
    auto AddValue(const Value &left, const Value &right) const -> Value;
    template <class T1, class T2>
    auto SubtractValue(const Value &left, const Value &right) const -> Value;
    template <class T1, class T2>
    auto MultiplyValue(const Value &left, const Value &right) const -> Value;
    template <class T1, class T2>
    auto DivideValue(const Value &left, const Value &right) const -> Value;
    template <class T1, class T2>
    auto ModuloValue(const Value &left, const Value &right) const -> Value;

    auto OperateNull(const Value &left, const Value &right) const -> Value override = 0;

    auto IsZero(const Value &val) const -> bool override = 0;
  };

  template <class T1, class T2>
  auto IntegerParentType::AddValue(const Value &left, const Value &right) const -> Value
  {
    auto x = left.GetAs<T1>();
    auto y = right.GetAs<T2>();
    auto sum1 = static_cast<T1>(x + y);
    auto sum2 = static_cast<T2>(x + y);

    if ((x + y) != sum1 && (x + y) != sum2)
    {
      throw Exception("Numeric value out of range.");
    }
    if (sizeof(x) >= sizeof(y))
    {
      if ((x > 0 && y > 0 && sum1 < 0) || (x < 0 && y < 0 && sum1 > 0))
      {
        throw Exception("Numeric value out of range.");
      }
      return Value(left.GetDataType(), sum1);
    }
    if ((x > 0 && y > 0 && sum2 < 0) || (x < 0 && y < 0 && sum2 > 0))
    {
      throw Exception("Numeric value out of range.");
    }
    return Value(right.GetDataType(), sum2);
  }

  template <class T1, class T2>
  auto IntegerParentType::SubtractValue(const Value &left, const Value &right) const -> Value
  {
    auto x = left.GetAs<T1>();
    auto y = right.GetAs<T2>();
    auto diff1 = static_cast<T1>(x - y);
    auto diff2 = static_cast<T2>(x - y);
    if ((x - y) != diff1 && (x - y) != diff2)
    {
      throw Exception("Numeric value out of range.");
    }
    if (sizeof(x) >= sizeof(y))
    {
      if ((x > 0 && y < 0 && diff1 < 0) || (x < 0 && y > 0 && diff1 > 0))
      {
        throw Exception("Numeric value out of range.");
      }
      return Value(left.GetDataType(), diff1);
    }
    if ((x > 0 && y < 0 && diff2 < 0) || (x < 0 && y > 0 && diff2 > 0))
    {
      throw Exception("Numeric value out of range.");
    }
    return Value(right.GetDataType(), diff2);
  }

  template <class T1, class T2>
  auto IntegerParentType::MultiplyValue(const Value &left, const Value &right) const -> Value
  {
    auto x = left.GetAs<T1>();
    auto y = right.GetAs<T2>();
    auto prod1 = static_cast<T1>(x * y);
    auto prod2 = static_cast<T2>(x * y);
    if ((x * y) != prod1 && (x * y) != prod2)
    {
      throw Exception("Numeric value out of range.");
    }
    if (sizeof(x) >= sizeof(y))
    {
      if ((y != 0 && prod1 / y != x))
      {
        throw Exception("Numeric value out of range.");
      }
      return Value(left.GetDataType(), prod1);
    }
    if (y != 0 && prod2 / y != x)
    {
      throw Exception("Numeric value out of range.");
    }
    return Value(right.GetDataType(), prod2);
  }

  template <class T1, class T2>
  auto IntegerParentType::DivideValue(const Value &left, const Value &right) const -> Value
  {
    auto x = left.GetAs<T1>();
    auto y = right.GetAs<T2>();
    if (y == 0)
    {
      throw Exception("Division by zero.");
    }
    auto quot1 = static_cast<T1>(x / y);
    auto quot2 = static_cast<T2>(x / y);
    if (sizeof(x) >= sizeof(y))
    {
      return Value(left.GetDataType(), quot1);
    }
    return Value(right.GetDataType(), quot2);
  }

  template <class T1, class T2>
  auto IntegerParentType::ModuloValue(const Value &left, const Value &right) const -> Value
  {
    auto x = left.GetAs<T1>();
    auto y = right.GetAs<T2>();
    if (y == 0)
    {
      throw Exception("Division by zero.");
    }
    auto quot1 = static_cast<T1>(x % y);
    auto quot2 = static_cast<T2>(x % y);
    if (sizeof(x) >= sizeof(y))
    {
      return Value(left.GetDataType(), quot1);
    }
    return Value(right.GetDataType(), quot2);
  }
}

#endif