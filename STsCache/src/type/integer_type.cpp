#include <cassert>
#include <cmath>
#include <iostream>
#include <string>

#include "common/type.h"
#include "type/integer_type.h"

namespace SemanticCache
{
#define INT_COMPARE_FUNC(OP)                                             \
  switch (right.GetDataType())                                           \
  {                                                                      \
  case DATA_TYPE::TINYINT:                                               \
    return GetCmpBool(left.value_.integer_ OP right.GetAs<int8_t>());    \
  case DATA_TYPE::SMALLINT:                                              \
    return GetCmpBool(left.value_.integer_ OP right.GetAs<int16_t>());   \
  case DATA_TYPE::INTEGER:                                               \
    return GetCmpBool(left.value_.integer_ OP right.GetAs<int32_t>());   \
  case DATA_TYPE::BIGINT:                                                \
    return GetCmpBool(left.value_.integer_ OP right.GetAs<int64_t>());   \
  case DATA_TYPE::DECIMAL:                                               \
    return GetCmpBool(left.value_.integer_ OP right.GetAs<double>());    \
  case DATA_TYPE::VARCHAR:                                               \
  {                                                                      \
    auto r_value = right.CastAs(DATA_TYPE::INTEGER);                     \
    return GetCmpBool(left.value_.integer_ OP r_value.GetAs<int32_t>()); \
  }                                                                      \
  default:                                                               \
    break;                                                               \
  }

#define INT_MODIFY_FUNC(METHOD, OP)                                                  \
  switch (right.GetDataType())                                                       \
  {                                                                                  \
  case DATA_TYPE::TINYINT:                                                           \
    return METHOD<int32_t, int8_t>(left, right);                                     \
  case DATA_TYPE::SMALLINT:                                                          \
    return METHOD<int32_t, int16_t>(left, right);                                    \
  case DATA_TYPE::INTEGER:                                                           \
    return METHOD<int32_t, int32_t>(left, right);                                    \
  case DATA_TYPE::BIGINT:                                                            \
    return METHOD<int32_t, int64_t>(left, right);                                    \
  case DATA_TYPE::DECIMAL:                                                           \
    return Value(DATA_TYPE::DECIMAL, left.value_.integer_ OP right.GetAs<double>()); \
  case DATA_TYPE::VARCHAR:                                                           \
  {                                                                                  \
    auto r_value = right.CastAs(DATA_TYPE::INTEGER);                                 \
    return METHOD<int32_t, int32_t>(left, r_value);                                  \
  }                                                                                  \
  default:                                                                           \
    break;                                                                           \
  }

  IntegerType::IntegerType(DATA_TYPE type) : IntegerParentType(type)
  {
  }

  auto IntegerType::IsZero(const Value &val) const -> bool { return (val.value_.integer_ == 0); }

  auto IntegerType::Add(const Value &left, const Value &right) const -> Value
  {
    assert(left.CheckInteger());
    assert(left.CheckComparable(right));
    if (left.IsNull() || right.IsNull())
    {
      return left.OperateNull(right);
    }

    INT_MODIFY_FUNC(AddValue, +);
    throw Exception("type error");
  }

  auto IntegerType::Subtract(const Value &left, const Value &right) const -> Value
  {
    assert(left.CheckInteger());
    assert(left.CheckComparable(right));
    if (left.IsNull() || right.IsNull())
    {
      return left.OperateNull(right);
    }

    INT_MODIFY_FUNC(SubtractValue, -);

    throw Exception("type error");
  }

  auto IntegerType::Multiply(const Value &left, const Value &right) const -> Value
  {
    assert(left.CheckInteger());
    assert(left.CheckComparable(right));
    if (left.IsNull() || right.IsNull())
    {
      return left.OperateNull(right);
    }

    INT_MODIFY_FUNC(MultiplyValue, *);

    throw Exception("type error");
  }

  auto IntegerType::Divide(const Value &left, const Value &right) const -> Value
  {
    assert(left.CheckInteger());
    assert(left.CheckComparable(right));
    if (left.IsNull() || right.IsNull())
    {
      return left.OperateNull(right);
    }

    if (right.IsZero())
    {
      throw Exception("Division by zero on right-hand side");
    }

    INT_MODIFY_FUNC(DivideValue, /);

    throw Exception("type error");
  }

  auto IntegerType::Modulo(const Value &left, const Value &right) const -> Value
  {
    assert(left.CheckInteger());
    assert(left.CheckComparable(right));
    if (left.IsNull() || right.IsNull())
    {
      return left.OperateNull(right);
    }

    if (right.IsZero())
    {
      throw Exception("Division by zero on right-hand side");
    }

    switch (right.GetDataType())
    {
    case DATA_TYPE::TINYINT:
      return ModuloValue<int32_t, int8_t>(left, right);
    case DATA_TYPE::SMALLINT:
      return ModuloValue<int32_t, int16_t>(left, right);
    case DATA_TYPE::INTEGER:
      return ModuloValue<int32_t, int32_t>(left, right);
    case DATA_TYPE::BIGINT:
      return ModuloValue<int32_t, int64_t>(left, right);
    case DATA_TYPE::DECIMAL:
      return {DATA_TYPE::DECIMAL, ValMod(left.value_.integer_, right.GetAs<double>())};
    case DATA_TYPE::VARCHAR:
    {
      auto r_value = right.CastAs(DATA_TYPE::INTEGER);
      return ModuloValue<int32_t, int32_t>(left, r_value);
    }
    default:
      break;
    }

    throw Exception("type error");
  }

  auto IntegerType::Sqrt(const Value &val) const -> Value
  {
    assert(val.CheckInteger());
    if (val.IsNull())
    {
      return OperateNull(val, val);
    }

    if (val.value_.integer_ < 0)
    {
      throw Exception("Cannot take square root of a negative number.");
    }
    return {DATA_TYPE::DECIMAL, std::sqrt(val.value_.integer_)};
  }

  auto IntegerType::OperateNull(const Value &left __attribute__((unused)), const Value &right) const -> Value
  {
    switch (right.GetDataType())
    {
    case DATA_TYPE::TINYINT:
    case DATA_TYPE::SMALLINT:
    case DATA_TYPE::INTEGER:
      return {DATA_TYPE::INTEGER, static_cast<int32_t>(BUSTUB_INT32_NULL)};
    case DATA_TYPE::BIGINT:
      return {DATA_TYPE::BIGINT, static_cast<int64_t>(BUSTUB_INT64_NULL)};
    case DATA_TYPE::DECIMAL:
      return {DATA_TYPE::DECIMAL, static_cast<double>(BUSTUB_DECIMAL_NULL)};
    default:
      break;
    }

    throw Exception("type error");
  }

  auto IntegerType::CompareEquals(const Value &left, const Value &right) const -> CmpBool
  {
    assert(left.CheckInteger());
    assert(left.CheckComparable(right));

    if (left.IsNull() || right.IsNull())
    {
      return CmpBool::CmpNull;
    }

    INT_COMPARE_FUNC(==);

    throw Exception("type error");
  }

  auto IntegerType::CompareNotEquals(const Value &left, const Value &right) const -> CmpBool
  {
    assert(left.CheckInteger());
    assert(left.CheckComparable(right));
    if (left.IsNull() || right.IsNull())
    {
      return CmpBool::CmpNull;
    }

    INT_COMPARE_FUNC(!=);

    throw Exception("type error");
  }

  auto IntegerType::CompareLessThan(const Value &left, const Value &right) const -> CmpBool
  {
    assert(left.CheckInteger());
    assert(left.CheckComparable(right));
    if (left.IsNull() || right.IsNull())
    {
      return CmpBool::CmpNull;
    }

    INT_COMPARE_FUNC(<);

    throw Exception("type error");
  }

  auto IntegerType::CompareLessThanEquals(const Value &left, const Value &right) const -> CmpBool
  {
    assert(left.CheckInteger());
    assert(left.CheckComparable(right));
    if (left.IsNull() || right.IsNull())
    {
      return CmpBool::CmpNull;
    }

    INT_COMPARE_FUNC(<=);

    throw Exception("type error");
  }

  auto IntegerType::CompareGreaterThan(const Value &left, const Value &right) const -> CmpBool
  {
    assert(left.CheckInteger());
    assert(left.CheckComparable(right));
    if (left.IsNull() || right.IsNull())
    {
      return CmpBool::CmpNull;
    }

    INT_COMPARE_FUNC(>);

    throw Exception("type error");
  }

  auto IntegerType::CompareGreaterThanEquals(const Value &left, const Value &right) const -> CmpBool
  {
    assert(left.CheckInteger());
    assert(left.CheckComparable(right));
    if (left.IsNull() || right.IsNull())
    {
      return CmpBool::CmpNull;
    }

    INT_COMPARE_FUNC(>=);

    throw Exception("type error");
  }

  auto IntegerType::ToString(const Value &val) const -> std::string
  {
    assert(val.CheckInteger());

    if (val.IsNull())
    {
      return "integer_null";
    }
    return std::to_string(val.value_.integer_);
  }

  void IntegerType::SerializeTo(const Value &val, char *storage) const
  {
    *reinterpret_cast<int32_t *>(storage) = val.value_.integer_;
  }

  auto IntegerType::DeserializeFrom(const char *storage) const -> Value
  {
    int32_t val = *reinterpret_cast<const int32_t *>(storage);
    return {type_id_, val};
  }

  auto IntegerType::Copy(const Value &val) const -> Value
  {
    assert(val.CheckInteger());
    return {val.GetDataType(), val.value_.integer_};
  }

  auto IntegerType::CastAs(const Value &val, const DATA_TYPE type_id) const -> Value
  {
    switch (type_id)
    {
    case DATA_TYPE::TINYINT:
    {
      if (val.IsNull())
      {
        return {type_id, BUSTUB_INT8_NULL};
      }
      if (val.GetAs<int32_t>() > BUSTUB_INT8_MAX || val.GetAs<int32_t>() < BUSTUB_INT8_MIN)
      {
        throw Exception("Numeric value out of range.");
      }
      return {type_id, static_cast<int8_t>(val.GetAs<int32_t>())};
    }
    case DATA_TYPE::SMALLINT:
    {
      if (val.IsNull())
      {
        return {type_id, BUSTUB_INT16_NULL};
      }
      if (val.GetAs<int32_t>() > BUSTUB_INT16_MAX || val.GetAs<int32_t>() < BUSTUB_INT16_MIN)
      {
        throw Exception("Numeric value out of range.");
      }
      return {type_id, static_cast<int16_t>(val.GetAs<int32_t>())};
    }
    case DATA_TYPE::INTEGER:
    {
      if (val.IsNull())
      {
        return {type_id, BUSTUB_INT32_NULL};
      }
      return {type_id, static_cast<int32_t>(val.GetAs<int32_t>())};
    }
    case DATA_TYPE::BIGINT:
    {
      if (val.IsNull())
      {
        return {type_id, BUSTUB_INT64_NULL};
      }
      return {type_id, static_cast<int64_t>(val.GetAs<int32_t>())};
    }
    case DATA_TYPE::DECIMAL:
    {
      if (val.IsNull())
      {
        return {type_id, BUSTUB_DECIMAL_NULL};
      }
      return {type_id, static_cast<double>(val.GetAs<int32_t>())};
    }
    case DATA_TYPE::VARCHAR:
    {
      if (val.IsNull())
      {
        return {DATA_TYPE::VARCHAR, nullptr, 0, false};
      }
      return {DATA_TYPE::VARCHAR, val.ToString()};
    }
    default:
      break;
    }
    throw Exception("Integer is not coercable to " + CompareType::TypeIdToString(type_id));
  }
}