#include <cassert>
#include <cmath>
#include <iostream>
#include <string>

#include "common/exception.h"
#include "common/type.h"
#include "type/tinyint_type.h"

namespace SemanticCache
{
#define TINYINT_COMPARE_FUNC(OP)                                        \
  switch (right.GetDataType())                                          \
  {                                                                     \
  case DATA_TYPE::TINYINT:                                              \
    return GetCmpBool(left.value_.tinyint_ OP right.GetAs<int8_t>());   \
  case DATA_TYPE::SMALLINT:                                             \
    return GetCmpBool(left.value_.tinyint_ OP right.GetAs<int16_t>());  \
  case DATA_TYPE::INTEGER:                                              \
    return GetCmpBool(left.value_.tinyint_ OP right.GetAs<int32_t>());  \
  case DATA_TYPE::BIGINT:                                               \
    return GetCmpBool(left.value_.tinyint_ OP right.GetAs<int64_t>());  \
  case DATA_TYPE::DECIMAL:                                              \
    return GetCmpBool(left.value_.tinyint_ OP right.GetAs<double>());   \
  case DATA_TYPE::VARCHAR:                                              \
  {                                                                     \
    auto r_value = right.CastAs(DATA_TYPE::TINYINT);                    \
    return GetCmpBool(left.value_.tinyint_ OP r_value.GetAs<int8_t>()); \
  }                                                                     \
  default:                                                              \
    break;                                                              \
  }

#define TINYINT_MODIFY_FUNC(METHOD, OP)                                              \
  switch (right.GetDataType())                                                       \
  {                                                                                  \
  case DATA_TYPE::TINYINT:                                                           \
    return METHOD<int8_t, int8_t>(left, right);                                      \
  case DATA_TYPE::SMALLINT:                                                          \
    return METHOD<int8_t, int16_t>(left, right);                                     \
  case DATA_TYPE::INTEGER:                                                           \
    return METHOD<int8_t, int32_t>(left, right);                                     \
  case DATA_TYPE::BIGINT:                                                            \
    return METHOD<int8_t, int64_t>(left, right);                                     \
  case DATA_TYPE::DECIMAL:                                                           \
    return Value(DATA_TYPE::DECIMAL, left.value_.tinyint_ OP right.GetAs<double>()); \
  case DATA_TYPE::VARCHAR:                                                           \
  {                                                                                  \
    auto r_value = right.CastAs(DATA_TYPE::TINYINT);                                 \
    return METHOD<int8_t, int8_t>(left, r_value);                                    \
  }                                                                                  \
  default:                                                                           \
    break;                                                                           \
  }

  TinyintType::TinyintType() : IntegerParentType(TINYINT)
  {
  }

  auto TinyintType::IsZero(const Value &val) const -> bool { return (val.value_.tinyint_ == 0); }

  auto TinyintType::Add(const Value &left, const Value &right) const -> Value
  {
    assert(left.CheckInteger());
    assert(left.CheckComparable(right));
    if (left.IsNull() || right.IsNull())
    {
      return left.OperateNull(right);
    }

    TINYINT_MODIFY_FUNC(AddValue, +);

    throw Exception("type error");
  }

  auto TinyintType::Subtract(const Value &left, const Value &right) const -> Value
  {
    assert(left.CheckInteger());
    assert(left.CheckComparable(right));
    if (left.IsNull() || right.IsNull())
    {
      return left.OperateNull(right);
    }

    TINYINT_MODIFY_FUNC(SubtractValue, -);

    throw Exception("type error");
  }

  auto TinyintType::Multiply(const Value &left, const Value &right) const -> Value
  {
    assert(left.CheckInteger());
    assert(left.CheckComparable(right));
    if (left.IsNull() || right.IsNull())
    {
      return left.OperateNull(right);
    }

    TINYINT_MODIFY_FUNC(MultiplyValue, *);

    throw Exception("type error");
  }

  auto TinyintType::Divide(const Value &left, const Value &right) const -> Value
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

    TINYINT_MODIFY_FUNC(DivideValue, /);

    throw Exception("type error");
  }

  auto TinyintType::Modulo(const Value &left, const Value &right) const -> Value
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
      return ModuloValue<int8_t, int8_t>(left, right);
    case DATA_TYPE::SMALLINT:
      return ModuloValue<int8_t, int16_t>(left, right);
    case DATA_TYPE::INTEGER:
      return ModuloValue<int8_t, int32_t>(left, right);
    case DATA_TYPE::BIGINT:
      return ModuloValue<int8_t, int64_t>(left, right);
    case DATA_TYPE::DECIMAL:
      return {DATA_TYPE::DECIMAL, ValMod(left.value_.tinyint_, right.GetAs<double>())};
    case DATA_TYPE::VARCHAR:
    {
      auto r_value = right.CastAs(DATA_TYPE::TINYINT);
      return ModuloValue<int8_t, int8_t>(left, r_value);
    }
    default:
      break;
    }

    throw Exception("type error");
  }

  auto TinyintType::Sqrt(const Value &val) const -> Value
  {
    assert(val.CheckInteger());
    if (val.IsNull())
    {
      return {DATA_TYPE::DECIMAL, BUSTUB_DECIMAL_NULL};
    }

    if (val.value_.tinyint_ < 0)
    {
      throw Exception("Cannot take square root of a negative number.");
    }
    return {DATA_TYPE::DECIMAL, std::sqrt(val.value_.tinyint_)};

    throw Exception("type error");
  }

  auto TinyintType::OperateNull(const Value &left __attribute__((unused)), const Value &right) const -> Value
  {
    switch (right.GetDataType())
    {
    case DATA_TYPE::TINYINT:
      return {DATA_TYPE::TINYINT, static_cast<int8_t>(BUSTUB_INT8_NULL)};
    case DATA_TYPE::SMALLINT:
      return {DATA_TYPE::SMALLINT, static_cast<int16_t>(BUSTUB_INT16_NULL)};
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

  auto TinyintType::CompareEquals(const Value &left, const Value &right) const -> CmpBool
  {
    assert(left.CheckInteger());
    assert(left.CheckComparable(right));

    if (left.IsNull() || right.IsNull())
    {
      return CmpBool::CmpNull;
    }

    TINYINT_COMPARE_FUNC(==);

    throw Exception("type error");
  }

  auto TinyintType::CompareNotEquals(const Value &left, const Value &right) const -> CmpBool
  {
    assert(left.CheckInteger());
    assert(left.CheckComparable(right));
    if (left.IsNull() || right.IsNull())
    {
      return CmpBool::CmpNull;
    }

    TINYINT_COMPARE_FUNC(!=);

    throw Exception("type error");
  }

  auto TinyintType::CompareLessThan(const Value &left, const Value &right) const -> CmpBool
  {
    assert(left.CheckInteger());
    assert(left.CheckComparable(right));
    if (left.IsNull() || right.IsNull())
    {
      return CmpBool::CmpNull;
    }

    TINYINT_COMPARE_FUNC(<);

    throw Exception("type error");
  }

  auto TinyintType::CompareLessThanEquals(const Value &left, const Value &right) const -> CmpBool
  {
    assert(left.CheckInteger());
    assert(left.CheckComparable(right));
    if (left.IsNull() || right.IsNull())
    {
      return CmpBool::CmpNull;
    }

    TINYINT_COMPARE_FUNC(<=);

    throw Exception("type error");
  }

  auto TinyintType::CompareGreaterThan(const Value &left, const Value &right) const -> CmpBool
  {
    assert(left.CheckInteger());
    assert(left.CheckComparable(right));
    if (left.IsNull() || right.IsNull())
    {
      return CmpBool::CmpNull;
    }

    TINYINT_COMPARE_FUNC(>);

    throw Exception("type error");
  }

  auto TinyintType::CompareGreaterThanEquals(const Value &left, const Value &right) const -> CmpBool
  {
    assert(left.CheckInteger());
    assert(left.CheckComparable(right));
    if (left.IsNull() || right.IsNull())
    {
      return CmpBool::CmpNull;
    }

    TINYINT_COMPARE_FUNC(>=);

    throw Exception("type error");
  }

  auto TinyintType::ToString(const Value &val) const -> std::string
  {
    assert(val.CheckInteger());
    if (val.IsNull())
    {
      return "tinyint_null";
    }
    return std::to_string(val.value_.tinyint_);
  }

  void TinyintType::SerializeTo(const Value &val, char *storage) const
  {
    *reinterpret_cast<int8_t *>(storage) = val.value_.tinyint_;
  }

  auto TinyintType::DeserializeFrom(const char *storage) const -> Value
  {
    int8_t val = *reinterpret_cast<const int8_t *>(storage);
    return {type_id_, val};
  }

  auto TinyintType::Copy(const Value &val) const -> Value
  {
    assert(val.CheckInteger());
    return {DATA_TYPE::TINYINT, val.value_.tinyint_};
  }

  auto TinyintType::CastAs(const Value &val, const DATA_TYPE type_id) const -> Value
  {
    switch (type_id)
    {
    case DATA_TYPE::TINYINT:
    {
      if (val.IsNull())
      {
        return {type_id, BUSTUB_INT8_NULL};
      }
      return Copy(val);
    }
    case DATA_TYPE::SMALLINT:
    {
      if (val.IsNull())
      {
        return {type_id, BUSTUB_INT16_NULL};
      }
      return {type_id, static_cast<int16_t>(val.GetAs<int8_t>())};
    }
    case DATA_TYPE::INTEGER:
    {
      if (val.IsNull())
      {
        return {type_id, BUSTUB_INT32_NULL};
      }
      return {type_id, static_cast<int32_t>(val.GetAs<int8_t>())};
    }
    case DATA_TYPE::BIGINT:
    {
      if (val.IsNull())
      {
        return {type_id, BUSTUB_INT64_NULL};
      }
      return {type_id, static_cast<int64_t>(val.GetAs<int8_t>())};
    }
    case DATA_TYPE::DECIMAL:
    {
      if (val.IsNull())
      {
        return {type_id, BUSTUB_DECIMAL_NULL};
      }
      return {type_id, static_cast<double>(val.GetAs<int8_t>())};
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
    throw Exception("tinyint is not coercable to " + CompareType::TypeIdToString(type_id));
  }
}
