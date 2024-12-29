#include <cassert>
#include <cmath>
#include <iostream>
#include <string>

#include "common/exception.h"
#include "common/type.h"
#include "type/decimal_type.h"

namespace SemanticCache {
#define DECIMAL_COMPARE_FUNC(OP)                                          \
  switch (right.GetDataType()) {                                            \
    case DATA_TYPE::TINYINT:                                                 \
      return GetCmpBool(left.value_.decimal_ OP right.GetAs<int8_t>());   \
    case DATA_TYPE::SMALLINT:                                                \
      return GetCmpBool(left.value_.decimal_ OP right.GetAs<int16_t>());  \
    case DATA_TYPE::INTEGER:                                                 \
      return GetCmpBool(left.value_.decimal_ OP right.GetAs<int32_t>());  \
    case DATA_TYPE::BIGINT:                                                  \
      return GetCmpBool(left.value_.decimal_ OP right.GetAs<int64_t>());  \
    case DATA_TYPE::DECIMAL:                                                 \
      return GetCmpBool(left.value_.decimal_ OP right.GetAs<double>());   \
    case DATA_TYPE::VARCHAR: {                                               \
      auto r_value = right.CastAs(DATA_TYPE::DECIMAL);                       \
      return GetCmpBool(left.value_.decimal_ OP r_value.GetAs<double>()); \
    }                                                                     \
    default:                                                              \
      break;                                                              \
  }

#define DECIMAL_MODIFY_FUNC(OP)                                                       \
  switch (right.GetDataType()) {                                                        \
    case DATA_TYPE::TINYINT:                                                             \
      return Value(DATA_TYPE::DECIMAL, left.value_.decimal_ OP right.GetAs<int8_t>());   \
    case DATA_TYPE::SMALLINT:                                                            \
      return Value(DATA_TYPE::DECIMAL, left.value_.decimal_ OP right.GetAs<int16_t>());  \
    case DATA_TYPE::INTEGER:                                                             \
      return Value(DATA_TYPE::DECIMAL, left.value_.decimal_ OP right.GetAs<int32_t>());  \
    case DATA_TYPE::BIGINT:                                                              \
      return Value(DATA_TYPE::DECIMAL, left.value_.decimal_ OP right.GetAs<int64_t>());  \
    case DATA_TYPE::DECIMAL:                                                             \
      return Value(DATA_TYPE::DECIMAL, left.value_.decimal_ OP right.GetAs<double>());   \
    case DATA_TYPE::VARCHAR: {                                                           \
      auto r_value = right.CastAs(DATA_TYPE::DECIMAL);                                   \
      return Value(DATA_TYPE::DECIMAL, left.value_.decimal_ OP r_value.GetAs<double>()); \
    }                                                                                 \
    default:                                                                          \
      break;                                                                          \
  }

DecimalType::DecimalType() : NumericType(DATA_TYPE::DECIMAL) {}

auto DecimalType::IsZero(const Value &val) const -> bool {
  assert(GetDataType() == DATA_TYPE::DECIMAL);
  return (val.value_.decimal_ == 0);
}

auto DecimalType::Add(const Value &left, const Value &right) const -> Value {
  assert(GetDataType() == DATA_TYPE::DECIMAL);
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return left.OperateNull(right);
  }

  DECIMAL_MODIFY_FUNC(+);
  throw Exception("type error");
}

auto DecimalType::Subtract(const Value &left, const Value &right) const -> Value {
  assert(GetDataType() == DATA_TYPE::DECIMAL);
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return left.OperateNull(right);
  }

  DECIMAL_MODIFY_FUNC(-);

  throw Exception("type error");
}

auto DecimalType::Multiply(const Value &left, const Value &right) const -> Value {
  assert(GetDataType() == DATA_TYPE::DECIMAL);
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return left.OperateNull(right);
  }

  DECIMAL_MODIFY_FUNC(*);
  throw Exception("type error");
}

auto DecimalType::Divide(const Value &left, const Value &right) const -> Value {
  assert(GetDataType() == DATA_TYPE::DECIMAL);
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return left.OperateNull(right);
  }

  if (right.IsZero()) {
    throw Exception( "Division by zero on right-hand side");
  }

  DECIMAL_MODIFY_FUNC(/);

  throw Exception("type error");
}

auto DecimalType::Modulo(const Value &left, const Value &right) const -> Value {
  assert(GetDataType() == DATA_TYPE::DECIMAL);
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return OperateNull(left, right);
  }

  if (right.IsZero()) {
    throw Exception( "Division by zero on right-hand side");
  }
  switch (right.GetDataType()) {
    case DATA_TYPE::TINYINT:
      return {DATA_TYPE::DECIMAL, ValMod(left.value_.decimal_, right.GetAs<int8_t>())};
    case DATA_TYPE::SMALLINT:
      return {DATA_TYPE::DECIMAL, ValMod(left.value_.decimal_, right.GetAs<int16_t>())};
    case DATA_TYPE::INTEGER:
      return {DATA_TYPE::DECIMAL, ValMod(left.value_.decimal_, right.GetAs<int32_t>())};
    case DATA_TYPE::BIGINT:
      return {DATA_TYPE::DECIMAL, ValMod(left.value_.decimal_, right.GetAs<int64_t>())};
    case DATA_TYPE::DECIMAL:
      return {DATA_TYPE::DECIMAL, ValMod(left.value_.decimal_, right.GetAs<double>())};
    case DATA_TYPE::VARCHAR: {
      auto r_value = right.CastAs(DATA_TYPE::DECIMAL);
      return {DATA_TYPE::DECIMAL, ValMod(left.value_.decimal_, r_value.GetAs<double>())};
    }
    default:
      break;
  }
  throw Exception("type error");
}

auto DecimalType::Min(const Value &left, const Value &right) const -> Value {
  assert(GetDataType() == DATA_TYPE::DECIMAL);
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return left.OperateNull(right);
  }

  if (left.CompareLessThanEquals(right) == CmpBool::CmpTrue) {
    return left.Copy();
  }
  return right.Copy();
}

auto DecimalType::Max(const Value &left, const Value &right) const -> Value {
  assert(GetDataType() == DATA_TYPE::DECIMAL);
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return left.OperateNull(right);
  }

  if (left.CompareGreaterThanEquals(right) == CmpBool::CmpTrue) {
    return left.Copy();
  }
  return right.Copy();
}

auto DecimalType::Sqrt(const Value &val) const -> Value {
  assert(GetDataType() == DATA_TYPE::DECIMAL);
  if (val.IsNull()) {
    return {DATA_TYPE::DECIMAL, BUSTUB_DECIMAL_NULL};
  }
  if (val.value_.decimal_ < 0) {
    throw Exception( "Cannot take square root of a negative number.");
  }
  return {DATA_TYPE::DECIMAL, std::sqrt(val.value_.decimal_)};
}

auto DecimalType::OperateNull(const Value &left __attribute__((unused)),
                              const Value &right __attribute__((unused))) const -> Value {
  return {DATA_TYPE::DECIMAL, BUSTUB_DECIMAL_NULL};
}

auto DecimalType::CompareEquals(const Value &left, const Value &right) const -> CmpBool {
  assert(GetDataType() == DATA_TYPE::DECIMAL);
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::CmpNull;
  }

  DECIMAL_COMPARE_FUNC(==);

  throw Exception("type error");
}

auto DecimalType::CompareNotEquals(const Value &left, const Value &right) const -> CmpBool {
  assert(GetDataType() == DATA_TYPE::DECIMAL);
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::CmpNull;
  }

  DECIMAL_COMPARE_FUNC(!=);

  throw Exception("type error");
}

auto DecimalType::CompareLessThan(const Value &left, const Value &right) const -> CmpBool {
  assert(GetDataType() == DATA_TYPE::DECIMAL);
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::CmpNull;
  }

  DECIMAL_COMPARE_FUNC(<);

  throw Exception("type error");
}

auto DecimalType::CompareLessThanEquals(const Value &left, const Value &right) const -> CmpBool {
  assert(GetDataType() == DATA_TYPE::DECIMAL);
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::CmpNull;
  }

  DECIMAL_COMPARE_FUNC(<=);

  throw Exception("type error");
}

auto DecimalType::CompareGreaterThan(const Value &left, const Value &right) const -> CmpBool {
  assert(GetDataType() == DATA_TYPE::DECIMAL);
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::CmpNull;
  }

  DECIMAL_COMPARE_FUNC(>);

  throw Exception("type error");
}

auto DecimalType::CompareGreaterThanEquals(const Value &left, const Value &right) const -> CmpBool {
  assert(GetDataType() == DATA_TYPE::DECIMAL);
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::CmpNull;
  }

  DECIMAL_COMPARE_FUNC(>=);

  throw Exception("type error");
}

auto DecimalType::CastAs(const Value &val, const DATA_TYPE type_id) const -> Value {
  switch (type_id) {
    case DATA_TYPE::TINYINT: {
      if (val.IsNull()) {
        return {type_id, BUSTUB_INT8_NULL};
      }
      if (val.GetAs<double>() > BUSTUB_INT8_MAX || val.GetAs<double>() < BUSTUB_INT8_MIN) {
        throw Exception("Numeric value out of range.");
      }
      return {type_id, static_cast<int8_t>(val.GetAs<double>())};
    }
    case DATA_TYPE::SMALLINT: {
      if (val.IsNull()) {
        return {type_id, BUSTUB_INT16_NULL};
      }
      if (val.GetAs<double>() > BUSTUB_INT16_MAX || val.GetAs<double>() < BUSTUB_INT16_MIN) {
        throw Exception("Numeric value out of range.");
      }
      return {type_id, static_cast<int16_t>(val.GetAs<double>())};
    }
    case DATA_TYPE::INTEGER: {
      if (val.IsNull()) {
        return {type_id, BUSTUB_INT32_NULL};
      }
      if (val.GetAs<double>() > BUSTUB_INT32_MAX || val.GetAs<double>() < BUSTUB_INT32_MIN) {
        throw Exception( "Numeric value out of range.");
      }
      return {type_id, static_cast<int32_t>(val.GetAs<double>())};
    }
    case DATA_TYPE::BIGINT: {
      if (val.IsNull()) {
        return {type_id, BUSTUB_INT64_NULL};
      }
      if (val.GetAs<double>() >= static_cast<double>(BUSTUB_INT64_MAX) ||
          val.GetAs<double>() < static_cast<double>(BUSTUB_INT64_MIN)) {
        throw Exception( "Numeric value out of range.");
      }
      return {type_id, static_cast<int64_t>(val.GetAs<double>())};
    }
    case DATA_TYPE::DECIMAL:
      return val.Copy();
    case DATA_TYPE::VARCHAR: {
      if (val.IsNull()) {
        return {DATA_TYPE::VARCHAR, nullptr, 0, false};
      }
      return {DATA_TYPE::VARCHAR, val.ToString()};
    }
    default:
      break;
  }
  throw Exception("DECIMAL is not coercable to " + CompareType::TypeIdToString(type_id));
}

auto DecimalType::ToString(const Value &val) const -> std::string {
  if (val.IsNull()) {
    return "decimal_null";
  }
  return std::to_string(val.value_.decimal_);
}

void DecimalType::SerializeTo(const Value &val, char *storage) const {
  *reinterpret_cast<double *>(storage) = val.value_.decimal_;
}

auto DecimalType::DeserializeFrom(const char *storage) const -> Value {
  double val = *reinterpret_cast<const double *>(storage);
  return {type_id_, val};
}

auto DecimalType::Copy(const Value &val) const -> Value { return {DATA_TYPE::DECIMAL, val.value_.decimal_}; }
}  
