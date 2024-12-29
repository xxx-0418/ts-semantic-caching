#include <cassert>

#include <string>
#include "common/type.h"
#include "type/boolean_type.h"

namespace SemanticCache {
#define BOOLEAN_COMPARE_FUNC(OP) GetCmpBool(left.value_.boolean_ OP right.CastAs(DATA_TYPE::BOOLEAN).value_.boolean_)

BooleanType::BooleanType() : CompareType(DATA_TYPE::BOOLEAN) {}

auto BooleanType::CompareEquals(const Value &left, const Value &right) const -> CmpBool {
  assert(GetDataType() == DATA_TYPE::BOOLEAN);
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::CmpNull;
  }
  return BOOLEAN_COMPARE_FUNC(==);
}

auto BooleanType::CompareNotEquals(const Value &left, const Value &right) const -> CmpBool {
  assert(GetDataType() == DATA_TYPE::BOOLEAN);
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::CmpNull;
  }
  return BOOLEAN_COMPARE_FUNC(!=);
}

auto BooleanType::CompareLessThan(const Value &left, const Value &right) const -> CmpBool {
  assert(GetDataType() == DATA_TYPE::BOOLEAN);
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::CmpNull;
  }
  return BOOLEAN_COMPARE_FUNC(<);
}

auto BooleanType::CompareLessThanEquals(const Value &left, const Value &right) const -> CmpBool {
  assert(GetDataType() == DATA_TYPE::BOOLEAN);
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::CmpNull;
  }
  return BOOLEAN_COMPARE_FUNC(<=);
}

auto BooleanType::CompareGreaterThan(const Value &left, const Value &right) const -> CmpBool {
  assert(GetDataType() == DATA_TYPE::BOOLEAN);
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::CmpNull;
  }
  return BOOLEAN_COMPARE_FUNC(>);
}

auto BooleanType::CompareGreaterThanEquals(const Value &left, const Value &right) const -> CmpBool {
  assert(GetDataType() == DATA_TYPE::BOOLEAN);
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::CmpNull;
  }
  return BOOLEAN_COMPARE_FUNC(>=);
}

auto BooleanType::ToString(const Value &val) const -> std::string {
  assert(GetDataType() == DATA_TYPE::BOOLEAN);
  if (val.value_.boolean_ == 1) {
    return "true";
  }
  if (val.value_.boolean_ == 0) {
    return "false";
  }
  return "boolean_null";
}

void BooleanType::SerializeTo(const Value &val, char *storage) const {
  *reinterpret_cast<int8_t *>(storage) = val.value_.boolean_;
}

auto BooleanType::DeserializeFrom(const char *storage) const -> Value {
  int8_t val = *reinterpret_cast<const int8_t *>(storage);
  return {DATA_TYPE::BOOLEAN, val};
}

auto BooleanType::Copy(const Value &val) const -> Value { return {DATA_TYPE::BOOLEAN, val.value_.boolean_}; }

auto BooleanType::CastAs(const Value &val, const DATA_TYPE type_id) const -> Value {
  switch (type_id) {
    case DATA_TYPE::BOOLEAN:
      return Copy(val);
    case DATA_TYPE::VARCHAR: {
      if (val.IsNull()) {
        return {DATA_TYPE::VARCHAR, nullptr, 0, false};
      }
      return {DATA_TYPE::VARCHAR, val.ToString()};
    }
    default:
      break;
  }
  throw Exception( CompareType::TypeIdToString(type_id));
}
} 
