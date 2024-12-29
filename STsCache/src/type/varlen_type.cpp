#include <algorithm>
#include <string>

#include "common/exception.h"
#include "common/type.h"
#include "type/type_util.h"
#include "type/varlen_type.h"

namespace SemanticCache {
#define VARLEN_COMPARE_FUNC(OP)                                               \
  const char *str1 = left.GetData();                                          \
  uint32_t len1 = GetLength(left) - 1;                                        \
  const char *str2;                                                           \
  uint32_t len2;                                                              \
  if (right.GetDataType() == DATA_TYPE::VARCHAR) {                                 \
    str2 = right.GetData();                                                   \
    len2 = GetLength(right) - 1;                                              \
    return GetCmpBool(TypeUtil::CompareStrings(str1, len1, str2, len2) OP 0); \
  } else {                                                                    \
    auto r_value = right.CastAs(DATA_TYPE::VARCHAR);                             \
    str2 = r_value.GetData();                                                 \
    len2 = GetLength(r_value) - 1;                                            \
    return GetCmpBool(TypeUtil::CompareStrings(str1, len1, str2, len2) OP 0); \
  }

VarlenType::VarlenType(DATA_TYPE type) : CompareType(type) {}

VarlenType::~VarlenType() = default;

auto VarlenType::GetData(const Value &val) const -> const char * { return val.value_.varlen_; }

auto VarlenType::GetLength(const Value &val) const -> uint32_t { return val.size_.len_; }

auto VarlenType::CompareEquals(const Value &left, const Value &right) const -> CmpBool {
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::CmpNull;
  }
  if (GetLength(left) == BUSTUB_VARCHAR_MAX_LEN || GetLength(right) == BUSTUB_VARCHAR_MAX_LEN) {
    return GetCmpBool(GetLength(left) == GetLength(right));
  }

  VARLEN_COMPARE_FUNC(==);
}

auto VarlenType::CompareNotEquals(const Value &left, const Value &right) const -> CmpBool {
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::CmpNull;
  }
  if (GetLength(left) == BUSTUB_VARCHAR_MAX_LEN || GetLength(right) == BUSTUB_VARCHAR_MAX_LEN) {
    return GetCmpBool(GetLength(left) != GetLength(right));
  }

  VARLEN_COMPARE_FUNC(!=);
}

auto VarlenType::CompareLessThan(const Value &left, const Value &right) const -> CmpBool {
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::CmpNull;
  }
  if (GetLength(left) == BUSTUB_VARCHAR_MAX_LEN || GetLength(right) == BUSTUB_VARCHAR_MAX_LEN) {
    return GetCmpBool(GetLength(left) < GetLength(right));
  }

  VARLEN_COMPARE_FUNC(<);
}

auto VarlenType::CompareLessThanEquals(const Value &left, const Value &right) const -> CmpBool {
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::CmpNull;
  }
  if (GetLength(left) == BUSTUB_VARCHAR_MAX_LEN || GetLength(right) == BUSTUB_VARCHAR_MAX_LEN) {
    return GetCmpBool(GetLength(left) <= GetLength(right));
  }

  VARLEN_COMPARE_FUNC(<=);
}

auto VarlenType::CompareGreaterThan(const Value &left, const Value &right) const -> CmpBool {
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::CmpNull;
  }
  if (GetLength(left) == BUSTUB_VARCHAR_MAX_LEN || GetLength(right) == BUSTUB_VARCHAR_MAX_LEN) {
    return GetCmpBool(GetLength(left) > GetLength(right));
  }

  VARLEN_COMPARE_FUNC(>);
}

auto VarlenType::CompareGreaterThanEquals(const Value &left, const Value &right) const -> CmpBool {
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::CmpNull;
  }
  if (GetLength(left) == BUSTUB_VARCHAR_MAX_LEN || GetLength(right) == BUSTUB_VARCHAR_MAX_LEN) {
    return GetCmpBool(GetLength(left) >= GetLength(right));
  }

  VARLEN_COMPARE_FUNC(>=);
}

auto VarlenType::Min(const Value &left, const Value &right) const -> Value {
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return left.OperateNull(right);
  }
  if (left.CompareLessThan(right) == CmpBool::CmpTrue) {
    return left.Copy();
  }
  return right.Copy();
}

auto VarlenType::Max(const Value &left, const Value &right) const -> Value {
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return left.OperateNull(right);
  }
  if (left.CompareGreaterThan(right) == CmpBool::CmpTrue) {
    return left.Copy();
  }
  return right.Copy();
}

auto VarlenType::ToString(const Value &val) const -> std::string {
  uint32_t len = GetLength(val);

  if (val.IsNull()) {
    return "varlen_null";
  }
  if (len == BUSTUB_VARCHAR_MAX_LEN) {
    return "varlen_max";
  }
  if (len == 0) {
    return "";
  }
  return {GetData(val), len - 1};
}

void VarlenType::SerializeTo(const Value &val, char *storage) const {
  uint32_t len = GetLength(val);
  if (len == BUSTUB_VALUE_NULL) {
    memcpy(storage, &len, sizeof(uint32_t));
    return;
  }
  memcpy(storage, &len, sizeof(uint32_t));
  memcpy(storage + sizeof(uint32_t), val.value_.varlen_, len);
}

auto VarlenType::DeserializeFrom(const char *storage) const -> Value {
  uint32_t len = *reinterpret_cast<const uint32_t *>(storage);
  if (len == BUSTUB_VALUE_NULL) {
    return {type_id_, nullptr, len, false};
  }
  return {type_id_, storage + sizeof(uint32_t), len, true};
}

auto VarlenType::Copy(const Value &val) const -> Value { return {val}; }

auto VarlenType::CastAs(const Value &value, const DATA_TYPE type_id) const -> Value {
  std::string str;
  switch (type_id) {
    case DATA_TYPE::BOOLEAN: {
      str = value.ToString();
      std::transform(str.begin(), str.end(), str.begin(), ::tolower);
      if (str == "true" || str == "1" || str == "t") {
        return {type_id, 1};
      }
      if (str == "false" || str == "0" || str == "f") {
        return {type_id, 0};
      }
      throw Exception("Boolean value format error.");
    }
    case DATA_TYPE::TINYINT: {
      str = value.ToString();
      int8_t tinyint = 0;
      try {
        tinyint = static_cast<int8_t>(stoi(str));
      } catch (std::out_of_range &e) {
        throw Exception("Numeric value out of range.");
      }
      if (tinyint < BUSTUB_INT8_MIN) {
        throw Exception( "Numeric value out of range.");
      }
      return {type_id, tinyint};
    }
    case DATA_TYPE::SMALLINT: {
      str = value.ToString();
      int16_t smallint = 0;
      try {
        smallint = static_cast<int16_t>(stoi(str));
      } catch (std::out_of_range &e) {
        throw Exception( "Numeric value out of range.");
      }
      if (smallint < BUSTUB_INT16_MIN) {
        throw Exception("Numeric value out of range.");
      }
      return {type_id, smallint};
    }
    case DATA_TYPE::INTEGER: {
      str = value.ToString();
      int32_t integer = 0;
      try {
        integer = stoi(str);
      } catch (std::out_of_range &e) {
        throw Exception( "Numeric value out of range.");
      }
      if (integer > BUSTUB_INT32_MAX || integer < BUSTUB_INT32_MIN) {
        throw Exception( "Numeric value out of range.");
      }
      return {type_id, integer};
    }
    case DATA_TYPE::BIGINT: {
      str = value.ToString();
      int64_t bigint = 0;
      try {
        bigint = stoll(str);
      } catch (std::out_of_range &e) {
        throw Exception("Numeric value out of range.");
      }
      if (bigint > BUSTUB_INT64_MAX || bigint < BUSTUB_INT64_MIN) {
        throw Exception( "Numeric value out of range.");
      }
      return {type_id, bigint};
    }
    case DATA_TYPE::DECIMAL: {
      str = value.ToString();
      double res = 0;
      try {
        res = stod(str);
      } catch (std::out_of_range &e) {
        throw Exception( "Numeric value out of range.");
      }
      if (res > BUSTUB_DECIMAL_MAX || res < BUSTUB_DECIMAL_MIN) {
        throw Exception( "Numeric value out of range.");
      }
      return {type_id, res};
    }
    case DATA_TYPE::VARCHAR:
      return value.Copy();
    default:
      break;
  }
  throw Exception("VARCHAR is not coercable to " + TypeIdToString(type_id));
}
}  