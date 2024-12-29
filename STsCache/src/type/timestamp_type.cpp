#include "type/timestamp_type.h"
#include <string>
#include <assert.h>

#include "common/type.h"
#include "type/boolean_type.h"
#include "type/value_factory.h"
#include "type/varlen_type.h"

namespace SemanticCache
{

  TimestampType::TimestampType() : CompareType(DATA_TYPE::TIMESTAMP) {}

  auto TimestampType::CompareEquals(const Value &left, const Value &right) const -> CmpBool
  {
    assert(left.CheckComparable(right));
    if (left.IsNull() || right.IsNull())
    {
      return CmpBool::CmpNull;
    }
    return GetCmpBool(left.GetAs<uint64_t>() == right.GetAs<uint64_t>());
  }

  auto TimestampType::CompareNotEquals(const Value &left, const Value &right) const -> CmpBool
  {
    assert(left.CheckComparable(right));
    if (right.IsNull())
    {
      return CmpBool::CmpNull;
    }
    return GetCmpBool(left.GetAs<uint64_t>() != right.GetAs<uint64_t>());
  }

  auto TimestampType::CompareLessThan(const Value &left, const Value &right) const -> CmpBool
  {
    assert(left.CheckComparable(right));
    if (left.IsNull() || right.IsNull())
    {
      return CmpBool::CmpNull;
    }
    return GetCmpBool(left.GetAs<uint64_t>() < right.GetAs<uint64_t>());
  }

  auto TimestampType::CompareLessThanEquals(const Value &left, const Value &right) const -> CmpBool
  {
    assert(left.CheckComparable(right));
    if (left.IsNull() || right.IsNull())
    {
      return CmpBool::CmpNull;
    }
    return GetCmpBool(left.GetAs<uint64_t>() <= right.GetAs<uint64_t>());
  }

  auto TimestampType::CompareGreaterThan(const Value &left, const Value &right) const -> CmpBool
  {
    assert(left.CheckComparable(right));
    if (left.IsNull() || right.IsNull())
    {
      return CmpBool::CmpNull;
    }
    return GetCmpBool(left.GetAs<int64_t>() > right.GetAs<int64_t>());
  }

  auto TimestampType::CompareGreaterThanEquals(const Value &left, const Value &right) const -> CmpBool
  {
    assert(left.CheckComparable(right));
    if (left.IsNull() || right.IsNull())
    {
      return CmpBool::CmpNull;
    }
    return GetCmpBool(left.GetAs<uint64_t>() >= right.GetAs<uint64_t>());
  }

  auto TimestampType::Min(const Value &left, const Value &right) const -> Value
  {
    assert(left.CheckComparable(right));
    if (left.IsNull() || right.IsNull())
    {
      return left.OperateNull(right);
    }
    if (left.CompareLessThan(right) == CmpBool::CmpTrue)
    {
      return left.Copy();
    }
    return right.Copy();
  }

  auto TimestampType::Max(const Value &left, const Value &right) const -> Value
  {
    assert(left.CheckComparable(right));
    if (left.IsNull() || right.IsNull())
    {
      return left.OperateNull(right);
    }
    if (left.CompareGreaterThanEquals(right) == CmpBool::CmpTrue)
    {
      return left.Copy();
    }
    return right.Copy();
  }

  auto TimestampType::ToString(const Value &val) const -> std::string
  {
    if (val.IsNull())
    {
      return "timestamp_null";
    }
    uint64_t tm = val.value_.timestamp_;
    auto micro = static_cast<uint32_t>(tm % 1000000);
    tm /= 1000000;
    auto second = static_cast<uint32_t>(tm % 100000);
    auto sec = static_cast<uint16_t>(second % 60);
    second /= 60;
    auto min = static_cast<uint16_t>(second % 60);
    second /= 60;
    auto hour = static_cast<uint16_t>(second % 24);
    tm /= 100000;
    auto year = static_cast<uint16_t>(tm % 10000);
    tm /= 10000;
    auto tz = static_cast<int>(tm % 27);
    tz -= 12;
    tm /= 27;
    auto day = static_cast<uint16_t>(tm % 32);
    tm /= 32;
    auto month = static_cast<uint16_t>(tm);
    const size_t date_str_len = 30;
    const size_t zone_len = 5;
    char str[date_str_len];
    char zone[zone_len];
    snprintf(str, date_str_len, "%04d-%02d-%02d %02d:%02d:%02d.%06d", year, month, day, hour, min, sec, micro);
    if (tz >= 0)
    {
      str[26] = '+';
    }
    else
    {
      str[26] = '-';
    }
    if (tz < 0)
    {
      tz = -tz;
    }
    snprintf(zone, zone_len, "%02d", tz);
    str[27] = 0;
    return std::string(std::string(str) + std::string(zone));
  }

  void TimestampType::SerializeTo(const Value &val, char *storage) const
  {
    *reinterpret_cast<uint64_t *>(storage) = val.value_.timestamp_;
  }

  auto TimestampType::DeserializeFrom(const char *storage) const -> Value
  {
    uint64_t val = *reinterpret_cast<const uint64_t *>(storage);
    return {type_id_, val};
  }

  auto TimestampType::Copy(const Value &val) const -> Value { return {val}; }

  auto TimestampType::CastAs(const Value &val, const DATA_TYPE type_id) const -> Value
  {
    switch (type_id)
    {
    case DATA_TYPE::TIMESTAMP:
      return Copy(val);
    case DATA_TYPE::VARCHAR:
      if (val.IsNull())
      {
        return ValueFactory::GetVarcharValue(nullptr, false);
      }
      return ValueFactory::GetVarcharValue(val.ToString());
    default:
      break;
    }
    throw Exception("TIMESTAMP is not coercable to " + CompareType::GetInstance(type_id)->ToString(val));
  }

}