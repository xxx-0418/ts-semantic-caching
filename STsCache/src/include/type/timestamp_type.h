#pragma once
#ifndef TIMESTAMP_TYPE_H
#define TIMESTAMP_TYPE_H
#include <string>
#include "type/abstract_pool.h"
#include "type/value.h"
#include "common/type.h"

namespace SemanticCache
{

  class TimestampType : public CompareType
  {
  public:
    static constexpr uint64_t K_USECS_PER_DATE = 86400000000UL;

    ~TimestampType() override = default;
    TimestampType();

    auto CompareEquals(const Value &left, const Value &right) const -> CmpBool override;
    auto CompareNotEquals(const Value &left, const Value &right) const -> CmpBool override;
    auto CompareLessThan(const Value &left, const Value &right) const -> CmpBool override;
    auto CompareLessThanEquals(const Value &left, const Value &right) const -> CmpBool override;
    auto CompareGreaterThan(const Value &left, const Value &right) const -> CmpBool override;
    auto CompareGreaterThanEquals(const Value &left, const Value &right) const -> CmpBool override;

    auto Min(const Value &left, const Value &right) const -> Value override;
    auto Max(const Value &left, const Value &right) const -> Value override;

    auto IsInlined(const Value &val) const -> bool override { return true; }

    auto ToString(const Value &val) const -> std::string override;

    void SerializeTo(const Value &val, char *storage) const override;

    auto DeserializeFrom(const char *storage) const -> Value override;

    auto Copy(const Value &val) const -> Value override;

    auto CastAs(const Value &val, DATA_TYPE type_id) const -> Value override;
  };

}
#endif