#pragma once

#ifndef COMPARE_TYPE_H
#define COMPARE_TYPE_H

#include <cstdint>
#include <string>
#include "common/type.h"

namespace SemanticCache
{
  class Value;
  enum class CmpBool
  {
    CmpFalse = 0,
    CmpTrue = 1,
    CmpNull = 2
  };
  class CompareType
  {
  public:
    explicit CompareType(DATA_TYPE type_id) : type_id_(type_id) {}

    virtual ~CompareType() = default;
    static auto GetTypeSize(DATA_TYPE type_id) -> uint64_t;

    auto IsCoercableFrom(DATA_TYPE type_id) const -> bool;

    static auto TypeIdToString(DATA_TYPE type_id) -> std::string;

    static auto GetMinValue(DATA_TYPE type_id) -> Value;
    static auto GetMaxValue(DATA_TYPE type_id) -> Value;

    inline static auto GetInstance(DATA_TYPE type_id) -> CompareType * { return k_types[type_id]; }

    inline auto GetDataType() const -> DATA_TYPE { return type_id_; }

    virtual auto CompareEquals(const Value &left, const Value &right) const -> CmpBool;
    virtual auto CompareNotEquals(const Value &left, const Value &right) const -> CmpBool;
    virtual auto CompareLessThan(const Value &left, const Value &right) const -> CmpBool;
    virtual auto CompareLessThanEquals(const Value &left, const Value &right) const -> CmpBool;
    virtual auto CompareGreaterThan(const Value &left, const Value &right) const -> CmpBool;
    virtual auto CompareGreaterThanEquals(const Value &left, const Value &right) const -> CmpBool;

    virtual auto Add(const Value &left, const Value &right) const -> Value;
    virtual auto Subtract(const Value &left, const Value &right) const -> Value;
    virtual auto Multiply(const Value &left, const Value &right) const -> Value;
    virtual auto Divide(const Value &left, const Value &right) const -> Value;
    virtual auto Modulo(const Value &left, const Value &right) const -> Value;
    virtual auto Min(const Value &left, const Value &right) const -> Value;
    virtual auto Max(const Value &left, const Value &right) const -> Value;
    virtual auto Sqrt(const Value &val) const -> Value;
    virtual auto OperateNull(const Value &val, const Value &right) const -> Value;
    virtual auto IsZero(const Value &val) const -> bool;

    virtual auto IsInlined(const Value &val) const -> bool;

    virtual auto ToString(const Value &val) const -> std::string;

    virtual void SerializeTo(const Value &val, char *storage) const;

    virtual auto DeserializeFrom(const char *storage) const -> Value;

    virtual auto Copy(const Value &val) const -> Value;

    virtual auto CastAs(const Value &val, DATA_TYPE type_id) const -> Value;

    virtual auto GetData(const Value &val) const -> const char *;

    virtual auto GetLength(const Value &val) const -> uint32_t;

    virtual auto GetData(char *storage) -> char *;

  protected:
    DATA_TYPE type_id_;
    static CompareType *k_types[14];
  };

}

#endif