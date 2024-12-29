#pragma once
#ifndef VALUE_H
#define VALUE_H

#include <cstdint>
#include <memory>
#include <string>
#include <iostream>
#include <utility>
#include "common/type.h"
#include "type/compare_type.h"
#include "common/limits.h"

namespace SemanticCache
{
  inline auto GetCmpBool(bool boolean) -> CmpBool { return boolean ? CmpBool::CmpTrue : CmpBool::CmpFalse; }
  class Value
  {
    friend class Type;
    friend class NumericType;
    friend class IntegerParentType;
    friend class TinyintType;
    friend class SmallintType;
    friend class IntegerType;
    friend class BigintType;
    friend class DecimalType;
    friend class TimestampType;
    friend class BooleanType;
    friend class VarlenType;

  public:
    explicit Value(const DATA_TYPE type) : manage_data_(false), type_id_(type) { size_.len_ = BUSTUB_VALUE_NULL; }
    Value(DATA_TYPE type, int8_t i);
    Value(DATA_TYPE type, double d);
    Value(DATA_TYPE type, float f);
    Value(DATA_TYPE type, int16_t i);
    Value(DATA_TYPE type, int32_t i);
    Value(DATA_TYPE type, int64_t i);
    Value(DATA_TYPE type, uint64_t i);
    Value(DATA_TYPE type, const char *data, uint32_t len, bool manage_data);
    Value(DATA_TYPE type, const std::string &data);

    Value() : Value(DATA_TYPE::INVALID_DATA_TYPE) {}
    Value(const Value &other);
    auto operator=(Value other) -> Value &;
    ~Value();
    friend void Swap(Value &first, Value &second)
    {
      std::swap(first.value_, second.value_);
      std::swap(first.size_, second.size_);
      std::swap(first.manage_data_, second.manage_data_);
      std::swap(first.type_id_, second.type_id_);
    }
    auto CheckInteger() const -> bool;
    auto CheckComparable(const Value &o) const -> bool;

    inline auto GetDataType() const -> DATA_TYPE { return type_id_; }

    inline auto GetLength() const -> uint32_t
    {
      if (type_id_ == DATA_TYPE::VARCHAR)
      {
        return size_.len_;
      }
      return CompareType::GetInstance(type_id_)->GetLength(*this);
    }
    inline auto GetData() const -> const char *
    {
      if (type_id_ == DATA_TYPE::VARCHAR)
      {
        return value_.varlen_;
      }
      return CompareType::GetInstance(type_id_)->GetData(*this);
    }

    template <class T>
    inline auto GetAs() const -> T
    {
      return *reinterpret_cast<const T *>(&value_);
    }

    inline auto CastAs(const DATA_TYPE type_id) const -> Value
    {
      return CompareType::GetInstance(type_id_)->CastAs(*this, type_id);
    }
    inline auto CompareEquals(const Value &o) const -> CmpBool
    {
      return CompareType::GetInstance(type_id_)->CompareEquals(*this, o);
    }
    inline auto CompareNotEquals(const Value &o) const -> CmpBool
    {
      return CompareType::GetInstance(type_id_)->CompareNotEquals(*this, o);
    }
    inline auto CompareLessThan(const Value &o) const -> CmpBool
    {
      return CompareType::GetInstance(type_id_)->CompareLessThan(*this, o);
    }
    inline auto CompareLessThanEquals(const Value &o) const -> CmpBool
    {
      return CompareType::GetInstance(type_id_)->CompareLessThanEquals(*this, o);
    }
    inline auto CompareGreaterThan(const Value &o) const -> CmpBool
    {
      return CompareType::GetInstance(type_id_)->CompareGreaterThan(*this, o);
    }
    inline auto CompareGreaterThanEquals(const Value &o) const -> CmpBool
    {
      return CompareType::GetInstance(type_id_)->CompareGreaterThanEquals(*this, o);
    }

    inline auto Add(const Value &o) const -> Value { return CompareType::GetInstance(type_id_)->Add(*this, o); }
    inline auto Subtract(const Value &o) const -> Value { return CompareType::GetInstance(type_id_)->Subtract(*this, o); }
    inline auto Multiply(const Value &o) const -> Value { return CompareType::GetInstance(type_id_)->Multiply(*this, o); }
    inline auto Divide(const Value &o) const -> Value { return CompareType::GetInstance(type_id_)->Divide(*this, o); }
    inline auto Modulo(const Value &o) const -> Value { return CompareType::GetInstance(type_id_)->Modulo(*this, o); }
    inline auto Min(const Value &o) const -> Value { return CompareType::GetInstance(type_id_)->Min(*this, o); }
    inline auto Max(const Value &o) const -> Value { return CompareType::GetInstance(type_id_)->Max(*this, o); }
    inline auto Sqrt() const -> Value { return CompareType::GetInstance(type_id_)->Sqrt(*this); }

    inline auto OperateNull(const Value &o) const -> Value { return CompareType::GetInstance(type_id_)->OperateNull(*this, o); }
    inline auto IsZero() const -> bool { return CompareType::GetInstance(type_id_)->IsZero(*this); }
    inline auto IsNull() const -> bool { return size_.len_ == BUSTUB_VALUE_NULL; }

    inline void SerializeTo(char *storage) const { CompareType::GetInstance(type_id_)->SerializeTo(*this, storage); }

    inline static auto DeserializeFrom(const char *storage, const DATA_TYPE type_id) -> Value
    {
      return CompareType::GetInstance(type_id)->DeserializeFrom(storage);
    }

    inline auto ToString() const -> std::string { return CompareType::GetInstance(type_id_)->ToString(*this); }
    inline auto Copy() const -> Value { return CompareType::GetInstance(type_id_)->Copy(*this); }

  protected:
    union Val
    {
      int8_t boolean_;
      int8_t tinyint_;
      int16_t smallint_;
      int32_t integer_;
      int64_t bigint_;
      double decimal_;
      uint64_t timestamp_;
      char *varlen_;
      const char *const_varlen_;
    } value_;

    union
    {
      uint32_t len_;
      DATA_TYPE elem_type_id_;
    } size_;

    bool manage_data_;
    DATA_TYPE type_id_;
  };
}

#endif