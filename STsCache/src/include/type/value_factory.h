#pragma once
#ifndef VALUE_FACTORY_H
#define VALUE_FACTORY_H

#include "common/type.h"
#include <cstring>
#include <algorithm>
#include <stdexcept>
#include <string>

#include "common/macros.h"
#include "common/util/string_util.h"
#include "type/abstract_pool.h"
#include "type/boolean_type.h"
#include "type/decimal_type.h"
#include "type/numeric_type.h"
#include "type/timestamp_type.h"
#include "type/value.h"
#include "type/varlen_type.h"

namespace SemanticCache
{

  class ValueFactory
  {
  public:
    static inline auto Clone(const Value &src, __attribute__((__unused__)) AbstractPool *dataPool = nullptr) -> Value
    {
      return src.Copy();
    }

    static inline auto GetTinyIntValue(int8_t value) -> Value { return {DATA_TYPE::TINYINT, value}; }

    static inline auto GetSmallIntValue(int16_t value) -> Value { return {DATA_TYPE::SMALLINT, value}; }

    static inline auto GetIntegerValue(int32_t value) -> Value { return {DATA_TYPE::INTEGER, value}; }

    static inline auto GetBigIntValue(int64_t value) -> Value { return {DATA_TYPE::BIGINT, value}; }

    static inline auto GetTimestampValue(int64_t value) -> Value { return {DATA_TYPE::TIMESTAMP, value}; }

    static inline auto GetDecimalValue(double value) -> Value { return {DATA_TYPE::DECIMAL, value}; }

    static inline auto GetBooleanValue(CmpBool value) -> Value
    {
      return {DATA_TYPE::BOOLEAN, value == CmpBool::CmpNull ? BUSTUB_BOOLEAN_NULL : static_cast<int8_t>(value)};
    }

    static inline auto GetBooleanValue(bool value) -> Value { return {DATA_TYPE::BOOLEAN, static_cast<int8_t>(value)}; }

    static inline auto GetBooleanValue(int8_t value) -> Value { return {DATA_TYPE::BOOLEAN, value}; }

    static inline auto GetVarcharValue(const char *value, bool manage_data,
                                       __attribute__((__unused__)) AbstractPool *pool = nullptr) -> Value
    {
      auto len = static_cast<uint32_t>(value == nullptr ? 0U : std::strlen(value) + 1);
      return GetVarcharValue(value, len, manage_data);
    }

    static inline auto GetVarcharValue(const char *value, uint32_t len, bool manage_data,
                                       __attribute__((__unused__)) AbstractPool *pool = nullptr) -> Value
    {
      return {DATA_TYPE::VARCHAR, value, len, manage_data};
    }

    static inline auto GetVarcharValue(const std::string &value, __attribute__((__unused__)) AbstractPool *pool = nullptr)
        -> Value
    {
      return {DATA_TYPE::VARCHAR, value};
    }

    static inline auto GetNullValueByType(DATA_TYPE type_id) -> Value
    {
      Value ret_value;
      switch (type_id)
      {
      case DATA_TYPE::BOOLEAN:
        ret_value = GetBooleanValue(BUSTUB_BOOLEAN_NULL);
        break;
      case DATA_TYPE::TINYINT:
        ret_value = GetTinyIntValue(BUSTUB_INT8_NULL);
        break;
      case DATA_TYPE::SMALLINT:
        ret_value = GetSmallIntValue(BUSTUB_INT16_NULL);
        break;
      case DATA_TYPE::INTEGER:
        ret_value = GetIntegerValue(BUSTUB_INT32_NULL);
        break;
      case DATA_TYPE::BIGINT:
        ret_value = GetBigIntValue(BUSTUB_INT64_NULL);
        break;
      case DATA_TYPE::DECIMAL:
        ret_value = GetDecimalValue(BUSTUB_DECIMAL_NULL);
        break;
      case DATA_TYPE::VARCHAR:
        ret_value = GetVarcharValue(nullptr, false, nullptr);
        break;
      default:
      {
        throw Exception("Attempting to create invalid null type");
      }
      }
      return ret_value;
    }

    static inline auto GetZeroValueByType(DATA_TYPE type_id) -> Value
    {
      std::string zero_string("0");

      switch (type_id)
      {
      case DATA_TYPE::BOOLEAN:
        return GetBooleanValue(false);
      case DATA_TYPE::TINYINT:
        return GetTinyIntValue(0);
      case DATA_TYPE::SMALLINT:
        return GetSmallIntValue(0);
      case DATA_TYPE::INTEGER:
        return GetIntegerValue(0);
      case DATA_TYPE::BIGINT:
        return GetBigIntValue(0);
      case DATA_TYPE::DECIMAL:
        return GetDecimalValue(static_cast<double>(0));
      case DATA_TYPE::VARCHAR:
        return GetVarcharValue(zero_string);
      default:
        break;
      }
      throw Exception("Unknown type for GetZeroValueType");
    }

    static inline auto CastAsBigInt(const Value &value) -> Value
    {
      if (CompareType::GetInstance(DATA_TYPE::BIGINT)->IsCoercableFrom(value.GetDataType()))
      {
        if (value.IsNull())
        {
          return ValueFactory::GetBigIntValue(static_cast<int64_t>(BUSTUB_INT64_NULL));
        }
        switch (value.GetDataType())
        {
        case DATA_TYPE::TINYINT:
          return ValueFactory::GetBigIntValue(static_cast<int64_t>(value.GetAs<int8_t>()));
        case DATA_TYPE::SMALLINT:
          return ValueFactory::GetBigIntValue(static_cast<int64_t>(value.GetAs<int16_t>()));
        case DATA_TYPE::INTEGER:
          return ValueFactory::GetBigIntValue(static_cast<int64_t>(value.GetAs<int32_t>()));
        case DATA_TYPE::BIGINT:
          return ValueFactory::GetBigIntValue(value.GetAs<int64_t>());
        case DATA_TYPE::DECIMAL:
        {
          if (value.GetAs<double>() > static_cast<double>(BUSTUB_INT64_MAX) ||
              value.GetAs<double>() < static_cast<double>(BUSTUB_INT64_MIN))
          {
            throw Exception("Numeric value out of range.");
          }
          return ValueFactory::GetBigIntValue(static_cast<int64_t>(value.GetAs<double>()));
        }
        case DATA_TYPE::VARCHAR:
        {
          std::string str = value.ToString();
          int64_t bigint = 0;
          try
          {
            bigint = stoll(str);
          }
          catch (std::out_of_range &e)
          {
            throw Exception("Numeric value out of range.");
          }
          catch (std::invalid_argument &e)
          {
            throw Exception("Invalid input syntax for bigint: \'" + str + "\'");
          }
          if (bigint > BUSTUB_INT64_MAX || bigint < BUSTUB_INT64_MIN)
          {
            throw Exception("Numeric value out of range.");
          }
          return ValueFactory::GetBigIntValue(bigint);
        }
        default:
          break;
        }
      }
      throw Exception(CompareType::GetInstance(value.GetDataType())->ToString(value) + " is not coercable to BIGINT.");
    }

    static inline auto CastAsInteger(const Value &value) -> Value
    {
      if (CompareType::GetInstance(DATA_TYPE::INTEGER)->IsCoercableFrom(value.GetDataType()))
      {
        if (value.IsNull())
        {
          return ValueFactory::GetIntegerValue(BUSTUB_INT32_NULL);
        }
        switch (value.GetDataType())
        {
        case DATA_TYPE::TINYINT:
          return ValueFactory::GetIntegerValue(static_cast<int32_t>(value.GetAs<int8_t>()));
        case DATA_TYPE::SMALLINT:
          return ValueFactory::GetIntegerValue(static_cast<int32_t>(value.GetAs<int16_t>()));
        case DATA_TYPE::INTEGER:
          return ValueFactory::GetIntegerValue(value.GetAs<int32_t>());
        case DATA_TYPE::BIGINT:
        {
          if (value.GetAs<int64_t>() > static_cast<int64_t>(BUSTUB_INT32_MAX) ||
              value.GetAs<int64_t>() < static_cast<int64_t>(BUSTUB_INT32_MIN))
          {
            throw Exception("Numeric value out of range.");
          }
          return ValueFactory::GetIntegerValue(static_cast<int32_t>(value.GetAs<int64_t>()));
        }
        case DATA_TYPE::DECIMAL:
        {
          if (value.GetAs<double>() > static_cast<double>(BUSTUB_INT32_MAX) ||
              value.GetAs<double>() < static_cast<double>(BUSTUB_INT32_MIN))
          {
            throw Exception("Numeric value out of range.");
          }
          return ValueFactory::GetIntegerValue(static_cast<int32_t>(value.GetAs<double>()));
        }
        case DATA_TYPE::VARCHAR:
        {
          std::string str = value.ToString();
          int32_t integer = 0;
          try
          {
            integer = stoi(str);
          }
          catch (std::out_of_range &e)
          {
            throw Exception("Numeric value out of range.");
          }
          catch (std::invalid_argument &e)
          {
            throw Exception("Invalid input syntax for integer: \'" + str + "\'");
          }

          if (integer > BUSTUB_INT32_MAX || integer < BUSTUB_INT32_MIN)
          {
            throw Exception("Numeric value out of range.");
          }
          return ValueFactory::GetIntegerValue(integer);
        }
        default:
          break;
        }
      }
      throw Exception(CompareType::GetInstance(value.GetDataType())->ToString(value) + " is not coercable to INTEGER.");
    }

    static inline auto CastAsSmallInt(const Value &value) -> Value
    {
      if (CompareType::GetInstance(DATA_TYPE::SMALLINT)->IsCoercableFrom(value.GetDataType()))
      {
        if (value.IsNull())
        {
          return ValueFactory::GetSmallIntValue(static_cast<int16_t>(BUSTUB_INT16_NULL));
        }
        switch (value.GetDataType())
        {
        case DATA_TYPE::TINYINT:
          return ValueFactory::GetSmallIntValue(static_cast<int16_t>(value.GetAs<int8_t>()));
        case DATA_TYPE::SMALLINT:
          return ValueFactory::GetSmallIntValue(value.GetAs<int16_t>());
        case DATA_TYPE::INTEGER:
        {
          if (value.GetAs<int32_t>() > static_cast<int32_t>(BUSTUB_INT16_MAX) ||
              value.GetAs<int32_t>() < static_cast<int32_t>(BUSTUB_INT16_MIN))
          {
            throw Exception("Numeric value out of range.");
          }
          return ValueFactory::GetSmallIntValue(static_cast<int16_t>(value.GetAs<int32_t>()));
        }
        case DATA_TYPE::BIGINT:
        {
          if (value.GetAs<int64_t>() > static_cast<int64_t>(BUSTUB_INT16_MAX) ||
              value.GetAs<int64_t>() < static_cast<int64_t>(BUSTUB_INT16_MIN))
          {
            throw Exception("Numeric value out of range.");
          }
          return ValueFactory::GetSmallIntValue(static_cast<int16_t>(value.GetAs<int64_t>()));
        }
        case DATA_TYPE::DECIMAL:
        {
          if (value.GetAs<double>() > static_cast<double>(BUSTUB_INT16_MAX) ||
              value.GetAs<double>() < static_cast<double>(BUSTUB_INT16_MIN))
          {
            throw Exception("Numeric value out of range.");
          }
          return ValueFactory::GetSmallIntValue(static_cast<int16_t>(value.GetAs<double>()));
        }
        case DATA_TYPE::VARCHAR:
        {
          std::string str = value.ToString();
          int16_t smallint = 0;
          try
          {
            smallint = static_cast<int16_t>(stoi(str));
          }
          catch (std::out_of_range &e)
          {
            throw Exception("Numeric value out of range.");
          }
          catch (std::invalid_argument &e)
          {
            throw Exception("Invalid input syntax for smallint: \'" + str + "\'");
          }
          if (smallint < BUSTUB_INT16_MIN)
          {
            throw Exception("Numeric value out of range.");
          }
          return ValueFactory::GetSmallIntValue(smallint);
        }
        default:
          break;
        }
      }
      throw Exception(CompareType::GetInstance(value.GetDataType())->ToString(value) + " is not coercable to SMALLINT.");
    }

    static inline auto CastAsTinyInt(const Value &value) -> Value
    {
      if (CompareType::GetInstance(DATA_TYPE::TINYINT)->IsCoercableFrom(value.GetDataType()))
      {
        if (value.IsNull())
        {
          return ValueFactory::GetTinyIntValue(BUSTUB_INT8_NULL);
        }
        switch (value.GetDataType())
        {
        case DATA_TYPE::TINYINT:
          return ValueFactory::GetTinyIntValue(value.GetAs<int8_t>());
        case DATA_TYPE::SMALLINT:
        {
          if (value.GetAs<int16_t>() > static_cast<int16_t>(BUSTUB_INT8_MAX) ||
              value.GetAs<int16_t>() < static_cast<int16_t>(BUSTUB_INT8_MIN))
          {
            throw Exception("Numeric value out of range.");
          }
          return ValueFactory::GetTinyIntValue(static_cast<int8_t>(value.GetAs<int16_t>()));
        }
        case DATA_TYPE::INTEGER:
        {
          if (value.GetAs<int32_t>() > static_cast<int32_t>(BUSTUB_INT8_MAX) ||
              value.GetAs<int32_t>() < static_cast<int32_t>(BUSTUB_INT8_MIN))
          {
            throw Exception("Numeric value out of range.");
          }
          return ValueFactory::GetTinyIntValue(static_cast<int8_t>(value.GetAs<int32_t>()));
        }
        case DATA_TYPE::BIGINT:
        {
          if (value.GetAs<int64_t>() > static_cast<int64_t>(BUSTUB_INT8_MAX) ||
              value.GetAs<int64_t>() < static_cast<int64_t>(BUSTUB_INT8_MIN))
          {
            throw Exception("Numeric value out of range.");
          }
          return ValueFactory::GetTinyIntValue(static_cast<int8_t>(value.GetAs<int64_t>()));
        }
        case DATA_TYPE::DECIMAL:
        {
          if (value.GetAs<double>() > static_cast<double>(BUSTUB_INT8_MAX) ||
              value.GetAs<double>() < static_cast<double>(BUSTUB_INT8_MIN))
          {
            throw Exception("Numeric value out of range.");
          }
          return ValueFactory::GetTinyIntValue(static_cast<int8_t>(value.GetAs<double>()));
        }
        case DATA_TYPE::VARCHAR:
        {
          std::string str = value.ToString();
          int8_t tinyint = 0;
          try
          {
            tinyint = static_cast<int8_t>(stoi(str));
          }
          catch (std::out_of_range &e)
          {
            throw Exception("Numeric value out of range.");
          }
          catch (std::invalid_argument &e)
          {
            throw Exception("Invalid input syntax for tinyint: \'" + str + "\'");
          }
          if (tinyint < BUSTUB_INT8_MIN)
          {
            throw Exception("Numeric value out of range.");
          }
          return ValueFactory::GetTinyIntValue(tinyint);
        }
        default:
          break;
        }
      }
      throw Exception(CompareType::GetInstance(value.GetDataType())->ToString(value) + " is not coercable to TINYINT.");
    }

    static inline auto CastAsDecimal(const Value &value) -> Value
    {
      if (CompareType::GetInstance(DATA_TYPE::DECIMAL)->IsCoercableFrom(value.GetDataType()))
      {
        if (value.IsNull())
        {
          return ValueFactory::GetDecimalValue(static_cast<double>(BUSTUB_DECIMAL_NULL));
        }
        switch (value.GetDataType())
        {
        case DATA_TYPE::TINYINT:
          return ValueFactory::GetDecimalValue(static_cast<double>(value.GetAs<int8_t>()));
        case DATA_TYPE::SMALLINT:
          return ValueFactory::GetDecimalValue(static_cast<double>(value.GetAs<int16_t>()));
        case DATA_TYPE::INTEGER:
          return ValueFactory::GetDecimalValue(static_cast<double>(value.GetAs<int32_t>()));
        case DATA_TYPE::BIGINT:
          return ValueFactory::GetDecimalValue(static_cast<double>(value.GetAs<int64_t>()));
        case DATA_TYPE::DECIMAL:
          return ValueFactory::GetDecimalValue(value.GetAs<double>());
        case DATA_TYPE::VARCHAR:
        {
          std::string str = value.ToString();
          double res = 0;
          try
          {
            res = stod(str);
          }
          catch (std::out_of_range &e)
          {
            throw Exception("Numeric value out of range.");
          }
          catch (std::invalid_argument &e)
          {
            throw Exception("Invalid input syntax for decimal: \'" + str + "\'");
          }
          if (res > BUSTUB_DECIMAL_MAX || res < BUSTUB_DECIMAL_MIN)
          {
            throw Exception("Numeric value out of range.");
          }
          return ValueFactory::GetDecimalValue(res);
        }
        default:
          break;
        }
      }
      throw Exception(CompareType::GetInstance(value.GetDataType())->ToString(value) + " is not coercable to DECIMAL.");
    }

    static inline auto CastAsVarchar(const Value &value) -> Value
    {
      if (CompareType::GetInstance(DATA_TYPE::VARCHAR)->IsCoercableFrom(value.GetDataType()))
      {
        if (value.IsNull())
        {
          return ValueFactory::GetVarcharValue(nullptr, false);
        }
        switch (value.GetDataType())
        {
        case DATA_TYPE::BOOLEAN:
        case DATA_TYPE::TINYINT:
        case DATA_TYPE::SMALLINT:
        case DATA_TYPE::INTEGER:
        case DATA_TYPE::BIGINT:
        case DATA_TYPE::DECIMAL:
        case DATA_TYPE::VARCHAR:
          return ValueFactory::GetVarcharValue(value.ToString());
        default:
          break;
        }
      }
      throw Exception(CompareType::GetInstance(value.GetDataType())->ToString(value) + " is not coercable to VARCHAR.");
    }

    static inline auto CastAsTimestamp(const Value &value) -> Value
    {
      if (CompareType::GetInstance(DATA_TYPE::TIMESTAMP)->IsCoercableFrom(value.GetDataType()))
      {
        if (value.IsNull())
        {
          return ValueFactory::GetTimestampValue(BUSTUB_TIMESTAMP_NULL);
        }
        switch (value.GetDataType())
        {
        case DATA_TYPE::TIMESTAMP:
          return ValueFactory::GetTimestampValue(value.GetAs<uint64_t>());
        case DATA_TYPE::VARCHAR:
        {
          std::string str = value.ToString();
          if (str.length() == 22)
          {
            str = str.substr(0, 19) + ".000000" + str.substr(19, 3);
          }
          if (str.length() != 29)
          {
            throw Exception("Timestamp format error.");
          }
          if (str[10] != ' ' || str[4] != '-' || str[7] != '-' || str[13] != ':' || str[16] != ':' || str[19] != '.' ||
              (str[26] != '+' && str[26] != '-'))
          {
            throw Exception("Timestamp format error.");
          }
          bool is_digit[29] = {true, true, true, true, false, true, true, false, true, true,
                               false, true, true, false, true, true, false, true, true, false,
                               true, true, true, true, true, true, false, true, true};
          for (int i = 0; i < 29; i++)
          {
            if (is_digit[i])
            {
              if (str[i] < '0' || str[i] > '9')
              {
                throw Exception("Timestamp format error.");
              }
            }
          }
          int tz = 0;
          uint32_t year = 0;
          uint32_t month = 0;
          uint32_t day = 0;
          uint32_t hour = 0;
          uint32_t min = 0;
          uint32_t sec = 0;
          uint32_t micro = 0;
          uint64_t res = 0;
          if (sscanf(str.c_str(), "%4u-%2u-%2u %2u:%2u:%2u.%6u%3d", &year, &month, &day, &hour, &min, &sec, &micro,
                     &tz) != 8)
          {
            throw Exception("Timestamp format error.");
          }
          if (year > 9999 || month > 12 || day > 31 || hour > 23 || min > 59 || sec > 59 || micro > 999999 ||
              day == 0 || month == 0)
          {
            throw Exception("Timestamp value out of range.");
          }
          uint32_t max_day[13] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
          uint32_t max_day_lunar[13] = {0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
          if ((year % 4 == 0 && year % 100 != 0) || year % 400 == 0)
          {
            if (day > max_day_lunar[month])
            {
              throw Exception("Timestamp value out of range.");
            }
          }
          else if (day > max_day[month])
          {
            throw Exception("Timestamp value out of range.");
          }
          uint32_t timezone = tz + 12;
          if (tz > 26)
          {
            throw Exception("Timestamp format error.");
          }
          res += month;
          res *= 32;
          res += day;
          res *= 27;
          res += timezone;
          res *= 10000;
          res += year;
          res *= 100000;
          res += hour * 3600 + min * 60 + sec;
          res *= 1000000;
          res += micro;
          return ValueFactory::GetTimestampValue(res);
        }
        default:
          break;
        }
      }
      throw Exception(CompareType::GetInstance(value.GetDataType())->ToString(value) + " is not coercable to TIMESTAMP.");
    }

    static inline auto CastAsBoolean(const Value &value) -> Value
    {
      if (CompareType::GetInstance(DATA_TYPE::BOOLEAN)->IsCoercableFrom(value.GetDataType()))
      {
        if (value.IsNull())
        {
          return ValueFactory::GetBooleanValue(BUSTUB_BOOLEAN_NULL);
        }
        switch (value.GetDataType())
        {
        case DATA_TYPE::BOOLEAN:
          return ValueFactory::GetBooleanValue(value.GetAs<int8_t>());
        case DATA_TYPE::VARCHAR:
        {
          std::string str = value.ToString();
          std::transform(str.begin(), str.end(), str.begin(), ::tolower);
          if (str == "true" || str == "1" || str == "t")
          {
            return ValueFactory::GetBooleanValue(true);
          }
          if (str == "false" || str == "0" || str == "f")
          {
            return ValueFactory::GetBooleanValue(false);
          }
          throw Exception("Boolean value format error.");
        }
        default:
          break;
        }
      }
      throw Exception(CompareType::GetInstance(value.GetDataType())->ToString(value) + " is not coercable to BOOLEAN.");
    }
  };

}
#endif
