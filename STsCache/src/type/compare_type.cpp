#include "type/compare_type.h"
#include <string>
#include "common/exception.h"
#include "common/type.h"
#include "type/bigint_type.h"
#include "type/boolean_type.h"
#include "type/decimal_type.h"
#include "type/integer_type.h"
#include "type/smallint_type.h"
#include "type/tinyint_type.h"
#include "type/value.h"
#include "type/varlen_type.h"

namespace SemanticCache
{
  CompareType *CompareType::k_types[] = {
      new CompareType(DATA_TYPE::INVALID_DATA_TYPE),
      new BooleanType(),
      new TinyintType(),
      new SmallintType(),
      new IntegerType(DATA_TYPE::INTEGER),
      new BigintType(),
      new DecimalType(),
      new VarlenType(DATA_TYPE::VARCHAR),
  };

  auto CompareType::GetTypeSize(const DATA_TYPE type_id) -> uint64_t
  {
    switch (type_id)
    {
    case BOOLEAN:
    case TINYINT:
      return 1;
    case SMALLINT:
      return 2;
    case INTEGER:
      return 4;
    case BIGINT:
    case DECIMAL:
    case TIMESTAMP:
      return 8;
    case VARCHAR:
      return 0;
    default:
      break;
    }
    throw Exception("Unknown type.");
  }

  auto CompareType::IsCoercableFrom(const DATA_TYPE type_id) const -> bool
  {
    switch (type_id_)
    {
    case INVALID_DATA_TYPE:
      return false;
    case BOOLEAN:
      return true;
    case TINYINT:
    case SMALLINT:
    case INTEGER:
    case BIGINT:
    case DECIMAL:
      switch (type_id)
      {
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
      case DECIMAL:
      case VARCHAR:
        return true;
      default:
        return false;
      }
      break;
    case TIMESTAMP:
      return (type_id == VARCHAR || type_id == TIMESTAMP);
    case VARCHAR:
      switch (type_id)
      {
      case BOOLEAN:
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
      case DECIMAL:
      case TIMESTAMP:
      case VARCHAR:
        return true;
      default:
        return false;
      }
      break;
    default:
      return (type_id == type_id_);
    }
  }

  auto CompareType::TypeIdToString(const DATA_TYPE type_id) -> std::string
  {
    switch (type_id)
    {
    case INVALID_DATA_TYPE:
      return "INVALID";
    case BOOLEAN:
      return "BOOLEAN";
    case TINYINT:
      return "TINYINT";
    case SMALLINT:
      return "SMALLINT";
    case INTEGER:
      return "INTEGER";
    case BIGINT:
      return "BIGINT";
    case DECIMAL:
      return "DECIMAL";
    case TIMESTAMP:
      return "TIMESTAMP";
    case VARCHAR:
      return "VARCHAR";
    default:
      return "INVALID";
    }
  }

  auto CompareType::GetMinValue(DATA_TYPE type_id) -> Value
  {
    switch (type_id)
    {
    case BOOLEAN:
      return {type_id, 0};
    case TINYINT:
      return {type_id, BUSTUB_INT8_MIN};
    case SMALLINT:
      return {type_id, BUSTUB_INT16_MIN};
    case INTEGER:
      return {type_id, BUSTUB_INT32_MIN};
    case BIGINT:
      return {type_id, BUSTUB_INT64_MIN};
    case DECIMAL:
      return {type_id, BUSTUB_DECIMAL_MIN};
    case TIMESTAMP:
      return {type_id, 0};
    case VARCHAR:
      return {type_id, ""};
    default:
      break;
    }
    throw Exception("Cannot get minimal value.");
  }

  auto CompareType::GetMaxValue(DATA_TYPE type_id) -> Value
  {
    switch (type_id)
    {
    case BOOLEAN:
      return {type_id, 1};
    case TINYINT:
      return {type_id, BUSTUB_INT8_MAX};
    case SMALLINT:
      return {type_id, BUSTUB_INT16_MAX};
    case INTEGER:
      return {type_id, BUSTUB_INT32_MAX};
    case BIGINT:
      return {type_id, BUSTUB_INT64_MAX};
    case DECIMAL:
      return {type_id, BUSTUB_DECIMAL_MAX};
    case TIMESTAMP:
      return {type_id, BUSTUB_TIMESTAMP_MAX};
    case VARCHAR:
      return {type_id, nullptr, 0, false};
    default:
      break;
    }
    throw Exception("Cannot get max value.");
  }

  auto CompareType::CompareEquals(const Value &left __attribute__((unused)), const Value &right __attribute__((unused))) const
      -> CmpBool
  {
    throw Exception("CompareEquals not implemented");
  }

  auto CompareType::CompareNotEquals(const Value &left __attribute__((unused)), const Value &right __attribute__((unused))) const
      -> CmpBool
  {
    throw Exception("CompareNotEquals not implemented");
  }

  auto CompareType::CompareLessThan(const Value &left __attribute__((unused)), const Value &right __attribute__((unused))) const
      -> CmpBool
  {
    throw Exception("CompareLessThan not implemented");
  }
  auto CompareType::CompareLessThanEquals(const Value &left __attribute__((unused)),
                                          const Value &right __attribute__((unused))) const -> CmpBool
  {
    throw Exception("CompareLessThanEqual not implemented");
  }
  auto CompareType::CompareGreaterThan(const Value &left __attribute__((unused)),
                                       const Value &right __attribute__((unused))) const -> CmpBool
  {
    throw Exception("CompareGreaterThan not implemented");
  }
  auto CompareType::CompareGreaterThanEquals(const Value &left __attribute__((unused)),
                                             const Value &right __attribute__((unused))) const -> CmpBool
  {
    throw Exception("CompareGreaterThanEqual not implemented");
  }

  auto CompareType::Add(const Value &left __attribute__((unused)), const Value &right __attribute__((unused))) const -> Value
  {
    throw Exception("Add not implemented");
  }

  auto CompareType::Subtract(const Value &left __attribute__((unused)), const Value &right __attribute__((unused))) const
      -> Value
  {
    throw Exception("Subtract not implemented");
  }

  auto CompareType::Multiply(const Value &left __attribute__((unused)), const Value &right __attribute__((unused))) const
      -> Value
  {
    throw Exception("Multiply not implemented");
  }

  auto CompareType::Divide(const Value &left __attribute__((unused)), const Value &right __attribute__((unused))) const
      -> Value
  {
    throw Exception("Divide not implemented");
  }

  auto CompareType::Modulo(const Value &left __attribute__((unused)), const Value &right __attribute__((unused))) const
      -> Value
  {
    throw Exception("Modulo not implemented");
  }

  auto CompareType::Min(const Value &left __attribute__((unused)), const Value &right __attribute__((unused))) const -> Value
  {
    throw Exception("Min not implemented");
  }

  auto CompareType::Max(const Value &left __attribute__((unused)), const Value &right __attribute__((unused))) const -> Value
  {
    throw Exception("Max not implemented");
  }

  auto CompareType::Sqrt(const Value &val __attribute__((unused))) const -> Value
  {
    throw Exception("Sqrt not implemented");
  }

  auto CompareType::OperateNull(const Value &val __attribute__((unused)), const Value &right __attribute__((unused))) const
      -> Value
  {
    throw Exception("OperateNull not implemented");
  }

  auto CompareType::IsZero(const Value &val __attribute__((unused))) const -> bool
  {
    throw Exception("isZero not implemented");
  }
  auto CompareType::IsInlined(const Value &val __attribute__((unused))) const -> bool
  {
    throw Exception("IsLined not implemented");
  }
  auto CompareType::ToString(const Value &val __attribute__((unused))) const -> std::string
  {
    throw Exception("ToString not implemented");
  }
  void CompareType::SerializeTo(const Value &val __attribute__((unused)), char *storage __attribute__((unused))) const
  {
    throw Exception("SerializeTo not implemented");
  }

  auto CompareType::DeserializeFrom(const char *storage __attribute__((unused))) const -> Value
  {
    throw Exception("DeserializeFrom not implemented");
  }

  auto CompareType::Copy(const Value &val __attribute__((unused))) const -> Value
  {
    throw Exception("Copy not implemented");
  }

  auto CompareType::CastAs(const Value &val __attribute__((unused)), const DATA_TYPE type_id __attribute__((unused))) const
      -> Value
  {
    throw Exception("CastAs not implemented");
  }

  auto CompareType::GetData(const Value &val __attribute__((unused))) const -> const char *
  {
    throw Exception("GetData from value not implemented");
  }
  auto CompareType::GetLength(const Value &val __attribute__((unused))) const -> uint32_t
  {
    throw Exception("GetLength not implemented");
  }

  auto CompareType::GetData(char *storage __attribute__((unused))) -> char *
  {
    throw Exception("GetData not implemented");
  }
}