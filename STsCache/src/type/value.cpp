#include "type/value.h"
#include "common/type.h"
#include "common/exception.h"

#include <cassert>
#include <string>
#include <utility>
#include <cstring>

namespace SemanticCache
{
  Value::Value(const Value &other)
  {
    type_id_ = other.type_id_;
    size_ = other.size_;
    manage_data_ = other.manage_data_;
    value_ = other.value_;
    switch (type_id_)
    {
    case DATA_TYPE::VARCHAR:
      if (size_.len_ == BUSTUB_VALUE_NULL)
      {
        value_.varlen_ = nullptr;
      }
      else
      {
        if (manage_data_)
        {
          value_.varlen_ = new char[size_.len_];
          memcpy(value_.varlen_, other.value_.varlen_, size_.len_);
        }
        else
        {
          value_ = other.value_;
        }
      }
      break;
    default:
      value_ = other.value_;
    }
  }

  auto Value::operator=(Value other) -> Value &
  {
    Swap(*this, other);
    return *this;
  }

  Value::Value(DATA_TYPE type, int8_t i) : Value(type)
  {
    switch (type)
    {
    case DATA_TYPE::BOOLEAN:
      value_.boolean_ = i;
      size_.len_ = (value_.boolean_ == BUSTUB_BOOLEAN_NULL ? BUSTUB_VALUE_NULL : 0);
      break;
    case DATA_TYPE::TINYINT:
      value_.tinyint_ = i;
      size_.len_ = (value_.tinyint_ == BUSTUB_INT8_NULL ? BUSTUB_VALUE_NULL : 0);
      break;
    case DATA_TYPE::SMALLINT:
      value_.smallint_ = i;
      size_.len_ = (value_.smallint_ == BUSTUB_INT16_NULL ? BUSTUB_VALUE_NULL : 0);
      break;
    case DATA_TYPE::INTEGER:
      value_.integer_ = i;
      size_.len_ = (value_.integer_ == BUSTUB_INT32_NULL ? BUSTUB_VALUE_NULL : 0);
      break;
    case DATA_TYPE::BIGINT:
      value_.bigint_ = i;
      size_.len_ = (value_.bigint_ == BUSTUB_INT64_NULL ? BUSTUB_VALUE_NULL : 0);
      break;
    default:
      throw Exception("Invalid Type for one-byte Value constructor");
    }
  }

  Value::Value(DATA_TYPE type, int16_t i) : Value(type)
  {
    switch (type)
    {
    case DATA_TYPE::BOOLEAN:
      value_.boolean_ = i;
      size_.len_ = (value_.boolean_ == BUSTUB_BOOLEAN_NULL ? BUSTUB_VALUE_NULL : 0);
      break;
    case DATA_TYPE::TINYINT:
      value_.tinyint_ = i;
      size_.len_ = (value_.tinyint_ == BUSTUB_INT8_NULL ? BUSTUB_VALUE_NULL : 0);
      break;
    case DATA_TYPE::SMALLINT:
      value_.smallint_ = i;
      size_.len_ = (value_.smallint_ == BUSTUB_INT16_NULL ? BUSTUB_VALUE_NULL : 0);
      break;
    case DATA_TYPE::INTEGER:
      value_.integer_ = i;
      size_.len_ = (value_.integer_ == BUSTUB_INT32_NULL ? BUSTUB_VALUE_NULL : 0);
      break;
    case DATA_TYPE::BIGINT:
      value_.bigint_ = i;
      size_.len_ = (value_.bigint_ == BUSTUB_INT64_NULL ? BUSTUB_VALUE_NULL : 0);
      break;
    case DATA_TYPE::TIMESTAMP:
      value_.timestamp_ = i;
      size_.len_ = (value_.timestamp_ == BUSTUB_TIMESTAMP_NULL ? BUSTUB_VALUE_NULL : 0);
      break;
    default:
      throw Exception("Invalid Type for two-byte Value constructor");
    }
  }

  Value::Value(DATA_TYPE type, int32_t i) : Value(type)
  {
    switch (type)
    {
    case DATA_TYPE::BOOLEAN:
      value_.boolean_ = i;
      size_.len_ = (value_.boolean_ == BUSTUB_BOOLEAN_NULL ? BUSTUB_VALUE_NULL : 0);
      break;
    case DATA_TYPE::TINYINT:
      value_.tinyint_ = i;
      size_.len_ = (value_.tinyint_ == BUSTUB_INT8_NULL ? BUSTUB_VALUE_NULL : 0);
      break;
    case DATA_TYPE::SMALLINT:
      value_.smallint_ = i;
      size_.len_ = (value_.smallint_ == BUSTUB_INT16_NULL ? BUSTUB_VALUE_NULL : 0);
      break;
    case DATA_TYPE::INTEGER:
      value_.integer_ = i;
      size_.len_ = (value_.integer_ == BUSTUB_INT32_NULL ? BUSTUB_VALUE_NULL : 0);
      break;
    case DATA_TYPE::BIGINT:
      value_.bigint_ = i;
      size_.len_ = (value_.bigint_ == BUSTUB_INT64_NULL ? BUSTUB_VALUE_NULL : 0);
      break;
    case DATA_TYPE::TIMESTAMP:
      value_.timestamp_ = i;
      size_.len_ = (value_.timestamp_ == BUSTUB_TIMESTAMP_NULL ? BUSTUB_VALUE_NULL : 0);
      break;
    default:
      throw Exception("Invalid Type for integer_ Value constructor");
    }
  }

  Value::Value(DATA_TYPE type, int64_t i) : Value(type)
  {
    switch (type)
    {
    case DATA_TYPE::BOOLEAN:
      value_.boolean_ = i;
      size_.len_ = (value_.boolean_ == BUSTUB_BOOLEAN_NULL ? BUSTUB_VALUE_NULL : 0);
      break;
    case DATA_TYPE::TINYINT:
      value_.tinyint_ = i;
      size_.len_ = (value_.tinyint_ == BUSTUB_INT8_NULL ? BUSTUB_VALUE_NULL : 0);
      break;
    case DATA_TYPE::SMALLINT:
      value_.smallint_ = i;
      size_.len_ = (value_.smallint_ == BUSTUB_INT16_NULL ? BUSTUB_VALUE_NULL : 0);
      break;
    case DATA_TYPE::INTEGER:
      value_.integer_ = i;
      size_.len_ = (value_.integer_ == BUSTUB_INT32_NULL ? BUSTUB_VALUE_NULL : 0);
      break;
    case DATA_TYPE::BIGINT:
      value_.bigint_ = i;
      size_.len_ = (value_.bigint_ == BUSTUB_INT64_NULL ? BUSTUB_VALUE_NULL : 0);
      break;
    case DATA_TYPE::TIMESTAMP:
      value_.timestamp_ = i;
      size_.len_ = (value_.timestamp_ == BUSTUB_TIMESTAMP_NULL ? BUSTUB_VALUE_NULL : 0);
      break;
    default:
      throw Exception("Invalid Type for eight-byte Value constructor");
    }
  }

  Value::Value(DATA_TYPE type, uint64_t i) : Value(type)
  {
    switch (type)
    {
    case DATA_TYPE::BIGINT:
      value_.bigint_ = i;
      size_.len_ = (value_.bigint_ == BUSTUB_INT64_NULL ? BUSTUB_VALUE_NULL : 0);
      break;
    case DATA_TYPE::TIMESTAMP:
      value_.timestamp_ = i;
      size_.len_ = (value_.timestamp_ == BUSTUB_TIMESTAMP_NULL ? BUSTUB_VALUE_NULL : 0);
      break;
    default:
      throw Exception("Invalid Type for timestamp_ Value constructor");
    }
  }

  Value::Value(DATA_TYPE type, double d) : Value(type)
  {
    switch (type)
    {
    case DATA_TYPE::DECIMAL:
      value_.decimal_ = d;
      size_.len_ = (value_.decimal_ == BUSTUB_DECIMAL_NULL ? BUSTUB_VALUE_NULL : 0);
      break;
    default:
      throw Exception("Invalid Type for double Value constructor");
    }
  }

  Value::Value(DATA_TYPE type, float f) : Value(type)
  {
    switch (type)
    {
    case DATA_TYPE::DECIMAL:
      value_.decimal_ = f;
      size_.len_ = (value_.decimal_ == BUSTUB_DECIMAL_NULL ? BUSTUB_VALUE_NULL : 0);
      break;
    default:
      throw Exception("Invalid Type for float value constructor");
    }
  }

  Value::Value(DATA_TYPE type, const char *data, uint32_t len, bool manage_data) : Value(type)
  {
    switch (type)
    {
    case DATA_TYPE::VARCHAR:
      if (data == nullptr)
      {
        value_.varlen_ = nullptr;
        size_.len_ = BUSTUB_VALUE_NULL;
      }
      else
      {
        manage_data_ = manage_data;
        if (manage_data_)
        {
          assert(len < BUSTUB_VARCHAR_MAX_LEN);
          value_.varlen_ = new char[len];
          assert(value_.varlen_ != nullptr);
          size_.len_ = len;
          memcpy(value_.varlen_, data, len);
        }
        else
        {
          value_.const_varlen_ = data;
          size_.len_ = len;
        }
      }
      break;
    default:
      throw Exception("Invalid Type  for variable-length Value constructor");
    }
  }

  Value::Value(DATA_TYPE type, const std::string &data) : Value(type)
  {
    switch (type)
    {
    case DATA_TYPE::VARCHAR:
    {
      manage_data_ = true;
      uint32_t len = static_cast<uint32_t>(data.length()) + 1;
      value_.varlen_ = new char[len];
      assert(value_.varlen_ != nullptr);
      size_.len_ = len;
      memcpy(value_.varlen_, data.c_str(), len);
      break;
    }
    default:
      throw Exception("Invalid Type  for variable-length Value constructor");
    }
  }

  Value::~Value()
  {
    switch (type_id_)
    {
    case DATA_TYPE::VARCHAR:
      if (manage_data_)
      {
        delete[] value_.varlen_;
      }
      break;
    default:
      break;
    }
  }

  auto Value::CheckComparable(const Value &o) const -> bool
  {
    switch (GetDataType())
    {
    case DATA_TYPE::BOOLEAN:
      return (o.GetDataType() == DATA_TYPE::BOOLEAN || o.GetDataType() == DATA_TYPE::VARCHAR);
    case DATA_TYPE::TINYINT:
    case DATA_TYPE::SMALLINT:
    case DATA_TYPE::INTEGER:
    case DATA_TYPE::BIGINT:
    case DATA_TYPE::DECIMAL:
      switch (o.GetDataType())
      {
      case DATA_TYPE::TINYINT:
      case DATA_TYPE::SMALLINT:
      case DATA_TYPE::INTEGER:
      case DATA_TYPE::BIGINT:
      case DATA_TYPE::DECIMAL:
      case DATA_TYPE::VARCHAR:
        return true;
      default:
        break;
      }
      break;
    case DATA_TYPE::VARCHAR:
      return true;
      break;
    default:
      break;
    }
    return false;
  }

  auto Value::CheckInteger() const -> bool
  {
    switch (GetDataType())
    {
    case DATA_TYPE::TINYINT:
    case DATA_TYPE::SMALLINT:
    case DATA_TYPE::INTEGER:
    case DATA_TYPE::BIGINT:
      return true;
    default:
      break;
    }
    return false;
  }
}