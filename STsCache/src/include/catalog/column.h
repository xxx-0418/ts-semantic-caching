#pragma once

#ifndef COLUMN_H
#define COLUMN_H

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include "common/macros.h"
#include "common/type.h"
#include "type/compare_type.h"

namespace SemanticCache {
class Column {
  friend class Schema;

 public:
  Column(std::string column_name, DATA_TYPE type)
      : column_name_(std::move(column_name)), column_type_(type), fixed_length_(TypeSize(type)) {
    if(type == DATA_TYPE::VARCHAR){
        variable_length_ = TypeSize(type);
    }
  }

  Column(const Column &col) {
    column_name_ = col.column_name_;
    column_type_ = col.column_type_;
    fixed_length_ = col.fixed_length_;
    variable_length_ = col.variable_length_;
    column_offset_ = col.column_offset_;
  }
  Column &operator=(const Column &col) {
    if (this == &col) {
      return *this;
    }
    column_name_ = col.column_name_;
    column_type_ = col.column_type_;
    fixed_length_ = col.fixed_length_;
    variable_length_ = col.variable_length_;
    column_offset_ = col.column_offset_;
    return *this;
  }

  auto operator==(const Column &col)const ->bool{
    if(this == &col){
      return true;
    }
    if(this->column_name_ == col.column_name_ && this->column_type_ == col.column_type_){
      return true;
    }
    return false;
  }
  auto operator!=(const Column &col)const ->bool{
      if(this==&col){
          return false;
      }
      if((this->column_name_ != col.column_name_)||(this->column_type_ != col.column_type_)){
          return true;
      }
      return false;
  }
  Column(std::string column_name, DATA_TYPE type, uint32_t length)
      : column_name_(std::move(column_name)),
        column_type_(type),
        fixed_length_(TypeSize(type)),
        variable_length_(length) {
  }

  Column(std::string column_name, const Column &column)
      : column_name_(std::move(column_name)),
        column_type_(column.column_type_),
        fixed_length_(column.fixed_length_),
        variable_length_(column.variable_length_),
        column_offset_(column.column_offset_) {}

  auto GetName() const -> std::string { return column_name_; }

  auto GetLength() const -> uint32_t {
    if (IsInlined()) {
      return fixed_length_;
    }
    return variable_length_;
  }

  auto GetFixedLength() const -> uint32_t { return fixed_length_; }

  auto GetVariableLength() const -> uint32_t { return variable_length_; }

  auto GetOffset() const -> uint32_t { return column_offset_; }

  auto GetType() const -> DATA_TYPE { return column_type_; }

  auto IsInlined() const -> bool { return column_type_ != DATA_TYPE::VARCHAR; }

  auto ToString(bool simplified = true) const -> std::string;

 private:
  static auto TypeSize(DATA_TYPE type) -> uint8_t {
    switch (type) {
      case DATA_TYPE::BOOLEAN:
      case DATA_TYPE::TINYINT:
        return 1;
      case DATA_TYPE::SMALLINT:
        return 2;
      case DATA_TYPE::INTEGER:
        return 4;
      case DATA_TYPE::BIGINT:
      case DATA_TYPE::DECIMAL:
      case DATA_TYPE::TIMESTAMP:
        return 8;
      case DATA_TYPE::VARCHAR:
        return 32;
      default: {
        UNREACHABLE("Cannot get size of invalid type");
      }
    }
  }

  std::string column_name_;

  DATA_TYPE column_type_;

  uint32_t fixed_length_;

  uint32_t variable_length_{0};

  uint32_t column_offset_{0};
};

}

#endif