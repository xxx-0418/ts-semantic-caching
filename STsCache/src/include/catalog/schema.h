#ifndef SCHEMA_H
#define SCHEMA_H

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "catalog/column.h"
#include "common/macros.h"

namespace SemanticCache {
class Schema;
using SchemaRef = std::shared_ptr<Schema>;

class Schema {
 public:
  explicit Schema(const std::vector<Column> &columns);

  Schema() = default;

  Schema(const Schema &schema) {
    length_ = schema.length_;
    columns_ = schema.columns_;
    tuple_is_inlined_ = schema.tuple_is_inlined_;
    uninlined_columns_ = schema.uninlined_columns_;
  }

  auto operator=(const Schema &schema) -> Schema & {
    this->length_ = schema.length_;
    this->columns_ = schema.columns_;
    this->length_ = schema.length_;
    this->tuple_is_inlined_ = schema.tuple_is_inlined_;
    this->uninlined_columns_ = schema.uninlined_columns_;
    return *this;
  }

  auto operator==(const Schema &sche)const -> bool{
      if(this == &sche){
          return true;
      }
      if(this->columns_.size() != sche.columns_.size()){
          return false;
      }
      for(const auto &it:this->columns_){
          auto idx = sche.TryGetColIdx(it.GetName());
          if(idx == std::nullopt){
            return false;
          }
          if(it != sche.columns_[idx.value()]){
            return false;
          }
      }
      return true;
  }

  static auto CopySchema(const Schema *from, const std::vector<uint32_t> &attrs) -> Schema {
    std::vector<Column> cols;
    cols.reserve(attrs.size());
    for (const auto i : attrs) {
      cols.emplace_back(from->columns_[i]);
    }
    return Schema{cols};
  }

  static auto MergeSchema(const std::shared_ptr<Schema> &schema1,
                          const std::shared_ptr<Schema> &schema2)
      -> std::pair<std::shared_ptr<Schema>, std::vector<size_t>>;

  static auto SubSchema(const std::shared_ptr<Schema> &schema1,
                        const std::shared_ptr<Schema> &schema2) -> std::shared_ptr<Schema>;

  static auto DifferenceSchema(const std::shared_ptr<Schema> &schema1,
                               const std::shared_ptr<Schema> &schema2) -> std::shared_ptr<Schema>;

  static auto GetSubSchemaIdx(const std::shared_ptr<Schema> &input_schema,const std::shared_ptr<Schema> &output_schema,std::vector<std::optional<uint32_t>>&schema_idx)->void;

  auto GetColumns() const -> const std::vector<Column> & { return columns_; }

  auto GetColumn(const uint32_t col_idx) const -> const Column & { return columns_[col_idx]; }

  auto GetColIdx(const std::string &col_name) const -> uint32_t {
    if (auto col_idx = TryGetColIdx(col_name)) {
      return *col_idx;
    }
    UNREACHABLE("Column does not exist");
  }

  auto TryGetColIdx(const std::string &col_name) const -> std::optional<uint32_t> {
        for (uint32_t i = 0; i < columns_.size(); ++i) {
          if (columns_[i].GetName() == col_name) {
            return std::optional{i};
          }
        }
    return std::nullopt;
  }

  auto GetUnlinedColumns() const -> const std::vector<uint32_t> & { return uninlined_columns_; }

  auto GetColumnCount() const -> uint32_t { return static_cast<uint32_t>(columns_.size()); }

  auto GetUnlinedColumnCount() const -> uint32_t {
    return static_cast<uint32_t>(uninlined_columns_.size());
  }

  inline auto GetLength() const -> uint32_t { return length_; }

  inline auto IsInlined() const -> bool { return tuple_is_inlined_; }

  auto ToString(const bool &simplified = true) const -> std::string;

 private:
  uint32_t length_;

  std::vector<Column> columns_;

  bool tuple_is_inlined_{true};

  std::vector<uint32_t> uninlined_columns_;
};
}

#endif