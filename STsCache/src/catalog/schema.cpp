#include "catalog/schema.h"
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <utility>
#include <vector>
#include "catalog/column.h"

namespace SemanticCache {
Schema::Schema(const std::vector<Column> &columns) {
  uint32_t curr_offset = 0;
  for (uint32_t index = 0; index < columns.size(); index++) {
    Column column = columns[index];
    if (!column.IsInlined()) {
      tuple_is_inlined_ = false;
      uninlined_columns_.push_back(index);
    }
    column.column_offset_ = curr_offset;
    curr_offset += column.GetFixedLength();

    this->columns_.push_back(column);
  }
  length_ = curr_offset;
}

auto Schema::MergeSchema(const std::shared_ptr<Schema> &schema1,
                         const std::shared_ptr<Schema> &schema2)
    -> std::pair<std::shared_ptr<Schema>, std::vector<size_t>> {
  auto used = std::vector<size_t>();
  auto columns = std::vector<Column>();
  auto tmp = schema1->GetColumns();
  columns.insert(columns.end(), tmp.begin(), tmp.end());
  for (uint32_t i = 0, sz = schema2->GetColumnCount(); i < sz; i++) {
    auto col = schema2->GetColumn(i);
    if (schema1->TryGetColIdx(col.column_name_) == std::nullopt) {
      columns.emplace_back(col);
      used.emplace_back(i);
    }
  }
  return std::make_pair(std::make_shared<Schema>(columns), used);
}

auto Schema::SubSchema(const std::shared_ptr<Schema> &schema1,
                       const std::shared_ptr<Schema> &schema2) -> std::shared_ptr<Schema> {
  auto columns = std::vector<Column>();
  auto tmp = schema1->GetColumns();
  uint32_t len2 = schema2->GetColumnCount();
  for (uint32_t i = 0; i < len2; i++) {
    auto col = schema2->GetColumn(i);
    if (schema1->TryGetColIdx(col.column_name_) != std::nullopt) {
      columns.emplace_back(col);
    }
  }
  return std::make_shared<Schema>(columns);
}

auto Schema::DifferenceSchema(const std::shared_ptr<Schema> &schema1,
                              const std::shared_ptr<Schema> &schema2) -> std::shared_ptr<Schema> {
  auto columns = std::vector<Column>();
  auto tmp = schema1->GetColumns();
  uint32_t len2 = schema2->GetColumnCount();
  for (uint32_t i = 0; i < len2; i++) {
    auto col = schema2->GetColumn(i);
    if (schema1->TryGetColIdx(col.column_name_) == std::nullopt) {
      columns.emplace_back(col);
    }
  }
  return std::make_shared<Schema>(columns);
}

auto Schema::GetSubSchemaIdx(const std::shared_ptr<Schema> &input_schema, const std::shared_ptr<Schema> &output_schema,
                            std::vector<std::optional<uint32_t>> &schema_idx) -> void {
    schema_idx.reserve(output_schema->GetColumnCount());
    for(const auto &it:output_schema->GetColumns()){
        schema_idx.emplace_back(input_schema->TryGetColIdx(it.column_name_));
    }
}

auto Schema::ToString(const bool &simplified) const -> std::string {
  if (simplified) {
    std::ostringstream os;
    bool first = true;
    os << "(";
    for (uint32_t i = 0; i < GetColumnCount(); i++) {
      if (first) {
        first = false;
      } else {
        os << ", ";
      }
      os << columns_[i].ToString(simplified);
    }
    os << ")";
    return (os.str());
  }

  std::ostringstream os;

  os << "Schema["
     << "NumColumns:" << GetColumnCount() << ", "
     << "IsInlined:" << tuple_is_inlined_ << ", "
     << "Length:" << length_ << "]";

  bool first = true;
  os << " :: (";
  for (uint32_t i = 0; i < GetColumnCount(); i++) {
    if (first) {
      first = false;
    } else {
      os << ", ";
    }
    os << columns_[i].ToString();
  }
  os << ")";

  return os.str();
}
}