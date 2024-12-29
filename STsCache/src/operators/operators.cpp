#include "include/operators/operators.h"
#include <limits>
#include "common/macros.h"
namespace SemanticCache {

std::mutex op_mtx;
uint8_t *operator_buf_ = static_cast<uint8_t *>(fc_mmap(30 * MB));
std::atomic<uint32_t> op_buf_offset_ = 0;
auto SetOpBufOffset(uint32_t num) -> void { op_buf_offset_ = 0; }
auto GetAggIntervalTypeToSecondNum(Aggregation_Interval_Type type) -> uint64_t {
  switch (type) {
    case NATURAL:
      return 1;
    case SECOND:
      return 1;
    case MINUTE:
      return 60;
    case HOUR:
      return 3600;
    case DAY:
      return 24 * 3600;
    case WEEK:
      return 7 * 24 * 3600;
    case MONTH:
      return 30 * 7 * 24 * 3600;
    case YEAR:
      return 365 * 30 * 7 * 24 * (uint64_t)3600;
    default:
      return 0;
  }
}
Projection::Projection(std::vector<std::pair<uint8_t *, uint32_t>> &input,
                       SemanticCache::SchemaRef input_schema,
                       SemanticCache::SchemaRef output_schema,
                       SemanticCache::PredicatesRef predicates) {
  input_ = input;
  output_.first = nullptr;
  output_.second = 0;
  input_schema_ = input_schema;
  output_schema_ = output_schema;
  predicates_ = predicates;
}
Projection::~Projection() = default;

auto Projection::Execute() -> void {
  std::vector<std::optional<uint32_t>> schema_idx;
  Schema::GetSubSchemaIdx(input_schema_, output_schema_, schema_idx);

  uint32_t output_item_num = 0;
  uint32_t input_len = input_schema_->GetLength() + TIMESTAMP_LEN;
  for (const auto &it : input_) {
    output_item_num += it.second / input_len;
  }

  op_mtx.lock();
  auto new_addr = operator_buf_ + op_buf_offset_;
  uint32_t off_set = op_buf_offset_;
  op_buf_offset_ += output_item_num * (output_schema_->GetLength() + TIMESTAMP_LEN) + TIMESTAMP_LEN;
  op_mtx.unlock();

  for (const auto &it : input_) {
    auto item_num = it.second / input_len;
    auto addr = it.first;
    for (uint32_t i = 0; i < item_num; i++) {
      auto item = (addr + i * input_len + TIMESTAMP_LEN);
      auto temp_time = *(uint64_t *)(item - TIMESTAMP_LEN);
      memcpy(operator_buf_ + off_set, &temp_time, TIMESTAMP_LEN);
      off_set += TIMESTAMP_LEN;
      for (uint32_t j = 0; j < schema_idx.size(); j++) {
        auto val = *(double *)(item + input_schema_->GetColumn(schema_idx[j].value()).GetOffset());
        memcpy(operator_buf_ + off_set, &val, sizeof(val));
        off_set += sizeof(val);
      }
    }
  }

  output_.first = new_addr;
  output_.second = output_item_num * (output_schema_->GetLength() + TIMESTAMP_LEN);
}

auto Projection::GetOutput() -> std::pair<uint8_t *, uint32_t> { return output_; }

Aggregation::Aggregation(std::vector<std::pair<uint8_t *, uint32_t>> &input, TimeRange time_range,
                         SemanticCache::SchemaRef input_schema,
                         SemanticCache::SchemaRef output_schema,
                         SemanticCache::Aggregation_Type type,
                         SemanticCache::Aggregation_Interval_Type interval_type,
                         uint32_t interval_length) {
  input_ = input;
  output_.first = nullptr;
  output_.second = 0;
  time_range_ = time_range;
  input_schema_ = input_schema;
  output_schema_ = output_schema;
  type_ = type;
  interval_type_ = interval_type;
  interval_length_ = interval_length;
}
Aggregation::~Aggregation() = default;

auto Aggregation::Execute() -> void {
  std::vector<AggComputer> answer;
  bool s = ExecuteOp(answer, type_);
  ST_ASSERT(s == true, "Aggregation::Execute failed: ExecuteOp failed");
}

auto Aggregation::ExecuteOp(std::vector<AggComputer> &answer, const Aggregation_Type &type)
    -> bool {
  std::vector<std::optional<uint32_t>> schema_idx;
  Schema::GetSubSchemaIdx(input_schema_, output_schema_, schema_idx);
  auto hash_interval = interval_length_ * GetAggIntervalTypeToSecondNum(interval_type_);
  uint32_t output_item_num = (time_range_.time_end_ - time_range_.time_start_) / hash_interval;
  auto output_col_count = output_schema_->GetColumnCount();
  for (auto it = time_range_.time_start_; it <= time_range_.time_end_; it += hash_interval) {
    for (uint32_t i = 0; i < output_col_count; i++) {
      switch (type) {
        case MIN:
          answer.emplace_back(it, std::numeric_limits<double>::max());
          break;
        case MAX:
          answer.emplace_back(it, std::numeric_limits<double>::min());
          break;
        case MEAN:
          answer.emplace_back(it, 0, 0);
          break;
        default:
          UNREACHABLE("unimplement");
      }
    }
  }

  uint32_t input_len = input_schema_->GetLength() + TIMESTAMP_LEN;
  uint32_t idx = 0;
  uint64_t hash_time_start = answer[0].timestamp_;
  uint64_t hash_time_end = answer[answer.size() - 1].timestamp_;
  for (const auto &it : input_) {
    auto item_num = it.second / input_len;
    auto addr = it.first;
    for (uint32_t i = 0; i < item_num; i++) {
      auto item = (addr + i * input_len + TIMESTAMP_LEN);
      auto temp_time = *(uint64_t *)(item - TIMESTAMP_LEN);
      if (idx > output_item_num) {
        break;
      }
      if (temp_time < hash_time_start) {
        continue;
      }
      if (temp_time > hash_time_end) {
        break;
      }
      if (temp_time >= answer[idx * schema_idx.size()].timestamp_) {
        idx++;
      }
      for (uint32_t j = 0; j < schema_idx.size(); j++) {
        double val = 0;
        if(input_schema_->GetColumn(schema_idx[j].value()).GetType()==INTEGER){
          val = *(int64_t *)(item + input_schema_->GetColumn(schema_idx[j].value()).GetOffset());
        }else{
          val = *(double *)(item + input_schema_->GetColumn(schema_idx[j].value()).GetOffset());
        }
        if ((type == MIN && answer[(idx - 1) * schema_idx.size() + j].val_ > val) ||
            (type == MAX && answer[(idx - 1) * schema_idx.size() + j].val_ < val)) {
          answer[(idx - 1) * schema_idx.size() + j].val_ = val;
        }
        if (type == MEAN) {
          answer[(idx - 1) * schema_idx.size() + j].val_ += val;
          answer[(idx - 1) * schema_idx.size() + j].cnt_++;
        }
      }
    }
  }
  op_mtx.lock();
  auto new_addr = operator_buf_ + op_buf_offset_;
  uint32_t off_set = op_buf_offset_;
  op_buf_offset_ += output_item_num * (output_schema_->GetLength() + TIMESTAMP_LEN) + TIMESTAMP_LEN;
  op_mtx.unlock();
  for (uint32_t i = 0; i < output_item_num; i++) {
    memcpy(operator_buf_ + off_set, &answer[i * schema_idx.size()].timestamp_, TIMESTAMP_LEN);
    off_set += TIMESTAMP_LEN;
    for (uint32_t j = 0; j < schema_idx.size(); j++) {
      auto ans = answer[i * schema_idx.size() + j].val_;
      if (type == MEAN) {
        if(answer[i * schema_idx.size() + j].cnt_==0){
          ans = 0;
        }else{
          ans /= answer[i * schema_idx.size() + j].cnt_;
        }

      }
      if(input_schema_->GetColumn(schema_idx[j].value()).GetType()==INTEGER){
        int64_t int_ans = (uint64_t)ans;
        memcpy(operator_buf_ + off_set, &int_ans, sizeof(ans));
      }else{
        memcpy(operator_buf_ + off_set, &ans, sizeof(ans));
      }
      off_set += 8;
    }
  }
  output_.first = new_addr;
  output_.second = output_item_num * (output_schema_->GetLength() + TIMESTAMP_LEN);
  return true;
}

auto Aggregation::GetOutput() -> std::pair<uint8_t *, uint32_t> { return output_; }
}