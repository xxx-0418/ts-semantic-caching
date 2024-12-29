#ifndef STSCACHE_OPERATORS_H
#define STSCACHE_OPERATORS_H
#include <string.h>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <future>
#include <iostream>
#include <mutex>
#include <string>
#include <tuple>
#include <utility>
#include <variant>
#include <vector>

#include "common/macros.h"
#include "common/type.h"
#include "src/include/storage/semantic_graph.h"
#include "src/include/storage/time_range_skiplist.h"
#include "storage/slab_management/item.h"
#include "storage/slab_management/settings.h"
#include "storage/slab_management/slab.h"
#include "storage/slab_management/slab_management.h"

namespace SemanticCache {
extern uint8_t *operator_buf_;
extern std::atomic<uint32_t> op_buf_offset_;
extern std::mutex op_mtx;

inline auto InitOp() -> void {
  operator_buf_ = static_cast<uint8_t *>(fc_mmap(150 * MB));
  op_buf_offset_ = 0;
}

auto SetOpBufOffset(uint32_t num) -> void;

inline auto FreeOpBuf() -> bool { return fc_munmap(operator_buf_, 60 * MB) == 0; }

class Operator {
 public:
  virtual ~Operator() = default;

  virtual auto Execute() -> void = 0;
  virtual auto GetOutput() -> std::pair<uint8_t *, uint32_t> = 0;
};

struct AggComputer {
  uint64_t timestamp_;
  double val_ = 0;
  uint64_t cnt_ = 0;
  AggComputer(uint64_t timestamp, double val, uint64_t cnt = 0) {
    timestamp_ = timestamp;
    val_ = val;
    cnt_ = cnt;
  }
};

class Projection : public Operator {
 private:
  SchemaRef input_schema_;
  SchemaRef output_schema_;
  PredicatesRef predicates_;
  std::vector<std::pair<uint8_t *, uint32_t>> input_;
  std::pair<uint8_t *, uint32_t> output_;

 public:
  Projection(std::vector<std::pair<uint8_t *, uint32_t>> &input, SchemaRef input_schema,
             SchemaRef output_schema, PredicatesRef predicates);
  ~Projection() override;
  auto Execute() -> void override;
  auto GetOutput() -> std::pair<uint8_t *, uint32_t> override;
};

class Aggregation : public Operator {
 private:
  SchemaRef input_schema_;
  SchemaRef output_schema_;
  Aggregation_Type type_;
  Aggregation_Interval_Type interval_type_;
  uint32_t interval_length_;
  TimeRange time_range_;
  std::vector<std::pair<uint8_t *, uint32_t>> input_;
  std::pair<uint8_t *, uint32_t> output_;

 public:
  Aggregation(std::vector<std::pair<uint8_t *, uint32_t>> &input, TimeRange time_range,
              SchemaRef input_schema, SchemaRef output_schema, Aggregation_Type type,
              Aggregation_Interval_Type interval_type, uint32_t interval_length);
  ~Aggregation() override;
  auto Execute() -> void override;
  auto GetOutput() -> std::pair<uint8_t *, uint32_t> override;

 private:
  auto ExecuteOp(std::vector<AggComputer> &answer, const Aggregation_Type &type) -> bool;
};

}
#endif
