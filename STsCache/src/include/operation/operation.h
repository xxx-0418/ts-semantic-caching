#ifndef SEMANTIC_CACHE_OPERATION_H
#define SEMANTIC_CACHE_OPERATION_H
#include <string.h>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <future>
#include <iostream>
#include <string>
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
#include "operators/operators.h"

namespace SemanticCache {

struct OpValue{
  bool flag = false;
  std::string key_;
  std::vector<std::string>segs_;
  std::vector<std::pair<uint8_t*,uint32_t>>items_pair_;
  std::vector<TimeRange>time_ranges_;
};

auto SetProcess(SlabManagement &slab_m, SemanticGraph &semantic_graph, const std::string &key,
                const std::string &seg, uint64_t time_start, uint64_t time_end, uint8_t *value,
                uint64_t v_len) -> bool;

auto SetByIndex(SlabManagement &slab_m, SemanticGraph &semantic_graph, const std::string &key,
                uint64_t time_start, uint64_t time_end, uint8_t *value, uint64_t v_len) -> bool;

auto GetByAtomicIndexSingleAndMultiIO(
    SlabManagement &slab_m, std::shared_ptr<SemanticCache::SemanticNode> node_ptr,
    const uint64_t time_start, const uint64_t time_end,
    std::vector<std::variant<std::pair<uint8_t *, uint32_t>,
                             std::future<std::pair<uint8_t *, uint32_t>>>> &future_results,
    std::vector<TimeRange> &per_segment_time_range) -> bool;

auto GetByAtomicIndexSingleIO(SlabManagement &slab_m,
                              std::shared_ptr<SemanticCache::SemanticNode> node_ptr,
                              const uint64_t time_start, const uint64_t time_end,
                              std::vector<uint8_t *> &items, std::vector<uint32_t> &items_len,
                              std::vector<TimeRange> &per_segment_time_range) -> bool;

auto GetByIndex(SlabManagement &slab_m, SemanticGraph &semantic_graph, const std::string &key,
                const uint64_t time_start, const uint64_t time_end, std::vector<uint8_t *> &items,
                std::vector<std::string> &segments, std::vector<uint32_t> &items_len,
                std::vector<uint32_t> &per_segment_items_num,
                std::vector<TimeRange> &per_segment_time_range, bool multi_flag = true) -> bool;

auto GetProcess(SlabManagement &slab_m, SemanticGraph &semantic_graph, const std::string &key,
                const uint64_t time_start, const uint64_t time_end, std::vector<uint8_t *> &items,
                std::vector<std::string> &segments, std::vector<uint32_t> &items_len,
                std::vector<uint32_t> &per_segment_items_num,
                std::vector<TimeRange> &per_segment_time_range, OpValue op_value,bool multi_flag = true) -> bool;

auto Compaction(SlabManagement &slab_m, std::shared_ptr<TimeRangeNode> node_ptr) -> bool;

auto UseOperator(SlabManagement &slab_m,std::shared_ptr<SemanticEdge> semantic_edge,std::string &seg,uint64_t time_start,uint64_t time_end)->std::pair<uint8_t*,uint32_t>;

}
#endif
