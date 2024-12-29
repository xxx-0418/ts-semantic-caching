#include "include/operation/operation.h"
namespace SemanticCache {
class SlabManagement;
auto settings = Settings();
auto SetProcess(SlabManagement &slab_m, SemanticGraph &semantic_graph, const std::string &key,
                const std::string &seg, uint64_t time_start, uint64_t time_end, uint8_t *value,
                uint64_t v_len) -> bool {
  std::shared_ptr<SemanticNode> node_ptr;
  std::shared_ptr<TimeRangeNode> ptr = nullptr;
  if (semantic_graph.IsExistNode(key)) {
    node_ptr = semantic_graph.FindSemanticNode(key);
  } else {
    node_ptr = semantic_graph.SemanticSegmentToNode(key);
    uint8_t cid = slab_m.GetSlabCid(node_ptr->GetRowLength());
    node_ptr->SetCid(cid);
    semantic_graph.AddNode(node_ptr);
  }
  auto skiplist_ptr = node_ptr->GetTimeRangeSkiplistPtr(seg);
  if (skiplist_ptr == nullptr) {
    auto tmp_skiplist_ptr = std::make_shared<TimeRangeSkipList>();
    node_ptr->SetTimeSkiplistPtr(seg, tmp_skiplist_ptr);
  }
  skiplist_ptr = node_ptr->GetTimeRangeSkiplistPtr(seg);

  if (value == nullptr || v_len == 0) {
    skiplist_ptr->InsertEmptyNode(time_start, time_end);
    return true;
  }
  uint32_t cid = node_ptr->GetCid();
  uint32_t row_len = node_ptr->GetRowLength();

  uint32_t row_num = v_len / (row_len + 8);
  uint32_t remain = row_num;
  uint32_t actual_total_insert_len = 0;
  uint32_t actual_insert_len = 0;

  std::vector<InsertPoint> insert_point_list;
  skiplist_ptr->SearchInsertPoint(time_start, time_end, insert_point_list);
  for (auto it : insert_point_list) {
    if (it.sid_ != SLAB_INVALID_ID) {
      slab_m.AddSemanticValue(it.sid_, settings.slab_size_ * 100);
    }
  }
  uint64_t next_time = *(uint64_t *)(value + actual_total_insert_len);
  std::shared_ptr<TimeRangeNode> time_range_node_ptr = nullptr;
  for (auto &it : insert_point_list) {
    uint32_t minx = 1;
    uint32_t maxn = remain;
    uint32_t start_idx = minx;
    uint32_t end_idx = maxn;
    bool flag1 = false;
    bool flag2 = false;

    while (minx <= maxn) {
      uint32_t mid = (minx + maxn) / 2;
      uint64_t temp_time =
          *(uint64_t *)(value + actual_total_insert_len + (mid - 1) * (row_len + TIMESTAMP_LEN));
      if (temp_time >= it.time_start_) {
        flag1 = true;
        start_idx = mid;
        maxn = mid - 1;
      } else {
        minx = mid + 1;
      }
    }

    minx = 1;
    maxn = remain;

    while (minx <= maxn) {
      uint32_t mid = (minx + maxn) / 2;
      uint64_t temp_time =
          *(uint64_t *)(value + actual_total_insert_len + (mid - 1) * (row_len + TIMESTAMP_LEN));
      if (temp_time < it.time_end_) {
        flag2 = true;
        end_idx = mid;
        minx = mid + 1;
      } else {
        maxn = mid - 1;
      }
    }
    if (start_idx > end_idx || !flag1 || !flag2) {
      skiplist_ptr->InsertEmptyNode(it.time_start_, it.time_end_);
      continue;
    }

    actual_insert_len = 0;
    uint32_t insert_len = (end_idx - start_idx + 1) * (row_len + TIMESTAMP_LEN);
    actual_total_insert_len += (start_idx - 1) * (row_len + TIMESTAMP_LEN);
    remain -= (start_idx - 1);
    if (it.sid_ != SLAB_INVALID_ID) {
      if (!slab_m.ValidSidTimeRange(it.sid_, skiplist_ptr)) {
        continue;
      }
      slab_m.InsertValue(it.sid_, value + actual_total_insert_len, actual_insert_len, insert_len);
      actual_total_insert_len += actual_insert_len;
      remain -= actual_insert_len / (TIMESTAMP_LEN + row_len);
      insert_len -= actual_insert_len;
      if (actual_insert_len != 0) {
        uint64_t last_time =
            *(uint64_t *)(value + actual_total_insert_len - TIMESTAMP_LEN - row_len);
        if (time_range_node_ptr == nullptr) {
          time_range_node_ptr = it.node_ptr_;
        }
        if (insert_len == 0) {
          skiplist_ptr->SetSidInfoEndTime(it.node_ptr_->GetTimeRange().time_start_,
                                          it.node_ptr_->GetTimeRange().time_end_, it.time_end_,
                                          last_time);
        } else {
          time_range_node_ptr->SetEndTime(last_time);
        }
        slab_m.SetSinfoTimeEnd(it.sid_, last_time);
        auto sid_info = it.node_ptr_->GetSidInfo()->back();
      }
    }
    uint32_t sid = 0;
    std::vector<SidInfo> temp_sid_info;
    if (remain != 0) {
      next_time = *(uint64_t *)(value + actual_total_insert_len);
    }
    while (remain != 0 & insert_len != 0 && next_time < it.time_end_ && next_time <= time_end) {
      actual_insert_len = 0;
      rstatus_t status1 = slab_m.CreateSlab(cid, &sid);
      if (status1 == FC_ERROR) {
        return false;
      }
      slab_m.InsertValue(sid, value + actual_total_insert_len, actual_insert_len, insert_len);
      insert_len -= actual_insert_len;
      actual_total_insert_len += actual_insert_len;
      remain -= actual_insert_len / (TIMESTAMP_LEN + row_len);
      uint64_t time_start1 = *(uint64_t *)(value + actual_total_insert_len - actual_insert_len);
      uint64_t time_end1 = *(uint64_t *)(value + actual_total_insert_len - TIMESTAMP_LEN - row_len);
      temp_sid_info.emplace_back(time_start1, time_end1, sid);
      slab_m.SetSinfoTimeRange(sid, time_start1, time_end1);
      slab_m.SetSkiplistPtr(sid, skiplist_ptr);
      slab_m.AddSemanticValue(sid, SLAB_SIZE);
      next_time = *(uint64_t *)(value + actual_total_insert_len);
    }
    if (temp_sid_info.size() != 0) {
      uint64_t time_start = 0;
      uint64_t time_end = 0;
      for (auto it : temp_sid_info) {
        time_start = it.time_start_;
        time_end = it.time_end_;
      }
      ptr = skiplist_ptr->InsertSidInfo1(it.time_start_, it.time_end_, temp_sid_info);
    }
  }
  for (auto it : insert_point_list) {
    if (it.sid_ != SLAB_INVALID_ID) {
      slab_m.ReduceSemanticValue(it.sid_, 100 * settings.slab_size_);
    }
  }
  if (ptr != nullptr && ptr->GetSidInfo()->size() >= 2) {
    Compaction(slab_m, ptr);
  }
  slab_m.DecaySemanticValue();
  return true;
}
auto SetByIndex(SlabManagement &slab_m, SemanticGraph &semantic_graph, const std::string &key,
                uint64_t time_start, uint64_t time_end, uint8_t *value, uint64_t v_len) -> bool {
  std::shared_ptr<SemanticNode> node_ptr;
  std::shared_ptr<TimeRangeNode> ptr = nullptr;
  if (semantic_graph.IsExistNode(key)) {
    node_ptr = semantic_graph.FindSemanticNode(key);
  } else {
    node_ptr = semantic_graph.SemanticSegmentToNode(key);
    uint8_t cid = slab_m.GetSlabCid(node_ptr->GetRowLength());
    node_ptr->SetCid(cid);
    semantic_graph.AddNode(node_ptr);
  }
  uint32_t cid = node_ptr->GetCid();
  uint32_t row_len = node_ptr->GetRowLength();

  std::shared_ptr<TimeRangeSkipList> skipList_ptr = node_ptr->GetTimeRangeSkiplistPtr();
  if (skipList_ptr == nullptr) {
    return false;
  }

  if (value == nullptr || v_len == 0) {
    skipList_ptr->InsertEmptyNode(time_start, time_end);
    return true;
  }

  uint32_t row_num = v_len / (row_len + 8);
  uint32_t remain = row_num;
  uint32_t actual_total_insert_len = 0;
  uint32_t actual_insert_len = 0;

  std::vector<InsertPoint> insert_point_list;
  skipList_ptr->SearchInsertPoint(time_start, time_end, insert_point_list);
  for (auto it : insert_point_list) {
    if (it.sid_ != SLAB_INVALID_ID) {
      slab_m.AddSemanticValue(it.sid_, settings.slab_size_ * 100);
    }
  }
  uint64_t next_time = *(uint64_t *)(value + actual_total_insert_len);
  std::shared_ptr<TimeRangeNode> time_range_node_ptr = nullptr;
  for (auto &it : insert_point_list) {
    uint32_t minx = 1;
    uint32_t maxn = remain;
    uint32_t start_idx = minx;
    uint32_t end_idx = maxn;
    bool flag1 = false;
    bool flag2 = false;

    while (minx <= maxn) {
      uint32_t mid = (minx + maxn) / 2;
      uint64_t temp_time =
          *(uint64_t *)(value + actual_total_insert_len + (mid - 1) * (row_len + TIMESTAMP_LEN));
      if (temp_time >= it.time_start_) {
        flag1 = true;
        start_idx = mid;
        maxn = mid - 1;
      } else {
        minx = mid + 1;
      }
    }

    minx = 1;
    maxn = remain;

    while (minx <= maxn) {
      uint32_t mid = (minx + maxn) / 2;
      uint64_t temp_time =
          *(uint64_t *)(value + actual_total_insert_len + (mid - 1) * (row_len + TIMESTAMP_LEN));
      if (temp_time < it.time_end_) {
        flag2 = true;
        end_idx = mid;
        minx = mid + 1;
      } else {
        maxn = mid - 1;
      }
    }
    if (start_idx > end_idx || !flag1 || !flag2) {
      skipList_ptr->InsertEmptyNode(it.time_start_, it.time_end_);
      continue;
    }

    actual_insert_len = 0;
    uint32_t insert_len = (end_idx - start_idx + 1) * (row_len + TIMESTAMP_LEN);
    actual_total_insert_len += (start_idx - 1) * (row_len + TIMESTAMP_LEN);
    remain -= (start_idx - 1);
    if (it.sid_ != SLAB_INVALID_ID) {
      if (!slab_m.ValidSidTimeRange(it.sid_, skipList_ptr)) {
        continue;
      }
      slab_m.InsertValue(it.sid_, value + actual_total_insert_len, actual_insert_len, insert_len);
      actual_total_insert_len += actual_insert_len;
      remain -= actual_insert_len / (TIMESTAMP_LEN + row_len);
      insert_len -= actual_insert_len;
      if (actual_insert_len != 0) {
        uint64_t last_time =
            *(uint64_t *)(value + actual_total_insert_len - TIMESTAMP_LEN - row_len);
        if (time_range_node_ptr == nullptr) {
          time_range_node_ptr = it.node_ptr_;
        }
        if (insert_len == 0) {
          skipList_ptr->SetSidInfoEndTime(it.node_ptr_->GetTimeRange().time_start_,
                                          it.node_ptr_->GetTimeRange().time_end_, it.time_end_,
                                          last_time);
        } else {
          time_range_node_ptr->SetEndTime(last_time);
        }
        slab_m.SetSinfoTimeEnd(it.sid_, last_time);
        auto sid_info = it.node_ptr_->GetSidInfo()->back();
      }
    }
    uint32_t sid = 0;
    std::vector<SidInfo> temp_sid_info;
    if (remain != 0) {
      next_time = *(uint64_t *)(value + actual_total_insert_len);
    }
    while (remain != 0 & insert_len != 0 && next_time < it.time_end_ && next_time <= time_end) {
      actual_insert_len = 0;
      rstatus_t status1 = slab_m.CreateSlab(cid, &sid);
      if (status1 == FC_ERROR) {
        return false;
      }
      slab_m.InsertValue(sid, value + actual_total_insert_len, actual_insert_len, insert_len);
      insert_len -= actual_insert_len;
      actual_total_insert_len += actual_insert_len;
      remain -= actual_insert_len / (TIMESTAMP_LEN + row_len);
      uint64_t time_start1 = *(uint64_t *)(value + actual_total_insert_len - actual_insert_len);
      uint64_t time_end1 = *(uint64_t *)(value + actual_total_insert_len - TIMESTAMP_LEN - row_len);
      temp_sid_info.emplace_back(time_start1, time_end1, sid);
      slab_m.SetSinfoTimeRange(sid, time_start1, time_end1);
      slab_m.SetSkiplistPtr(sid, skipList_ptr);
      slab_m.AddSemanticValue(sid, SLAB_SIZE);
      next_time = *(uint64_t *)(value + actual_total_insert_len);
    }
    if (temp_sid_info.size() != 0) {
      ptr = skipList_ptr->InsertSidInfo1(it.time_start_, it.time_end_, temp_sid_info);
    }
  }
  for (auto it : insert_point_list) {
    if (it.sid_ != SLAB_INVALID_ID) {
      slab_m.ReduceSemanticValue(it.sid_, 100 * settings.slab_size_);
    }
  }
  if (ptr != nullptr && ptr->GetSidInfo()->size() >= 2) {
    Compaction(slab_m, ptr);
  }
  slab_m.DecaySemanticValue();
  return true;
}

auto Get(SlabManagement &slab_m, uint32_t sid, uint64_t time_start, uint64_t time_end)
    -> std::pair<uint8_t *, uint32_t> {
  uint8_t *it = nullptr;
  uint32_t temp_items_len = 0;
  auto status = slab_m.GetRangeItem(sid, time_start, time_end, &it, &temp_items_len);
  return std::make_pair(it, temp_items_len);
}

auto GetByAtomicIndexSingleAndMultiIO(
    SlabManagement &slab_m, std::shared_ptr<SemanticCache::SemanticNode> node_ptr,
    const uint64_t time_start, const uint64_t time_end,
    std::vector<std::variant<std::pair<uint8_t *, uint32_t>,
                             std::future<std::pair<uint8_t *, uint32_t>>>> &future_results,
    std::vector<TimeRange> &per_segment_time_range) -> bool {
  if (node_ptr == nullptr) {
    return false;
  }
  std::shared_ptr<TimeRangeSkipList> skipList_ptr = node_ptr->GetTimeRangeSkiplistPtr();
  if (skipList_ptr == nullptr) {
    return false;
  }
  auto result = skipList_ptr->SearchHitNode(time_start, time_end);
  if (result.first.size() == 0) {
    per_segment_time_range.emplace_back(time_start, time_end);
    return false;
  }
  per_segment_time_range.emplace_back(result.second);
  for (auto it : result.first) {
    std::vector<SidInfo> sids;
    it->SearchAllSidInfoInNode(time_start, time_end, sids);
    for (auto sid : sids) {
      if (slab_m.SlabMem(sid.sid_)) {
        future_results.emplace_back(Get(slab_m, sid.sid_, time_start, time_end));
      } else {
        future_results.emplace_back(slab_m.pool.enqueue([&slab_m, sid, time_start, time_end] {
          return Get(slab_m, sid.sid_, time_start, time_end);
        }));
      }
    }
  }
  return true;
}

auto GetBySingleAndMultiIO(
    SlabManagement &slab_m, std::shared_ptr<TimeRangeSkipList> skiplist_ptr,
    const uint64_t time_start, const uint64_t time_end,
    std::vector<std::variant<std::pair<uint8_t *, uint32_t>,
                             std::future<std::pair<uint8_t *, uint32_t>>>> &future_results,
    std::vector<TimeRange> &per_segment_time_range) -> bool {
  if (skiplist_ptr == nullptr) {
    return false;
  }
  auto result = skiplist_ptr->SearchHitNode(time_start, time_end);
  if (result.first.size() == 0) {
    per_segment_time_range.emplace_back(time_start, time_end);
    return false;
  }
  per_segment_time_range.emplace_back(result.second);
  for (auto it : result.first) {
    std::vector<SidInfo> sids;
    it->SearchAllSidInfoInNode(time_start, time_end, sids);
    if (sids.size() == 0) {
      future_results.emplace_back(std::make_pair(nullptr, 0));
      continue;
    }
    for (auto sid : sids) {
      if (slab_m.SlabMem(sid.sid_)) {
        future_results.emplace_back(Get(slab_m, sid.sid_, time_start, time_end));
      } else {
        future_results.emplace_back(slab_m.pool.enqueue([&slab_m, sid, time_start, time_end] {
          return Get(slab_m, sid.sid_, time_start, time_end);
        }));
      }
    }
  }
  return true;
}

auto GetBySingleAndMultiIO(
    SlabManagement &slab_m, std::shared_ptr<TimeRangeSkipList> skiplist_ptr,
    const uint64_t time_start, const uint64_t time_end,
    std::vector<std::variant<std::pair<uint8_t *, uint32_t>,
                             std::future<std::pair<uint8_t *, uint32_t>>>> &future_results) -> std::pair<TimeRange,uint32_t > {
  if (skiplist_ptr == nullptr) {
    return std::make_pair(TimeRange(time_start,time_end),future_results.size());
  }
  auto result = skiplist_ptr->SearchHitNode(time_start, time_end);
  if (result.first.size() == 0) {
    return std::make_pair(TimeRange(time_start,time_end),future_results.size());
  }
  auto idx = future_results.size();
  bool flag = false;
  for (auto it : result.first) {
    std::vector<SidInfo> sids;
    it->SearchAllSidInfoInNode(time_start, time_end, sids);
    if (sids.size() == 0) {
      future_results.emplace_back(std::make_pair(nullptr, 0));
      continue;
    }
    for (auto sid : sids) {
      if (slab_m.SlabMem(sid.sid_)) {
        future_results.emplace_back(Get(slab_m, sid.sid_, time_start, time_end));
      } else {
        future_results.emplace_back(slab_m.pool.enqueue([&slab_m, sid, time_start, time_end] {
          return Get(slab_m, sid.sid_, time_start, time_end);
        }));
      }
    }
    if(!flag&&result.first.size()==2){
      idx = future_results.size();
      flag = true;
    }
  }
  if(result.second.time_start_==time_start){
    return std::make_pair(result.second,idx);
  }else if(result.second.time_end_==time_end){
    return std::make_pair(result.second,future_results.size());
  }else{
    return std::make_pair(result.second,idx);
  }
}


auto GetByAtomicIndexSingleIO(SlabManagement &slab_m,
                              std::shared_ptr<SemanticCache::SemanticNode> node_ptr,
                              const uint64_t time_start, const uint64_t time_end,
                              std::vector<uint8_t *> &items, std::vector<uint32_t> &items_len,
                              std::vector<TimeRange> &per_segment_time_range) -> bool {
  if (node_ptr == nullptr) {
    return false;
  }
  std::shared_ptr<TimeRangeSkipList> skipList_ptr = node_ptr->GetTimeRangeSkiplistPtr();
  if (skipList_ptr == nullptr) {
    return false;
  }
  auto node_list = skipList_ptr->SearchAllNode(time_start, time_end);
  if (node_list.size() == 0) {
    return false;
  }
  for (auto it : node_list) {
    rstatus_t status;
    std::vector<SidInfo> sids;
    it->SearchAllSidInfoInNode(time_start, time_end, sids);
    for (auto sid : sids) {
      uint8_t *it = nullptr;
      uint32_t temp_items_len;
      status = slab_m.GetRangeItem(sid.sid_, time_start, time_end, &it, &temp_items_len);
      if (status != FC_OK) {
        continue;
      }
      items.emplace_back(it);
      items_len.emplace_back(temp_items_len);
    }
  }
  return true;
}

auto GetByIndex(SlabManagement &slab_m, SemanticGraph &semantic_graph, const std::string &key,
                const uint64_t time_start, const uint64_t time_end, std::vector<uint8_t *> &items,
                std::vector<std::string> &segments, std::vector<uint32_t> &items_len,
                std::vector<uint32_t> &per_segment_items_num,
                std::vector<TimeRange> &per_segment_time_range, bool multi_flag) -> bool {
  std::shared_ptr<SemanticNode> node_ptr;
  if (semantic_graph.IsExistNode(key)) {
    node_ptr = semantic_graph.FindSemanticNode(key);
  } else {
    node_ptr = semantic_graph.SemanticSegmentToNode(key);
    uint8_t cid = slab_m.GetSlabCid(node_ptr->GetRowLength());
    node_ptr->SetCid(cid);
    semantic_graph.AddNode(node_ptr);
    auto new_keys = semantic_graph.SplitSegment(key);
    for (auto &seg : new_keys->first) {
      std::shared_ptr<SemanticNode> sub_node_ptr;
      if (!semantic_graph.IsExistNode(seg)) {
        sub_node_ptr = semantic_graph.SemanticSegmentToNode(seg);
        uint8_t cid = slab_m.GetSlabCid(sub_node_ptr->GetRowLength());
        sub_node_ptr->SetCid(cid);
        semantic_graph.AddNode(sub_node_ptr);
      } else {
        sub_node_ptr = semantic_graph.FindSemanticNode(seg);
      }
      SemanticCache::EdgeWeight edge_weight(SemanticCache::EDGE_WEIGHT_TYPE::Atomic_To_Non_Atomic,
                                            SemanticCache::EDGE_COMPUTE_TYPE::Not_Compute,
                                            SemanticCache::EDGE_INTERSECTION_TYPE::My_Subset,
                                            nullptr);
      SemanticCache::SemanticEdge semantic_edge(sub_node_ptr, node_ptr, edge_weight);
      semantic_graph.AddEdge(std::make_shared<SemanticCache::SemanticEdge>(semantic_edge));
    }
  }
  std::vector<
      std::variant<std::pair<uint8_t *, uint32_t>, std::future<std::pair<uint8_t *, uint32_t>>>>
      future_results;
  if (true) {
    if (multi_flag) {
      GetByAtomicIndexSingleAndMultiIO(slab_m, node_ptr, time_start, time_end, future_results,
                                       per_segment_time_range);
      per_segment_items_num.emplace_back(future_results.size());
    } else {
      GetByAtomicIndexSingleIO(slab_m, node_ptr, time_start, time_end, items, items_len,
                               per_segment_time_range);
      per_segment_items_num.emplace_back(items.size());
    }
    segments.emplace_back(key);
  } else {
    auto in_edges = node_ptr->GetInEdges();
    for (auto it : in_edges) {
      if (it->edge_weight_.edge_compute_type_ != Not_Compute &&
          it->edge_weight_.edge_intersection_type_ != My_Subset &&
          it->edge_weight_.edge_weight_type_ != Atomic_To_Non_Atomic) {
        continue;
      }
      if (multi_flag) {
        uint32_t now_total_num = future_results.size();
        GetByAtomicIndexSingleAndMultiIO(slab_m, it->GetSourceNode(), time_start, time_end,
                                         future_results, per_segment_time_range);
        uint32_t add_num = future_results.size() - now_total_num;
        per_segment_items_num.emplace_back(add_num);
      } else {
        uint32_t now_total_num = items.size();
        GetByAtomicIndexSingleIO(slab_m, it->GetSourceNode(), time_start, time_end, items,
                                 items_len, per_segment_time_range);
        uint32_t add_num = items.size() - now_total_num;
        per_segment_items_num.emplace_back(add_num);
      }
      segments.emplace_back(it->GetSourceNodeHashCode());
    }
  }
  if (multi_flag) {
    for (auto &it : future_results) {
      if (std::holds_alternative<std::pair<uint8_t *, uint32_t>>(it)) {
        auto ans = std::get<std::pair<uint8_t *, uint32_t>>(it);
        items.emplace_back(ans.first);
        items_len.emplace_back(ans.second);
      } else {
        auto ans = std::get<std::future<std::pair<uint8_t *, uint32_t>>>(it).get();
        items.emplace_back(ans.first);
        items_len.emplace_back(ans.second);
      }
    }
  }
  slab_m.SetReadBufOffset(0);
  slab_m.DecaySemanticValue();
  if (items.size() == 0) {
    return false;
  }
  return true;
}

auto GetProcess(SlabManagement &slab_m, SemanticGraph &semantic_graph, const std::string &key,
                const uint64_t time_start, const uint64_t time_end, std::vector<uint8_t *> &items,
                std::vector<std::string> &segments, std::vector<uint32_t> &items_len,
                std::vector<uint32_t> &per_segment_items_num,
                std::vector<TimeRange> &per_segment_time_range, OpValue op_value,bool multi_flag) -> bool {
  auto new_keys = semantic_graph.SplitSegment(key);
  auto key1 = new_keys->second;
  op_value.key_ = key1;
  auto node_ptr = semantic_graph.FindSemanticNode(key1);
  if (node_ptr == nullptr) {
    node_ptr = semantic_graph.SemanticSegmentToNode(key1);
    uint8_t cid = slab_m.GetSlabCid(node_ptr->GetRowLength());
    node_ptr->SetCid(cid);
    semantic_graph.AddNode(node_ptr);
  }
  std::vector<
      std::variant<std::pair<uint8_t *, uint32_t>, std::future<std::pair<uint8_t *, uint32_t>>>>
      future_results;
  for (auto &seg : new_keys->first) {
    if (multi_flag) {
      uint32_t now_total_num = future_results.size();
      auto skiplist_ptr = node_ptr->GetTimeRangeSkiplistPtr(seg);

      auto remain_info = GetBySingleAndMultiIO(slab_m, skiplist_ptr, time_start, time_end, future_results);

      uint32_t add_num = future_results.size() - now_total_num;
      per_segment_items_num.emplace_back(add_num);
      bool is_op = false;
      for(auto edge:node_ptr->GetInEdges()){
        if(edge->edge_weight_.edge_intersection_type_==EDGE_INTERSECTION_TYPE::Totally_Contained||edge->edge_weight_.edge_intersection_type_==EDGE_INTERSECTION_TYPE::Same){
          auto item_pair = UseOperator(slab_m,edge,seg,remain_info.first.time_start_,remain_info.first.time_end_);
          if(item_pair.first!= nullptr){
            future_results.insert(future_results.begin()+remain_info.second,item_pair);
            per_segment_items_num[per_segment_items_num.size()-1]++;
            is_op = true;
            op_value.flag = true;
            op_value.segs_.emplace_back(seg);
            op_value.items_pair_.emplace_back(item_pair);
            op_value.time_ranges_.emplace_back(remain_info.first);
            break;
          }
        }
      }
      if(!is_op){
        per_segment_time_range.emplace_back(remain_info.first);
      }else{
        per_segment_time_range.emplace_back(0,0);
      }
    } else {
    }
    segments.emplace_back(seg);
  }
  SetOpBufOffset(0);
  if (multi_flag) {
    for (auto &it : future_results) {
      if (std::holds_alternative<std::pair<uint8_t *, uint32_t>>(it)) {
        auto ans = std::get<std::pair<uint8_t *, uint32_t>>(it);
        items.emplace_back(ans.first);
        items_len.emplace_back(ans.second);
      } else {
        auto ans = std::get<std::future<std::pair<uint8_t *, uint32_t>>>(it).get();
        items.emplace_back(ans.first);
        items_len.emplace_back(ans.second);
      }
    }
  }
  slab_m.SetReadBufOffset(0);
  slab_m.DecaySemanticValue();
  if (items.size() == 0) {
    return false;
  }
  return true;
}

auto Compaction(SlabManagement &slab_m, std::shared_ptr<TimeRangeNode> node_ptr) -> bool {
  if (node_ptr == nullptr) {
    return false;
  }
  auto idx = slab_m.IsWorthCompaction(node_ptr->GetSidInfo());
  if (idx.has_value() == false) {
    return false;
  }
  auto new_sids = slab_m.Compaction(node_ptr->GetSidInfo(), idx.value().first, idx.value().second);
  auto time_end = node_ptr->GetTimeRange().time_end_;
  auto old_sids = node_ptr->GetSidInfo();
  uint32_t delete_sid_num = old_sids->size() - new_sids->size();
  for (uint32_t i = 0; i < delete_sid_num; i++) {
    auto sid = old_sids->back().sid_;
    slab_m.DeleteSlab(sid);
  }
  node_ptr->SetSidInfo(new_sids);
  node_ptr->SetNodeEndTime(time_end);
  return true;
}

auto GetSm(std::string &input)->std::string{
  size_t start_pos = input.find('{');
  size_t end_pos = input.find('}');
  if (start_pos < end_pos) {
    std::string sm = input.substr(start_pos + 1, end_pos - start_pos - 1);
    return sm;
  }
  return "";
}


auto GetBySingleAndMultiIOForOP(
    SlabManagement &slab_m, std::shared_ptr<TimeRangeSkipList> skiplist_ptr,
    const uint64_t time_start, const uint64_t time_end,
    std::vector<std::variant<std::pair<uint8_t *, uint32_t>,
                             std::future<std::pair<uint8_t *, uint32_t>>>> &future_results) -> bool {
  if (skiplist_ptr == nullptr) {
    return false;
  }
  auto result = skiplist_ptr->SearchAllHitNode(time_start, time_end);
  if (result == nullptr) {
    return false;
  }
  std::vector<SidInfo> sids;
  result->SearchAllSidInfoInNode(time_start, time_end, sids);
  if (sids.size() == 0) {
    return false;
  }
  for (auto sid : sids) {
    if (slab_m.SlabMem(sid.sid_)) {
      future_results.emplace_back(Get(slab_m, sid.sid_, time_start, time_end));
    } else {
      future_results.emplace_back(slab_m.pool.enqueue([&slab_m, sid, time_start, time_end] {
        return Get(slab_m, sid.sid_, time_start, time_end);
      }));
    }
  }
  return true;
}

auto UseOperator(SlabManagement &slab_m,std::shared_ptr<SemanticEdge> semantic_edge,std::string &seg,uint64_t time_start,uint64_t time_end)->std::pair<uint8_t*,uint32_t>{
  auto source_node = semantic_edge->GetSourceNode();
  auto destination_node = semantic_edge->GetDestinationNode();
  auto sm = GetSm(seg);
  auto time_range_skiplist = source_node->GetTimeRangeSkiplistPtrBySm(sm);
  if(time_range_skiplist== nullptr){
    return std::make_pair(nullptr,0);
  }
  std::vector<
      std::variant<std::pair<uint8_t *, uint32_t>, std::future<std::pair<uint8_t *, uint32_t>>>>
      future_results;
  auto flag = GetBySingleAndMultiIOForOP(slab_m, time_range_skiplist, time_start, time_end, future_results);
  if(!flag){
    return std::make_pair(nullptr,0);
  }
  std::vector<std::pair<uint8_t*,uint32_t>>input;
  for (auto &it : future_results) {
    if (std::holds_alternative<std::pair<uint8_t *, uint32_t>>(it)) {
      auto ans = std::get<std::pair<uint8_t *, uint32_t>>(it);
      if(ans.first == nullptr){
        continue;
      }
      input.emplace_back(ans.first,ans.second);
    } else {
      auto ans = std::get<std::future<std::pair<uint8_t *, uint32_t>>>(it).get();
      if(ans.first== nullptr){
        continue;
      }
      input.emplace_back(ans.first,ans.second);
    }
  }
  if(semantic_edge->edge_weight_.edge_compute_type_==Not_Compute){
    Projection pro_op(input,source_node->GetMetadataRef(),destination_node->GetMetadataRef(), destination_node->GetPrediactes());
    pro_op.Execute();
    return pro_op.GetOutput();
  }else if(semantic_edge->edge_weight_.edge_compute_type_==Aggregation_Compute){
    Aggregation agg_op(input, SemanticCache::TimeRange(time_start, time_end), source_node->GetMetadataRef(),
                      destination_node->GetMetadataRef(),destination_node->GetAggregationType(), destination_node->GetAggregationIntervalType(), destination_node->GetAggreationIntervalLength());
    agg_op.Execute();
    return agg_op.GetOutput();
  }
  return std::make_pair(nullptr,0);
}

}
