#ifndef STSCACHE_SLAB_MANAGEMENT_H
#define STSCACHE_SLAB_MANAGEMENT_H
#include <assert.h>
#include <bits/types.h>
#include <fcntl.h>
#include <linux/fs.h>
#include <sys/cdefs.h>
#include <optional>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cinttypes>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <atomic>
#include <mutex>
#include <list>
#include <memory>
#include <queue>

#include "common/log.h"
#include "common/macros.h"
#include "common/util.h"
#include "storage/slab_management/item.h"
#include "storage/slab_management/settings.h"
#include "storage/slab_management/slab.h"
#include "storage/time_range_skiplist.h"
#include "thread_pool/ThreadPool.h"

namespace SemanticCache {

struct SemanticValue{
    uint32_t sid_;
    uint64_t time_;
    uint32_t value_;
    SemanticValue(uint32_t sid,uint64_t time,uint32_t value){
        sid_ = sid;
        time_ = time;
        value_ = value;
    }
    bool operator<(const SemanticValue& other) const{
        if(value_ == other.value_){
            if(time_ == other.time_){
                return sid_ > other.sid_;
            }
            return time_ > other.time_;
        }
        return value_ > other.value_;
    }
};

class SlabManagement {
 public:
    SlabManagement();
  explicit SlabManagement(const Settings &settings);
  auto SetSlabManagement(const Settings &settings) -> void;
  ~SlabManagement();

  auto GetSlabCid(size_t size) -> uint8_t;

  inline auto ValidCid(uint8_t cid) const -> bool { return cid >= SLABCLASS_MIN_ID && cid < nctable_; }

  inline auto ValidSid(uint32_t sid) const -> bool { return sid < nstable_; }

  inline auto ValidSidTimeRange(uint32_t sid,std::shared_ptr<TimeRangeSkipList> ptr){
    auto sinfo = &stable_[sid];
    return ValidCid(sinfo->cid_)&& skiplists_[sid] == ptr;
  }

  static inline auto GetItemData(struct Slabitem *it) -> uint8_t * { return it->end_; }

  inline auto GetSkiplistPtr(uint32_t sid)-> std::shared_ptr<TimeRangeSkipList>&{
      return skiplists_[sid];
  }
  inline auto SetSkiplistPtr(uint32_t sid,std::shared_ptr<TimeRangeSkipList>& skiplist_ptr)->void{
      skiplists_[sid] = skiplist_ptr;
  }

  auto CreateSlab(const uint32_t &cid, uint32_t *sid, const uint32_t &evict_sid = SLAB_INVALID_SID)
      -> rstatus_t;

  auto InsertKV(const uint32_t &sid, const uint64_t &timestamp, const uint8_t *v,
                                  const uint32_t &v_len) -> rstatus_t;

  auto InsertMultiItem(uint32_t sid, const uint8_t *value, uint32_t row_len, const uint32_t row_num,
                         uint32_t &remain,uint64_t time_start_bound=0,uint64_t time_end_bound=0) -> rstatus_t;

  inline auto GetInsertValueLen(uint32_t sid,uint32_t cid,uint32_t insert_len) -> uint32_t {
      auto sinfo = &stable_[sid];
      auto c = &ctable_[sinfo->cid_];
      uint32_t slab_remain_len = (c->nitem_-sinfo->nalloc_)*(c->size_);
      if(slab_remain_len<insert_len){
          insert_len = slab_remain_len;
      }
      return insert_len;
  }

  auto InsertValue(uint32_t sid,uint8_t* value,uint32_t &actual_insert_len,uint32_t insert_len) -> rstatus_t;

  inline auto SetSinfoTimeRange(uint32_t sid,uint64_t time_start,uint64_t time_end) -> void{
      auto sinfo = &stable_[sid];
      sinfo->time_start_ = time_start;
      sinfo->time_end_ = time_end;
  }

    inline auto SetSinfoTimeEnd(uint32_t sid,uint64_t time_end) -> void{
        auto sinfo = &stable_[sid];
        sinfo->time_end_ = time_end;
    }



    auto GetRangeItem(const uint32_t &sid, const uint64_t &time_start, const uint64_t &time_end,uint8_t ** items, uint32_t *items_len) -> rstatus_t;

  auto DeleteSlab(const uint32_t &sid) -> rstatus_t;

  auto MoveSlabInMem(const uint32_t &sid, const uint32_t &evict_sid = SLAB_INVALID_SID)
      -> rstatus_t;

  auto SlabDrain(const uint32_t &sid) -> rstatus_t;

  inline auto SlabMem(const uint32_t &sid) const -> bool {
    return stable_[sid].mem_ == 1;
  }

  inline auto GetSlabDataSize() const -> size_t { return settings_.slab_size_ - SLAB_HDR_SIZE; }

  inline auto GetSlabItemSizeBySid(const uint32_t &sid) const -> size_t {
    return GetSlabItemSizeByCid(stable_[sid].cid_);
  }

  inline auto GetSlabItemSizeByCid(const uint8_t &cid) const -> size_t {
    return ctable_[cid].size_;
  }

  inline auto GetSlabFreeItemNumBySid(const uint32_t &sid) const -> uint32_t {
    auto sinfo = &stable_[sid];
    return ctable_[sinfo->cid_].nitem_ - sinfo->nalloc_;
  }

  inline auto GetSlabClassItemNumByCid(const uint32_t &cid) const -> uint32_t {
    return ctable_[cid].nitem_;
  }

  inline auto GetMaxValueLengthBySid(const uint32_t &sid) const -> uint32_t {
    return ctable_[stable_[sid].cid_].size_ - ITEM_HDR_SIZE;
  }

  inline auto GetMaxValueLengthByCid(const uint32_t &cid) const -> uint32_t {
    return ctable_[cid].size_ - ITEM_HDR_SIZE;
  }

  inline auto GetSlabClassByCid(uint32_t cid) -> Slabclass * {
    return ValidCid(cid) ? ctable_ + cid : nullptr;
  }

  inline auto GetMinValueLength() const -> size_t { return settings_.chunk_size_; }

  inline auto GetMemUsedSlabNum() const -> uint32_t { return nmslab_ - nfree_msinfoq_; }

  inline auto GetDiskUsedSlabNum() const -> uint32_t { return ndslab_ - nfree_dsinfoq_; }

  inline auto GetMemSlabTotalNum() const -> uint32_t { return nmslab_; }

  inline auto GetDiskSlaTotalbNum() const -> uint32_t { return ndslab_; }

    inline auto SetReadBufOffset(uint32_t size)->void{
        readbuf_offset_ = size;
    }
    inline auto DecaySemanticValue()->void{
        record_num_++;
        if(record_num_ >= (ndslab_+nmslab_)/10){
          value_factor_++;
        }
    }
    inline auto AddValueFactor(uint64_t num=1)->void{
      value_factor_+=num;
    }
    inline auto SubValueFactor(uint64_t num=1)->void{
      value_factor_-=num;
    }
    inline auto AddSemanticValue(uint32_t sid,uint32_t value)->void{
      semantic_value_[sid] += value<<(value_factor_-1);
    }

    inline auto ReduceSemanticValue(uint32_t sid,uint32_t value)->void{
      if(semantic_value_[sid]>=value<<(value_factor_-1)){
        return;
      }
      semantic_value_[sid] -= value<<(value_factor_-1);
    }
    auto GetSlabInfo(uint32_t sid)-> Slabinfo&{
      return stable_[sid];
    }

    auto IsWorthCompaction(std::shared_ptr<std::vector<SidInfo>> sids)-> std::optional<std::pair<uint32_t ,uint32_t >>;

    auto Compaction(std::shared_ptr<std::vector<SidInfo>> sids,uint32_t idx,uint32_t c_len)-> std::shared_ptr<std::vector<SidInfo>> ;
    auto InsertOldValue(uint32_t sid, uint8_t *value,uint32_t &actual_insert_len,
                        uint32_t insert_len,std::map<uint32_t,uint8_t*>&slab_addr) -> rstatus_t;

    private:

  void GetItemBoundAddr(const struct Slabinfo *sinfo, const uint8_t *k1, const uint8_t *k2,
                        uint32_t *addr1, uint32_t *addr2) const;

  inline auto ItemKey(struct Slabitem *item) const -> uint8_t * { return item->end_; }

  inline auto CompareKey(const size_t &k1, const size_t &k2) const -> int32_t {
    return k1 == k2 ? 0 : k1 > k2 ? 1 : -1;
  }

  auto GetItem(const struct Slabinfo *sinfo, const uint32_t &addr, const uint32_t &nbitem)
      -> uint8_t *;

  auto SlabFull(const struct Slabinfo *sinfo) const -> bool;

  auto SlabGetItem(const uint32_t &sid) -> struct Slabitem *;

  auto SlabFromMaddr(const uint32_t &addr, const bool &verify) const -> struct Slab *;

  auto SlabToItem(const struct Slab *slab, const uint32_t &idx, const size_t &size) const
      -> struct Slabitem *;

  auto SlabToDaddr(const struct Slabinfo *sinfo) const -> off_t;

  void SlabSwapAddr(struct Slabinfo *msinfo, struct Slabinfo *dsinfo);

  auto BatchWriteToSSD(std::vector<uint32_t>&batch_write_sids) -> rstatus_t;

  auto WriteToSSD(uint32_t msid,uint32_t dsid)->rstatus_t;

  auto BatchEvict(uint32_t batch_size)->rstatus_t;


  auto InitSlabManagement() -> rstatus_t;

  auto InitCtable() -> rstatus_t;

  auto InitStable() -> rstatus_t;

  void DeinitSlabManagement();
  void DeinitCtable();

  void DeinitStable();

  auto FcDeviceSize(const char *path, size_t *size) -> rstatus_t;

  Settings settings_;

  std::vector<std::shared_ptr<TimeRangeSkipList>> skiplists_;
  std::vector<uint32_t>semantic_value_;

  uint32_t nfree_msinfoq_;
  struct Slabhinfo free_msinfoq_;

  uint32_t nused_msinfoq_;
  struct Slabhinfo used_msinfoq_;

  uint32_t nfree_dsinfoq_;
  struct Slabhinfo free_dsinfoq_;

  uint32_t nused_dsinfoq_;
  struct Slabhinfo used_dsinfoq_;

  uint8_t nctable_;
  struct Slabclass *ctable_;

  uint32_t nstable_;
  struct Slabinfo *stable_;

  uint8_t *mstart_;
  uint8_t *mend_;

  off_t dstart_;
  off_t dend_;
  int fd_;

  size_t mspace_;
  size_t dspace_;
  uint32_t nmslab_;
  uint32_t ndslab_;

  uint8_t *readbuf_;
  std::list<uint32_t>evict_;
  uint32_t record_num_{0};
  uint32_t value_factor_{1};
  uint32_t batch_size_;
  std::atomic<uint32_t> readbuf_offset_{0};
  uint8_t *compress_buf_;
  uint32_t compress_buf_offset_{0};
  std::mutex mtx;
    public:
  ThreadPool pool;
};
}
#endif
