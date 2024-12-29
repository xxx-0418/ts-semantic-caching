#include "storage/slab_management/slab_management.h"
#include <cstdint>
#include <string>
#include <iostream>
#include "common/macros.h"

namespace SemanticCache {
    SlabManagement::SlabManagement() :skiplists_(),semantic_value_(), pool(16){}
    SlabManagement::SlabManagement(const Settings &settings):skiplists_(),semantic_value_(),pool(16) {
        settings_ = settings;
        ST_ASSERT(settings.ssd_device_ != nullptr,"ssd device can't be empty!");
        InitSlabManagement();
        semantic_value_.resize(ndslab_+nmslab_,0);
        skiplists_.resize(ndslab_+nmslab_, nullptr);
    }

    auto SlabManagement::SetSlabManagement(const SemanticCache::Settings &settings) -> void {
        settings_ = settings;
        ST_ASSERT(settings.ssd_device_ != nullptr,"ssd device can't be empty!");
        InitSlabManagement();
        semantic_value_.resize(ndslab_+nmslab_,0);
        skiplists_.resize(ndslab_+nmslab_, nullptr);
    }
    SlabManagement::~SlabManagement() { DeinitSlabManagement(); }

    auto SlabManagement::GetSlabCid(size_t size) -> uint8_t {
        uint8_t cid, imin, imax;

        ST_ASSERT(size != 0, "SetSlabCid failed: size can't be 0");

        size += ITEM_HDR_SIZE;

        imin = SLABCLASS_MIN_ID;
        imax = nctable_;
        while (imax >= imin) {
            cid = (imin + imax) / 2;
            if (size > ctable_[cid].size_) {
                imin = cid + 1;
            } else if (cid > SLABCLASS_MIN_ID && size <= ctable_[cid - 1].size_) {
                imax = cid - 1;
            } else {
                break;
            }
        }

        if (imin > imax) {
            return SLABCLASS_INVALID_ID;
        }
        ST_ASSERT(size == ctable_[cid].size_,"size must be equal");
        return cid;
    }

    auto SlabManagement::CreateSlab(const uint32_t &cid, uint32_t *sid, const uint32_t &evict_sid)
    -> rstatus_t {
        if(!ValidCid(cid)){
          return FC_ERROR;
        }
        if (TAILQ_EMPTY(&free_msinfoq_)) {
            if(evict_.size()>0){
                DeleteSlab(evict_.front());
                evict_.pop_front();
            }else{
                auto status = BatchEvict(batch_size_);
                if (status != FC_OK) {
                    return FC_ERROR;
                }
                if(TAILQ_EMPTY(&free_msinfoq_)){
                  DeleteSlab(evict_.front());
                  evict_.pop_front();
                }
            }
        }
        struct Slabinfo *sinfo = TAILQ_FIRST(&free_msinfoq_);
        TAILQ_REMOVE(&free_msinfoq_, sinfo, tqe_);
        nfree_msinfoq_--;

        nused_msinfoq_++;
        TAILQ_INSERT_TAIL(&used_msinfoq_, sinfo, tqe_);
        sinfo->free_ = 0;
        sinfo->nalloc_ = 0;
        sinfo->cid_ = cid;
        sinfo->nbound_ = 0;

        auto slab = SlabFromMaddr(sinfo->addr_, false);
        slab->cid_ = cid;
        slab->sid_ = sinfo->sid_;
        *sid = sinfo->sid_;
        return FC_OK;
    }

    auto SlabManagement::InsertKV(const uint32_t &sid, const uint64_t &timestamp, const uint8_t *v,
                                  const uint32_t &v_len) -> rstatus_t {
        if(!ValidSid(sid)){
          return FC_ERROR;
        }
        auto sinfo = &stable_[sid];

        if (SlabFull(sinfo)) {
            return FC_ERROR;
        }

        if (!SlabMem(sid)) {
            return FC_ERROR;
        }

        if (v_len > GetMaxValueLengthBySid(sid) || v_len <= 0) {
            return FC_ERROR;
        }

        auto c = &ctable_[sinfo->cid_];
        auto item = SlabGetItem(sid);
        if (item == nullptr) {
            return FC_ERROR;
        }
        item->timestamp_ = timestamp;
        memcpy(ItemKey(item), v, v_len);

        uint32_t addr = 0;
        if(sinfo->bound_ != nullptr&&sinfo->nbound_!=0){
            addr = (sinfo->bound_ + sinfo->nbound_ -1)->addr_;
        }
        if ((sinfo->nalloc_ - addr) * c->size_ > ITEM_BOUND_SIZE) {
            if(sinfo->bound_ == nullptr){
                uint32_t size = sizeof(struct Itembound) * settings_.slab_size_ / ITEM_BOUND_SIZE;
                sinfo->bound_ =  static_cast<struct Itembound *>(fc_alloc(size+1));
            }
            sinfo->bound_[sinfo->nbound_].addr_ = sinfo->nalloc_ ;
            sinfo->bound_[sinfo->nbound_].timestamp_ = item->timestamp_;
            sinfo->nbound_++;
        }
        semantic_value_[sid] += (c->size_<<(value_factor_-1));
        return FC_OK;
    }


    auto SlabManagement::InsertMultiItem(uint32_t sid, const uint8_t *value, uint32_t row_len,
                                         const uint32_t row_num, uint32_t &remain,uint64_t time_start_bound,uint64_t time_end_bound) -> rstatus_t {

        auto sinfo = &stable_[sid];

        if (!SlabMem(sid)) {
            return FC_ERROR;
        }

        if (SlabFull(sinfo)) {
            return FC_ERROR;
        }

        if (row_num == 0) {
            return FC_OK;
        }

        auto c = &ctable_[sinfo->cid_];

        uint32_t idx = 0;
        struct Slabitem *item;
        uint32_t addr;

        auto timestamp_len = sizeof(item->timestamp_);
        if(time_start_bound!=0){
            while(idx < row_num){
                uint64_t time_stamp = *(uint64_t*)(value+idx * (timestamp_len + row_len));
                if(time_stamp>time_start_bound){
                    break;
                }
                idx++;
            }
        }
        auto now_idx = idx;
        while (sinfo->nalloc_<c->nitem_ && idx < row_num) {
            uint64_t time_stamp = *(uint64_t*)(value+idx * (timestamp_len + row_len));
            if(time_end_bound!=0&&time_stamp>=time_end_bound)break;
            item = SlabGetItem(sid);
            if (item == nullptr) {
                break;
            }

            item->timestamp_ = time_stamp;

            memcpy(ItemKey(item), value + idx * (timestamp_len + row_len) + sizeof(item->timestamp_),
                   row_len);
            addr = 0;
            if(sinfo->bound_ != nullptr && sinfo->nbound_ != 0){
                addr = (sinfo->bound_ + sinfo->nbound_ -1)->addr_;
            }
            if ((sinfo->nalloc_ - addr) * c->size_ > ITEM_BOUND_SIZE) {
                if(sinfo->bound_ == nullptr){
                    uint32_t size = sizeof(struct Itembound) * settings_.slab_size_ / ITEM_BOUND_SIZE;
                    sinfo->bound_ =  static_cast<struct Itembound *>(fc_alloc(size+1));
                }
                sinfo->bound_[sinfo->nbound_].addr_ = sinfo->nalloc_ ;
                sinfo->bound_[sinfo->nbound_].timestamp_ = item->timestamp_;
                sinfo->nbound_ ++;
            }
            idx++;
        }
        remain = row_num - idx;
        semantic_value_[sid] += (idx - now_idx)*c->size_<<(value_factor_-1);
        return remain == 0 ? FC_OK : FC_ERROR;
    }


    auto SlabManagement::InsertValue(uint32_t sid, uint8_t *value,uint32_t &actual_insert_len,
                                     uint32_t insert_len) -> rstatus_t {
        if(!ValidSid(sid)){
          return FC_ERROR;
        }

        auto sinfo = &stable_[sid];

        if (!SlabMem(sid)) {
            return FC_ERROR;
        }
        if (SlabFull(sinfo)) {
            return FC_ERROR;
        }

        if (value == nullptr) {
            return FC_OK;
        }

        auto c = &ctable_[sinfo->cid_];

        uint32_t addr;

        uint32_t slab_remain_len = (c->nitem_-sinfo->nalloc_)*(c->size_);
        if(slab_remain_len<insert_len){
            insert_len = slab_remain_len;
        }

        auto it = (uint8_t*)SlabGetItem(sid);
        memcpy(it,value,insert_len);

        addr = 0;
        if(sinfo->bound_ != nullptr && sinfo->nbound_ != 0){
            addr = (sinfo->bound_ + sinfo->nbound_ -1)->addr_;
        }
        auto remanin_offset = (sinfo->nalloc_-addr-1)*c->size_;
        uint32_t add_bound_num = (insert_len+remanin_offset)/(ITEM_BOUND_SIZE);
        for(uint32_t i=1;i<=add_bound_num;i++){
            if(sinfo->bound_== nullptr){
                uint32_t size = sizeof(struct Itembound) * settings_.slab_size_ / ITEM_BOUND_SIZE;
                sinfo->bound_ =  static_cast<struct Itembound *>(fc_alloc(size+1));
            }
            sinfo->nbound_++;
            uint32_t offset_item_num = (i*ITEM_BOUND_SIZE)/(c->size_);
            (sinfo->bound_ + sinfo->nbound_ - 1)->addr_ = addr + offset_item_num;
            auto slab = SlabFromMaddr(sinfo->addr_, false);
            (sinfo->bound_ + sinfo->nbound_ - 1)->timestamp_ = SlabToItem(slab,addr+offset_item_num-1,c->size_)->timestamp_;

        }
        sinfo->nalloc_ += insert_len/(c->size_)-1;
        actual_insert_len = insert_len;
        semantic_value_[sid] += actual_insert_len<<(value_factor_-1);
        return FC_OK;
    }


    auto SlabManagement::GetRangeItem(const uint32_t &sid, const uint64_t &time_start, const uint64_t &time_end,
                                      uint8_t ** items, uint32_t *items_len) -> rstatus_t {
        if(!ValidSid(sid)){
          return FC_ERROR;
        }
        auto sinfo = &stable_[sid];

        auto c = &ctable_[sinfo->cid_];

        uint32_t addr_l;
        uint32_t addr_r;
        GetItemBoundAddr(sinfo, (uint8_t *)&time_start, (uint8_t *)&time_end, &addr_l, &addr_r);
        if(addr_l == 0){
            addr_l = 1;
        }
        if(addr_r == 0){
            addr_r = sinfo->nalloc_;
        }
        uint32_t nbitem = (addr_r - addr_l) + 1;

        *items = GetItem(sinfo, addr_l, nbitem);
        if (items == nullptr) {
            return FC_ERROR;
        }


        uint32_t minx = 1;
        uint32_t maxn = (addr_r - addr_l) + 1;
        uint32_t start_idx = minx;
        uint32_t end_idx = maxn;
        int flag1 =0 ;
        int flag2 =0;
        while(minx<=maxn){
            int mid = (minx+maxn)/2;
            uint64_t temp_timestamp = *(uint64_t*)(*items+(mid-1)*(c->size_));
            if(temp_timestamp>=time_start){
                start_idx = mid;
                maxn = mid - 1;
                flag1=1;
            }else{
                minx = mid + 1;
            }
        }
        minx = 1;
        maxn = (addr_r - addr_l) + 1;
        while(minx<=maxn){
            int mid = (minx+maxn)/2;
            uint64_t temp_timestamp = *(uint64_t*)(*items+(mid-1)*(c->size_));
            if(temp_timestamp>=time_end){
                maxn = mid - 1;
            }else{
                end_idx = mid;
                minx = mid + 1;
                flag2=1;
            }
        }
        if(flag1==0||flag2==0){
            return FC_ERROR;
        }
        *items = (*items+(start_idx-1)*(c->size_));
        *items_len = (end_idx - start_idx + 1)*c->size_;
        semantic_value_[sid] += (*items_len)<<(value_factor_-1);
        return FC_OK;
    }

    auto SlabManagement::DeleteSlab(const uint32_t &sid) -> rstatus_t {
        if(!ValidSid(sid)){
          return FC_ERROR;
        }
        semantic_value_[sid] = 0;

        auto sinfo = &stable_[sid];
        if(skiplists_[sid]!= nullptr){
            auto status = skiplists_[sid]->DeleteSidInfo(stable_[sid].time_start_,stable_[sid].time_end_,sid);
            if(status== false){
              status = skiplists_[sid]->DeleteSidInfo1(stable_[sid].time_start_,stable_[sid].time_end_,sid);
            }
        }
        skiplists_[sid] = nullptr;

        if (sinfo->free_ == 1) {
            return FC_ERROR;
        }

        if(sinfo->bound_ != nullptr){
            sinfo->nbound_ = 0;
        }
        sinfo->nalloc_ = 0;
        sinfo->cid_ = SLABCLASS_INVALID_ID;
        sinfo->free_ = 1;
        sinfo->time_start_ = 0;
        sinfo->time_end_ = 0;

        if (sinfo->mem_) {
            TAILQ_REMOVE(&used_msinfoq_, sinfo, tqe_);
            nused_msinfoq_--;
            TAILQ_INSERT_TAIL(&free_msinfoq_, sinfo, tqe_);
            nfree_msinfoq_++;
        } else {
            TAILQ_REMOVE(&used_dsinfoq_, sinfo, tqe_);
            nused_dsinfoq_--;
            TAILQ_INSERT_TAIL(&free_dsinfoq_, sinfo, tqe_);
            nfree_dsinfoq_++;
        }

        return FC_OK;
    }

    auto SlabManagement::MoveSlabInMem(const uint32_t &sid, const uint32_t &evict_sid) -> rstatus_t {
        if(!ValidSid(sid)){
          return FC_ERROR;
        }

        auto dsinfo = &stable_[sid];
        if (dsinfo->mem_ == 1) {
            return FC_OK;
        }

        if (TAILQ_EMPTY(&free_msinfoq_)) {
            auto status = SlabDrain(evict_sid);
            if (status != FC_OK) {
                return FC_ERROR;
            }
            return MoveSlabInMem(sid, evict_sid);
        }

        auto msinfo = TAILQ_FIRST(&free_msinfoq_);
        nfree_msinfoq_--;
        TAILQ_REMOVE(&free_msinfoq_, msinfo, tqe_);

        if (dsinfo->free_ == 1) {
            nfree_dsinfoq_--;
            TAILQ_REMOVE(&free_dsinfoq_, dsinfo, tqe_);
        } else {
            nused_dsinfoq_--;
            TAILQ_REMOVE(&used_dsinfoq_, dsinfo, tqe_);
        }

        auto mslab = SlabFromMaddr(msinfo->addr_, false);
        auto size = settings_.slab_size_;
        auto doff = SlabToDaddr(dsinfo);
        auto n = pread(fd_, mslab, size, doff);
        if (static_cast<size_t>(n) < size) {
            return FC_ERROR;
        }
        SlabSwapAddr(msinfo, dsinfo);

        if (dsinfo->free_ == 1) {
            nfree_dsinfoq_++;
            TAILQ_INSERT_TAIL(&free_msinfoq_, dsinfo, tqe_);
        } else {
            nused_dsinfoq_++;
            TAILQ_INSERT_TAIL(&used_msinfoq_, dsinfo, tqe_);
        }

        nfree_dsinfoq_++;
        TAILQ_INSERT_TAIL(&free_dsinfoq_, msinfo, tqe_);

        return FC_OK;
    }

    auto SlabManagement::SlabDrain(const uint32_t &sid) -> rstatus_t {
        if (TAILQ_EMPTY(&free_dsinfoq_) || nfree_dsinfoq_ == 0) {
            return FC_ERROR;
        }

        struct Slabinfo *msinfo;
        if (!ValidSid(sid)) {
            if (TAILQ_EMPTY(&used_msinfoq_) || nused_msinfoq_ == 0) {
                return FC_ERROR;
            }
            msinfo = TAILQ_FIRST(&used_msinfoq_);
        } else {
            msinfo = &stable_[sid];
        }

        if (msinfo->mem_ == 0) {
            return FC_OK;
        }

        if (msinfo->free_ == 1) {
            TAILQ_REMOVE(&free_msinfoq_, msinfo, tqe_);
            nfree_msinfoq_--;
        } else {
            TAILQ_REMOVE(&used_msinfoq_, msinfo, tqe_);
            nused_msinfoq_--;
        }

        Slabinfo * dsinfo = nullptr;
        if (TAILQ_EMPTY(&free_dsinfoq_) || nfree_dsinfoq_ == 0) {
            dsinfo = TAILQ_FIRST(&used_dsinfoq_);
        }else{
            dsinfo = TAILQ_FIRST(&free_dsinfoq_);
        }
        nfree_dsinfoq_--;
        TAILQ_REMOVE(&free_dsinfoq_, dsinfo, tqe_);

        auto slab = SlabFromMaddr(msinfo->addr_, true);
        auto size = SLAB_HDR_SIZE + msinfo->nalloc_*(ctable_[msinfo->cid_].size_);
        auto aligned_size = ROUND_UP(size, DISK_READ_GRAN);
        auto off = SlabToDaddr(dsinfo);
        auto n = pwrite(fd_, slab, aligned_size, off);
        if (static_cast<size_t>(n) < size) {
            UNREACHABLE("SlabDrain exit\n");
        }

        SlabSwapAddr(msinfo, dsinfo);

        nfree_msinfoq_++;
        TAILQ_INSERT_TAIL(&free_msinfoq_, dsinfo, tqe_);

        if (msinfo->free_ == 1) {
            nfree_dsinfoq_++;
            TAILQ_INSERT_TAIL(&free_dsinfoq_, msinfo, tqe_);
        } else {
            nused_dsinfoq_++;
            TAILQ_INSERT_TAIL(&used_dsinfoq_, msinfo, tqe_);
        }

        return FC_OK;
    }


    void SlabManagement::GetItemBoundAddr(const struct Slabinfo *sinfo, const uint8_t *k1,
                                          const uint8_t *k2, uint32_t *addr1, uint32_t *addr2) const {
        if(addr1 == nullptr || addr2 == nullptr){
            return;
        }
        *addr1 = 0;
        *addr2 = 0;

        Itembound *bitem;
        int32_t cmp;
        bool flag = false;

        for(uint32_t i=0;i<sinfo->nbound_;i++) {
            bitem = sinfo->bound_ + i;
            cmp = CompareKey(*(uint64_t *)k1, bitem->timestamp_);
            if (!flag&&cmp >= 0) {
                flag = true;
                *addr1 = bitem->addr_;
            }
            cmp = CompareKey(*(uint64_t *)k2, bitem->timestamp_);
            if (cmp < 0) {
                *addr2 = bitem->addr_;
                return;
            }
        }
    }

    auto SlabManagement::GetItem(const struct Slabinfo *sinfo, const uint32_t &addr,
                                 const uint32_t &nitem) -> uint8_t *{
        auto c = &ctable_[sinfo->cid_];
        if (sinfo->mem_ == 0) {
            auto off = SlabToDaddr(sinfo) + SLAB_HDR_SIZE + (addr-1) * c->size_;
            auto aligned_off = ROUND_DOWN(off, DISK_READ_GRAN);
            auto aligned_size = ROUND_UP((c->size_ * nitem + (off - aligned_off)), DISK_READ_GRAN);
            uint32_t my_readbuf_offset = 0;
            mtx.lock();
            readbuf_offset_ += aligned_size;
            my_readbuf_offset = readbuf_offset_;
            mtx.unlock();
            auto n = pread(fd_, readbuf_+my_readbuf_offset-aligned_size, aligned_size, aligned_off);
            if (static_cast<size_t>(n) < aligned_size) {
                return nullptr;
            }
            return readbuf_ + (my_readbuf_offset - aligned_size + off - aligned_off);
        }
        auto slab = SlabFromMaddr(sinfo->addr_, false);
        return (uint8_t *)SlabToItem(slab, addr-1, c->size_);
    }

    auto SlabManagement::SlabFull(const struct Slabinfo *sinfo) const -> bool {
        auto cid = sinfo->cid_;
        if(!ValidCid(cid)){
          return FC_ERROR;
        }
        auto c = &ctable_[cid];

        return c->nitem_ == sinfo->nalloc_;
    }

    auto SlabManagement::SlabGetItem(const uint32_t &sid) -> struct Slabitem * {
        if(!ValidSid(sid)){
          return nullptr;
        }
        auto sinfo = &stable_[sid];

        auto c = &ctable_[sinfo->cid_];
        auto slab = SlabFromMaddr(sinfo->addr_, false);

        auto item = SlabToItem(slab, sinfo->nalloc_, c->size_);
        sinfo->nalloc_++;

        return item;
    }

    auto SlabManagement::SlabFromMaddr(const uint32_t &addr, const bool &verify) const
    -> struct Slab * {
        struct Slab *slab;
        off_t off;

        off = static_cast<off_t>(addr) * settings_.slab_size_;
        slab = (struct Slab *)(mstart_ + off);

        return slab;
    }

    auto SlabManagement::SlabToItem(const struct Slab *slab, const uint32_t &idx,
                                    const size_t &size) const -> struct Slabitem * {
        auto it = (struct Slabitem *)((uint8_t *)slab->data_ + (idx * size));
        return it;
    }

    auto SlabManagement::SlabToDaddr(const struct Slabinfo *sinfo) const -> off_t {
        off_t off = dstart_ + (static_cast<off_t>(sinfo->addr_) * settings_.slab_size_);
        return off;
    }

    void SlabManagement::SlabSwapAddr(struct Slabinfo *msinfo, struct Slabinfo *dsinfo) {
        uint32_t m_addr;
        m_addr = msinfo->addr_;
        msinfo->addr_ = dsinfo->addr_;
        msinfo->mem_ = 0;
        dsinfo->addr_ = m_addr;
        dsinfo->mem_ = 1;
    }

    auto SlabManagement::BatchWriteToSSD(std::vector<uint32_t>&batch_write_sids) -> rstatus_t {
        std::vector<std::future<rstatus_t>>future_results;
        for(uint32_t i=0;i<batch_write_sids.size();i++){
            auto  dsinfo = TAILQ_FIRST(&free_dsinfoq_);
            auto msinfo = &stable_[batch_write_sids[i]];
            auto dsid = dsinfo->sid_;
            TAILQ_REMOVE(&free_dsinfoq_, dsinfo, tqe_);
            TAILQ_REMOVE(&used_msinfoq_,msinfo,tqe_);
            nfree_dsinfoq_--;
            nused_dsinfoq_++;
            TAILQ_INSERT_TAIL(&free_msinfoq_, dsinfo, tqe_);
            TAILQ_INSERT_TAIL(&used_dsinfoq_,msinfo,tqe_);
            nfree_msinfoq_++;
            nused_msinfoq_--;
            future_results.emplace_back(this->pool.enqueue([this,&batch_write_sids,i,dsid]  {
                return WriteToSSD(batch_write_sids[i],dsid);;
            }));
        }
        for(auto &it:future_results){
            if(it.get()!=FC_OK){
                return FC_ERROR;
            }
        }
        return FC_OK;
    }

    auto SlabManagement::WriteToSSD(uint32_t msid, uint32_t dsid) -> rstatus_t {
        auto msinfo = &stable_[msid];
        auto dsinfo = &stable_[dsid];
        auto slab = SlabFromMaddr(msinfo->addr_, true);
        auto size = SLAB_HDR_SIZE + msinfo->nalloc_*(ctable_[msinfo->cid_].size_);
        auto aligned_size = ROUND_UP(size, DISK_READ_GRAN);
        auto off = SlabToDaddr(dsinfo);
        auto n = pwrite(fd_, slab, aligned_size, off);
        if (static_cast<size_t>(n) < size) {
            UNREACHABLE("SlabDrain exit\n");
        }

        SlabSwapAddr(msinfo, dsinfo);
        return FC_OK;
    }

    auto SlabManagement::BatchEvict(uint32_t batch_size)->rstatus_t{
        std::priority_queue<SemanticValue>mem_queue;
        uint32_t slab_num = nmslab_ + ndslab_;
        std::vector<uint32_t>batch_write_sids;
        batch_write_sids.reserve(batch_size);
        if(nfree_dsinfoq_>=batch_size){
            for(uint32_t i=0;i<slab_num;i++){
                semantic_value_[i] = semantic_value_[i]>>(value_factor_-1);
                if((stable_+i)->mem_ == 1){
                    mem_queue.push({i,(stable_+i)->time_start_,semantic_value_[i]});
                }
            }
            value_factor_ = 1;
            record_num_ %= (slab_num/10);
            for(uint32_t i=0;i<batch_size;i++){
                auto mem_top = mem_queue.top();
                batch_write_sids.emplace_back(mem_top.sid_);
                mem_queue.pop();
            }
        }else{
            std::priority_queue<SemanticValue>disk_queue;
            for(uint32_t i=0;i<slab_num;i++){
                if((stable_+i)->mem_ == 1){
                    semantic_value_[i] = semantic_value_[i]>>(value_factor_-1);
                    mem_queue.push({i,(stable_+i)->time_start_,semantic_value_[i]});
                }else if((stable_+i)->free_ == 0){
                    semantic_value_[i] = semantic_value_[i]>>(value_factor_-1);
                    disk_queue.push({i,(stable_+i)->time_start_,semantic_value_[i]});
                }
            }
            value_factor_ = 1;
            record_num_ %= (slab_num/10);
            uint32_t num = 0;
            auto nfree_dsinfoq = nfree_dsinfoq_;
            if(disk_queue.empty()){
              disk_queue.push({UINT32_MAX,UINT32_MAX,UINT32_MAX});
            }
            while(mem_queue.empty()||disk_queue.empty()||num<batch_size - nfree_dsinfoq){
                auto mem_top = mem_queue.top();
                auto disk_top = disk_queue.top();
                if(mem_top.value_<disk_top.value_){
                    evict_.push_back(mem_top.sid_);
                    mem_queue.pop();
                }else{
                    DeleteSlab(disk_top.sid_);
                    disk_queue.pop();
                }
                num++;
            }
            num = 0;
            while(!mem_queue.empty()&&num<batch_size-evict_.size()){
                auto mem_top = mem_queue.top();
                batch_write_sids.emplace_back(mem_top.sid_);
                mem_queue.pop();
                num++;
            }
        }
        if(batch_write_sids.size() == 0){
            return FC_OK;
        }
        return BatchWriteToSSD(batch_write_sids);
    }

    auto SlabManagement::IsWorthCompaction(std::shared_ptr<std::vector<SidInfo>> sids)-> std::optional<std::pair<uint32_t ,uint32_t >>{
        if(sids == nullptr||(*sids).size() == 0){
            return std::nullopt;
        }
        auto sinfo1 = &stable_[sids->begin()->sid_];
        auto c = &ctable_[sinfo1->cid_];
        uint32_t idx = 0;
        for(auto &it:(*sids)){
            auto sinfo = &stable_[it.sid_];
            if(SlabFull(sinfo)){
                idx++;
            }else{
                break;
            }
        }
        if(sids->size()-idx>200){
          return std::nullopt;
        }
        uint32_t nitem = 0;
        uint32_t nslab = sids->size() - idx;
        for(uint32_t i=idx;i<sids->size();i++){
            auto sinfo = &stable_[(*sids)[i].sid_];
            nitem += sinfo->nalloc_;
        }
        if (c->nitem_ == 0) {
          return std::nullopt;
        }
        if(((nitem+c->nitem_-1)/c->nitem_)<nslab){
            return std::make_pair(idx,nitem*c->size_);
        }else{
            return std::nullopt;
        }
    }

    auto SlabManagement::InsertOldValue(uint32_t sid, uint8_t *value, uint32_t &actual_insert_len,
                                        uint32_t insert_len,std::map<uint32_t ,uint8_t *>&slab_addr) -> rstatus_t {
        auto sinfo = &stable_[sid];
        uint8_t *it = nullptr;
        if (!SlabMem(sid)) {
            auto slab = (Slab*)(slab_addr[sid]);
            slab->sid_ = sid;
            slab->cid_ = sinfo->cid_;
            it = (uint8_t *)(slab->data_);
            sinfo->nalloc_++;
        }else{
            it = (uint8_t*)SlabGetItem(sid);
        }
        if (SlabFull(sinfo)) {
            return FC_ERROR;
        }

        if (value == nullptr) {
            return FC_OK;
        }

        auto c = &ctable_[sinfo->cid_];
        uint32_t addr;

        uint32_t slab_remain_len = (c->nitem_-sinfo->nalloc_+1)*(c->size_);
        if(slab_remain_len<insert_len){
            insert_len = slab_remain_len;
        }


        memcpy(it,value,insert_len);

        addr = 0;
        if(sinfo->bound_ != nullptr && sinfo->nbound_ != 0){
            addr = (sinfo->bound_ + sinfo->nbound_ -1)->addr_;
        }
        uint32_t add_bound_num = insert_len/(ITEM_BOUND_SIZE);
        for(uint32_t i=1;i<=add_bound_num;i++){
            if(sinfo->bound_== nullptr){
                uint32_t size = sizeof(struct Itembound) * settings_.slab_size_ / ITEM_BOUND_SIZE;
                sinfo->bound_ =  static_cast<struct Itembound *>(fc_alloc(size+1));
            }
            sinfo->nbound_++;
            uint32_t offset_item_num = (i*ITEM_BOUND_SIZE)/(c->size_);
            (sinfo->bound_ + sinfo->nbound_ -1)->addr_ = sinfo->nalloc_ - 1 + offset_item_num;
            (sinfo->bound_ + sinfo->nbound_ -1)->timestamp_ = *(uint64_t*)(value + (offset_item_num-1)*(c->size_));
        }
        sinfo->nalloc_ += insert_len/(c->size_)-1;
        actual_insert_len = insert_len;
        if(!SlabMem(sid)){
            auto slab = slab_addr[sid];
            auto size = settings_.slab_size_;
            auto off = SlabToDaddr(sinfo);
            auto n = pwrite(fd_, slab, size, off);
            if (static_cast<size_t>(n) < size) {
                UNREACHABLE("InsertOldValue exit\n");
            }
        }
        return FC_OK;
    }

    auto SlabManagement::Compaction(std::shared_ptr<std::vector<SidInfo>> sids,uint32_t idx,uint32_t c_len) -> std::shared_ptr<std::vector<SidInfo>>{
        uint32_t offset = compress_buf_offset_;
        compress_buf_offset_ += (sids->size()-idx)*settings_.slab_size_;
        std::map<uint32_t,uint8_t*>slab_addr;
        auto it = compress_buf_ + offset;
        auto tmp_sinfo = &stable_[sids->begin()->sid_];
        auto c= &ctable_[tmp_sinfo->cid_];
        uint32_t   sum_semanctic_value = 0;
        for(uint32_t i = idx;i<sids->size();i++){
            auto sinfo = &stable_[(*sids)[i].sid_];
            sum_semanctic_value += semantic_value_[(*sids)[i].sid_];
            if(sinfo->mem_){
                auto slab = SlabFromMaddr(sinfo->addr_, false);
                memcpy(compress_buf_ + offset,slab->data_,sinfo->nalloc_*c->size_);
                offset+=sinfo->nalloc_*c->size_;
            }else{
                auto off = SlabToDaddr(sinfo);
                auto size = c->size_ * sinfo->nalloc_;
                auto aligned_size = ROUND_UP(size+SLAB_HDR_SIZE, DISK_READ_GRAN);
                auto n = pread(fd_, compress_buf_+ compress_buf_offset_, aligned_size, off);
                slab_addr[sinfo->sid_] = compress_buf_offset_ + compress_buf_;
                auto slab = (Slab*)(compress_buf_+compress_buf_offset_);
                memcpy(compress_buf_ + offset,slab->data_,size);
                offset += size;
                compress_buf_offset_ += settings_.slab_size_;
            }
        }
        std::vector<SidInfo>new_sids;
        new_sids.reserve(sids->size());
        for(uint32_t  i=0;i<idx;i++){
            new_sids.emplace_back((*sids)[i]);
        }
        uint32_t cur_idx = idx;
        uint32_t remain_len = c_len;
        uint32_t total_len = c_len;
        uint32_t actual_total_insert_len = 0;
        while(cur_idx<sids->size()&&remain_len>0&&actual_total_insert_len<total_len){
            uint32_t actual_insert_len = 0;
            auto sid = (*sids)[cur_idx].sid_;
            auto sinfo = &stable_[sid];
            sinfo->nalloc_ = 0;
            sinfo->nbound_ = 0;
            auto insert_len = remain_len;
            auto status = InsertOldValue(sid,it+actual_total_insert_len,actual_insert_len,insert_len,slab_addr);
            remain_len -= actual_insert_len;
            semantic_value_[sid] = 0;
            semantic_value_[sid] += ((double)actual_insert_len/total_len)*sum_semanctic_value;
            uint64_t new_time_start = *(uint64_t*)(it+actual_total_insert_len);
            uint64_t new_time_end = *(uint64_t*)(it+actual_total_insert_len+actual_insert_len-c->size_);
            new_sids.emplace_back(new_time_start,new_time_end,sid);
            sinfo->time_start_ = new_time_start;
            sinfo->time_end_ = new_time_end;
            actual_total_insert_len += actual_insert_len;
            cur_idx++;
        }
        compress_buf_offset_ = 0;
        return std::make_shared<std::vector<SidInfo>>(new_sids);
    }
    auto SlabManagement::InitSlabManagement() -> rstatus_t {
        rstatus_t status;
        size_t size;
        uint32_t ndchunk;
        record_num_ = 0;

        nfree_msinfoq_ = 0;
        TAILQ_INIT(&free_msinfoq_);
        nused_msinfoq_ = 0;
        TAILQ_INIT(&used_msinfoq_);

        nfree_dsinfoq_ = 0;
        TAILQ_INIT(&free_dsinfoq_);

        nused_dsinfoq_ = 0;
        TAILQ_INIT(&used_dsinfoq_);

        nctable_ = 0;
        ctable_ = nullptr;

        nstable_ = 0;
        stable_ = nullptr;

        mstart_ = nullptr;
        mend_ = nullptr;

        dstart_ = 0;
        dend_ = 0;
        fd_ = -1;

        mspace_ = 0;
        dspace_ = 0;
        nmslab_ = 0;
        ndslab_ = 0;

        readbuf_ = nullptr;
        compress_buf_ = nullptr;

        status = InitCtable();

        nmslab_ =
                std::max(static_cast<size_t>(nctable_), settings_.max_slab_memory_ / settings_.slab_size_);
        mspace_ = nmslab_ * settings_.slab_size_;
        mstart_ = static_cast<uint8_t *>(fc_mmap(mspace_));
        mend_ = mstart_ + mspace_;

        status = FcDeviceSize(settings_.ssd_device_, &size);
        ndchunk = size / settings_.slab_size_;
        ndslab_ = ndchunk / settings_.server_n_;
        dspace_ = ndslab_ * settings_.slab_size_;
        dstart_ = (settings_.server_id_ * ndslab_) * settings_.slab_size_;
        dend_ = ((settings_.server_id_ + 1) * ndslab_) * settings_.slab_size_;
        batch_size_ = nmslab_/16;

        fd_ = open(settings_.ssd_device_, O_RDWR | O_DIRECT, 0644);

        status = InitStable();

        readbuf_ = static_cast<uint8_t *>(fc_mmap(2*300*MB));
        compress_buf_ = static_cast<uint8_t *>(fc_mmap(2*300*MB));
        memset(readbuf_, 0xff, settings_.slab_size_);
        return FC_OK;
    }

    auto SlabManagement::InitCtable() -> rstatus_t{

        nctable_ = settings_.profile_last_id_ + 1;
        ctable_ = static_cast<struct Slabclass *>(fc_alloc(sizeof(*ctable_) * nctable_));
        if (ctable_ == nullptr) {
            return FC_ENOMEM;
        }

        auto profile = settings_.profile_;
        Slabclass *c;
        for (uint8_t cid = SLABCLASS_MIN_ID; cid < nctable_; cid++) {
            c = &ctable_[cid];
            c->nitem_ = GetSlabDataSize() / profile[cid];
            c->size_ = profile[cid];
        }

        return FC_OK;
    }

    auto SlabManagement::FcDeviceSize(const char *path, size_t *size) -> rstatus_t {
        int status;
        struct stat statinfo;
        int fd;

        status = stat(path, &statinfo);

        if (!S_ISREG(statinfo.st_mode) && !S_ISBLK(statinfo.st_mode)) {
            return FC_ERROR;
        }

        if (S_ISREG(statinfo.st_mode)) {
            *size = static_cast<size_t>(statinfo.st_size);
            return FC_OK;
        }

        fd = open(path, O_RDONLY, 0644);
        if (fd < 0) {
            return FC_ERROR;
        }

        status = ioctl(fd, BLKGETSIZE64, size);
        if (status < 0) {
            close(fd);
            return FC_ERROR;
        }

        close(fd);

        return FC_OK;
    }

    auto SlabManagement::InitStable() -> rstatus_t {
        struct Slabinfo *sinfo;
        uint32_t i;
        uint32_t j;

        nstable_ = nmslab_ + ndslab_;
        if (nstable_ > SLAB_MAX_SID) {
            return FC_ERROR;
        }

        stable_ = static_cast<struct Slabinfo *>(fc_alloc(sizeof(*stable_) * nstable_));
        if (stable_ == nullptr) {
            return FC_ENOMEM;
        }

        for (i = 0; i < nmslab_; i++) {
            sinfo = &stable_[i];

            sinfo->sid_ = i;
            sinfo->addr_ = i;
            sinfo->nalloc_ = 0;
            sinfo->cid_ = SLABCLASS_INVALID_ID;
            sinfo->mem_ = 1;
            sinfo->free_ = 1;

            nfree_msinfoq_++;
            TAILQ_INSERT_TAIL(&free_msinfoq_, sinfo, tqe_);
            sinfo->nbound_ = 0;
            uint32_t size = sizeof(struct Itembound) * settings_.slab_size_ / ITEM_BOUND_SIZE;
            sinfo->bound_ =  static_cast<struct Itembound *>(fc_alloc(size+1));
        }

        for (j = 0; j < ndslab_ && i < nstable_; i++, j++) {
            sinfo = &stable_[i];

            sinfo->sid_ = i;
            sinfo->addr_ = j;
            sinfo->nalloc_ = 0;
            sinfo->cid_ = SLABCLASS_INVALID_ID;
            sinfo->mem_ = 0;
            sinfo->free_ = 1;

            nfree_dsinfoq_++;
            TAILQ_INSERT_TAIL(&free_dsinfoq_, sinfo, tqe_);

            sinfo->nbound_ = 0;
            uint32_t size = sizeof(struct Itembound) * settings_.slab_size_ / ITEM_BOUND_SIZE;
            sinfo->bound_ =  static_cast<struct Itembound *>(fc_alloc(size+1));
        }

        return FC_OK;
    }


    void SlabManagement::DeinitSlabManagement() {
        DeinitCtable();
        DeinitStable();
        fc_munmap(mstart_,mspace_);
        mstart_ = nullptr;
        fc_munmap(readbuf_,2*300*MB);
        readbuf_ = nullptr;
        fc_munmap(compress_buf_,2*300*MB);
        compress_buf_ = nullptr;
        close(fd_);
    }

    void SlabManagement::DeinitCtable() {
        if (ctable_ != nullptr) {
            fc_free(ctable_);
            ctable_ = nullptr;
        }
    }

    void SlabManagement::DeinitStable() {
        if (stable_ == nullptr) {
            return;
        }

        struct Slabinfo *sinfo = nullptr;

        for (uint32_t i = 0; i < nstable_; i++) {
            sinfo = &stable_[i];
            if(sinfo->bound_ != nullptr){
                fc_free(sinfo->bound_);
                sinfo->bound_ = nullptr;
            }
        }

        fc_free(stable_);
        stable_ = nullptr;
    }


}
