#include "include/storage/time_range_skiplist.h"
namespace SemanticCache{
    TimeRangeNode::TimeRangeNode(uint64_t time_start, uint64_t time_end,std::shared_ptr<std::vector<SidInfo>>sid_info,uint8_t node_level):forward_(node_level,nullptr) {
        this->time_range_.time_start_ = time_start;
        this->time_range_.time_end_ = time_end;
        this->node_level_ = node_level;
        this->sid_info_ = sid_info;
    }
    TimeRangeNode::TimeRangeNode(const TimeRangeNode &tr_node) {
        this->time_range_.time_start_ = tr_node.time_range_.time_start_;
        this->time_range_.time_end_ = tr_node.time_range_.time_end_;
        this->forward_ = tr_node.forward_;
        this->sid_info_ = tr_node.sid_info_;
    }

    bool TimeRangeNode::operator<(const TimeRangeNode& tr_node) const{
        if(this->time_range_.time_start_ == tr_node.time_range_.time_start_){
            return this->time_range_.time_end_ < tr_node.time_range_.time_end_;
        }
        return this->time_range_.time_start_ < tr_node.time_range_.time_start_;
    }

    TimeRangeSkipList::TimeRangeSkipList():rnd_(0x12345678){
        CreateList();
    }
    TimeRangeSkipList::~TimeRangeSkipList()=default;

    auto TimeRangeSkipList::CreateList() -> void {
        CreateNode(1, footer_);

        footer_->time_range_.time_start_ = UINT64_MAX;
        footer_->time_range_.time_end_ = UINT64_MAX;
        this->level_ = 1;
        CreateNode(MAX_LEVEL_, header_);
        for (uint32_t i = 0; i < MAX_LEVEL_; ++i) {
            header_->forward_[i] = footer_;
        }
        node_count_ = 0;
    }
    auto TimeRangeSkipList::CreateNode(uint8_t level, std::shared_ptr<TimeRangeNode> &new_node) -> void {
        new_node = std::make_shared<TimeRangeNode>(0,0, nullptr,level);
        assert(new_node != NULL);
    }

    auto TimeRangeSkipList::CreateNode(uint64_t time_start,uint64_t  time_end,uint8_t level, std::shared_ptr<TimeRangeNode> &new_node) -> void {
        new_node = std::make_shared<TimeRangeNode>(time_start,time_end, nullptr,level);
        assert(new_node != NULL);
    }

    auto TimeRangeSkipList::InsertNode(std::shared_ptr<TimeRangeNode> &new_node) -> bool {
        uint8_t node_level = GetRandomLevel();
        std::vector<std::shared_ptr<TimeRangeNode>> update(MAX_LEVEL_, nullptr);
        if (node_level > level_) {
            node_level = ++level_;
            new_node->forward_.resize(node_level);
        }

        std::shared_ptr<TimeRangeNode> node = header_;

        for (int i = level_-1; i >= 0; --i) {
            while ((node->forward_[i])->time_range_ < new_node->time_range_) {
                node = node->forward_[i];
            }
            update[i] = node;
        }
        node = node->forward_[0];
        if (node->time_range_ == new_node->time_range_) {
            return false;
        }

        for (int i = node_level-1; i >= 0; --i) {
            node = update[i];
            new_node->forward_[i] = node->forward_[i];
            node->forward_[i] = new_node;
        }
        ++node_count_;
        return true;
    }

    auto TimeRangeSkipList::InsertNode(uint64_t time_start, uint64_t time_end,std::shared_ptr<std::vector<SidInfo>>sid_info) -> bool {
        uint32_t node_level = GetRandomLevel();
        std::vector<std::shared_ptr<TimeRangeNode>> update(MAX_LEVEL_, nullptr);
        if (node_level > level_) {
            node_level = ++level_;
        }
        std::shared_ptr<TimeRangeNode> new_node = std::make_shared<TimeRangeNode>(time_start,time_end, sid_info,node_level);

        std::shared_ptr<TimeRangeNode> node = header_;

        for (int i = level_-1; i >= 0; --i) {
            while ((node->forward_[i])->time_range_ < new_node->time_range_) {
                node = node->forward_[i];
            }
            update[i] = node;
        }
        node = node->forward_[0];

        if (node->time_range_ == new_node->time_range_) {
            return false;
        }
        for (int i = node_level-1; i >= 0; --i) {
            node = update[i];
            new_node->forward_[i] = node->forward_[i];
            node->forward_[i] = new_node;
        }
        ++node_count_;
        return true;
    }

    auto TimeRangeSkipList::SearchEquelNode(uint64_t time_start, uint64_t time_end) const -> std::shared_ptr<TimeRangeNode> {
        TimeRange target_time_range(time_start,time_end);
        std::shared_ptr<TimeRangeNode> node = header_;
        for (int i = level_-1; i >= 0; --i) {
            while ((node->forward_[i])->time_range_ < target_time_range) {
                node = node->forward_[i];
            }
        }
        node = node->forward_[0];
        if (node->time_range_ == target_time_range) {
            return node;
        } else {
            return nullptr;
        }
    }

    auto TimeRangeSkipList::SearchLastLessEquelNode(uint64_t time_start,
                                                    uint64_t time_end) -> std::shared_ptr<TimeRangeNode> {
        std::shared_ptr<TimeRangeNode>node=header_;
        TimeRange target_time_range(time_start,time_end);
        for (int i = level_-1; i >= 0; --i) {
            while ((node->forward_[i])->time_range_ < target_time_range) {
                node = node->forward_[i];
            }
        }
        if(node->forward_[0]->time_range_ == target_time_range){
            node = node->forward_[0];
        }
        return node;
    }


    auto TimeRangeSkipList::SearchAllNode(uint64_t time_start,
                                          uint64_t time_end) -> std::vector<std::shared_ptr<TimeRangeNode>> {
        auto first_node = SearchLastLessEquelNode(time_start,time_start);
        if(first_node->time_range_.time_end_<time_start ){
            first_node = first_node->forward_[0];
        }
        auto last_node = SearchLastLessEquelNode(time_end,time_end);
        if(last_node->forward_[0]->time_range_.time_start_<=time_end){
            last_node = last_node->forward_[0];
        }
        std::vector<std::shared_ptr<TimeRangeNode>> v;
        if(first_node == header_ || last_node == header_ || last_node->time_range_ < first_node->time_range_)return v;
        for(auto it = first_node;it!=last_node;it=it->forward_[0]){
            v.emplace_back(it);
        }
        v.emplace_back(last_node);
        return v;
    }

    auto TimeRangeSkipList::SearchHitNode(uint64_t time_start,
                                          uint64_t time_end) -> std::pair<std::vector<std::shared_ptr<TimeRangeNode>>, TimeRange> {
        auto first_node = SearchLastLessEquelNode(time_start,time_start);
        if(first_node->forward_[0]->time_range_.time_start_<=time_start+TIMESTAMP_LEN ){
            first_node = first_node->forward_[0];
        }
        if(first_node->time_range_.time_end_<time_start ){
            first_node = nullptr;
        }
        auto last_node = SearchLastLessEquelNode(time_end,time_end);
        if(last_node->forward_[0]->time_range_.time_start_ == time_end){
          last_node = last_node->forward_[0];
        }
        if(last_node->time_range_.time_end_<time_end){
            last_node = nullptr;
        }
        std::vector<std::shared_ptr<TimeRangeNode>> v;
        if(first_node!= nullptr && first_node==last_node){
            v.emplace_back(first_node);
            return std::pair(v,TimeRange(0,0));
        }else if(first_node == nullptr &&last_node!= nullptr){
            if(time_end-last_node->time_range_.time_start_<43200){
              return std::pair(v,TimeRange(time_start,time_end));
            }
            v.emplace_back(last_node);
            return std::pair(v,TimeRange(time_start,std::min(last_node->time_range_.time_start_,time_end)));
        }else if(first_node != nullptr &&last_node == nullptr){
            if(time_start-first_node->time_range_.time_end_<43200){
              return std::pair(v,TimeRange(time_start,time_end));
            }
            v.emplace_back(first_node);
            return std::pair(v,TimeRange(first_node->time_range_.time_end_,time_end));
        }else if(first_node != nullptr&&last_node!=first_node){
            if((time_end-last_node->time_range_.time_start_<43200)&&(time_start-first_node->time_range_.time_end_<43200)){
              return std::pair(v,TimeRange(time_start,time_end));
            }
            v.emplace_back(first_node);
            v.emplace_back(last_node);
            return std::pair(v,TimeRange(first_node->time_range_.time_end_,last_node->time_range_.time_start_));
        }else{
            return std::pair(v,TimeRange(time_start,time_end));
        }

    }

    auto TimeRangeSkipList::SearchAllHitNode(uint64_t time_start, uint64_t time_end) -> std::shared_ptr<TimeRangeNode> {
      auto first_node = SearchLastLessEquelNode(time_start,time_start);
      if(first_node->forward_[0]->time_range_.time_start_<=time_start+TIMESTAMP_LEN ){
        first_node = first_node->forward_[0];
      }
      if(first_node->time_range_.time_end_<time_start ){
        first_node = nullptr;
      }
      if(first_node== nullptr||first_node->time_range_.time_end_<time_end){
        first_node = nullptr;
      }
      return first_node;
    }




    auto TimeRangeSkipList::SearchAllNode(uint64_t time_start, uint64_t time_end,
                                          std::vector<std::shared_ptr<TimeRangeNode>> &node_list) -> bool {
        auto first_node = SearchLastLessEquelNode(time_start,time_start);
        if(first_node->time_range_.time_end_<time_start ){
            first_node = first_node->forward_[0];
        }
        auto last_node = SearchLastLessEquelNode(time_end,time_end);
        if(first_node == header_ || last_node == header_ || last_node->time_range_ < first_node->time_range_)return false;
        for(auto it = first_node;it!=last_node;it=it->forward_[0]){
            node_list.emplace_back(it);
        }
        return true;
    }




    auto TimeRangeSkipList::DeleteNode(uint64_t time_start, uint64_t time_end) -> bool {
        std::shared_ptr<TimeRangeNode> update[MAX_LEVEL_];
        std::shared_ptr<TimeRangeNode> node = header_;
        TimeRange target_time_range(time_start,time_end);
        for (int i = level_-1; i >= 0; --i) {
            while ((node->forward_[i])->time_range_ < target_time_range) {
                node = node->forward_[i];
            }
            update[i] = node;
        }
        node = node->forward_[0];
        if (node->time_range_ != target_time_range) {
            return false;
        }
        for (uint32_t i = 0; i < level_; ++i) {
            if (update[i]->forward_[i] != node) {
                break;
            }
            update[i]->forward_[i] = node->forward_[i];
        }
        while (level_ > 0 && header_->forward_[level_-1] == footer_) {
            --level_;
        }

        --node_count_;

        return true;
    }


    auto TimeRangeSkipList::InsertSidInfo(uint64_t time_start, uint64_t time_end, std::vector<SidInfo> &sids)-> bool {
       auto node_ptr = this->SearchLastLessEquelNode(time_start,time_end);
        if(node_ptr!=header_&&(time_start<=TIME_INTERVAL+node_ptr->time_range_.time_end_)){
            if(node_ptr->sid_info_ != nullptr){
                for(auto it:sids){
                    node_ptr->sid_info_->emplace_back(SidInfo(it));
                }
            }else{
                node_ptr->sid_info_ = std::make_shared<std::vector<SidInfo>>(sids);
            }
            node_ptr->time_range_.time_end_= time_end;
            if(node_ptr->forward_[0]!=footer_&&node_ptr->forward_[0]->time_range_.time_start_<=TIME_INTERVAL+time_end) {
              if (node_ptr->forward_[0]->sid_info_ != nullptr) {
                for (auto it : *node_ptr->forward_[0]->sid_info_) {
                  node_ptr->sid_info_->emplace_back(SidInfo(it));
                }
              }
              node_ptr->time_range_.time_end_ = node_ptr->forward_[0]->time_range_.time_end_;
              DeleteNode(node_ptr->forward_[0]->time_range_.time_start_,
                                       node_ptr->forward_[0]->time_range_.time_end_);
            }
        }else if(node_ptr->forward_[0]!=footer_&&node_ptr->forward_[0]->time_range_.time_start_<=TIME_INTERVAL+time_end){
            if(node_ptr->forward_[0]->sid_info_!= nullptr){
              for(auto it:*node_ptr->forward_[0]->sid_info_){
                sids.emplace_back(it);
              }
            }
            node_ptr->forward_[0]->sid_info_ = std::make_shared<std::vector<SidInfo>>(sids);
            node_ptr->forward_[0]->time_range_.time_start_ = time_start;
        }else{
           InsertNode(time_start,time_end,std::make_shared<std::vector<SidInfo>>(sids));
        }
        return true;
    }


    auto TimeRangeSkipList::InsertSidInfo1(uint64_t time_start, uint64_t time_end, std::vector<SidInfo> &sids) -> std::shared_ptr<TimeRangeNode> {
      auto node_ptr = this->SearchLastLessEquelNode(time_start,time_end);
      if(node_ptr!=header_&&(time_start<=TIME_INTERVAL+node_ptr->time_range_.time_end_)){
        if(node_ptr->sid_info_ != nullptr){
          for(auto it:sids){
            node_ptr->sid_info_->emplace_back(SidInfo(it));
          }
        }else{
          node_ptr->sid_info_ = std::make_shared<std::vector<SidInfo>>(sids);
        }
        node_ptr->time_range_.time_end_= time_end;
        if(node_ptr->forward_[0]!=footer_&&node_ptr->forward_[0]->time_range_.time_start_<=TIME_INTERVAL+time_end) {
          if (node_ptr->forward_[0]->sid_info_ != nullptr) {
            for (auto it : *node_ptr->forward_[0]->sid_info_) {
              node_ptr->sid_info_->emplace_back(SidInfo(it));
            }
          }
          node_ptr->time_range_.time_end_ = node_ptr->forward_[0]->time_range_.time_end_;
          DeleteNode(node_ptr->forward_[0]->time_range_.time_start_,
                                   node_ptr->forward_[0]->time_range_.time_end_);
        }
        return node_ptr;
      }else if(node_ptr->forward_[0]!=footer_&&node_ptr->forward_[0]->time_range_.time_start_<=TIME_INTERVAL+time_end){
        if(node_ptr->forward_[0]->sid_info_!= nullptr){
          for(auto it:*node_ptr->forward_[0]->sid_info_){
            sids.emplace_back(it);
          }
        }
        node_ptr->forward_[0]->sid_info_ = std::make_shared<std::vector<SidInfo>>(sids);
        node_ptr->forward_[0]->time_range_.time_start_ = time_start;
        return node_ptr->forward_[0];
      }else{
        InsertNode(time_start,time_end,std::make_shared<std::vector<SidInfo>>(sids));
      }
      return nullptr;
    }


    auto TimeRangeSkipList::InsertEmptyNode(uint64_t time_start, uint64_t time_end) -> bool {
        auto node_ptr = this->SearchLastLessEquelNode(time_start,time_end);
        if(node_ptr!=header_&&(time_start<=TIME_INTERVAL+node_ptr->time_range_.time_end_)){
            if(node_ptr->time_range_.time_end_ > time_end){
              return true;
            }
            node_ptr->time_range_.time_end_= time_end;
            if(node_ptr->forward_[0]!=footer_&&node_ptr->forward_[0]->time_range_.time_start_<=TIME_INTERVAL+time_end){
              if(node_ptr->sid_info_ == nullptr){
                node_ptr->sid_info_ = node_ptr->forward_[0]->sid_info_;
              }else{
                if(node_ptr->forward_[0]->sid_info_!= nullptr){
                  for(auto it:*node_ptr->forward_[0]->sid_info_){
                    node_ptr->GetSidInfo()->emplace_back(SidInfo(it));
                  }
                }
              }
              node_ptr->time_range_.time_end_=node_ptr->forward_[0]->time_range_.time_end_;
              DeleteNode(node_ptr->forward_[0]->time_range_.time_start_,node_ptr->forward_[0]->time_range_.time_end_);
            }
        }else if(node_ptr->forward_[0]!=footer_&&node_ptr->forward_[0]->time_range_.time_start_<=TIME_INTERVAL+time_end){
            node_ptr->forward_[0]->time_range_.time_start_ = time_start;
        }else{
            InsertNode(time_start,time_end);
        }
        return true;
    }

    auto TimeRangeSkipList::DeleteSidInfo(uint64_t time_start, uint64_t time_end) ->bool{
        auto node_ptr = this->SearchLastLessEquelNode(time_start,time_end);
        if(node_ptr == nullptr){
            return false;
        }
        if(node_ptr->time_range_.time_end_>=time_end){
            if(node_ptr->sid_info_== nullptr){
            }
            if(node_ptr->sid_info_->size() == 1){
                if(node_ptr->sid_info_->begin()->time_start_==time_start&&node_ptr->sid_info_->begin()->time_end_==time_end){
                    DeleteNode(node_ptr->time_range_.time_start_,node_ptr->time_range_.time_end_);
                    return true;
                }
                return false;
            }
            std::vector<SidInfo>temp_sids;
            for(auto it:*node_ptr->sid_info_){
                if(!(it.time_start_==time_start&&it.time_end_==time_end)){
                    temp_sids.emplace_back(it);
                }else{
                    break;
                }
            }
            if(temp_sids.size() == node_ptr->sid_info_->size()){
                return false;
            }else if(temp_sids.size()==0){
                node_ptr->sid_info_->erase(node_ptr->sid_info_->begin());
                node_ptr->time_range_.time_start_= node_ptr->sid_info_->begin()->time_start_;
            }else if(temp_sids.size() == node_ptr->sid_info_->size()-1){
                node_ptr->sid_info_->pop_back();
                node_ptr->time_range_.time_end_=(node_ptr->sid_info_->end()-1)->time_end_;
            }else{
                node_ptr->sid_info_->erase(node_ptr->sid_info_->begin(),node_ptr->sid_info_->begin()+temp_sids.size()+1);
                node_ptr->time_range_.time_start_= node_ptr->sid_info_->begin()->time_start_;
                InsertNode(temp_sids.begin()->time_start_,(temp_sids.end()-1)->time_end_,std::make_shared<std::vector<SidInfo>>(temp_sids));
            }
        }else{
            auto next_node = node_ptr->forward_[0];
            if(next_node== nullptr||next_node == footer_){
                return true;
            }
            if(next_node->sid_info_->size() == 1){
                if(next_node->sid_info_->begin()->time_start_==time_start&&next_node->sid_info_->begin()->time_end_==time_end){
                    DeleteNode(next_node->time_range_.time_start_,next_node->time_range_.time_end_);
                }
                return true;
            }
            auto first_sid_info = (*next_node->sid_info_)[0];
            if((first_sid_info.time_start_==time_start&&first_sid_info.time_end_ == time_end)){
                next_node->sid_info_->erase( next_node->sid_info_->begin());
                next_node->time_range_.time_start_ = next_node->sid_info_->begin()->time_start_;
                return true;
            }
            return false;
        }
        return true;
    }

    auto TimeRangeSkipList::DeleteSidInfo(uint64_t time_start, uint64_t time_end, uint32_t sid) -> bool {
      auto node_ptr = this->SearchLastLessEquelNode(time_end-1,time_end-1);
      if(node_ptr->time_range_.time_end_<time_end){
        if(node_ptr->forward_[0]==footer_){
          return false;
        }
        node_ptr = node_ptr->forward_[0];
      }
      if(node_ptr == nullptr){
        return false;
      }
      if(node_ptr->sid_info_ == nullptr){
        return false;
      }
      if(node_ptr->sid_info_->size() == 1) {
        if (node_ptr->sid_info_->begin()->sid_ == sid) {
          DeleteNode(node_ptr->GetTimeRange().time_start_, node_ptr->GetTimeRange().time_end_);
          if(node_ptr->GetTimeRange().time_start_<time_start-2*TIME_INTERVAL){
            InsertEmptyNode(node_ptr->GetTimeRange().time_start_,time_start);
          }
          if(node_ptr->GetTimeRange().time_end_>time_end+2*TIME_INTERVAL){
            InsertEmptyNode(time_end,node_ptr->GetTimeRange().time_end_);
          }
          return true;
        }
        return false;
      }else {
        std::vector<SidInfo> temp_sids;
        for (auto it : *node_ptr->sid_info_) {
          if (!(it.sid_ == sid)) {
            temp_sids.emplace_back(it.time_start_,it.time_end_,it.sid_);
          } else {
            break;
          }
        }
        if (temp_sids.size() == node_ptr->sid_info_->size()) {
          return false;
        } else if (temp_sids.size() == 0) {
          uint64_t temp_time_start = node_ptr->GetTimeRange().time_start_;
          node_ptr->time_range_.time_start_ = (node_ptr->sid_info_->begin()+1)->time_start_;
          node_ptr->sid_info_->erase(node_ptr->sid_info_->begin());
          if(temp_time_start>time_start){
            InsertEmptyNode(temp_time_start,time_start);
          }
        } else if (temp_sids.size() == node_ptr->sid_info_->size() - 1) {
          node_ptr->time_range_.time_end_ = (node_ptr->sid_info_->end() - 1)->time_start_;
          node_ptr->sid_info_->pop_back();
        } else {
          node_ptr->sid_info_->erase(node_ptr->sid_info_->begin(),
                                     node_ptr->sid_info_->begin() + temp_sids.size() + 1);
          auto pre_time_start = node_ptr->time_range_.time_start_;
          node_ptr->time_range_.time_start_ = std::min(time_end + 2*TIME_INTERVAL,node_ptr->sid_info_->begin()->time_start_);
          InsertNode(pre_time_start, time_start,
                     std::make_shared<std::vector<SidInfo>>(temp_sids));
        }
      }
      return true;
    }


    auto TimeRangeSkipList::DeleteSidInfo1(uint64_t time_start, uint64_t time_end, uint32_t sid) -> bool {
      auto first_node = SearchLastLessEquelNode(time_start,time_start);
      if(first_node->time_range_.time_end_<time_start ){
        first_node = first_node->forward_[0];
      }
      auto last_node = SearchLastLessEquelNode(time_end,time_end);
      if(last_node->forward_[0]->time_range_.time_start_<=time_end){
        last_node = last_node->forward_[0];
      }
      for(auto it = first_node;it!=last_node->forward_[0]&&it;it=it->forward_[0]){
        if(it->sid_info_!= nullptr){
          std::vector<SidInfo>v;
          v.reserve(it->sid_info_->size());
          for(auto & itx :(*it->sid_info_)){
            if(itx.sid_ == sid){
              if(v.size()==0){
                it->sid_info_->erase(it->sid_info_->begin());
              }else if(v.size()==it->sid_info_->size()-1){
                it->sid_info_->erase((it->sid_info_->end()-1));
              }else{
                this->InsertNode(it->time_range_.time_start_,itx.time_start_,std::make_shared<std::vector<SidInfo>>(v));
                it->sid_info_->erase(it->sid_info_->begin(),it->sid_info_->begin()+v.size()+1);
                it->time_range_.time_start_=it->sid_info_->begin()->time_start_;
              }
              break;
            }
            v.emplace_back(itx);
          }
        }
      }
      return true;
    }

    auto TimeRangeSkipList::SearchAllSidInfo(uint64_t time_start, uint64_t time_end,std::vector<uint32_t>&sids) ->bool{
        auto node_list = SearchAllNode(time_start,time_end);
        for(auto it:node_list){
            it->SearchAllSidInNode(time_start,time_end,sids);
        }
        return true;
    }

    auto TimeRangeSkipList::SetSidInfoEndTime(uint64_t time_start, uint64_t time_end, uint64_t new_time_end,uint64_t last_time) -> bool {
        auto node_ptr = this->SearchLastLessEquelNode(time_start,time_end);
        if(node_ptr == nullptr){
            return false;
        }
        if(node_ptr->time_range_.time_end_!=time_end){
            return false;
        }
        node_ptr->SetEndTime(new_time_end,last_time);
        if(node_ptr->time_range_.time_end_+ TIME_INTERVAL>=node_ptr->forward_[0]->time_range_.time_start_){
          auto sids_info = node_ptr->forward_[0]->sid_info_;
          if(sids_info != nullptr){
            for(auto it:*sids_info){
              node_ptr->sid_info_->emplace_back(it);
            }
          }

          node_ptr->time_range_.time_end_ = node_ptr->forward_[0]->time_range_.time_end_;
          DeleteNode(node_ptr->forward_[0]->time_range_.time_start_,node_ptr->forward_[0]->time_range_.time_end_);
        }

        return true;
    }


    auto TimeRangeSkipList::SearchInsertPoint(uint64_t time_start, uint64_t time_end,
                                              std::vector<InsertPoint> &insert_node_list) -> bool {
        std::vector<std::shared_ptr<TimeRangeNode>>node_list;
        auto first_node = SearchLastLessEquelNode(time_start,time_start);
        auto last_node = SearchLastLessEquelNode(time_end,time_end);
        if(first_node != header_ && first_node->time_range_.time_end_ + TIME_INTERVAL >= time_start){
            if(first_node->time_range_.time_end_<time_end){
              if(first_node->sid_info_ == nullptr){
                insert_node_list.emplace_back(first_node->time_range_.time_end_,std::min(first_node->forward_[0]->time_range_.time_start_,time_end),SLAB_INVALID_ID, nullptr);
              }else{
                insert_node_list.emplace_back(first_node->time_range_.time_end_,std::min(first_node->forward_[0]->time_range_.time_start_,time_end),first_node->GetLastSid(),first_node);
              }

            }
        }else{
            if(first_node->forward_[0]->time_range_.time_start_ == time_start){
                first_node = first_node->forward_[0];
                if(first_node->time_range_.time_end_ < time_end){
                   if(first_node->sid_info_ == nullptr){
                      insert_node_list.emplace_back(first_node->time_range_.time_end_,std::min(first_node->forward_[0]->time_range_.time_start_,time_end),SLAB_INVALID_ID, nullptr);
                    }else{
                      insert_node_list.emplace_back(first_node->time_range_.time_end_,std::min(first_node->forward_[0]->time_range_.time_start_,time_end),first_node->GetLastSid(),first_node);
                    }
                }
            }else{
                insert_node_list.emplace_back(time_start,std::min(first_node->forward_[0]->time_range_.time_start_,time_end),SLAB_INVALID_ID);
            }
        }

        if(first_node==last_node){
            return true;
        }
        for(auto it = first_node->forward_[0];it!=last_node;it=it->forward_[0]){
          if(it->sid_info_ == nullptr){
            insert_node_list.emplace_back(it->time_range_.time_end_,it->forward_[0]->time_range_.time_start_,SLAB_INVALID_ID);
          }else{
            insert_node_list.emplace_back(it->time_range_.time_end_,it->forward_[0]->time_range_.time_start_,it->GetLastSid(),it);
          }
        }
        if(time_end > last_node->time_range_.time_end_){
          if(last_node->sid_info_ == nullptr){
            insert_node_list.emplace_back(last_node->time_range_.time_end_,time_end,SLAB_INVALID_ID);
          }else {
            insert_node_list.emplace_back(last_node->time_range_.time_end_, time_end,
                                          last_node->GetLastSid(), last_node);
          }
        }
        return true;
    }
}