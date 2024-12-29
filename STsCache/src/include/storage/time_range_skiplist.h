#pragma once
#ifndef TIME_RANGE_SKIPLIS_H
#define TIME_RANGE_SKIPLIS_H
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>
#include "random.h"
#include <iostream>
#include <cassert>
#include <algorithm>
#include "common/macros.h"

namespace SemanticCache {

    class SemanticNode;
    class SemanticEdge;
    class SemanticGraph;

    struct TimeRange{
        uint64_t time_start_;
        uint64_t time_end_;
        TimeRange(uint64_t time_start=0,uint64_t time_end=0){
            time_start_ = time_start;
            time_end_ = time_end;
        }

        TimeRange(const TimeRange &other){
            time_start_ = other.time_start_;
            time_end_ = other.time_end_;
        }

        bool operator<(const TimeRange& other){
            if(this->time_start_ == other.time_start_){
                return this->time_end_ < other.time_end_;
            }
            return this->time_start_ < other.time_start_;
        }

        bool operator==(const TimeRange& other){
            if((this->time_start_ == other.time_start_)&&(this->time_end_ == other.time_end_)){
                return true;
            }
            return false;
        }

        bool operator!=(const TimeRange& other){
            if((this->time_start_ != other.time_start_)||(this->time_end_ != other.time_end_)){
                return true;
            }
            return false;
        }
        TimeRange& operator=(const TimeRange& other){
            time_start_ = other.time_start_;
            time_end_ = other.time_end_;
            return *this;
        }
    };
    class TimeRangeSkipList;
    class TimeRangeNode;

    struct SidInfo{
        uint64_t time_start_;
        uint64_t time_end_;
        uint32_t sid_;
        SidInfo(uint64_t time_start,uint64_t time_end,uint32_t sid){
            time_start_ = time_start;
            time_end_ = time_end;
            sid_ = sid;
        }

        SidInfo(const SidInfo & other){
            time_start_ = other.time_start_;
            time_end_ = other.time_end_;
            sid_ = other.sid_;
        }

        SidInfo& operator=(const SidInfo& other) {
            if (this != &other) {
                time_start_ = other.time_start_;
                time_end_ = other.time_end_;
                sid_ = other.sid_;
            }
            return *this;
        }
    };
    struct InsertPoint{
        std::shared_ptr<TimeRangeNode>node_ptr_;
        uint64_t time_start_;
        uint64_t time_end_;
        uint32_t sid_;
        InsertPoint(uint64_t time_start,uint64_t time_end,uint32_t sid){
            time_start_ = time_start;
            time_end_ = time_end;
            sid_ = sid;
            node_ptr_ = nullptr;
        }
        InsertPoint(uint64_t time_start,uint64_t time_end,uint32_t sid,std::shared_ptr<TimeRangeNode> node_ptr){
            time_start_ = time_start;
            time_end_ = time_end;
            sid_ = sid;
            node_ptr_ = node_ptr;
        }

        InsertPoint(const InsertPoint & other){
            time_start_ = other.time_start_;
            time_end_ = other.time_end_;
            sid_ = other.sid_;
            node_ptr_ = other.node_ptr_;
        }

        InsertPoint& operator=(const InsertPoint& other) {
            if (this != &other) {
                time_start_ = other.time_start_;
                time_end_ = other.time_end_;
                sid_ = other.sid_;
                node_ptr_ = other.node_ptr_;
            }
            return *this;
        }
    };


    class TimeRangeNode{
        friend  class TimeRangeSkipList;
    public:
        TimeRangeNode(uint64_t time_start,uint64_t time_end,std::shared_ptr<std::vector<SidInfo>>sids= nullptr,uint8_t node_level = 1);

        TimeRangeNode(const TimeRangeNode &tr_node);
        inline auto GetTimeRange() const -> const TimeRange&{
            return time_range_;
        }
        inline auto GetNodeLevel() const ->uint8_t {
            return node_level_;
        }
        inline auto SetEndTime(uint64_t time_end)->void{
            time_range_.time_end_ = time_end;
            (sid_info_->end()-1)->time_end_ = time_end;
        }
        inline auto SetNodeEndTime(uint64_t time_end)->void{
          time_range_.time_end_ = time_end;
        }
        inline auto SetEndTime(uint64_t node_time_end,uint64_t sid_time_end)->void{
            time_range_.time_end_ = node_time_end;
            (sid_info_->end()-1)->time_end_ = sid_time_end;
        }

        inline auto GetSidInfo() const -> const std::shared_ptr<std::vector<SidInfo>>&{
            return sid_info_;
        }
        inline auto GetLastSid() -> uint32_t {
            return (*sid_info_)[sid_info_->size()-1].sid_;
        }

        inline auto BinarySearchLowerBound(const std::vector<SidInfo>& nums, uint64_t target) ->uint32_t {
            int32_t left = 0;
            int32_t right = nums.size() - 1;
            uint32_t result = 0;

            while (left <= right) {
                uint32_t mid = (right + left) / 2;

                if (nums[mid].time_start_ <= target) {
                    result = mid;
                    left = mid + 1;
                } else {
                    right = mid - 1;
                }
            }

            return result;
        }
        inline auto SearchAllSidInNode(uint64_t time_start,uint64_t time_end,std::vector<uint32_t>&sids)-> bool{
            auto start_bound = BinarySearchLowerBound(*sid_info_,time_start);
            auto end_bound = BinarySearchLowerBound(*sid_info_,time_end);
            if(start_bound>end_bound){
                return false;
            }
            if((sid_info_->begin()+start_bound)->time_end_<time_start){
                start_bound++;
            }
            for(uint32_t i =start_bound;i<=end_bound;i++){
              if((sid_info_->begin()+i)->time_start_>time_end||(sid_info_->begin()+i)->time_end_<time_start){
                break;
              }
                sids.emplace_back((sid_info_->begin()+i)->sid_);
            }
            return true;
        }

        inline auto SearchAllSidInfoInNode(uint64_t time_start,uint64_t time_end,std::vector<SidInfo>&sids)-> bool{
          if(sid_info_ == nullptr){
              return true;
          }
          auto start_bound = BinarySearchLowerBound(*sid_info_,time_start);
          auto end_bound = BinarySearchLowerBound(*sid_info_,time_end);
          if(start_bound>end_bound){
            return false;
          }
          if((sid_info_->begin()+start_bound)->time_end_<time_start){
            start_bound++;
          }
          sids.reserve(end_bound-start_bound+2);
          for(uint32_t i =start_bound;i<=end_bound;i++){
            if((sid_info_->begin()+i)->time_start_>=time_end||(sid_info_->begin()+i)->time_end_<=time_start){
              break;
            }
            sids.emplace_back(*(sid_info_->begin()+i));
          }
          return true;
        }

        inline auto SetSidInfo(std::shared_ptr<std::vector<SidInfo>> &new_sid_info)->void {
            this->sid_info_ = new_sid_info;
        }

        inline auto Lock() ->void{
            mtx.lock();
        }
        inline auto UnLock()->void{
          mtx.unlock();
        }

        bool operator<(const TimeRangeNode& tr_node) const;

    private:
        std::vector<std::shared_ptr<TimeRangeNode>>forward_;
        TimeRange time_range_;
        std::shared_ptr<std::vector<SidInfo>>sid_info_{nullptr};
        uint32_t node_level_;
        std::mutex mtx;
    };


    class TimeRangeSkipList{
    public:
        TimeRangeSkipList();
        ~TimeRangeSkipList();
        auto CreateNode(uint8_t level, std::shared_ptr<TimeRangeNode> &new_node) -> void;
        auto CreateNode(uint64_t time_start,uint64_t time_end,uint8_t level, std::shared_ptr<TimeRangeNode> &new_node) -> void;
        inline auto GetHeader() const -> const std::shared_ptr<TimeRangeNode>&{
            return this->header_;
        }
        inline auto GetFooter() const -> const std::shared_ptr<TimeRangeNode>&{
            return this->footer_;
        }
        inline auto GetNodeCount() const -> uint32_t{
            return node_count_;
        }
        inline auto GetLevel() const -> uint32_t {
            return level_;
        }
        auto CreateList()-> void;
        inline auto GetRandomLevel() -> uint32_t{
            uint32_t level = static_cast<uint32_t>(rnd_.Uniform(MAX_LEVEL_));
            if (level == 0) {
                level = 1;
            }
            return level;
        }
        auto InsertNode(std::shared_ptr<TimeRangeNode> &new_node) -> bool;
        auto InsertNode(uint64_t time_start,uint64_t time_end,std::shared_ptr<std::vector<SidInfo>>sid_info = nullptr) -> bool;
        auto SearchEquelNode(uint64_t time_start,uint64_t time_end) const -> std::shared_ptr<TimeRangeNode>;
        auto SearchLastLessEquelNode(uint64_t time_start,uint64_t time_end) -> std::shared_ptr<TimeRangeNode>;
        auto SearchAllNode(uint64_t time_start,uint64_t time_end) -> std::vector<std::shared_ptr<TimeRangeNode>>;
        auto SearchHitNode(uint64_t time_start,uint64_t time_end) -> std::pair< std::vector<std::shared_ptr<TimeRangeNode>>,TimeRange>;
        auto SearchAllHitNode(uint64_t time_start,uint64_t time_end)->std::shared_ptr<TimeRangeNode>;
        auto SearchAllNode(uint64_t time_start,uint64_t time_end,std::vector<std::shared_ptr<TimeRangeNode>> &node_list) -> bool;

        auto DeleteNode(uint64_t time_start,uint64_t time_end) -> bool;


        auto InsertSidInfo(uint64_t time_start,uint64_t time_end,std::vector<SidInfo>&sids)->bool;
        auto InsertSidInfo1(uint64_t time_start,uint64_t time_end,std::vector<SidInfo>&sids)->std::shared_ptr<TimeRangeNode>;

        auto InsertEmptyNode(uint64_t time_start,uint64_t time_end) -> bool;

        auto DeleteSidInfo(uint64_t time_start,uint64_t time_end)->bool;

        auto DeleteSidInfo(uint64_t time_start,uint64_t time_end,uint32_t sid)->bool;

        auto DeleteSidInfo1(uint64_t time_start,uint64_t time_end,uint32_t sid)->bool;

        auto SearchAllSidInfo(uint64_t time_start,uint64_t time_end,std::vector<uint32_t>&sids)->bool;

        auto SetSidInfoEndTime(uint64_t time_start,uint64_t time_end,uint64_t new_time_end,uint64_t last_time)->bool;

        auto SearchInsertPoint(uint64_t time_start,uint64_t time_end,std::vector<InsertPoint>&insert_node_list) -> bool;

        inline auto PrintAllTimeRange()->bool{
          uint64_t time_start = header_->time_range_.time_start_;
          uint64_t time_end = header_->time_range_.time_end_;
          std::cout<<"========================================"<<std::endl;
          for(auto it = header_->forward_[0];it!=footer_;it=it->forward_[0]){
            time_start = it->time_range_.time_start_;
            if(time_end+TIME_INTERVAL>=time_start){
                          std::cout<<"node_time_end: "<<time_start<<std::endl;
            }
            std::cout<<"node_time_start: "<<it->time_range_.time_start_<<"    node_time_end: "<<it->time_range_.time_end_<<std::endl;
            std::cout<<"-----------------------------------------"<<std::endl;
            if(it->sid_info_== nullptr){
              continue;
            }
            for(auto idx : *it->sid_info_){
              std::cout<<idx.time_start_<<" "<<idx.time_end_<<" "<<idx.sid_<<std::endl;
            }
            std::cout<<"-----------------------------------------"<<std::endl;
            time_end = it->time_range_.time_end_;
          }
          std::cout<<"========================================"<<std::endl;
          return true;
        }
    private:
        uint32_t level_;
        std::shared_ptr<TimeRangeNode> header_;
        std::shared_ptr<TimeRangeNode> footer_;
        uint32_t node_count_;
        static const uint32_t MAX_LEVEL_ = 16;
        Random rnd_;
    };

}

#endif
