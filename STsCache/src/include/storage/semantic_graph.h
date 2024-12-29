#pragma once
#ifndef SEMANTIC_GRAPH_H
#define SEMANTIC_GRAPH_H
#include "catalog/predicate.h"
#include "catalog/schema.h"
#include "common/util/hash_util.h"
#include <unordered_map>
#include <memory>
#include <vector>
#include "type/tuple.h"
#include "storage/time_range_skiplist.h"
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <regex>

namespace SemanticCache {

class SemanticNode;
class SemanticEdge;
class SemanticGraph;
class SemanticNode {
  friend class SemanticGraph;

public:
  SemanticNode(std::string hash_code, std::string metric_name, SchemaRef metadata,
               bool node_is_atomic, std::shared_ptr<Predicates> predicates,
               Aggregation_Type aggregation_type,
               Aggregation_Interval_Type aggreation_internal_type,uint32_t aggreation_interval_length=1);

  SemanticNode(const SemanticNode &sem);
  ~SemanticNode();


  inline auto AddInEdge(const std::shared_ptr<SemanticEdge> &edge) -> void{
      this->in_edges_.push_back(edge);
  }

  auto AddOutEdge(const std::shared_ptr<SemanticEdge> &edge) -> void{
      this->out_edges_.push_back(edge);
  }

  inline auto SetCid(uint8_t cid) -> void{
      cid_ = cid;
  }

  inline auto SetTimeSkiplistPtr(const std::string &seg,std::shared_ptr<TimeRangeSkipList>&ptr) -> void{
      skiplist_hash_table_[seg] = ptr;
  }
  inline auto GetTimeRangeSkiplistPtrBySm(const std::string &sm)->std::shared_ptr<TimeRangeSkipList>{
    auto input = hash_code_;
   size_t start_pos = input.find('{');
    size_t end_pos = input.find('}');
    if(start_pos < end_pos){
      std::string other = input.substr(end_pos+1,input.size()-2-(end_pos - start_pos - 1));
      auto seg = "{"+sm+"}" + other;
      if(skiplist_hash_table_.find(seg)!=skiplist_hash_table_.end()){
        return skiplist_hash_table_[seg];
      }else{
        return nullptr;
      }
    }
    return nullptr;
  }

  inline auto GetInEdges() const -> const std::vector<std::shared_ptr<SemanticEdge>> &{
      return this->in_edges_;
  }

  inline auto GetOutEdges() const
      -> const std::vector<std::shared_ptr<SemanticEdge>> &{
      return this->out_edges_;
  }

  inline auto GetTimeRangeSkiplistPtr() const
      -> const std::shared_ptr<TimeRangeSkipList> & {
      return this->time_range_skiplist_ptr_;
  }

  inline auto GetTimeRangeSkiplistPtr(const std::string &seg) -> const std::shared_ptr<TimeRangeSkipList>&  {
    return this->skiplist_hash_table_[seg];
  }

  inline auto GetHashCode() const -> const std::string &{
    return this->hash_code_;
  }


  inline auto GetRowLength() const -> uint32_t{
      return this->metadata_->GetLength();
  }

  inline auto GetCid() const -> uint8_t { return this->cid_; }

  inline auto GetMetricNameSet() const -> const std::set<std::string> &{
      return this->metric_name_set_;
  }

  inline auto GetMetricName() const -> const std::string & {
      return this->metric_name_;
  }

  inline auto GetMetadata() const -> const Schema & {
      return *this->metadata_;
  }

  inline auto GetMetadataRef() const -> SchemaRef {
    return this->metadata_;
  }

  inline auto GetPrediactes() const -> const std::shared_ptr<Predicates> & {
      return this->predicates_;
  }

  inline auto GetFieldNameSet() const -> const std::set<std::string> & {
      return this->field_name_set_;
  }

  inline auto GetAggregationType() const -> const Aggregation_Type & {
      return this->aggregation_type_;
  }

  inline auto GetAggregationIntervalType() const -> const Aggregation_Interval_Type &{
      return this->aggreation_interval_type_;
  }

  auto GetAggreationIntervalLength() const -> uint8_t {
      return this->aggreation_interval_length_;
  }
  auto PrintSemanticNode() const -> void;

private:
  std::set<std::string> metric_name_set_;
  std::string metric_name_;

  std::string hash_code_;
  SchemaRef metadata_;

  std::set<std::string> field_name_set_;

  uint8_t cid_;
  std::shared_ptr<TimeRangeSkipList> time_range_skiplist_ptr_;
  std::unordered_map<std::string,std::shared_ptr<TimeRangeSkipList>>skiplist_hash_table_;
  std::vector<std::shared_ptr<SemanticEdge>> out_edges_;
  std::vector<std::shared_ptr<SemanticEdge>> in_edges_;
  std::shared_ptr<Predicates> predicates_;
  Aggregation_Type aggregation_type_;
  Aggregation_Interval_Type aggreation_interval_type_;
  uint32_t aggreation_interval_length_{1};
};

class EdgeWeight {
public:
  EdgeWeight();
  EdgeWeight(const EdgeWeight &edgeWeight);
  EdgeWeight(EDGE_WEIGHT_TYPE edge_weight_type,
             EDGE_COMPUTE_TYPE edge_compute_type,EDGE_INTERSECTION_TYPE edge_intersection_type, SchemaRef submetadata);
  EdgeWeight &operator=(const EdgeWeight &edgeWeight);
  EDGE_WEIGHT_TYPE edge_weight_type_{Atomic_To_Atomic};
  EDGE_COMPUTE_TYPE edge_compute_type_;
  EDGE_INTERSECTION_TYPE edge_intersection_type_;
  SchemaRef submetadata_;
};

class SemanticEdge {
public:
  SemanticEdge(const std::shared_ptr<SemanticNode> &source_node,
               const std::shared_ptr<SemanticNode> &destination_node,
               EdgeWeight &edge_weight);

  ~SemanticEdge();

  inline auto GetSourceNode() const -> const std::shared_ptr<SemanticNode> &{
      return this->source_node_;
  }

  inline auto GetDestinationNode() const -> const std::shared_ptr<SemanticNode> &{
      return this->destination_node_;
  }

  inline auto GetSourceNodeHashCode() const -> const std::string &{
      return this->source_node_->GetHashCode();
  }
  inline auto GetDestinationNodeHashCode() const ->const  std::string &{
      return this->destination_node_->GetHashCode();
  }

  inline auto GetEdgeWeight() const -> const EdgeWeight &{
      return this->edge_weight_;
  }
  std::shared_ptr<SemanticNode> source_node_;
  std::shared_ptr<SemanticNode> destination_node_;
  EdgeWeight edge_weight_;
};

class SemanticGraph {
public:
  SemanticGraph();

  ~SemanticGraph();

  auto SplitSegment(const std::string& input)-> std::shared_ptr<std::pair<std::vector<std::string>,std::string>> {
    size_t start_pos = input.find('{');
    size_t end_pos = input.find('}');
    if (start_pos < end_pos) {
      std::string sm = input.substr(start_pos + 1, end_pos - start_pos - 1);
      std::string other = input.substr(end_pos+1,input.size()-2-(end_pos - start_pos - 1));
      std::vector<std::string>segments;
      RebuildSegment(sm,other,segments);
      auto seg = "{("+ GetMetricName(sm)+".*)}"+other;
      return std::make_shared<std::pair<std::vector<std::string>,std::string>>(std::make_pair(segments,seg));
    }

    return nullptr;
  }

  auto GetMetricName(const std::string input)->std::string{
      std::size_t leftParenthesisPos = input.find('(');
      std::size_t dotPos = input.find('.');

      if (leftParenthesisPos != std::string::npos && dotPos != std::string::npos && leftParenthesisPos < dotPos) {
          std::size_t contentStartPos = leftParenthesisPos + 1;
          std::size_t contentLength = dotPos - contentStartPos;
          return input.substr(contentStartPos, contentLength);
      }

      return "";
  }

  auto RebuildSegment(const std::string& input,const std::string &other,std::vector<std::string>&result)->void {
    size_t startPos = input.find('(');
    size_t endPos = input.find(')');
    while (startPos != std::string::npos && endPos != std::string::npos) {
      if (startPos < endPos) {
        std::string content = "{"+input.substr(startPos, endPos - startPos+1)+"}"+other;
        result.push_back(content);
      }
      startPos = input.find('(', endPos);
      endPos = input.find(')', endPos + 1);
    }
  }


    std::vector<std::string> ExtractBraceContents(const std::string& input) {
        std::vector<std::string> result;
        size_t startPos = input.find('{');
        size_t endPos = input.find('}');
        while (startPos != std::string::npos && endPos != std::string::npos) {
            if (startPos < endPos) {
                std::string content = input.substr(startPos + 1, endPos - startPos - 1);
                result.push_back(content);
            }
            startPos = input.find('{', endPos);
            endPos = input.find('}', endPos + 1);
        }
        return result;
    }

    std::vector<std::string> ExtractContentInParentheses(const std::string& input) {
        std::vector<std::string> result;
        size_t startPos = input.find('(');
        size_t endPos = input.find(')');
        while (startPos != std::string::npos && endPos != std::string::npos) {
            if (startPos < endPos) {
                std::string content = input.substr(startPos + 1, endPos - startPos - 1);
                result.push_back(content);
            }
            startPos = input.find('(', endPos);
            endPos = input.find(')', endPos + 1);
        }
        return result;
    }

    struct InequalityElements {
        std::string variable;
        std::string op;
        std::string constant;
    };

    auto ExtractVariablesSymbolConstant(const std::string& inequality) -> InequalityElements{
        std::regex pattern1("([a-zA-Z_][a-zA-Z0-9_]*)\\s*([=<>]+)\\s*([-+]?[0-9]*\\.?[0-9]+)");
        std::regex pattern2("([a-zA-Z_][a-zA-Z0-9_]*) ([<>=!]+) ([a-zA-Z0-9]+)");

        std::smatch match;
        if (std::regex_search(inequality, match, pattern1)) {
            std::string variable = match[1];
            std::string symbol = match[2];
            std::string constant = match[3];

            return {match[1],match[2],match[3]};
        } else if(std::regex_search(inequality, match, pattern2)){
            std::string variable = match[1];
            std::string symbol = match[2];
            std::string constant = match[3];

            return {match[1],match[2],match[3]};
        } else {
            return {"","",""};
        }
    }

  auto SemanticSegmentToNode(std::string segment) -> std::shared_ptr<SemanticNode>;

  inline auto AddEdge(const std::shared_ptr<SemanticEdge> &new_edge) -> void{
      this->hash_table_[new_edge->GetSourceNodeHashCode()]->AddOutEdge(new_edge);
      this->hash_table_[new_edge->GetDestinationNodeHashCode()]->AddInEdge(new_edge);
      edge_num_++;
  }

  inline auto AddNode(const std::shared_ptr<SemanticNode> &new_node) -> void{
      this->hash_table_[new_node->GetHashCode()] = new_node;
      BuildRelationships(new_node);
      node_num_++;
  }

  auto FindAtomicRelationships(const std::shared_ptr<SemanticNode> &new_node,
                               const std::shared_ptr<SemanticNode> &node) -> std::pair<std::shared_ptr<EdgeWeight>,std::shared_ptr<EdgeWeight>>;

  auto BuildRelationships(const std::shared_ptr<SemanticNode> &new_node)
      -> void;

  inline auto GetNodeNum() -> uint32_t{
      return this->node_num_;
  }

  inline auto FindSemanticNode(const std::string &hash_code)
      -> const std::shared_ptr<SemanticNode> &{
      return this->hash_table_[hash_code];
  };

  inline auto IsExistNode(std::string hash_code) -> bool {
    return hash_table_.count(hash_code) > 0;
  }

  inline auto IsExistNonAtomicNode(std::string hash_code) -> bool {
    return hash_table_.count(hash_code) > 0;
  }

  inline auto GetHashTable()const -> const std::unordered_map<std::string, std::shared_ptr<SemanticNode>> &{
      return this->hash_table_;
  }

  auto PrintGraph() const -> void;

  std::unordered_map<std::string, std::shared_ptr<SemanticNode>> hash_table_;

  size_t node_num_{0};
  size_t edge_num_{0};
};

}

#endif
