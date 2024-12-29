#include "storage/semantic_graph.h"

namespace SemanticCache {
class SemanticNode;
class SemanticEdge;
class SemanticGraph;
SemanticNode::SemanticNode(std::string hash_code, std::string metric_name,
                           SchemaRef metadata, bool node_is_atomic,
                           std::shared_ptr<Predicates> predicates,
                           Aggregation_Type aggregation_type,
                           Aggregation_Interval_Type aggregation_interval_type,uint32_t aggreation_interval_length)
    : out_edges_(), in_edges_() {
  this->hash_code_ = hash_code;
  this->metric_name_ = metric_name;
  this->metadata_ = metadata;
  std::vector<Column> temp_column = metadata->GetColumns();
  for (uint32_t i = 0; i < temp_column.size(); i++) {
    field_name_set_.insert(temp_column[i].GetName());
  }
  this->time_range_skiplist_ptr_ = std::make_shared<TimeRangeSkipList>();
  this->predicates_ = predicates;
  this->aggregation_type_ = aggregation_type;
  this->aggreation_interval_type_ = aggregation_interval_type;
  this->aggreation_interval_length_ = aggreation_interval_length;
}
SemanticNode::~SemanticNode() =default;

SemanticNode::SemanticNode(const SemanticCache::SemanticNode &sem) {
  this->hash_code_ = sem.hash_code_;
  this->metric_name_set_ = sem.metric_name_set_;
  this->metric_name_ = sem.metric_name_;
  this->metadata_ = sem.metadata_;
  this->field_name_set_ = sem.field_name_set_;
  this->time_range_skiplist_ptr_ = sem.time_range_skiplist_ptr_;
  this->predicates_ = sem.predicates_;
  this->aggregation_type_ = sem.aggregation_type_;
  this->aggreation_interval_type_ = sem.aggreation_interval_type_;
}


auto PrintAggregationType(Aggregation_Type aggregation_type) -> std::string {
  if (aggregation_type == MAX) {
    return "MAX";
  } else if (aggregation_type == MIN) {
    return "MIN";
  } else if (aggregation_type == COUNT) {
    return "COUNT";
  } else if (aggregation_type == SUM) {
    return "SUM";
  } else if (aggregation_type == MEAN) {
    return "MEAN";
  } else if (aggregation_type == LAST) {
    return "LAST";
  } else if (aggregation_type == FIRST) {
    return "FRIST";
  } else {
    return "NONE";
  }
}

auto PrintAggregationIntervalType(
    Aggregation_Interval_Type aggregation_interval_type) -> std::string {
  if (aggregation_interval_type == SECOND) {
    return "SECOND";
  } else if (aggregation_interval_type == MINUTE) {
    return "MINUTE";
  } else if (aggregation_interval_type == HOUR) {
    return "HOUR";
  } else if (aggregation_interval_type == DAY) {
    return "DAY";
  } else if (aggregation_interval_type == WEEK) {
    return "WEEK";
  } else if (aggregation_interval_type == MONTH) {
    return "MONTH";
  } else if (aggregation_interval_type == YEAR) {
    return "YEAR";
  } else {
    return "NATURAL";
  }
}

auto SemanticNode::PrintSemanticNode() const -> void {
  std::cout << "=====================================\n";
  std::cout << "hash_code:" << this->hash_code_ << std::endl;
  std::cout << "=====================================\n";
  std::cout << "Semantic Segment:\n";
  std::cout << "Metric:\n" << this->metric_name_<<std::endl;
  std::cout << "Field:\n";
  for (auto it : this->field_name_set_) {
    std::cout << it << " ";
  }
  std::cout << std::endl;
  std::cout << "Predicate:\n";
  if (this->predicates_ != nullptr) {
    for (auto it : this->predicates_->v_pres_) {
      std::cout << it.ToString() << std::endl;
    }
  } else {
    std::cout << "NO Predicate\n";
  }
  std::cout << "Aggregation:\n";
  std::cout << "AggregationType: "
            << PrintAggregationType(this->aggregation_type_)
            << " AggregationIntervalType: "
            << PrintAggregationIntervalType(this->aggreation_interval_type_)
            << " AggregationIntervalLength: "
            << this->aggreation_interval_length_
            << std::endl;
}

EdgeWeight::EdgeWeight(const SemanticCache::EdgeWeight &edgeWeight) {
  this->submetadata_ = edgeWeight.submetadata_;
  this->edge_weight_type_ = edgeWeight.edge_weight_type_;
  this->edge_compute_type_ = edgeWeight.edge_compute_type_;
  this->edge_intersection_type_ = edgeWeight.edge_intersection_type_;
}

EdgeWeight::EdgeWeight(SemanticCache::EDGE_WEIGHT_TYPE edge_weight_type,
                       SemanticCache::EDGE_COMPUTE_TYPE edge_compute_type,
                       SemanticCache::EDGE_INTERSECTION_TYPE edge_intersection_type,
                       SemanticCache::SchemaRef submetadata) {
    this->submetadata_ = submetadata;
    this->edge_weight_type_ = edge_weight_type;
    this->edge_intersection_type_ = edge_intersection_type;
    this->edge_compute_type_ = edge_compute_type;
}

EdgeWeight &EdgeWeight::operator=(const SemanticCache::EdgeWeight &edgeWeight) {
  if (this == &edgeWeight) {
    return *this;
  }
  this->edge_weight_type_ = edgeWeight.edge_weight_type_;
  this->edge_compute_type_ = edgeWeight.edge_compute_type_;
  this->edge_intersection_type_ = edgeWeight.edge_intersection_type_;
  this->submetadata_ = edgeWeight.submetadata_;
  return *this;
}

SemanticEdge::SemanticEdge(
    const std::shared_ptr<SemanticNode> &source_node,
    const std::shared_ptr<SemanticNode> &destination_node,
    SemanticCache::EdgeWeight &edge_weight)
    : edge_weight_(edge_weight) {
  this->source_node_ = source_node;
  this->destination_node_ = destination_node;
}

SemanticEdge::~SemanticEdge() = default;


SemanticGraph::SemanticGraph() : hash_table_() {
  this->edge_num_ = 0;
  this->node_num_ = 0;
}

SemanticGraph::~SemanticGraph() = default;

auto SemanticGraph::SemanticSegmentToNode(std::string segment) -> std::shared_ptr<SemanticNode> {
    std::vector<std::string> segments = ExtractBraceContents(segment);
    bool  node_is_atomic = false;
    if(std::count(segments[0].begin(), segments[0].end(),'(') == 1){
        node_is_atomic = true;
    }
    std::vector<SemanticCache::Column>cols;
    std::stringstream field_str(segments[1]);
    std::string temp;
    while (std::getline(field_str, temp, ',')) {
        size_t start = temp.find('[');
        size_t end = temp.find(']');
        std::string field_name = temp.substr(0, start);
        std::string field_type = temp.substr(start + 1, end - start - 1);
        SemanticCache::DATA_TYPE data_type;
        if(field_type == "float64"){
            data_type = SemanticCache::DATA_TYPE::DECIMAL;
        }else if(field_type == "int64"){
            data_type = SemanticCache::DATA_TYPE::BIGINT;
        }else if(field_type == "bool"){
            data_type = SemanticCache::DATA_TYPE::BOOLEAN;
        }else{
            data_type = SemanticCache::DATA_TYPE::VARCHAR;
        }
        cols.emplace_back(SemanticCache::Column(field_name,data_type));
    }
    SemanticCache::Schema metadata(cols);

    std::vector<SemanticCache::Predicate> preds;
    std::optional<std::shared_ptr<SemanticCache::Predicates>>predicates;
    if(segments[2] != "empty"){
        std::vector<std::string> pred_str = ExtractContentInParentheses(segments[2]);
        for(auto temp:pred_str){
            size_t start = temp.find('[');
            size_t end = temp.find(']');
            std::string field_name = temp.substr(0, start);
            std::string field_type = temp.substr(start + 1, end - start - 1);
            InequalityElements element = ExtractVariablesSymbolConstant(field_name);
            SemanticCache::PREDICATE_OP_TYPE op_type;
            SemanticCache::DATA_TYPE data_type;
            SemanticCache::OpNumValue constant;
            if(element.op == ">"){
                op_type = SemanticCache::PREDICATE_OP_TYPE ::Greater;
            }else if(element.op == ">="){
                op_type = SemanticCache::PREDICATE_OP_TYPE ::Greater_And_Equel;
            }else if(element.op == "="){
                op_type = SemanticCache::PREDICATE_OP_TYPE ::Equel;
            }else if(element.op == "<="){
                op_type = SemanticCache::PREDICATE_OP_TYPE ::Less_And_Equel;
            }else{
                op_type = SemanticCache::PREDICATE_OP_TYPE ::Less;
            }

            if(field_type == "string"){
                SemanticCache::Column col(element.variable,SemanticCache::DATA_TYPE::VARCHAR);
                SemanticCache::Predicate pred(std::make_shared<SemanticCache::Column>(col),op_type,constant,element.constant);
                preds.emplace_back(pred);
            }else{
                if(field_type == "float64"){
                    data_type = SemanticCache::DATA_TYPE::DECIMAL;
                    constant.float_value = std::stod(element.constant);
                }else if(field_type == "int64"){
                    data_type = SemanticCache::DATA_TYPE::BIGINT;
                    constant.int_value = std::stoi(element.constant);
                }else{
                    data_type = SemanticCache::DATA_TYPE::BOOLEAN;
                    if(element.constant == "false"){
                        constant.bool_value = false;
                    }else{
                        constant.bool_value = true;
                    }
                }
                SemanticCache::Column col(element.variable,data_type);
                SemanticCache::Predicate pred(std::make_shared<SemanticCache::Column>(col),op_type,constant,element.constant);
                preds.push_back(pred);
            }
        }
        predicates.emplace(std::make_shared<SemanticCache::Predicates>(SemanticCache::Predicates(preds)));
    }

    SemanticCache::Aggregation_Type agg_type = SemanticCache::Aggregation_Type::NONE;
    SemanticCache::Aggregation_Interval_Type agg_interval_type = SemanticCache::Aggregation_Interval_Type::NATURAL;
    uint32_t agg_interval_length = 1;
    std::stringstream aggregate_str(segments[3]);
    std::getline(aggregate_str, temp, ',');
    if(temp!="empty"){
        if(temp == "max"){
            agg_type = SemanticCache::Aggregation_Type::MAX;
        }else if(temp == "min"){
            agg_type = SemanticCache::Aggregation_Type::MIN;
        }else if(temp == "count"){
            agg_type = SemanticCache::Aggregation_Type::COUNT;
        }else if(temp == "sum"){
            agg_type = SemanticCache::Aggregation_Type::SUM;
        }else if(temp == "mean"){
            agg_type = SemanticCache::Aggregation_Type::MEAN;
        }else{
            agg_type = SemanticCache::Aggregation_Type::NONE;
        }
        std::getline(aggregate_str, temp, ',');
        if(temp != "empty"){
            size_t pos = 0;
            agg_interval_length = std::stoi(temp, &pos);
            if (pos < temp.size()) {
                std::string unit = temp.substr(pos);
                if(unit == "s"){
                    agg_interval_type = SemanticCache::Aggregation_Interval_Type::SECOND;
                }else if(unit == "m"){
                    agg_interval_type = SemanticCache::Aggregation_Interval_Type::MINUTE;
                }else if(unit == "h"){
                    agg_interval_type = SemanticCache::Aggregation_Interval_Type::HOUR;
                }else if(unit == "D"){
                    agg_interval_type = SemanticCache::Aggregation_Interval_Type::DAY;
                }else if(unit == "M"){
                    agg_interval_type = SemanticCache::Aggregation_Interval_Type::MONTH;
                }else if(unit == "Y"){
                    agg_interval_type = SemanticCache::Aggregation_Interval_Type::YEAR;
                }
            }
        }

    }
    if(predicates.has_value()){
        return std::make_shared<SemanticNode>(segment,segments[0],std::make_shared<Schema>(metadata),node_is_atomic,predicates.value(),agg_type,agg_interval_type,agg_interval_length);
    }else{
        return std::make_shared<SemanticNode>(segment,segments[0],std::make_shared<Schema>(metadata),node_is_atomic, nullptr,agg_type,agg_interval_type,agg_interval_length);
    }

}

bool isSubset(const std::set<std::string> &set1,
              const std::set<std::string> &set2) {
  for (const auto &element : set1) {
    if (set2.find(element) == set2.end()) {
      return false;
    }
  }
  return true;
}

std::set<std::string> IntersectionSet(const std::set<std::string> set1,
                                      const std::set<std::string> set2) {
  std::set<std::string> intersection_set;
  for (const auto &it : set1) {
    if (set2.find(it) != set2.end()) {
      intersection_set.insert(it);
    }
  }
  return intersection_set;
}

auto SemanticGraph::FindAtomicRelationships(
    const std::shared_ptr<SemanticNode> &new_node,
    const std::shared_ptr<SemanticNode> &node)
    -> std::pair<std::shared_ptr<EdgeWeight>,std::shared_ptr<EdgeWeight>>{

  if (new_node == nullptr || node == nullptr)
    return std::make_pair(nullptr,nullptr);

  if (new_node->GetMetricName() != node->GetMetricName())return std::make_pair(nullptr,nullptr);

  if (new_node->aggregation_type_ != node->aggregation_type_ &&
      (new_node->aggregation_type_ != NONE &&
       node->aggregation_type_ != NONE)) {
    return std::make_pair(nullptr,nullptr);
  }

  std::set<std::string> sub_field_set =
      IntersectionSet(new_node->field_name_set_, node->field_name_set_);
  if (sub_field_set.size() == 0)
    return std::make_pair(nullptr,nullptr);

  EDGE_INTERSECTION_TYPE intersection_type1 = Intersection;
  EDGE_INTERSECTION_TYPE intersection_type2 = Intersection;
  bool flag1 = isSubset(new_node->field_name_set_,node->field_name_set_);
  bool flag2 = isSubset(node->field_name_set_,new_node->field_name_set_);
  if(flag1 == true && flag2 == false){
      intersection_type1 = My_Subset;
      intersection_type2 = Totally_Contained;
  }
  if(flag1 == false && flag2 == true){
      intersection_type1 = Totally_Contained;
      intersection_type2 = My_Subset;
  }
  if(flag1==true&&flag2==true){
      intersection_type1=Same;
      intersection_type2=Same;
  }
  std::shared_ptr<Schema> sub_schema =Schema::SubSchema(new_node->metadata_,node->metadata_);
  if(new_node->aggregation_type_ == node->aggregation_type_){
      if((new_node->predicates_ == nullptr && node->predicates_ == nullptr)||new_node->predicates_ == node->predicates_){
          if(new_node->aggreation_interval_type_ < node->aggreation_interval_type_||(new_node->aggreation_interval_type_ == node->aggreation_interval_type_&&new_node->aggreation_interval_length_<node->aggreation_interval_length_)){
              std::shared_ptr<EdgeWeight> edge_weight_ptr = std::make_shared<EdgeWeight>(Atomic_To_Atomic,Aggregation_Compute,intersection_type1,sub_schema);
              return std::make_pair(edge_weight_ptr, nullptr);
          }else if(new_node->aggreation_interval_type_ > node->aggreation_interval_type_||(new_node->aggreation_interval_type_ == node->aggreation_interval_type_&&new_node->aggreation_interval_length_>node->aggreation_interval_length_)){
              std::shared_ptr<EdgeWeight> edge_weight_ptr = std::make_shared<EdgeWeight>(Atomic_To_Atomic,Aggregation_Compute,intersection_type2,sub_schema);
              return std::make_pair(nullptr,edge_weight_ptr);
          }else{
              std::shared_ptr<EdgeWeight> edge_weight_ptr1 = std::make_shared<EdgeWeight>(Atomic_To_Atomic,Not_Compute,intersection_type1,sub_schema);
              std::shared_ptr<EdgeWeight> edge_weight_ptr2 = std::make_shared<EdgeWeight>(Atomic_To_Atomic,Not_Compute,intersection_type2,sub_schema);
              return std::make_pair(edge_weight_ptr1,edge_weight_ptr2);
          }
      }else{
          if(new_node->aggreation_interval_type_ == node->aggreation_interval_type_){
              if(new_node->predicates_ == nullptr && node->predicates_){
                  std::shared_ptr<EdgeWeight> edge_weight_ptr = std::make_shared<EdgeWeight>(Atomic_To_Atomic,Predicate_Compute,intersection_type1,sub_schema);
                  return std::make_pair(edge_weight_ptr, nullptr);
              }else if(new_node->predicates_ && node->predicates_ == nullptr){
                  std::shared_ptr<EdgeWeight> edge_weight_ptr = std::make_shared<EdgeWeight>(Atomic_To_Atomic,Predicate_Compute,intersection_type2,sub_schema);
                  return std::make_pair(nullptr,edge_weight_ptr);
              }else{
                  if(new_node->predicates_->IsTotallyContained(node->predicates_)){
                      std::shared_ptr<EdgeWeight> edge_weight_ptr = std::make_shared<EdgeWeight>(Atomic_To_Atomic,Predicate_Compute,intersection_type2,sub_schema);
                      return std::make_pair(nullptr,edge_weight_ptr);
                  }else if(node->predicates_->IsTotallyContained(new_node->predicates_)){
                      std::shared_ptr<EdgeWeight> edge_weight_ptr = std::make_shared<EdgeWeight>(Atomic_To_Atomic,Predicate_Compute,intersection_type1,sub_schema);
                      return std::make_pair(edge_weight_ptr, nullptr);
                  }
              }
          }
      }
  }else if(new_node->aggregation_type_== NONE || node->aggregation_type_ ==NONE){
      if((new_node->predicates_ == nullptr && node->predicates_ == nullptr)||new_node->predicates_ == node->predicates_){
          if(new_node->aggregation_type_ == NONE){
              std::shared_ptr<EdgeWeight> edge_weight_ptr1 = std::make_shared<EdgeWeight>(Atomic_To_Atomic,Aggregation_Compute,intersection_type1,sub_schema);
              return std::make_pair(edge_weight_ptr1, nullptr);
          }else{
              std::shared_ptr<EdgeWeight> edge_weight_ptr2 = std::make_shared<EdgeWeight>(Atomic_To_Atomic,Aggregation_Compute,intersection_type2,sub_schema);
              return std::make_pair(nullptr,edge_weight_ptr2);
          }
      }
  }
  return std::make_pair(nullptr,nullptr);
}

auto SemanticGraph::BuildRelationships(
    const std::shared_ptr<SemanticNode> &new_node) -> void {
  if (new_node == nullptr)
    return;

    for (auto it : hash_table_) {
        if(new_node->hash_code_ == it.first) continue;
        auto edge_weight_pair = FindAtomicRelationships(new_node, it.second);
        if(edge_weight_pair.first){
            auto edge_ptr = std::make_shared<SemanticEdge>(new_node,it.second,*edge_weight_pair.first);
            this->AddEdge(edge_ptr);
        }
        if(edge_weight_pair.second){
            auto edge_ptr = std::make_shared<SemanticEdge>(it.second,new_node,*edge_weight_pair.second);
            this->AddEdge(edge_ptr);
        }
    }
}

auto SemanticGraph::PrintGraph() const -> void {
  for (auto it : hash_table_) {
    it.second->PrintSemanticNode();
  }
}
}