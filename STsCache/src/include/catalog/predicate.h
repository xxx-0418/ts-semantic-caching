#ifndef SEMANTIC_CACHE_PREDICATE_H
#define SEMANTIC_CACHE_PREDICATE_H
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "catalog/column.h"
#include "common/macros.h"

namespace SemanticCache {
class SemanticNode;
class Predicate;
class Predicates;
using PredicatesRef = std::shared_ptr<Predicates>;
using PREDICATE_OP_TYPE = enum uint8 { Greater, Greater_And_Equel, Equel, Less, Less_And_Equel };
union OpNumValue {
  int int_value;
  double float_value;
  bool bool_value;
};
class Predicate {
 public:
  friend class Predicates;
  Predicate();
  Predicate(std::shared_ptr<Column> column, PREDICATE_OP_TYPE op_type, OpNumValue op_num_value,
            std::string op_str_value);
  Predicate(const Predicate &pre);
  Predicate &operator=(const Predicate &pre);
  auto operator==(const Predicate &pre) const -> bool;
  auto operator<(const Predicate &pre) const -> bool;
  auto IsContained(const std::shared_ptr<Predicate> pre) -> bool;
  auto ToString() -> std::string;

 private:
  std::shared_ptr<Column> column_;
  PREDICATE_OP_TYPE op_type_;
  bool is_string_{false};
  OpNumValue op_num_value_;
  std::string op_str_value_;
};
class Predicates {
  friend class SemanticNode;

 public:
  Predicates();
  explicit Predicates(const std::vector<Predicate> &v_pres);
  Predicates(const Predicates &pres);
  Predicates &operator=(const Predicates &pres);
  auto operator==(const Predicates &pres) const -> bool;
  auto GetPredicateNum()const->uint32_t;
  auto IsContainedPredicate(const Predicate &pre) ->bool;
  auto IsTotallyContained(const std::shared_ptr<Predicates> predicates) -> bool;

 private:
  std::vector<Predicate> v_pres_;
  std::set<Predicate> pres_set_;
  std::map<std::string, std::map<PREDICATE_OP_TYPE, uint32_t>> index_;
};
}
#endif