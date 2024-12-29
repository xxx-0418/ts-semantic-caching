#include "catalog/predicate.h"
#include <iostream>
namespace SemanticCache {
auto CompareOpNumValue(OpNumValue num1, OpNumValue num2, DATA_TYPE data_type) -> int {
  if (data_type == INTEGER) {
    if (num1.int_value < num2.int_value)
      return -1;
    else if (num1.int_value == num2.int_value)
      return 0;
    else
      return 1;
  } else if (data_type == DECIMAL) {
    if (num1.float_value < num2.float_value)
      return -1;
    else if (num1.float_value == num2.float_value)
      return 0;
    else
      return 1;
  } else {
    if (num1.bool_value == num2.bool_value)
      return 0;
    else if (num1.bool_value)
      return 1;
    else
      return -1;
  }
}
Predicate::Predicate(std::shared_ptr<Column> column, PREDICATE_OP_TYPE op_type,
                     OpNumValue op_num_value, std::string op_str_value) {
  this->column_ = column;
  this->op_type_ = op_type;
  if (this->column_) {
    if (this->column_->GetType() == VARCHAR) {
      this->is_string_ = true;
      this->op_str_value_ = op_str_value;
      this->op_num_value_ = op_num_value;
    } else {
      this->op_num_value_ = op_num_value;
    }
  }
}
Predicate::Predicate(const SemanticCache::Predicate &pre) {
  this->column_ = pre.column_;
  this->op_type_ = pre.op_type_;
  this->op_num_value_ = pre.op_num_value_;
  this->op_str_value_ = pre.op_str_value_;
  this->is_string_ = pre.is_string_;
}

Predicate &Predicate::operator=(const SemanticCache::Predicate &pre) {
  if (this == &pre) {
    return *this;
  }
  this->column_ = pre.column_;
  this->op_type_ = pre.op_type_;
  this->op_num_value_ = pre.op_num_value_;
  this->op_str_value_ = pre.op_str_value_;
  this->is_string_ = pre.is_string_;
  return *this;
}

auto Predicate::operator==(const SemanticCache::Predicate &pre) const -> bool {
  if ((this->column_ == pre.column_) == false) return false;
  if (this->op_type_ != pre.op_type_) return false;
  if (this->is_string_ != pre.is_string_) return false;
  if (this->is_string_) {
    if (this->op_str_value_ != pre.op_str_value_) return false;
  } else {
    if (CompareOpNumValue(this->op_num_value_, pre.op_num_value_, this->column_->GetType()) != 0)
      return false;
  }
  return true;
}

auto Predicate::operator<(const SemanticCache::Predicate &pre) const -> bool {
  if (this->column_->GetName() == pre.column_->GetName()) {
    if (this->op_type_ == pre.op_type_) {
      if (this->is_string_)
        return this->op_str_value_ < pre.op_str_value_;
      else
        return (CompareOpNumValue(this->op_num_value_, pre.op_num_value_,
                                  this->column_->GetType()) == -1);
    } else {
      return this->op_type_ < pre.op_type_;
    }
  }
  return this->column_->GetName() < this->column_->GetName();
}

auto Predicate::IsContained(const std::shared_ptr<Predicate> pre) -> bool {
  if (pre == nullptr) return true;
  if (this->column_->GetName() == pre->column_->GetName()) {
    if (this->op_type_ == Greater &&
        (pre->op_type_ == Greater || pre->op_type_ == Greater_And_Equel)) {
      int x = CompareOpNumValue(this->op_num_value_, pre->op_num_value_,
                                this->column_->GetType());
      if (x == -1 || x == 0) return true;
    } else if (this->op_type_ == Greater_And_Equel &&
               (pre->op_type_ == Greater || pre->op_type_ == Greater_And_Equel)) {
      int x = CompareOpNumValue(this->op_num_value_, pre->op_num_value_,
                                this->column_->GetType());
      if (pre->op_type_ == Greater_And_Equel) {
        if (x == -1 || x == 0) return true;
      } else {
        if (x == -1) return true;
      }
    } else if (this->op_type_ == Equel && pre->op_type_ == Equel) {
      if (this->is_string_) {
        if (this->op_str_value_ == pre->op_str_value_) return true;
      } else {
        int x = CompareOpNumValue(this->op_num_value_, pre->op_num_value_,
                                  this->column_->GetType());
        if (x == 0) return true;
      }
    } else if (this->op_type_ == Less &&
               (pre->op_type_ == Less || pre->op_type_ == Less_And_Equel)) {
      int x = CompareOpNumValue(this->op_num_value_, pre->op_num_value_,
                                this->column_->GetType());
      if (x == 0 || x == 1) return true;
    } else if (this->op_type_ == Less_And_Equel &&
               (pre->op_type_ == Less || pre->op_type_ == Less_And_Equel)) {
      int x = CompareOpNumValue(this->op_num_value_, pre->op_num_value_,
                                this->column_->GetType());
      if (pre->op_type_ == Less) {
        if (x == 0 || x == 1) return true;
      } else {
        if (x == 1) return true;
      }
    }
  }
  return false;
}

auto Predicate::ToString() -> std::string {
  std::string str = "";
  str += this->column_->GetName();

  if (this->op_type_ == Greater) {
    str += " > ";
  } else if (this->op_type_ == Greater_And_Equel) {
    str += " >= ";
  } else if (this->op_type_ == Equel) {
    str += " = ";
  } else if (this->op_type_ == Less_And_Equel) {
    str += " <= ";
  } else if (this->op_type_ == Less) {
    str += " < ";
  }
  if (is_string_) {
    str += this->op_str_value_;
  } else if (this->column_->GetType() == BOOLEAN) {
    if (this->op_num_value_.bool_value == true) {
      str += " TRUE ";
    } else {
      str += " FALSE ";
    }
  } else if (this->column_->GetType() == INTEGER) {
    str += std::to_string(this->op_num_value_.int_value);
  } else if (this->column_->GetType() == DECIMAL) {
    str += std::to_string(this->op_num_value_.float_value);
  }
  return str;
}

Predicates::Predicates(const std::vector<Predicate> &v_pres) {
  for (uint32_t i = 0; i < v_pres.size(); i++) {
    this->v_pres_.push_back(v_pres[i]);
    this->pres_set_.insert(v_pres[i]);
    this->index_[v_pres[i].column_->GetName()][v_pres[i].op_type_] = i;
  }
}

Predicates::Predicates(const SemanticCache::Predicates &pres) {
  v_pres_ = pres.v_pres_;
  pres_set_ = pres.pres_set_;
  index_ = pres.index_;
}

Predicates &Predicates::operator=(const SemanticCache::Predicates &pres) {
  if (this == &pres) {
    return *this;
  }
  this->v_pres_ = pres.v_pres_;
  this->index_ = pres.index_;
  return *this;
}

auto Predicates::operator==(const SemanticCache::Predicates &pres) const -> bool {
  return (this->pres_set_ == pres.pres_set_);
}

auto Predicates::GetPredicateNum() const -> uint32_t {
    return v_pres_.size();
}

auto Predicates::IsContainedPredicate(const SemanticCache::Predicate &pre) -> bool {
    if(pres_set_.find(pre)!=pres_set_.end()){
        return true;
    }
    return false;
}

auto Predicates::IsTotallyContained(const std::shared_ptr<Predicates> pres) -> bool {
  for (uint32_t i = 0; i < pres->v_pres_.size(); i++) {
    uint32_t temp = this->index_[pres->v_pres_[i].column_->GetName()]
                                [pres->v_pres_[i].op_type_];
    if (temp == 0) {
      PREDICATE_OP_TYPE temp_op_value;
      if (pres->v_pres_[i].op_type_ == Less) {
        temp_op_value = Less_And_Equel;
      } else if (pres->v_pres_[i].op_type_ == Less_And_Equel) {
        temp_op_value = Less;
      } else if (pres->v_pres_[i].op_type_ == Greater) {
        temp_op_value = Greater_And_Equel;
      } else if (pres->v_pres_[i].op_type_ == Greater_And_Equel) {
        temp_op_value = Greater;
      }
      if(this->index_.find(pres->v_pres_[i].column_->GetName())==this->index_.end()||this->index_[pres->v_pres_[i].column_->GetName()].find(temp_op_value)==index_[pres->v_pres_[i].column_->GetName()].end())
          continue;
      temp = this->index_[pres->v_pres_[i].column_->GetName()][temp_op_value];
    }
    if (temp == 0) continue;
    std::shared_ptr<Predicate> temp_ptr(new Predicate(pres->v_pres_[temp]));
    if (temp != 0 && this->v_pres_[temp - 1].IsContained(temp_ptr) == false) {
      return false;
    }
  }
  return true;
}
}