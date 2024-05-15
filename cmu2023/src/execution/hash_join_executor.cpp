//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  // throw NotImplementedException("HashJoinExecutor is not implemented");
  this->ht_.clear();
  Schema left_s = this->left_child_->GetOutputSchema();
  Schema right_s = this->right_child_->GetOutputSchema();
  Tuple tuple;
  RID rid;
  this->left_child_->Init();
  while (this->left_child_->Next(&tuple, &rid)) {
    std::vector<Value> left_key;
    for (auto &expr : this->plan_->LeftJoinKeyExpressions()) {
      left_key.emplace_back(expr->Evaluate(&tuple, left_s));
    }
    JoinHashKey join_key{left_key};
    if (this->ht_.count(join_key) == 0) {
      this->ht_.insert({join_key, {std::vector<Tuple>{}, std::vector<Tuple>{}}});
    }
    this->ht_[join_key].left_tuple_.push_back(tuple);
  }
  this->right_child_->Init();
  while (this->right_child_->Next(&tuple, &rid)) {
    std::vector<Value> right_key;
    for (auto &expr : this->plan_->RightJoinKeyExpressions()) {
      right_key.emplace_back(expr->Evaluate(&tuple, right_s));
    }
    JoinHashKey join_key{right_key};
    if (this->ht_.count(join_key) == 0) {
      this->ht_.insert({join_key, {std::vector<Tuple>{}, std::vector<Tuple>{}}});
    }
    this->ht_[join_key].right_tuple_.push_back(tuple);
  }
  auto tuple_join = [this](const Tuple *left_tuple, const Schema &left_schema, const Tuple *right_tuple,
                           const Schema &right_schema) -> Tuple {
    std::vector<Value> res;
    for (uint32_t i = 0; i < left_schema.GetColumnCount(); ++i) {
      res.emplace_back(left_tuple->GetValue(&left_schema, i));
    }
    for (uint32_t i = 0; i < right_schema.GetColumnCount(); ++i) {
      res.emplace_back(right_tuple->GetValue(&right_schema, i));
    }
    return Tuple{res, &this->GetOutputSchema()};
  };
  this->join_result_.clear();
  for (auto &[key, val] : this->ht_) {
    if (val.left_tuple_.empty()) {
      continue;
    }
    if (val.right_tuple_.empty()) {
      if (this->plan_->GetJoinType() == JoinType::LEFT) {
        std::vector<Value> empty;
        for (uint32_t i = 0; i < right_s.GetColumnCount(); ++i) {
          empty.emplace_back(ValueFactory::GetNullValueByType(right_s.GetColumn(i).GetType()));
        }
        Tuple tup_empty{empty, &right_s};
        for (auto &x : val.left_tuple_) {
          this->join_result_.emplace_back(tuple_join(&x, left_s, &tup_empty, right_s));
        }
      }
      continue;
    }
    for (auto &x : val.left_tuple_) {
      for (auto &y : val.right_tuple_) {
        this->join_result_.emplace_back(tuple_join(&x, left_s, &y, right_s));
      }
    }
  }
  this->it_ = this->join_result_.begin();
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (this->it_ == this->join_result_.end()) {
    return false;
  }
  *tuple = *this->it_;
  ++this->it_;
  return true;
}

}  // namespace bustub
