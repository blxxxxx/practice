//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  // throw NotImplementedException("NestedLoopJoinExecutor is not implemented");
  auto child_tuplejoin = [this](const Tuple *left_tuple, const Schema &left_schema, const Tuple *right_tuple,
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
  Schema left_s = this->left_executor_->GetOutputSchema();
  Schema right_s = this->right_executor_->GetOutputSchema();
  this->join_result_.clear();
  Tuple tuple;
  RID rid;
  this->left_executor_->Init();
  while (this->left_executor_->Next(&tuple, &rid)) {
    bool flag = false;
    std::vector<Tuple> right_res;
    this->right_executor_->Init();
    Tuple child_tuple;
    RID child_rid;
    while (this->right_executor_->Next(&child_tuple, &child_rid)) {
      right_res.emplace_back(child_tuple);
    }
    for (auto &t : right_res) {
      if (this->plan_->Predicate()
              ->EvaluateJoin(&tuple, left_s, &t, right_s)
              .CompareEquals(ValueFactory::GetBooleanValue(true)) == CmpBool::CmpTrue) {
        flag = true;
        this->join_result_.emplace_back(child_tuplejoin(&tuple, left_s, &t, right_s));
      }
    }
    if (!flag && this->plan_->GetJoinType() == JoinType::LEFT) {
      std::vector<Value> empty;
      for (uint32_t i = 0; i < right_s.GetColumnCount(); ++i) {
        empty.emplace_back(ValueFactory::GetNullValueByType(right_s.GetColumn(i).GetType()));
      }
      Tuple tup_empty{empty, &right_s};
      this->join_result_.emplace_back(child_tuplejoin(&tuple, left_s, &tup_empty, right_s));
    }
  }
  this->it_ = this->join_result_.begin();
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (this->it_ == this->join_result_.end()) {
    return false;
  }
  *tuple = *this->it_;
  ++this->it_;
  return true;
}

}  // namespace bustub
