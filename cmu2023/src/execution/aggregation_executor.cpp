//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      agg_ht_(plan->aggregates_, plan->agg_types_),
      agg_ht_it_(agg_ht_.Begin()) {}

void AggregationExecutor::Init() {
  this->agg_ht_.Clear();
  this->child_executor_->Init();
  Tuple tuple;
  RID rid;
  while (this->child_executor_->Next(&tuple, &rid)) {
    auto ht_key = this->MakeAggregateKey(&tuple);
    auto ht_val = this->MakeAggregateValue(&tuple);
    this->agg_ht_.InsertCombine(ht_key, ht_val);
  }
  if (this->plan_->group_bys_.empty() && agg_ht_.Begin() == agg_ht_.End()) {
    std::vector<Value> values;
    for (const auto &expr : plan_->GetGroupBys()) {
      values.emplace_back(ValueFactory::GetNullValueByType(expr->GetReturnType()));
    }
    this->agg_ht_.SolveEmpty(AggregateKey{values});
  }
  agg_ht_it_ = agg_ht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (agg_ht_it_ == agg_ht_.End()) {
    return false;
  }
  std::vector<Value> values;
  values.reserve(this->GetOutputSchema().GetColumnCount());
  for (auto &x : agg_ht_it_.Key().group_bys_) {
    values.push_back(x);
  }
  for (auto &x : agg_ht_it_.Val().aggregates_) {
    values.push_back(x);
  }
  *tuple = Tuple(values, &this->GetOutputSchema());
  ++agg_ht_it_;
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
