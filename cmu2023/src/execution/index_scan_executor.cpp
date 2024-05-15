//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  // throw NotImplementedException("IndexScanExecutor is not implemented");
  this->is_index_scan_executor_ = false;
}
/*
 * set force_optimizer_starter_rule=yes;
 * create table t1(v1 int, v2 int, v3 int);
 * insert into t1 values (1, 50, 645), (2, 40, 721), (4, 20, 445), (5, 10, 445), (3, 30, 645);
 * create index t1v1 on t1(v1);
 * select * from t1 where v1 = 1;
 */
auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (this->is_index_scan_executor_) {
    return false;
  }
  this->is_index_scan_executor_ = true;
  auto table = this->exec_ctx_->GetCatalog()->GetTable(this->plan_->table_oid_);
  auto index = this->exec_ctx_->GetCatalog()->GetIndex(this->plan_->index_oid_);
  std::vector<RID> result;
  Value val(this->plan_->pred_key_->val_);
  std::vector<Value> args1(1, val);
  Schema args2 = index->key_schema_;
  Tuple tup(args1, &args2);
  index->index_->ScanKey(tup, &result, this->exec_ctx_->GetTransaction());
  assert(result.size() <= static_cast<size_t>(1));
  if (result.empty()) {
    return false;
  }
  *rid = result[0];
  auto pair_tuple = table->table_->GetTuple(*rid);
  if (pair_tuple.first.is_deleted_) {
    return false;
  }
  *tuple = pair_tuple.second;
  auto filter = this->plan_->filter_predicate_;
  if (filter != nullptr) {
    auto t = filter->Evaluate(tuple, this->plan_->OutputSchema());
    Value value_true(TypeId::BOOLEAN, 1);
    if (t.CompareEquals(value_true) == CmpBool::CmpFalse) {
      return false;
    }
  }
  return true;
}

}  // namespace bustub
