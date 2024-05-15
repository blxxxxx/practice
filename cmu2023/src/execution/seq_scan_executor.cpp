//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), it_(nullptr) {}

void SeqScanExecutor::Init() {
  // throw NotImplementedException("SeqScanExecutor is not implemented");
  auto table = this->exec_ctx_->GetCatalog()->GetTable(this->plan_->table_oid_);
  it_ = std::make_shared<TableIterator>(table->table_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Value value_true(TypeId::BOOLEAN, 1);
  while (!this->it_->IsEnd()) {
    auto val = it_->GetTuple();
    if (val.first.is_deleted_) {
      ++(*it_);
      continue;
    }
    auto filter = this->plan_->filter_predicate_;
    if (filter != nullptr) {
      auto t = filter->Evaluate(&val.second, this->plan_->OutputSchema());
      if (t.CompareEquals(value_true) == CmpBool::CmpFalse) {
        ++(*it_);
        continue;
      }
    }
    *tuple = val.second;
    *rid = it_->GetRID();
    ++(*it_);
    return true;
  }
  return false;
}

}  // namespace bustub
