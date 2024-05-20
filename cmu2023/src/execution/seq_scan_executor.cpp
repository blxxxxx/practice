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
#include "concurrency/transaction_manager.h"
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
    TupleMeta tuple_meta = it_->GetTuple().first;
    timestamp_t rd_s = this->exec_ctx_->GetTransaction()->GetReadTs();
    timestamp_t txn_id = this->exec_ctx_->GetTransaction()->GetTransactionId();
    auto opt = this->exec_ctx_->GetTransactionManager()->ReadTimeTuple(
        it_->GetRID(), rd_s, txn_id, &this->GetOutputSchema(), it_->GetTuple().second, tuple_meta);
    if (!opt.has_value()) {
      ++(*it_);
      continue;
    }
    auto val = opt.value();
    /*
    if (val.first.is_deleted_) {
      ++(*it_);
      continue;
    }
    */
    auto filter = this->plan_->filter_predicate_;
    if (filter != nullptr) {
      auto t = filter->Evaluate(&val, this->plan_->OutputSchema());
      if (t.CompareEquals(value_true) == CmpBool::CmpFalse) {
        ++(*it_);
        continue;
      }
    }
    *tuple = val;
    *rid = it_->GetRID();
    ++(*it_);
    return true;
  }
  return false;
}

}  // namespace bustub
