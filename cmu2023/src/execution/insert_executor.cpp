//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "concurrency/transaction_manager.h"
#include "execution/executors/insert_executor.h"
namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  //  plan->CloneWithChildren(child_executor);
}

void InsertExecutor::Init() {
  // throw NotImplementedException("InsertExecutor is not implemented");
  this->is_insert_ = false;
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (this->is_insert_) {
    return false;
  }
  TableInfo *tar_info = this->exec_ctx_->GetCatalog()->GetTable(this->plan_->table_oid_);
  auto tar_indexes = this->exec_ctx_->GetCatalog()->GetTableIndexes(tar_info->name_);
  auto get_index_tuple = [&](const Schema &c1, const Schema &c2, const Tuple &tup) -> Tuple {
    auto column_equal = [](const Column &x, const Column &y) -> bool {
      if (x.GetName() != y.GetName()) {
        return false;
      }
      if (x.GetType() != y.GetType()) {
        return false;
      }
      return true;
    };
    std::vector<Value> vec;
    vec.reserve(c2.GetColumnCount());
    for (uint32_t i = 0; i < c1.GetColumnCount(); ++i) {
      for (uint32_t j = 0; j < c2.GetColumnCount(); ++j) {
        if (column_equal(c1.GetColumn(i), c2.GetColumn(j))) {
          vec.push_back(tup.GetValue(&c2, j));
        }
      }
    }
    Tuple key(vec, &c2);
    return key;
  };
  auto get_prime_key = [&]() -> IndexInfo * {
    IndexInfo *prime_index = nullptr;
    for (auto &x : tar_indexes) {
      if (x->is_primary_key_) {
        prime_index = x;
        break;
      }
    }
    return prime_index;
  };
  int32_t cnt = 0;
  Tuple val;
  RID tmp;
  auto prime = get_prime_key();
  this->child_executor_->Init();
  while (this->child_executor_->Next(&val, &tmp)) {
    Tuple key_tup{};
    if (prime != nullptr) {
      key_tup = get_index_tuple(tar_info->schema_, prime->key_schema_, val);
      std::vector<RID> result;
      prime->index_->ScanKey(key_tup, &result, this->exec_ctx_->GetTransaction());
      if (!result.empty()) {
        this->exec_ctx_->GetTransaction()->SetTainted();
        throw ExecutionException("write_write_conflict");
      }
    }
    timestamp_t ts = this->exec_ctx_->GetTransaction()->GetTransactionId();
    auto op_rid = tar_info->table_->InsertTuple({ts, false}, val, this->exec_ctx_->GetLockManager(),
                                                this->exec_ctx_->GetTransaction(), this->plan_->table_oid_);
    assert(op_rid.has_value());
    auto undo_link = this->exec_ctx_->GetTransactionManager()->GetUndoLink(op_rid.value());
    assert(!undo_link.has_value());
    this->exec_ctx_->GetTransaction()->AppendWriteSet(this->plan_->table_oid_, op_rid.value());
    if (prime != nullptr) {
      bool flag = prime->index_->InsertEntry(key_tup, op_rid.value(), this->exec_ctx_->GetTransaction());
      if (!flag) {
        this->exec_ctx_->GetTransaction()->SetTainted();
        throw ExecutionException("write_write_conflict");
      }
    }
    for (auto &index : tar_indexes) {
      if (index->is_primary_key_) {
        continue;
      }
      auto c1 = index->key_schema_;
      auto c2 = tar_info->schema_;
      Tuple key(get_index_tuple(c1, c2, val));
      index->index_->InsertEntry(key, op_rid.value(), this->exec_ctx_->GetTransaction());
    }
    cnt++;
  }
  std::vector<Value> vec1(1, Value(INTEGER, cnt));
  *tuple = Tuple(vec1, &this->plan_->OutputSchema());
  this->is_insert_ = true;
  return true;
}

}  // namespace bustub
