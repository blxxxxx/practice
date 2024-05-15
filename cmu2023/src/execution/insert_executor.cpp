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
  int32_t cnt = 0;
  Tuple val;
  RID tmp;
  this->child_executor_->Init();
  while (this->child_executor_->Next(&val, &tmp)) {
    auto op_rid = tar_info->table_->InsertTuple({0, false}, val, this->exec_ctx_->GetLockManager(),
                                                this->exec_ctx_->GetTransaction(), this->plan_->table_oid_);
    assert(op_rid.has_value());
    for (auto &index : tar_indexes) {
      std::vector<Value> vec;
      vec.reserve(index->key_schema_.GetColumnCount());
      auto c1 = index->key_schema_;
      auto c2 = tar_info->schema_;
      auto column_equal = [](const Column &x, const Column &y) -> bool {
        if (x.GetName() != y.GetName()) {
          return false;
        }
        if (x.GetType() != y.GetType()) {
          return false;
        }
        return true;
      };
      for (uint32_t i = 0; i < c1.GetColumnCount(); ++i) {
        for (uint32_t j = 0; j < c2.GetColumnCount(); ++j) {
          if (column_equal(c1.GetColumn(i), c2.GetColumn(j))) {
            vec.push_back(val.GetValue(&c2, j));
          }
        }
      }
      Tuple key(vec, &index->key_schema_);
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
