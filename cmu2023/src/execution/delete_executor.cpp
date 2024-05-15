//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  // throw NotImplementedException("DeleteExecutor is not implemented");
  this->is_delete_ = false;
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (this->is_delete_) {
    return false;
  }
  TableInfo *tar_info = this->exec_ctx_->GetCatalog()->GetTable(this->plan_->table_oid_);
  auto tar_indexes = this->exec_ctx_->GetCatalog()->GetTableIndexes(tar_info->name_);
  this->child_executor_->Init();
  RID child_rid;
  Tuple val;
  int cnt = 0;
  while (this->child_executor_->Next(&val, &child_rid)) {
    this->exec_ctx_->GetCatalog()->GetTable(this->plan_->table_oid_)->table_->UpdateTupleMeta({0, true}, child_rid);
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
      index->index_->DeleteEntry(key, child_rid, this->exec_ctx_->GetTransaction());
    }
    cnt++;
  }
  std::vector<Value> vec1(1, Value(INTEGER, cnt));
  *tuple = Tuple(vec1, &this->GetOutputSchema());
  this->is_delete_ = true;
  return true;
}

}  // namespace bustub
