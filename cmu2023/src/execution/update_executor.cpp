//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  this->table_info_ = exec_ctx->GetCatalog()->GetTable(plan->table_oid_);
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  // throw NotImplementedException("UpdateExecutor is not implemented");
  this->is_update_ = false;
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (this->is_update_) {
    return false;
  }
  TableInfo *tar_info = this->exec_ctx_->GetCatalog()->GetTable(this->plan_->table_oid_);
  auto tar_indexes = this->exec_ctx_->GetCatalog()->GetTableIndexes(tar_info->name_);
  this->child_executor_->Init();
  RID child_rid;
  Tuple val;
  int cnt = 0;
  Schema child_schema = this->child_executor_->GetOutputSchema();
  Schema schema = this->GetOutputSchema();
  while (this->child_executor_->Next(&val, &child_rid)) {
    this->table_info_->table_->UpdateTupleMeta({0, true}, child_rid);
    std::vector<Value> values(child_schema.GetColumnCount());
    for (uint32_t i = 0; i < child_schema.GetColumnCount(); ++i) {
      values[i] = this->plan_->target_expressions_[i]->Evaluate(&val, child_schema);
    }
    Tuple new_val(values, &child_schema);
    auto opt_rid = this->table_info_->table_->InsertTuple({0, false}, new_val, this->exec_ctx_->GetLockManager(),
                                                          this->exec_ctx_->GetTransaction(), this->plan_->table_oid_);
    for (auto &index : tar_indexes) {
      std::vector<Value> vec;
      std::vector<Value> vecc;
      vec.reserve(index->key_schema_.GetColumnCount());
      vecc.reserve(index->key_schema_.GetColumnCount());
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
            vec.push_back(new_val.GetValue(&c2, j));
            vecc.push_back(val.GetValue(&c2, j));
          }
        }
      }
      Tuple key1(vecc, &index->key_schema_);
      index->index_->DeleteEntry(key1, child_rid, this->exec_ctx_->GetTransaction());
      Tuple key2(vec, &index->key_schema_);
      index->index_->InsertEntry(key2, opt_rid.value(), this->exec_ctx_->GetTransaction());
    }
    cnt++;
  }
  std::vector<Value> vec1(1, Value(INTEGER, cnt));
  *tuple = Tuple(vec1, &schema);
  this->is_update_ = true;
  return true;
}

}  // namespace bustub
