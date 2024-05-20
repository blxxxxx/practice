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

#include "execution/execution_common.h"
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
  timestamp_t txn_id = this->exec_ctx_->GetTransaction()->GetTransactionId();
  timestamp_t rd_ts = this->exec_ctx_->GetTransaction()->GetReadTs();
  auto rids = GetChileRids(this->child_executor_.get());
  for (auto &ch_rid : rids) {
    TupleMeta meta = tar_info->table_->GetTupleMeta(ch_rid);
    if (CheckWriteConflict(txn_id, rd_ts, meta) == 0) {
      this->exec_ctx_->GetTransaction()->SetTainted();
      throw ExecutionException("write_write_conflict");
    }
  }
  RID child_rid;
  Tuple val;
  int cnt = 0;
  Schema child_schema = this->child_executor_->GetOutputSchema();
  Schema schema = this->GetOutputSchema();
  this->child_executor_->Init();
  while (this->child_executor_->Next(&val, &child_rid)) {
    // this->table_info_->table_->UpdateTupleMeta({0, true}, child_rid);
    std::vector<Value> values(child_schema.GetColumnCount());
    for (uint32_t i = 0; i < child_schema.GetColumnCount(); ++i) {
      values[i] = this->plan_->target_expressions_[i]->Evaluate(&val, child_schema);
    }
    Tuple new_val(values, &child_schema);
    // auto opt_rid = this->table_info_->table_->InsertTuple({0, false}, new_val, this->exec_ctx_->GetLockManager(),
    //                                                      this->exec_ctx_->GetTransaction(), this->plan_->table_oid_);
    UndoLog log{};
    if (IsTupleContentEqual(new_val, val)) {
      continue;
    }
    auto meta = tar_info->table_->GetTupleMeta(child_rid);
    log.is_deleted_ = false;
    log.modified_fields_.resize(child_schema.GetColumnCount(), false);
    log.ts_ = meta.ts_;
    std::vector<Value> tuple_vec;
    std::vector<Column> tuple_sc;
    for (uint32_t i = 0; i < child_schema.GetColumnCount(); ++i) {
      Value x = val.GetValue(&child_schema, i);
      Value y = new_val.GetValue(&child_schema, i);
      if (!x.CompareExactlyEquals(y)) {
        log.modified_fields_[i] = true;
        tuple_vec.emplace_back(x);
        tuple_sc.emplace_back(child_schema.GetColumn(i));
      }
    }
    Schema tuple_schema(tuple_sc);
    log.tuple_ = Tuple(tuple_vec, &tuple_schema);
    auto type = CheckWriteConflict(txn_id, rd_ts, meta);
    if (type == 1) {
      AddUndoLog(log, this->exec_ctx_->GetTransaction(), this->exec_ctx_->GetTransactionManager(), child_rid);
    } else if (type == 2) {
      UpdateUndoLog(log, this->exec_ctx_->GetTransaction(), this->exec_ctx_->GetTransactionManager(), child_rid,
                    &this->child_executor_->GetOutputSchema());
    }
    tar_info->table_->UpdateTupleInPlace({txn_id, false}, new_val, child_rid);
    this->exec_ctx_->GetTransaction()->AppendWriteSet(this->plan_->table_oid_, child_rid);
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
      index->index_->InsertEntry(key2, child_rid, this->exec_ctx_->GetTransaction());
    }
    cnt++;
  }
  std::vector<Value> vec1(1, Value(INTEGER, cnt));
  *tuple = Tuple(vec1, &schema);
  this->is_update_ = true;
  return true;
}

}  // namespace bustub
