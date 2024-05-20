#include "execution/execution_common.h"
#include "catalog/catalog.h"
#include "common/config.h"
#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {
auto UndoTuple(const Schema *schema, const Tuple &base_tuple, const UndoLog &log) -> Tuple {
  std::vector<Value> res;
  uint32_t undo_cnt = 0;
  std::vector<Column> undo_schema_cols;
  for (uint32_t i = 0; i < schema->GetColumnCount(); ++i) {
    if (log.modified_fields_.at(i)) {
      undo_schema_cols.emplace_back(schema->GetColumn(i));
    }
  }
  Schema undo_schema(undo_schema_cols);
  for (uint32_t i = 0; i < schema->GetColumnCount(); ++i) {
    if (!log.modified_fields_.at(i)) {
      res.emplace_back(base_tuple.GetValue(schema, i));
    } else {
      res.emplace_back(log.tuple_.GetValue(&undo_schema, undo_cnt));
      undo_cnt++;
    }
  }
  Tuple tuple_res(res, schema);
  return tuple_res;
}

auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  // UNIMPLEMENTED("not implemented");
  if (undo_logs.empty() && base_meta.is_deleted_) {
    return std::nullopt;
  }
  if (undo_logs.empty()) {
    return base_tuple;
  }
  if (undo_logs.back().is_deleted_) {
    return std::nullopt;
  }
  Tuple res = base_tuple;
  for (auto &log : undo_logs) {
    res = UndoTuple(schema, res, log);
  }
  std::optional<Tuple> opt_res{res};
  return opt_res;
}

auto GetChileRids(AbstractExecutor *child_executor) -> std::vector<RID> {
  child_executor->Init();
  Tuple tuple;
  RID rid;
  std::vector<RID> res;
  while (child_executor->Next(&tuple, &rid)) {
    res.emplace_back(rid);
  }
  return res;
}

auto CheckWriteConflict(timestamp_t txn_id, timestamp_t rd_ts, const TupleMeta &tar) -> uint32_t {
  if (tar.ts_ >= TXN_START_ID) {
    if (tar.ts_ == txn_id) {
      return 2;
    }
    return 0;
  }
  if (tar.ts_ > rd_ts) {
    return 0;
  }
  return 1;
}

void AddUndoLog(UndoLog &log, Transaction *tnx, TransactionManager *tm, const RID &rid) {
  auto opt_link = tm->GetUndoLink(rid);
  if (!opt_link.has_value() || !opt_link.value().IsValid()) {
    log.prev_version_ = UndoLink{};
    auto undo_link = tnx->AppendUndoLog(log);
    tm->UpdateUndoLink(rid, undo_link);
    return;
  }
  log.prev_version_ = opt_link.value();
  auto undo_link = tnx->AppendUndoLog(log);
  tm->UpdateUndoLink(rid, undo_link);
}

auto GetUndoSchema(const UndoLog &log, const Schema *schema) -> Schema {
  std::vector<Column> undo_schema_cols;
  for (uint32_t i = 0; i < schema->GetColumnCount(); ++i) {
    if (log.modified_fields_.at(i)) {
      undo_schema_cols.emplace_back(schema->GetColumn(i));
    }
  }
  Schema undo_schema(undo_schema_cols);
  return undo_schema;
}

void UpdateUndoLog(const UndoLog &new_log, Transaction *tnx, TransactionManager *tm, const RID &rid,
                   const Schema *schema) {
  auto opt_link = tm->GetUndoLink(rid);
  if (!opt_link.has_value()) {
    return;
  }
  auto old_log = tm->GetUndoLog(opt_link.value());
  std::vector<Value> out_vals;
  auto old_schema = GetUndoSchema(old_log, schema);
  auto new_schema = GetUndoSchema(new_log, schema);
  int new_idx = -1;
  int old_idx = -1;
  for (uint32_t i = 0; i < schema->GetColumnCount(); ++i) {
    if (new_log.modified_fields_.at(i)) {
      new_idx++;
    }
    if (old_log.modified_fields_.at(i)) {
      old_idx++;
    }
    if (old_log.modified_fields_.at(i)) {
      out_vals.emplace_back(old_log.tuple_.GetValue(&old_schema, old_idx));
    } else if (new_log.modified_fields_.at(i)) {
      out_vals.emplace_back(new_log.tuple_.GetValue(&new_schema, new_idx));
      old_log.modified_fields_.at(i) = true;
    }
  }
  auto out_schema = GetUndoSchema(old_log, schema);
  old_log.tuple_ = Tuple(out_vals, &out_schema);
  old_log.is_deleted_ = new_log.is_deleted_;
  tnx->ModifyUndoLog(opt_link.value().prev_log_idx_, old_log);
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  fmt::println(stderr, "debug_hook: {}", info);
  /*
  fmt::println(
      stderr,
      "You see this line of text because you have not implemented `TxnMgrDbg`. You should do this once you have "
      "finished task 2. Implementing this helper function will save you a lot of time for debugging in later tasks.");
  */
  auto get_string = [&](timestamp_t t) -> std::string {
    std::string res;
    if (t >= TXN_START_ID) {
      res += "1-";
      t -= TXN_START_ID;
    } else {
      res += "0-";
    }
    std::vector<int64_t> tmp;
    while (t != 0) {
      tmp.push_back(t % 10);
      t /= 10;
    }
    if (tmp.empty()) {
      tmp.push_back(0);
    }
    std::reverse(tmp.begin(), tmp.end());
    for (auto &x : tmp) {
      res += static_cast<char>(x + '0');
    }
    return res;
  };
  TableIterator it(table_heap->MakeIterator());
  while (!it.IsEnd()) {
    fmt::println(stderr, "RID={} ts={} tuple={} delete={}", it.GetRID().ToString(), get_string(it.GetTuple().first.ts_),
                 it.GetTuple().second.ToString(&table_info->schema_), it.GetTuple().first.is_deleted_);
    auto undo_log_opt = txn_mgr->GetUndoLink(it.GetRID());
    while (undo_log_opt.has_value()) {
      if (!undo_log_opt.value().IsValid()) {
        break;
      }
      auto undo_log = txn_mgr->GetUndoLog(undo_log_opt.value());
      std::vector<Column> vec;
      std::string bits;
      for (uint32_t i = 0; i < table_info->schema_.GetColumnCount(); ++i) {
        if (undo_log.modified_fields_.at(i)) {
          vec.emplace_back(table_info->schema_.GetColumn(i));
          bits += '1';
        } else {
          bits += '0';
        }
      }
      Schema schema{vec};
      fmt::println(stderr, "ts={}, tuple={} bits={} delete={}", get_string(undo_log.ts_),
                   undo_log.tuple_.ToString(&schema), bits, undo_log.is_deleted_);
      if (undo_log.prev_version_.IsValid()) {
        undo_log_opt.emplace(undo_log.prev_version_);
      } else {
        undo_log_opt.reset();
      }
    }
    fmt::println(stderr, "-------------------------------------------------------------");
    ++it;
  }
  // We recommend implementing this function as traversing the table heap and print the version chain. An example output
  // of our reference solution:
  //
  // debug_hook: before verify scan
  // RID=0/0 ts=txn8 tuple=(1, <NULL>, <NULL>)
  //   txn8@0 (2, _, _) ts=1
  // RID=0/1 ts=3 tuple=(3, <NULL>, <NULL>)
  //   txn5@0 <del> ts=2
  //   txn3@0 (4, <NULL>, <NULL>) ts=1
  // RID=0/2 ts=4 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn7@0 (5, <NULL>, <NULL>) ts=3
  // RID=0/3 ts=txn6 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn6@0 (6, <NULL>, <NULL>) ts=2
  //   txn3@1 (7, _, _) ts=1
}

}  // namespace bustub
