//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction * {
  std::unique_lock<std::shared_mutex> l(txn_map_mutex_);
  auto txn_id = next_txn_id_++;
  auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
  auto *txn_ref = txn.get();
  txn_map_.insert(std::make_pair(txn_id, std::move(txn)));

  // TODO(fall2023): set the timestamps here. Watermark updated below.
  txn_ref->read_ts_.store(this->last_commit_ts_.load());
  // std::cerr << "insert" << txn_ref->read_ts_ << '\n';
  running_txns_.AddTxn(txn_ref->read_ts_);
  return txn_ref;
}

auto TransactionManager::VerifyTxn(Transaction *txn) -> bool { return true; }

auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);
  // std::cerr << "delete" << txn->read_ts_ << '\n';
  // TODO(fall2023): acquire commit ts

  if (txn->state_ != TransactionState::RUNNING) {
    throw Exception("txn not in running state");
  }

  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) {
      commit_lck.unlock();
      Abort(txn);
      return false;
    }
  }

  this->last_commit_ts_++;
  // TODO(fall2023): Implement the commit logic!
  for (auto &[tid, rids] : txn->GetWriteSets()) {
    auto table = this->catalog_->GetTable(tid);
    for (auto &rid : rids) {
      auto meta = table->table_->GetTupleMeta(rid);
      meta.ts_ = this->last_commit_ts_.load();
      table->table_->UpdateTupleMeta(meta, rid);
    }
  }
  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);

  // TODO(fall2023): set commit timestamp + update last committed timestamp here.

  txn->commit_ts_.store(this->last_commit_ts_.load());

  txn->state_ = TransactionState::COMMITTED;
  running_txns_.UpdateCommitTs(txn->commit_ts_);
  running_txns_.RemoveTxn(txn->read_ts_);

  return true;
}

void TransactionManager::Abort(Transaction *txn) {
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running / tainted state");
  }

  // TODO(fall2023): Implement the abort logic!

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);
}
auto TransactionManager::GetTransaction(txn_id_t idx) -> Transaction * {
  std::unique_lock<std::shared_mutex> l(txn_map_mutex_);
  if (this->txn_map_.count(idx) == 0) {
    return nullptr;
  }
  auto res = this->txn_map_[idx].get();
  return res;
}
void TransactionManager::GarbageCollection() {
  // UNIMPLEMENTED("not implemented");
  std::unique_lock lk{this->delete_cnt_mutex_};
  auto tables = this->catalog_->GetTableNames();
  timestamp_t lowest_ts = this->GetWatermark();
  auto solve_rid = [&](const RID &rid, const Tuple &tuple, const TupleMeta &meta) -> void {
    bool flag = false;
    if (meta.ts_ <= lowest_ts) {
      flag = true;
    }
    auto undo_link_opt = this->GetUndoLink(rid);
    if (!undo_link_opt.has_value()) {
      return;
    }
    UndoLink undo_link = undo_link_opt.value();
    UndoLink pre_link{};
    while (undo_link.IsValid()) {
      auto log = this->GetUndoLog(undo_link);
      if (flag) {
        this->delete_cnt_[undo_link.prev_txn_]++;
        if (pre_link.IsValid()) {
          auto new_log = this->GetTransaction(pre_link.prev_txn_)->GetUndoLog(pre_link.prev_log_idx_);
          new_log.prev_version_.prev_txn_ = INVALID_TXN_ID;
          this->GetTransaction(pre_link.prev_txn_)->ModifyUndoLog(pre_link.prev_log_idx_, new_log);
        } else {
          this->UpdateUndoLink(rid, std::nullopt);
        }
      } else {
        if (log.ts_ <= lowest_ts) {
          flag = true;
        }
      }
      pre_link = undo_link;
      undo_link = log.prev_version_;
    }
  };
  for (auto &table_name : tables) {
    auto table = this->catalog_->GetTable(table_name);
    TableIterator it(table->table_->MakeIterator());
    while (!it.IsEnd()) {
      RID rid = it.GetRID();
      auto tuple = it.GetTuple();
      solve_rid(rid, tuple.second, tuple.first);
      ++it;
    }
  }
  std::vector<txn_id_t> delete_txn;

  for (auto &[txn_id, cnt] : this->delete_cnt_) {
    if (this->GetTransaction(txn_id) == nullptr) {
      continue;
    }
    if (this->GetTransaction(txn_id)->GetUndoLogNum() == cnt) {
      auto state = this->GetTransaction(txn_id)->GetTransactionState();
      if (state == TransactionState::COMMITTED || state == TransactionState::ABORTED) {
        delete_txn.emplace_back(txn_id);
      }
    }
  }
  std::unique_lock l{this->txn_map_mutex_};
  for (auto &[txn_id, ptr] : this->txn_map_) {
    if (ptr->GetUndoLogNum() == 0) {
      auto state = ptr->GetTransactionState();
      if (state == TransactionState::COMMITTED || state == TransactionState::ABORTED) {
        delete_txn.emplace_back(txn_id);
      }
    }
  }
  for (auto &id : delete_txn) {
    this->txn_map_.erase(id);
    if (this->delete_cnt_.count(id) != 0) {
      this->delete_cnt_.erase(id);
    }
  }
}

auto TransactionManager::ReadTimeTuple(const RID &rid, const timestamp_t &rd_t, const timestamp_t &txn_id,
                                       const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta)
    -> std::optional<Tuple> {
  std::vector<UndoLog> undo_logs;
  auto check_ts = [&](const timestamp_t &ts) -> bool {
    if (ts < TXN_START_ID && ts <= rd_t) {
      return true;
    }
    if (ts >= TXN_START_ID && ts == txn_id) {
      return true;
    }
    return false;
  };
  if (check_ts(base_meta.ts_)) {
    return ReconstructTuple(schema, base_tuple, base_meta, undo_logs);
  }
  auto undo_link_opt = this->GetUndoLink(rid);
  if (!undo_link_opt.has_value()) {
    return std::nullopt;
  }
  auto undo_link = undo_link_opt.value();
  if (!undo_link.IsValid()) {
    return std::nullopt;
  }
  auto undo_log = this->GetUndoLog(undo_link);
  while (!check_ts(undo_log.ts_)) {
    undo_logs.emplace_back(undo_log);
    undo_link = undo_log.prev_version_;
    if (!undo_link.IsValid()) {
      return std::nullopt;
    }
    undo_log = this->GetUndoLog(undo_link);
  }
  undo_logs.emplace_back(undo_log);
  return ReconstructTuple(schema, base_tuple, base_meta, undo_logs);
}
}  // namespace bustub
