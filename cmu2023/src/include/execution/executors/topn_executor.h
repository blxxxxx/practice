//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// topn_executor.h
//
// Identification: src/include/execution/executors/topn_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/topn_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * The TopNExecutor executor executes a topn.
 */
class TopNExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new TopNExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The TopN plan to be executed
   */
  TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the TopN */
  void Init() override;

  /**
   * Yield the next tuple from the TopN.
   * @param[out] tuple The next tuple produced by the TopN
   * @param[out] rid The next tuple RID produced by the TopN
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the TopN */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  /** Sets new child executor (for testing only) */
  void SetChildExecutor(std::unique_ptr<AbstractExecutor> &&child_executor) {
    child_executor_ = std::move(child_executor);
  }

  /** @return The size of top_entries_ container, which will be called on each child_executor->Next(). */
  auto GetNumInHeap() -> size_t;

 private:
  /** The TopN plan node to be executed */
  const TopNPlanNode *plan_;
  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;

  std::vector<std::pair<Tuple, RID>> heap_{};

  uint32_t cnt_{0};
  [[nodiscard]] auto Cmp(const std::pair<Tuple, RID> &x, const std::pair<Tuple, RID> &y) const -> bool {
    for (auto &[type, expr] : this->plan_->GetOrderBy()) {
      Value x_res = expr->Evaluate(&x.first, this->GetOutputSchema());
      Value y_res = expr->Evaluate(&y.first, this->GetOutputSchema());
      assert(type != OrderByType::INVALID);
      if (type == OrderByType::DESC) {
        if (x_res.CompareLessThan(y_res) == CmpBool::CmpTrue) {
          return false;
        }
        if (x_res.CompareGreaterThan(y_res) == CmpBool::CmpTrue) {
          return true;
        }
      }
      if (type == OrderByType::ASC || type == OrderByType::DEFAULT) {
        if (x_res.CompareLessThan(y_res) == CmpBool::CmpTrue) {
          return true;
        }
        if (x_res.CompareGreaterThan(y_res) == CmpBool::CmpTrue) {
          return false;
        }
      }
    }
    return true;
  }
  static void Swap(std::pair<Tuple, RID> &x, std::pair<Tuple, RID> &y) {
    auto t = y;
    y = x;
    x = t;
  }
  void HeapPushDown(uint32_t top, uint32_t end) {
    uint32_t child = top * 2 + 1;
    while (child < end) {
      if (child + 1 < end && Cmp(heap_[child + 1], heap_[child])) {
        child++;
      }
      if (Cmp(heap_[child], heap_[top])) {
        Swap(heap_[child], heap_[top]);
        top = child;
        child = top * 2 + 1;
      } else {
        return;
      }
    }
  }
};
}  // namespace bustub
