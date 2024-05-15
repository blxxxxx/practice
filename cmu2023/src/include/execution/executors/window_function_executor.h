//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// window_function_executor.h
//
// Identification: src/include/execution/executors/window_function_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/executors/aggregation_executor.h"
#include "execution/plans/aggregation_plan.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * The WindowFunctionExecutor executor executes a window function for columns using window function.
 *
 * Window function is different from normal aggregation as it outputs one row for each inputing rows,
 * and can be combined with normal selected columns. The columns in WindowFunctionPlanNode contains both
 * normal selected columns and placeholder columns for window functions.
 *
 * For example, if we have a query like:
 *    SELECT 0.1, 0.2, SUM(0.3) OVER (PARTITION BY 0.2 ORDER BY 0.3), SUM(0.4) OVER (PARTITION BY 0.1 ORDER BY 0.2,0.3)
 *      FROM table;
 *
 * The WindowFunctionPlanNode contains following structure:
 *    columns: std::vector<AbstractExpressionRef>{0.1, 0.2, 0.-1(placeholder), 0.-1(placeholder)}
 *    window_functions_: {
 *      3: {
 *        partition_by: std::vector<AbstractExpressionRef>{0.2}
 *        order_by: std::vector<AbstractExpressionRef>{0.3}
 *        functions: std::vector<AbstractExpressionRef>{0.3}
 *        window_func_type: WindowFunctionType::SumAggregate
 *      }
 *      4: {
 *        partition_by: std::vector<AbstractExpressionRef>{0.1}
 *        order_by: std::vector<AbstractExpressionRef>{0.2,0.3}
 *        functions: std::vector<AbstractExpressionRef>{0.4}
 *        window_func_type: WindowFunctionType::SumAggregate
 *      }
 *    }
 *
 * Your executor should use child executor and exprs in columns to produce selected columns except for window
 * function columns, and use window_agg_indexes, partition_bys, order_bys, functionss and window_agg_types to
 * generate window function columns results. Directly use placeholders for window function columns in columns is
 * not allowed, as it contains invalid column id.
 *
 * Your WindowFunctionExecutor does not need to support specified window frames (eg: 1 preceding and 1 following).
 * You can assume that all window frames are UNBOUNDED FOLLOWING AND CURRENT ROW when there is ORDER BY clause, and
 * UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING when there is no ORDER BY clause.
 *
 */
class WindowFunctionExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new WindowFunctionExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The window aggregation plan to be executed
   */
  WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                         std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the window aggregation */
  void Init() override;

  /**
   * Yield the next tuple from the window aggregation.
   * @param[out] tuple The next tuple produced by the window aggregation
   * @param[out] rid The next tuple RID produced by the window aggregation
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the window aggregation plan */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** The window aggregation plan node to be executed */
  const WindowFunctionPlanNode *plan_;

  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;

  std::vector<std::pair<Tuple, RID>> child_result_{};
  std::vector<Tuple> result_{};
  uint32_t cur_idx_{0};
  /** @return The tuple as an AggregateKey */
  auto MakeAggregateKey(const Tuple *tuple, const std::vector<AbstractExpressionRef> &vec) -> AggregateKey {
    std::vector<Value> keys;
    keys.reserve(vec.size());
    for (const auto &expr : vec) {
      keys.emplace_back(expr->Evaluate(tuple, child_executor_->GetOutputSchema()));
    }
    AggregateKey res{keys};
    return res;
  }

  /** @return The tuple as an AggregateValue */
  auto MakeAggregateValue(const Tuple *tuple, const AbstractExpressionRef &expr) -> AggregateValue {
    std::vector<Value> vals;
    vals.emplace_back(expr->Evaluate(tuple, child_executor_->GetOutputSchema()));
    AggregateValue res{vals};
    return res;
  }
  auto SolveOrdered() -> bool {
    auto &x = this->plan_->window_functions_;
    if (x.size() == static_cast<uint32_t>(0)) {
      return false;
    }
    auto &ordered_by = x.begin()->second.order_by_;
    if (ordered_by.empty()) {
      return false;
    }
    std::sort(this->child_result_.begin(), this->child_result_.end(),
              [&](const std::pair<Tuple, RID> &x, const std::pair<Tuple, RID> &y) -> bool {
                for (auto &[type, expr] : ordered_by) {
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
              });
    return true;
  }
};
}  // namespace bustub
