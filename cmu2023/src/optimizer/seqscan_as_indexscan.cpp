#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  auto check = [](AbstractExpressionRef &exp, column_oid_t *idx) -> bool {
    // std::cerr << "in check\n";
    if (exp == nullptr) {
      return false;
    }
    auto cmp = std::dynamic_pointer_cast<ComparisonExpression>(exp);
    if (cmp == nullptr) {
      return false;
    }
    auto col_exp = std::dynamic_pointer_cast<ColumnValueExpression>(cmp->GetChildAt(0));
    if (col_exp == nullptr) {
      return false;
    }
    auto con_exp = std::dynamic_pointer_cast<ConstantValueExpression>(cmp->GetChildAt(1));
    if (con_exp == nullptr) {
      return false;
    }
    *idx = col_exp->GetColIdx();
    return true;
  };
  if (optimized_plan->GetType() == PlanType::SeqScan) {
    const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
    AbstractExpressionRef filter = seq_scan_plan.filter_predicate_;
    column_oid_t idx;
    if (check(filter, &idx)) {
      table_oid_t table_idx = seq_scan_plan.table_oid_;
      std::string table_name = this->catalog_.GetTable(table_idx)->name_;
      auto indexes = this->catalog_.GetTableIndexes(table_name);
      std::optional<index_oid_t> opt_idx;
      for (auto &index : indexes) {
        if (index->key_schema_.GetColumnCount() != 1) {
          continue;
        }
        auto column_equal = [](const Column &x, const Column &y) -> bool {
          if (x.GetName() != y.GetName()) {
            return false;
          }
          if (x.GetType() != y.GetType()) {
            return false;
          }
          return true;
        };
        if (column_equal(index->key_schema_.GetColumn(0), this->catalog_.GetTable(table_idx)->schema_.GetColumn(idx))) {
          opt_idx.emplace(index->index_oid_);
          break;
        }
      }
      if (opt_idx.has_value()) {
        auto ptr = std::dynamic_pointer_cast<ConstantValueExpression>(filter->GetChildAt(1)).get();
        return std::make_shared<IndexScanPlanNode>(plan->output_schema_, seq_scan_plan.table_oid_, opt_idx.value(),
                                                   filter, ptr);
      }
    }
  }
  return optimized_plan;
}

}  // namespace bustub
