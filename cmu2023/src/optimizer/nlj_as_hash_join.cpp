#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Fall: You should support join keys of any number of conjunction of equi-condistions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    auto pred = nlj_plan.Predicate();
    std::vector<AbstractExpressionRef> left_key_expressions;
    std::vector<AbstractExpressionRef> right_key_expressions;
    std::function<void(const AbstractExpressionRef &)> get_lr = [&](const AbstractExpressionRef &u) -> void {
      auto and_u = std::dynamic_pointer_cast<LogicExpression>(u);
      if (and_u != nullptr) {
        assert(and_u->logic_type_ == LogicType::And);
        get_lr(and_u->GetChildAt(0));
        get_lr(and_u->GetChildAt(1));
        return;
      }
      auto equal_u = std::dynamic_pointer_cast<ComparisonExpression>(u);
      if (equal_u != nullptr) {
        assert(equal_u->comp_type_ == ComparisonType::Equal);
        auto ll = std::dynamic_pointer_cast<ColumnValueExpression>(equal_u->GetChildAt(0));
        auto rr = std::dynamic_pointer_cast<ColumnValueExpression>(equal_u->GetChildAt(1));
        assert(ll != nullptr && rr != nullptr);
        assert(ll->GetTupleIdx() != rr->GetTupleIdx());
        if (ll->GetTupleIdx() == 0) {
          left_key_expressions.emplace_back(equal_u->GetChildAt(0));
          right_key_expressions.emplace_back(equal_u->GetChildAt(1));
        } else {
          left_key_expressions.emplace_back(equal_u->GetChildAt(1));
          right_key_expressions.emplace_back(equal_u->GetChildAt(0));
        }
        return;
      }
    };
    get_lr(pred);
    if (left_key_expressions.empty()) {
      return optimized_plan;
    }
    return std::make_shared<HashJoinPlanNode>(optimized_plan->output_schema_, optimized_plan->GetChildAt(0),
                                              optimized_plan->GetChildAt(1), left_key_expressions,
                                              right_key_expressions, nlj_plan.GetJoinType());
  }
  return optimized_plan;
}

}  // namespace bustub
