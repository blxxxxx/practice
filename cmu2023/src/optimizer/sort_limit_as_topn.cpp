#include "execution/executors/limit_executor.h"
#include "execution/executors/sort_executor.h"
#include "execution/executors/topn_executor.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::Limit) {
    const auto &lmt_plan = dynamic_cast<const LimitPlanNode &>(*optimized_plan);
    if (lmt_plan.GetChildPlan()->GetType() != PlanType::Sort) {
      return optimized_plan;
    }
    const auto &sort_plan = dynamic_cast<const SortPlanNode &>(*lmt_plan.GetChildPlan());
    return std::make_shared<TopNPlanNode>(lmt_plan.output_schema_, sort_plan.GetChildPlan(), sort_plan.GetOrderBy(),
                                          lmt_plan.limit_);
  }
  return optimized_plan;
}

}  // namespace bustub
