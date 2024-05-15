#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  // throw NotImplementedException("SortExecutor is not implemented");
  this->result_.clear();
  Tuple tuple;
  RID rid;
  this->child_executor_->Init();
  while (this->child_executor_->Next(&tuple, &rid)) {
    this->result_.emplace_back(tuple, rid);
  }
  std::sort(this->result_.begin(), this->result_.end(),
            [&](const std::pair<Tuple, RID> &x, const std::pair<Tuple, RID> &y) -> bool {
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
            });
  this->it_ = this->result_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (this->it_ == this->result_.end()) {
    return false;
  }
  *tuple = this->it_->first;
  *rid = this->it_->second;
  ++this->it_;
  return true;
}

}  // namespace bustub
