#include "execution/executors/window_function_executor.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void WindowFunctionExecutor::Init() {
  // throw NotImplementedException("WindowFunctionExecutor is not implemented");
  this->child_result_.clear();
  Tuple tuple;
  RID rid;
  this->child_executor_->Init();
  while (this->child_executor_->Next(&tuple, &rid)) {
    this->child_result_.emplace_back(tuple, rid);
  }
  bool is_sorted = this->SolveOrdered();
  Schema schema = this->GetOutputSchema();
  Schema child_schema = this->child_executor_->GetOutputSchema();
  std::vector<std::vector<Value>> res(this->child_result_.size(), std::vector<Value>(schema.GetColumnCount()));
  for (uint32_t i = 0; i < schema.GetColumnCount(); ++i) {
    const auto &col_exp = dynamic_cast<const ColumnValueExpression &>(*this->plan_->columns_[i]);
    auto idx = col_exp.GetColIdx();
    if (idx < child_schema.GetColumnCount()) {
      for (uint32_t j = 0; j < this->child_result_.size(); ++j) {
        res[j][i] = this->child_result_[j].first.GetValue(&child_schema, idx);
      }
      continue;
    }
    assert(this->plan_->window_functions_.count(i) != 0);

    const auto &windows_f = this->plan_->window_functions_.find(i)->second;
    int type_id = static_cast<int>(windows_f.type_);
    AggregationType tar_type;
    if (type_id != 5) {
      tar_type = static_cast<AggregationType>(type_id);
    } else {
      tar_type = AggregationType::CountStarAggregate;
    }
    std::vector<AbstractExpressionRef> temp1{1, windows_f.function_};
    std::vector<AggregationType> temp2{1, tar_type};
    SimpleAggregationHashTable ht{temp1, temp2};
    // rank 和这个组（key相同）的前一个的tuple相同就继承前面的rk
    std::unordered_map<AggregateKey, uint32_t> pre;
    for (uint32_t j = 0; j < this->child_result_.size(); ++j) {
      auto key = MakeAggregateKey(&this->child_result_[j].first, windows_f.partition_by_);
      auto val = MakeAggregateValue(&this->child_result_[j].first, windows_f.function_);
      ht.InsertCombine(key, val);
      if (is_sorted) {
        if (windows_f.type_ == WindowFunctionType::Rank) {
          if (pre.count(key) != 0 &&
              IsTupleContentEqual(this->child_result_[j].first, this->child_result_[pre[key]].first)) {
            res[j][i] = res[pre[key]][i];
          } else {
            res[j][i] = ht.ht_.find(key)->second.aggregates_[0];
          }
          pre[key] = j;
        } else {
          res[j][i] = ht.ht_.find(key)->second.aggregates_[0];
        }
      }
    }
    if (!is_sorted) {
      for (uint32_t j = 0; j < this->child_result_.size(); ++j) {
        auto key = MakeAggregateKey(&this->child_result_[j].first, windows_f.partition_by_);
        res[j][i] = ht.ht_.find(key)->second.aggregates_[0];
      }
    }
  }
  this->result_.clear();
  for (uint32_t i = 0; i < this->child_result_.size(); ++i) {
    this->result_.emplace_back(res[i], &schema);
  }
  cur_idx_ = 0;
}

auto WindowFunctionExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cur_idx_ == this->result_.size()) {
    return false;
  }
  *tuple = this->result_[cur_idx_];
  *rid = this->child_result_[cur_idx_].second;
  ++cur_idx_;
  return true;
}
}  // namespace bustub
