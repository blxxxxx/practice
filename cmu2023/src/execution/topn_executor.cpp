#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  // throw NotImplementedException("TopNExecutor is not implemented");
  this->child_executor_->Init();
  this->heap_.clear();
  this->cnt_ = this->plan_->GetN();
  Tuple tuple;
  RID rid;
  while (this->child_executor_->Next(&tuple, &rid)) {
    this->heap_.emplace_back(tuple, rid);
  }
  int size = static_cast<int>(this->heap_.size());
  if (size == 0) {
    return;
  }
  for (int i = size - 1; i >= 0; --i) {
    this->HeapPushDown(i, size);
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (this->cnt_ == 0) {
    return false;
  }
  if (heap_.size() == static_cast<uint32_t>(0)) {
    return false;
  }
  this->cnt_--;
  Swap(heap_[0], heap_.back());
  *tuple = heap_.back().first;
  *rid = heap_.back().second;
  heap_.pop_back();
  this->HeapPushDown(0, heap_.size());
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t {
  // throw NotImplementedException("TopNExecutor is not implemented");
  return this->plan_->GetN() - this->cnt_;
};

}  // namespace bustub
