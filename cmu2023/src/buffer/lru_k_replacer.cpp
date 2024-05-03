//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"
//[[maybe_unused]] std::list<size_t> history_;
//[[maybe_unused]] size_t k_;
// frame_id_t fid_;
//[[maybe_unused]] bool is_evictable_{false};

//[[maybe_unused]] std::unordered_map<frame_id_t, LRUKNode> node_store_;
//[[maybe_unused]] size_t current_timestamp_{0};
// size_t replacer_size_;
//[[maybe_unused]] size_t k_;
//[[maybe_unused]] std::mutex latch_;
namespace bustub {
LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  this->node_store_.clear();
  this->evi_node_.clear();
  assert(k >= 2);
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::unique_lock lk{this->latch_};
  if (this->evi_node_.empty()) {
    return false;
  }
  LRUKNode res = *this->evi_node_.begin();
  this->evi_node_.erase(this->evi_node_.begin());
  this->node_store_.erase(res.fid_);
  *frame_id = res.fid_;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::unique_lock lk{this->latch_};
  if (static_cast<size_t>(frame_id) > this->replacer_size_) {
    throw std::runtime_error("error frame_id");
  }

  this->current_timestamp_++;

  if (this->node_store_.count(frame_id) == 0) {
    LRUKNode new_node(this->current_timestamp_, this->k_, frame_id);
    this->node_store_.insert(std::make_pair(frame_id, new_node));
    return;
  }
  if (this->node_store_.at(frame_id).is_evictable_) {
    const auto it = this->evi_node_.find(this->node_store_.at(frame_id));
    assert(it != this->evi_node_.end());
    LRUKNode val = *it;
    val.Update(current_timestamp_);
    this->evi_node_.erase(it);
    this->evi_node_.insert(val);
  }
  this->node_store_.at(frame_id).Update(current_timestamp_);
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::unique_lock lk{this->latch_};
  if (static_cast<size_t>(frame_id) > this->replacer_size_) {
    throw std::runtime_error("error frame_id");
  }
  if (this->node_store_.count(frame_id) == 0) {
    return;
  }
  bool old_evi = this->node_store_.at(frame_id).is_evictable_;
  if (old_evi == set_evictable) {
    return;
  }
  if (!old_evi) {
    this->node_store_.at(frame_id).is_evictable_ = true;
    this->evi_node_.insert(this->node_store_.at(frame_id));
  } else {
    auto it = this->evi_node_.find(this->node_store_.at(frame_id));
    assert(it != this->evi_node_.end());
    this->evi_node_.erase(it);
    this->node_store_.at(frame_id).is_evictable_ = false;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::unique_lock lk{this->latch_};
  if (this->node_store_.count(frame_id) == 0) {
    return;
  }
  auto it = this->evi_node_.find(this->node_store_.at(frame_id));
  if (it == this->evi_node_.end()) {
    throw std::runtime_error("this frame can not be removed!");
  }
  this->node_store_.erase(frame_id);
  this->evi_node_.erase(it);
}

auto LRUKReplacer::Size() -> size_t {
  std::unique_lock lk{this->latch_};
  return evi_node_.size();
}

}  // namespace bustub
