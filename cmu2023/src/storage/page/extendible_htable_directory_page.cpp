//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_directory_page.cpp
//
// Identification: src/storage/page/extendible_htable_directory_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_directory_page.h"

#include <algorithm>
#include <unordered_map>

#include "common/config.h"
#include "common/logger.h"

namespace bustub {

void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth) {
  //  throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  this->max_depth_ = max_depth;
  memset(this->bucket_page_ids_, INVALID_PAGE_ID, sizeof(this->bucket_page_ids_));
  this->global_depth_ = 0;
  memset(this->local_depths_, 0, sizeof(this->local_depths_));
}

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t {
  return (hash & this->GetGlobalDepthMask());
}

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t {
  assert(bucket_idx < static_cast<uint32_t>(1) << this->global_depth_);
  return this->bucket_page_ids_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  //  throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  assert(bucket_idx < static_cast<uint32_t>(1) << this->global_depth_);
  this->bucket_page_ids_[bucket_idx] = bucket_page_id;
}

auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t {
  assert(bucket_idx < static_cast<uint32_t>(1 << this->global_depth_));
  if (this->GetLocalDepth(bucket_idx) == 0) {
    return bucket_idx;
  }
  uint32_t offset = static_cast<uint32_t>(1) << (this->GetLocalDepth(bucket_idx) - 1);
  return (bucket_idx ^ offset);
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t { return this->global_depth_; }

void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
  //  throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  assert(this->global_depth_ < this->max_depth_);
  uint32_t offset = static_cast<uint32_t>(1) << this->global_depth_;
  for (uint32_t i = offset; i < 2 * offset; ++i) {
    this->local_depths_[i] = this->local_depths_[i - offset];
    this->bucket_page_ids_[i] = this->bucket_page_ids_[i - offset];
  }
  this->global_depth_++;
}

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() {
  //  throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  assert(this->global_depth_ != 0);
  this->global_depth_--;
  uint32_t offset = static_cast<uint32_t>(1) << this->global_depth_;
  for (uint32_t i = 0; i < offset; ++i) {
    assert(this->local_depths_[i] == this->local_depths_[i + offset]);
    assert(this->bucket_page_ids_[i] == this->bucket_page_ids_[i + offset]);
  }
}

auto ExtendibleHTableDirectoryPage::CanShrink() -> bool {
  if (this->global_depth_ == 0) {
    return false;
  }
  uint32_t offset = static_cast<uint32_t>(1) << (this->global_depth_ - 1);
  for (uint32_t i = 0; i < offset; ++i) {
    if (this->bucket_page_ids_[i] != this->bucket_page_ids_[i + offset]) {
      return false;
    }
    assert(this->local_depths_[i] == this->local_depths_[i + offset]);
  }
  return true;
}

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t { return static_cast<uint32_t>(1) << this->global_depth_; }

auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t {
  assert(bucket_idx < static_cast<uint32_t>(1) << this->global_depth_);
  return this->local_depths_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  //  throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  assert(bucket_idx < static_cast<uint32_t>(1) << this->global_depth_);
  this->local_depths_[bucket_idx] = local_depth;
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) {
  //  throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  assert(bucket_idx < static_cast<uint32_t>(1) << this->global_depth_);
  assert(this->local_depths_[bucket_idx] < this->global_depth_);
  this->local_depths_[bucket_idx]++;
}

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) {
  //  throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  assert(bucket_idx < static_cast<uint32_t>(1) << this->global_depth_);
  assert(this->local_depths_[bucket_idx] != 0);
  this->local_depths_[bucket_idx]--;
}

auto ExtendibleHTableDirectoryPage::MaxSize() const -> uint32_t {
  return (static_cast<uint32_t>(1) << this->max_depth_);
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepthMask() const -> uint32_t {
  return (static_cast<uint32_t>(1) << this->global_depth_) - static_cast<uint32_t>(1);
}

auto ExtendibleHTableDirectoryPage::GetLocalDepthMask(uint32_t bucket_idx) const -> uint32_t {
  return (static_cast<uint32_t>(1) << this->local_depths_[bucket_idx]) - static_cast<uint32_t>(1);
}

auto ExtendibleHTableDirectoryPage::GetMaxDepth() const -> uint32_t { return this->max_depth_; }
}  // namespace bustub
