//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_header_page.cpp
//
// Identification: src/storage/page/extendible_htable_header_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_header_page.h"

#include <cstring>
#include "common/exception.h"

namespace bustub {

void ExtendibleHTableHeaderPage::Init(uint32_t max_depth) {
  //  throw NotImplementedException("ExtendibleHTableHeaderPage is not implemented");
  this->max_depth_ = max_depth;
  memset(this->directory_page_ids_, INVALID_PAGE_ID, sizeof(this->directory_page_ids_));
}

auto ExtendibleHTableHeaderPage::HashToDirectoryIndex(uint32_t hash) const -> uint32_t {
  if (this->max_depth_ == 0) {
    return 0;
  }
  uint32_t move_cnt = 32 - this->max_depth_;
  uint32_t header = hash >> move_cnt;
  return header;
}

auto ExtendibleHTableHeaderPage::GetDirectoryPageId(uint32_t directory_idx) const -> int32_t {
  assert(directory_idx < static_cast<uint32_t>(1 << this->max_depth_));
  return this->directory_page_ids_[directory_idx];
}

void ExtendibleHTableHeaderPage::SetDirectoryPageId(uint32_t directory_idx, page_id_t directory_page_id) {
  //  throw NotImplementedException("ExtendibleHTableHeaderPage is not implemented");
  assert(directory_idx < static_cast<uint32_t>(1 << this->max_depth_));
  this->directory_page_ids_[directory_idx] = directory_page_id;
}

auto ExtendibleHTableHeaderPage::MaxSize() const -> uint32_t { return (1 << this->max_depth_); }

}  // namespace bustub
