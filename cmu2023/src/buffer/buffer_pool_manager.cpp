//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  //  throw NotImplementedException(
  //      "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //      "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::unique_lock all_lk{this->simple_safe_};
  if (this->free_list_.empty() && this->replacer_->Size() == 0) {
    *page_id = INVALID_PAGE_ID;
    return nullptr;
  }
  frame_id_t alloc_f;
  if (!this->free_list_.empty()) {
    alloc_f = free_list_.back();
    free_list_.pop_back();
  } else {
    this->replacer_->Evict(&alloc_f);
    if (pages_[alloc_f].is_dirty_) {
      // write back
      this->disk_scheduler_->disk_manager_->WritePage(this->pages_[alloc_f].page_id_, this->pages_[alloc_f].data_);
    }
    this->page_table_.erase(this->pages_[alloc_f].page_id_);
  }
  {
    std::unique_lock lk{this->latch_};
    pages_[alloc_f].page_id_ = AllocatePage();
  }
  pages_[alloc_f].is_dirty_ = false;
  pages_[alloc_f].pin_count_ = 1;
  memset(this->pages_[alloc_f].data_, 0, BUSTUB_PAGE_SIZE);
  replacer_->RecordAccess(alloc_f);
  *page_id = pages_[alloc_f].page_id_;
  this->page_table_[*page_id] = alloc_f;
  return pages_ + alloc_f;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::unique_lock all_lk{this->simple_safe_};
  if (this->page_table_.count(page_id) == 0 && this->replacer_->Size() == 0 && this->free_list_.empty()) {
    return nullptr;
  }
  frame_id_t fid = INVALID_PAGE_ID;
  if (this->page_table_.count(page_id) != 0) {
    fid = this->page_table_[page_id];
    pages_[fid].pin_count_++;
    if (pages_[fid].pin_count_ == 1) {
      this->replacer_->SetEvictable(fid, false);
    }
    this->replacer_->RecordAccess(fid);
    return pages_ + fid;
  }
  if (!this->free_list_.empty()) {
    fid = this->free_list_.back();
    this->free_list_.pop_back();
  } else {
    this->replacer_->Evict(&fid);
    if (this->pages_[fid].is_dirty_) {
      // write back
      this->disk_scheduler_->disk_manager_->WritePage(this->pages_[fid].page_id_, this->pages_[fid].data_);
    }
    this->page_table_.erase(this->pages_[fid].page_id_);
  }
  this->page_table_[page_id] = fid;
  this->pages_[fid].pin_count_ = 1;
  this->pages_[fid].page_id_ = page_id;
  this->pages_[fid].is_dirty_ = false;
  this->replacer_->RecordAccess(fid);
  this->disk_scheduler_->disk_manager_->ReadPage(page_id, this->pages_[fid].data_);
  return pages_ + fid;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::unique_lock all_lk{this->simple_safe_};
  if (this->page_table_.count(page_id) == 0 || this->pages_[this->page_table_[page_id]].pin_count_ <= 0) {
    return false;
  }
  frame_id_t fid = this->page_table_[page_id];
  pages_[fid].pin_count_--;
  if (pages_[fid].pin_count_ == 0) {
    this->replacer_->SetEvictable(fid, true);
  }
  if (is_dirty) {
    this->pages_[fid].is_dirty_ = true;
  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::unique_lock all_lk{this->simple_safe_};
  if (this->page_table_.count(page_id) == 0) {
    return false;
  }
  frame_id_t fid = this->page_table_[page_id];
  this->disk_scheduler_->disk_manager_->WritePage(page_id, this->pages_[fid].data_);
  this->pages_[fid].is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::unique_lock all_lk{this->simple_safe_};
  for (size_t i = 0; i < pool_size_; ++i) {
    this->disk_scheduler_->disk_manager_->WritePage(this->pages_[i].page_id_, this->pages_[i].data_);
    this->pages_[i].is_dirty_ = false;
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::unique_lock all_lk{this->simple_safe_};
  if (this->page_table_.count(page_id) == 0) {
    return true;
  }
  frame_id_t fid = this->page_table_[page_id];
  if (this->pages_[fid].pin_count_ >= 1) {
    return false;
  }
  this->free_list_.push_back(fid);
  this->replacer_->Remove(fid);
  this->page_table_.erase(page_id);
  this->pages_[fid].pin_count_ = 0;
  this->pages_[fid].page_id_ = INVALID_PAGE_ID;
  this->pages_[fid].is_dirty_ = false;
  memset(this->pages_[fid].data_, 0, BUSTUB_PAGE_SIZE);
  this->DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
