#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  this->bpm_ = that.bpm_;
  that.bpm_ = nullptr;
  this->page_ = that.page_;
  that.page_ = nullptr;
  this->is_dirty_ = that.is_dirty_;
  that.is_dirty_ = false;
}

void BasicPageGuard::Drop() {
  if (this->bpm_ == nullptr) {
    return;
  }
  assert(this->page_ != nullptr);
  this->bpm_->UnpinPage(this->page_->GetPageId(), this->is_dirty_);
  this->bpm_ = nullptr;
  this->page_ = nullptr;
  this->is_dirty_ = false;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  this->Drop();
  this->bpm_ = that.bpm_;
  that.bpm_ = nullptr;
  this->page_ = that.page_;
  that.page_ = nullptr;
  this->is_dirty_ = that.is_dirty_;
  that.is_dirty_ = false;
  return *this;
}

BasicPageGuard::~BasicPageGuard() {
  if (this->bpm_ != nullptr) {
    this->Drop();
  }
}  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept : guard_(std::move(that.guard_)) {}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  this->Drop();
  this->guard_ = std::move(that.guard_);
  return *this;
}

void ReadPageGuard::Drop() {
  if (this->guard_.bpm_ == nullptr) {
    return;
  }
  assert(this->guard_.page_ != nullptr);
  this->guard_.page_->RUnlatch();
  this->guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() {
  if (this->guard_.bpm_ != nullptr) {
    this->Drop();
  }
}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept : guard_(std::move(that.guard_)) {}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  this->Drop();
  this->guard_ = std::move(that.guard_);
  return *this;
}

void WritePageGuard::Drop() {
  if (this->guard_.bpm_ == nullptr) {
    return;
  }
  assert(this->guard_.page_ != nullptr);
  this->guard_.page_->WUnlatch();
  this->guard_.Drop();
}

WritePageGuard::~WritePageGuard() {
  if (this->guard_.bpm_ != nullptr) {
    this->Drop();
  }
}  // NOLINT

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  assert(this->page_ != nullptr);
  this->page_->RLatch();
  ReadPageGuard res(this->bpm_, this->page_);
  this->bpm_ = nullptr;
  this->page_ = nullptr;
  return res;
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  assert(this->page_ != nullptr);
  this->page_->WLatch();
  WritePageGuard res(this->bpm_, this->page_);
  this->bpm_ = nullptr;
  this->page_ = nullptr;
  return res;
}
}  // namespace bustub
