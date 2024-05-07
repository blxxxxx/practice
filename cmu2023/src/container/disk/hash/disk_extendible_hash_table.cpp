//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  //  throw NotImplementedException("DiskExtendibleHashTable is not implemented");
  this->index_name_ = name;
  page_id_t pid = INVALID_PAGE_ID;
  BasicPageGuard guard_b = this->bpm_->NewPageGuarded(&pid);
  assert(pid != INVALID_PAGE_ID);
  WritePageGuard guard_w = guard_b.UpgradeWrite();
  auto *header = guard_w.AsMut<ExtendibleHTableHeaderPage>();
  header->Init(header_max_depth);
  this->header_page_id_ = pid;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  uint32_t hash = this->Hash(key);
  ReadPageGuard guard_r = this->bpm_->FetchPageRead(this->header_page_id_);
  if (guard_r.GetData() == nullptr) {
    return false;
  }
  auto *header = guard_r.As<ExtendibleHTableHeaderPage>();
  uint32_t directory_idx = header->HashToDirectoryIndex(hash);
  page_id_t dir_pid = header->GetDirectoryPageId(directory_idx);
  if (dir_pid == INVALID_PAGE_ID) {
    return false;
  }
  guard_r.Drop();
  guard_r = this->bpm_->FetchPageRead(dir_pid);
  if (guard_r.GetData() == nullptr) {
    return false;
  }
  auto *directory = guard_r.As<ExtendibleHTableDirectoryPage>();
  uint32_t bucket_idx = directory->HashToBucketIndex(hash);
  page_id_t buc_pid = directory->GetBucketPageId(bucket_idx);
  if (buc_pid == INVALID_PAGE_ID) {
    return false;
  }
  guard_r.Drop();
  guard_r = this->bpm_->FetchPageRead(buc_pid);
  if (guard_r.GetData() == nullptr) {
    return false;
  }
  auto *bucket = guard_r.As<ExtendibleHTableBucketPage<K, V, KC>>();
  V res;
  if (!bucket->Lookup(key, res, this->cmp_)) {
    return false;
  }
  result->clear();
  result->push_back(res);
  return true;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  uint32_t hash = this->Hash(key);
  WritePageGuard guard_w = this->bpm_->FetchPageWrite(this->header_page_id_);
  if (guard_w.GetData() == nullptr) {
    return false;
  }
  auto *header = guard_w.As<ExtendibleHTableHeaderPage>();
  uint32_t directory_idx = header->HashToDirectoryIndex(hash);
  page_id_t dir_pid = header->GetDirectoryPageId(directory_idx);
  if (dir_pid == INVALID_PAGE_ID) {
    // 没有对应的目录
    auto *header_w = guard_w.AsMut<ExtendibleHTableHeaderPage>();
    return this->InsertToNewDirectory(header_w, directory_idx, key, value);
  }
  guard_w.Drop();
  return this->InsertToDirectory(dir_pid, hash, key, value);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToDirectory(page_id_t dir_pid, uint32_t hash, const K &key,
                                                          const V &value) -> bool {
  WritePageGuard guard_w = this->bpm_->FetchPageWrite(dir_pid);
  if (guard_w.GetData() == nullptr) {
    return false;
  }
  auto *directory = guard_w.As<ExtendibleHTableDirectoryPage>();
  uint32_t bucket_idx = directory->HashToBucketIndex(hash);
  page_id_t buc_pid = directory->GetBucketPageId(bucket_idx);
  if (buc_pid == INVALID_PAGE_ID) {
    auto *directory_w = guard_w.AsMut<ExtendibleHTableDirectoryPage>();
    return this->InsertToNewBucket(directory_w, bucket_idx, key, value);
  }
  // 不能放锁，以防别人把目录改了
  WritePageGuard guard_w_b = this->bpm_->FetchPageWrite(buc_pid);
  if (guard_w_b.GetData() == nullptr) {
    return false;
  }
  auto *bucket = guard_w_b.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  if (!bucket->IsFull()) {
    guard_w.Drop();
    return bucket->Insert(key, value, this->cmp_);
  }
  //目录一直锁着 不用担心任何问题
  guard_w_b.Drop();
  auto *directory_w = guard_w.AsMut<ExtendibleHTableDirectoryPage>();
  return this->InsertToFullBucket(directory_w, hash, key, value);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToFullBucket(ExtendibleHTableDirectoryPage *directory, uint32_t hash,
                                                           const K &key, const V &value) -> bool {
  while (true) {
    uint32_t bucket_idx = directory->HashToBucketIndex(hash);
    page_id_t tar_pid = directory->GetBucketPageId(bucket_idx);
    WritePageGuard guard_w = this->bpm_->FetchPageWrite(tar_pid);
    if (guard_w.GetData() == nullptr) {
      return false;
    }
    auto *bucket = guard_w.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    if (!bucket->IsFull()) {
      return bucket->Insert(key, value, this->cmp_);
    }
    if (directory->GetGlobalDepth() == directory->GetLocalDepth(bucket_idx)) {
      if (directory->GetMaxDepth() == directory->GetGlobalDepth()) {
        return false;
      }
      directory->IncrGlobalDepth();
    }
    page_id_t new_pid = this->SplitFullBucket(bucket, directory->GetLocalDepth(bucket_idx));
    uint32_t start_idx = bucket_idx & directory->GetLocalDepthMask(bucket_idx);
    uint32_t offset = (1 << directory->GetLocalDepth(bucket_idx));
    uint32_t tar_depth = directory->GetLocalDepth(bucket_idx) + 1;
    for (uint32_t i = start_idx; i < directory->Size(); i += offset) {
      if (static_cast<bool>(i & offset)) {
        this->UpdateDirectoryMapping(directory, i, new_pid, tar_depth);
      } else {
        this->UpdateDirectoryMapping(directory, i, tar_pid, tar_depth);
      }
    }
    guard_w.Drop();
  }
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             const K &key, const V &value) -> bool {
  page_id_t pid = INVALID_PAGE_ID;
  BasicPageGuard guard_b = this->bpm_->NewPageGuarded(&pid);
  if (pid == INVALID_PAGE_ID) {
    return false;
  }
  WritePageGuard guard_w = guard_b.UpgradeWrite();
  auto *new_directory = guard_w.AsMut<ExtendibleHTableDirectoryPage>();
  new_directory->Init(this->directory_max_depth_);
  header->SetDirectoryPageId(directory_idx, pid);
  return this->InsertToNewBucket(new_directory, 0, key, value);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  page_id_t pid = INVALID_PAGE_ID;
  BasicPageGuard guard_b = this->bpm_->NewPageGuarded(&pid);
  if (pid == INVALID_PAGE_ID) {
    return false;
  }
  WritePageGuard guard_w = guard_b.UpgradeWrite();
  auto *new_bucket = guard_w.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  new_bucket->Init(this->bucket_max_size_);
  new_bucket->Insert(key, value, this->cmp_);
  this->UpdateDirectoryMapping(directory, bucket_idx, pid, 0);
  return true;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::SplitFullBucket(ExtendibleHTableBucketPage<K, V, KC> *origin_bucket,
                                                        uint32_t depth) -> page_id_t {
  page_id_t new_pid = INVALID_PAGE_ID;
  BasicPageGuard guard_b = this->bpm_->NewPageGuarded(&new_pid);
  if (new_pid == INVALID_PAGE_ID) {
    return INVALID_PAGE_ID;
  }
  WritePageGuard guard_w = guard_b.UpgradeWrite();
  auto *new_bucket = guard_w.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  new_bucket->Init(this->bucket_max_size_);
  std::vector<K> tmp;
  for (uint32_t i = 0; i < origin_bucket->Size(); ++i) {
    auto kv = origin_bucket->EntryAt(i);
    uint32_t hash = this->Hash(kv.first);
    if (static_cast<bool>(hash >> depth & 1)) {
      new_bucket->Insert(kv.first, kv.second, this->cmp_);
      tmp.push_back(kv.first);
    }
  }
  for (auto &key : tmp) {
    origin_bucket->Remove(key, this->cmp_);
  }
  return new_pid;
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth) {
  //  throw NotImplementedException("DiskExtendibleHashTable is not implemented");
  directory->SetBucketPageId(new_bucket_idx, new_bucket_page_id);
  directory->SetLocalDepth(new_bucket_idx, new_local_depth);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  uint32_t hash = this->Hash(key);
  ReadPageGuard guard_r = this->bpm_->FetchPageRead(this->header_page_id_);
  if (guard_r.GetData() == nullptr) {
    return false;
  }
  auto *header = guard_r.As<ExtendibleHTableHeaderPage>();
  uint32_t directory_idx = header->HashToDirectoryIndex(hash);
  page_id_t dir_pid = header->GetDirectoryPageId(directory_idx);
  if (dir_pid == INVALID_PAGE_ID) {
    return false;
  }
  guard_r.Drop();
  WritePageGuard guard_w_d = this->bpm_->FetchPageWrite(dir_pid);
  if (guard_w_d.GetData() == nullptr) {
    return false;
  }
  auto *directory = guard_w_d.As<ExtendibleHTableDirectoryPage>();
  uint32_t bucket_idx = directory->HashToBucketIndex(hash);
  page_id_t buc_pid = directory->GetBucketPageId(bucket_idx);
  if (buc_pid == INVALID_PAGE_ID) {
    return false;
  }
  WritePageGuard guard_w_b = this->bpm_->FetchPageWrite(buc_pid);
  if (guard_w_b.GetData() == nullptr) {
    return false;
  }
  auto *bucket = guard_w_b.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  if (!bucket->Remove(key, this->cmp_)) {
    return false;
  }
  if (!bucket->IsEmpty()) {
    return true;
  }
  guard_w_b.Drop();
  //目录节点锁住了 桶节点应该是不会出现问题的
  auto *directory_w = guard_w_d.AsMut<ExtendibleHTableDirectoryPage>();
  if (!this->SolveEmptyBucket(directory_w, bucket_idx)) {
    return false;
  }
  while (directory_w->CanShrink()) {
    directory_w->DecrGlobalDepth();
  }
  return true;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::SolveEmptyBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx)
    -> bool {
  while (true) {
    if (directory->GetLocalDepth(bucket_idx) == 0) {
      return true;
    }
    uint32_t split_idx = directory->GetSplitImageIndex(bucket_idx);
    if (directory->GetLocalDepth(bucket_idx) != directory->GetLocalDepth(split_idx)) {
      return true;
    }
    uint32_t buc_pid = directory->GetBucketPageId(bucket_idx);
    WritePageGuard guard_w_b = this->bpm_->FetchPageWrite(buc_pid);
    if (guard_w_b.GetData() == nullptr) {
      return false;
    }
    auto *bucket = guard_w_b.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    if (!bucket->IsEmpty()) {
      return true;
    }
    page_id_t split_pid = directory->GetBucketPageId(split_idx);
    uint32_t tar_depth = directory->GetLocalDepth(bucket_idx) - 1;
    uint32_t start_idx = bucket_idx & ((static_cast<uint32_t>(1) << tar_depth) - 1);
    uint32_t offset = (static_cast<uint32_t>(1) << tar_depth);
    for (uint32_t i = start_idx; i < directory->Size(); i += offset) {
      this->UpdateDirectoryMapping(directory, i, split_pid, tar_depth);
    }
    this->bpm_->DeletePage(buc_pid);
    bucket_idx = directory->GetSplitImageIndex(bucket_idx);
  }
}
// template <typename K, typename V, typename KC>
// void DiskExtendibleHashTable<K, V, KC>::MigrateEntries(ExtendibleHTableBucketPage<K, V, KC> *old_bucket,
//                                                        ExtendibleHTableBucketPage<K, V, KC> *new_bucket,
//                                                        uint32_t new_bucket_idx, uint32_t local_depth_mask){
//
// }
template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
