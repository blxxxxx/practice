//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"
#include "common/exception.h"
#include "storage/disk/disk_manager.h"
namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  // TODO(P1): remove this line after you have implemented the disk scheduler API
  //  throw NotImplementedException(
  //      "DiskScheduler is not implemented yet. If you have finished implementing the disk scheduler, please remove the
  //      " "throw exception line in `disk_scheduler.cpp`.");

  // Spawn the background thread
  background_thread_.emplace([&] { StartWorkerThread(); });
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  request_queue_.Put(std::nullopt);
  if (background_thread_.has_value()) {
    background_thread_->join();
  }
}

void DiskScheduler::Schedule(DiskRequest r) { this->request_queue_.Put(std::optional<DiskRequest>(std::move(r))); }

void DiskScheduler::StartWorkerThread() {
  while (true) {
    std::optional<DiskRequest> res = this->request_queue_.Get();
    if (!res.has_value()) {
      break;
    }
    DiskRequest req = std::move(res.value());
    if (req.is_write_) {
      std::string str;
      char *p = req.data_;
      while (*p != 0) {
        str += *p;
        p++;
      }
      this->mp_[req.page_id_] = str;
    } else {
      char *p = req.data_;
      if (this->mp_.count(req.page_id_) == 0) {
        *p = '\0';
      } else {
        std::string str = this->mp_[req.page_id_];
        for (auto &c : str) {
          *p = c;
          p++;
        }
        *p = '\0';
      }
    }
    req.callback_.set_value(true);
  }
}

}  // namespace bustub
