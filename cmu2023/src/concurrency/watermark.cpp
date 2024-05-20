#include "concurrency/watermark.h"
#include <exception>
#include "common/exception.h"

namespace bustub {

auto Watermark::AddTxn(timestamp_t read_ts) -> void {
  if (read_ts < commit_ts_) {
    throw Exception("read ts < commit ts");
  }
  // TODO(fall2023): implement me!
  if (this->current_reads_.count(read_ts) == 0) {
    this->current_reads_.insert(std::make_pair(read_ts, 0));
  }
  this->current_reads_[read_ts]++;
}

auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
  // TODO(fall2023): implement me!
  this->current_reads_[read_ts]--;
  if (this->current_reads_[read_ts] == 0) {
    this->current_reads_.erase(read_ts);
  }
  while (watermark_ != commit_ts_ && this->current_reads_.count(watermark_) == 0) {
    watermark_++;
  }
}

}  // namespace bustub
