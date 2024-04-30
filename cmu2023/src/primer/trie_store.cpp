#include "primer/trie_store.h"
#include "common/exception.h"

namespace bustub {

template <class T>
auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<T>> {
  // Pseudo-code:
  // (1) Take the root lock, get the root, and release the root lock. Don't lookup the value in the
  //     trie while holding the root lock.
  // (2) Lookup the value in the trie.
  // (3) If the value is found, return a ValueGuard object that holds a reference to the value and the
  //     root. Otherwise, return std::nullopt.
  Trie rt;
  {
    std::unique_lock lk{this->root_lock_};
    rt = this->root_;
  }
  const T *res = rt.Get<T>(key);
  if (res == nullptr) {
    return std::nullopt;
  }
  ValueGuard<T> vg(rt, *res);
  return vg;
  //  throw NotImplementedException("TrieStore::Get is not implemented.");
}

template <class T>
void TrieStore::Put(std::string_view key, T value) {
  // You will need to ensure there is only one writer at a time. Think of how you can achieve this.
  // The logic should be somehow similar to `TrieStore::Get`.
  std::unique_lock lk1{this->write_lock_};
  Trie rt;
  {
    std::unique_lock lk{this->root_lock_};
    rt = this->root_;
  }
  Trie new_rt = rt.Put<T>(key, std::move(value));
  {
    std::unique_lock lk{this->root_lock_};
    this->root_ = new_rt;
  }
  //  throw NotImplementedException("TrieStore::Put is not implemented.");
}

void TrieStore::Remove(std::string_view key) {
  // You will need to ensure there is only one writer at a time. Think of how you can achieve this.
  // The logic should be somehow similar to `TrieStore::Get`.
  std::unique_lock lk1{this->write_lock_};
  Trie rt;
  {
    std::unique_lock lk{this->root_lock_};
    rt = this->root_;
  }
  Trie new_rt = rt.Remove(key);
  {
    std::unique_lock lk{this->root_lock_};
    this->root_ = new_rt;
  }
  //  throw NotImplementedException("TrieStore::Remove is not implemented.");
}

// Below are explicit instantiation of template functions.

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<uint32_t>>;
template void TrieStore::Put(std::string_view key, uint32_t value);

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<std::string>>;
template void TrieStore::Put(std::string_view key, std::string value);

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<Integer>>;
template void TrieStore::Put(std::string_view key, Integer value);

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<MoveBlocked>>;
template void TrieStore::Put(std::string_view key, MoveBlocked value);

}  // namespace bustub
