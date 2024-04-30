#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  //  throw NotImplementedException("Trie::Get is not implemented.");
  std::shared_ptr<const TrieNode> nowp = this->root_;
  if (nowp == nullptr) {
    return nullptr;
  }
  for (char i : key) {
    if (nowp->children_.count(i)) {
      auto &mp = nowp->children_;
      nowp = mp.at(i);
    } else {
      return nullptr;
    }
  }
  if (!nowp->is_value_node_) {
    return nullptr;
  }
  std::shared_ptr<const TrieNodeWithValue<T>> valp = std::dynamic_pointer_cast<const TrieNodeWithValue<T>>(nowp);
  if (valp == nullptr) {
    return nullptr;
  }
  return valp->value_.get();
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  //  throw NotImplementedException("Trie::Put is not implemented.");
  if (key.empty()) {
    std::shared_ptr<T> p1 = std::make_shared<T>(std::move(value));
    if (this->root_ == nullptr) {
      std::shared_ptr<TrieNode> res = std::make_shared<TrieNodeWithValue<T>>(p1);
      return Trie(res);
    }
    std::shared_ptr<TrieNode> res = std::make_shared<TrieNodeWithValue<T>>(this->root_->children_, p1);
    return Trie(res);
  }
  std::shared_ptr<TrieNode> nowp;
  if (this->root_ == nullptr) {
    nowp = std::make_shared<TrieNode>();
  } else {
    nowp = this->root_->Clone();
  }
  std::shared_ptr<TrieNode> res = nowp;
  std::shared_ptr<TrieNode> last = nowp;
  for (char i : key) {
    last = nowp;
    if (nowp->children_.count(i)) {
      std::shared_ptr<TrieNode> newp = nowp->children_.at(i)->Clone();
      nowp->children_[i] = newp;
      nowp = newp;
    } else {
      std::shared_ptr<TrieNode> newp = std::make_shared<TrieNode>();
      nowp->children_[i] = newp;
      nowp = newp;
    }
  }
  //    if(nowp->is_value_node_){
  //        auto up_p = std::dynamic_pointer_cast<TrieNodeWithValue<T>>(nowp);
  //        assert(up_p != nullptr);
  //        up_p->value_ = std::make_shared<T>(std::move(value));
  //
  //        std::shared_ptr<T> p1 = std::make_shared<T>(std::move(value));
  //        std::shared_ptr<TrieNode> p2 = std::make_shared<TrieNodeWithValue<T>>(nowp->children_,p1);
  //
  //    }else{
  std::shared_ptr<T> p1 = std::make_shared<T>(std::move(value));
  std::shared_ptr<TrieNode> p2 = std::make_shared<TrieNodeWithValue<T>>(nowp->children_, p1);
  last->children_[key.back()] = p2;
  //    }
  //    int cnt = 0;
  //    std::function<void(std::shared_ptr<const TrieNode>)> dfs = [&](std::shared_ptr<const TrieNode> u) -> void{
  //        std::cout << ++cnt << ":  ";
  //        if(u->is_value_node_)std::cout << "!!";
  //        for(auto & x : u->children_){
  //            std::cout << x.first << ' ';
  //        }
  //        std::cout << '\n';
  //        for(auto & x : u->children_){
  //            dfs(x.second);
  //        }
  //    };
  //    dfs(res);
  return Trie(res);
  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
}

auto Trie::Remove(std::string_view key) const -> Trie {
  //  throw NotImplementedException("Trie::Remove is not implemented.");
  if (this->root_ == nullptr) {
    return *this;
  }
  if (key.empty()) {
    if (!this->root_->is_value_node_) {
      return *this;
    }
    std::shared_ptr<TrieNode> res = std::make_shared<TrieNode>(this->root_->children_);
    return Trie(res);
  }
  {
    std::shared_ptr<const TrieNode> nowp = this->root_;
    for (char i : key) {
      if (nowp->children_.count(i) == 1) {
        nowp = nowp->children_.at(i);
      } else {
        return *this;
      }
    }
  }

  std::shared_ptr<TrieNode> nowp = this->root_->Clone();
  std::shared_ptr<TrieNode> res = nowp;
  std::shared_ptr<TrieNode> last = nowp;
  std::shared_ptr<TrieNode> fa = nowp;
  char c = key[0];
  for (char i : key) {
    fa = nowp;
    if (nowp->is_value_node_ || nowp->children_.size() >= 2) {
      last = nowp;
      c = i;
    }
    if (nowp->children_.count(i) == 1) {
      std::shared_ptr<TrieNode> newp = nowp->children_.at(i)->Clone();
      nowp->children_[i] = newp;
      nowp = newp;
    }
  }
  if (nowp->children_.empty()) {
    last->children_.erase(c);
  } else {
    std::shared_ptr<TrieNode> p1 = std::make_shared<TrieNode>(nowp->children_);
    fa->children_[key.back()] = p1;
  }
  if (res->children_.empty() && !res->is_value_node_) {
    return Trie(nullptr);
  }
  //    int cnt = 0;
  //    std::function<void(std::shared_ptr<const TrieNode>)> dfs = [&](std::shared_ptr<const TrieNode> u) -> void{
  //        std::cout << ++cnt << ":  ";
  //        if(u->is_value_node_)std::cout << "!!";
  //        for(auto & x : u->children_){
  //            std::cout << x.first << ' ';
  //        }
  //        std::cout << '\n';
  //        for(auto & x : u->children_){
  //            dfs(x.second);
  //        }
  //    };
  //    dfs(res);
  return Trie(res);
  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
