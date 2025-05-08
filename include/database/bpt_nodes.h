#ifndef INSOMNIA_BPT_NODES_H
#define INSOMNIA_BPT_NODES_H

#include <cassert>
#include <shared_mutex>

#include "index_pool.h"

namespace insomnia {

template <class T>
concept Trivial = std::is_trivially_copyable_v<T>;

class BptNodeBase {
public:
  using index_t = IndexPool::index_t;
  index_t nullpos = IndexPool::nullpos;
  enum class NodeType { Invalid, Internal, Leaf };

  BptNodeBase(NodeType ntype, int max_size, int min_size, int merge_bound)
    : ntype_(ntype), max_size_(max_size), min_size_(min_size), merge_bound_(merge_bound) {}

  NodeType type() const { return ntype_; }

  bool is_leaf() const { return ntype_ == NodeType::Leaf; }

  int size() const { return size_; }

  void set_root() { min_size_ = 2; is_root_ = true; }

  int max_size() const { return max_size_; }

  int min_size() const { return min_size_; }

  int merge_bound() const { return merge_bound_; }

  bool is_insert_safe() const { return size_ < max_size_; }

  bool is_remove_safe() const { return size_ > min_size_; }

  bool is_valid() const { return min_size_ <= size_ && size_ <= max_size_; }

  bool is_root() const { return is_root_; }

protected:
  const NodeType ntype_;
  const int max_size_, merge_bound_;
  int min_size_;
  int size_{0};
  bool is_root_{false};
};

template <Trivial KeyT, Trivial ValueT, class KeyCompare = std::less<KeyT>>
class BptInternalNode : public BptNodeBase {
public:
  static constexpr size_t SIZE_KV = sizeof(KeyT) + sizeof(ValueT);
  static constexpr size_t SIZE_INFO = sizeof(KeyCompare) + sizeof(KeyT) + sizeof(index_t);
  static constexpr int CAPACITY = std::max(8ul,
    ((SIZE_KV + SIZE_INFO + 4095) / 4096 * 4096 - SIZE_INFO) / SIZE_KV);
  static constexpr int MAX_SIZE = CAPACITY - 1, MIN_SIZE = CAPACITY * 0.40, MERGE_BOUND = CAPACITY * 0.90;
  BptInternalNode(): BptNodeBase(NodeType::Internal, MAX_SIZE, MIN_SIZE, MERGE_BOUND) {}

  void set_not_root() { min_size_ = MIN_SIZE; is_root_ = false; }

  KeyT& key(int index) { return pairs_[index].key; }
  ValueT& value(int index) { return pairs_[index].value; }
  KeyT& highkey() { return pairs_[size_ - 1].key; }

  const KeyT& key(int index) const { return pairs_[index].key; }
  const ValueT& value(int index) const { return pairs_[index].value; }
  const KeyT& highkey() const { return pairs_[size_ - 1].key; }

  // warning: if key > every key here, this function still returns size_ - 1.
  // you'll need to call highkey() to pre-judge.
  int upper_bound(const KeyT &key) const {
    int l = 0, r = size_ - 1;
    while(l < r) {
      int mid = (l + r) / 2;
      if(key_compare_(pairs_[mid].key, key)) l = mid + 1;
      else r = mid;
    }
    return l;
  }

  // be sure pos <= size_.
  void insert(int pos, const KeyT &key, const ValueT &value) {
    if(pos > size_) throw debug_exception("inserting in invalid place");
    memmove(pairs_ + pos + 1, pairs_ + pos, (size_ - pos) * SIZE_KV);
    memcpy(&pairs_[pos].key, &key, sizeof(KeyT));
    memcpy(&pairs_[pos].value, &value, sizeof(ValueT));
    ++size_;
  }

  void share(BptInternalNode &rhs, index_t rhs_index) {
    // rhs should be empty.
    int lft_size = size_ / 2;
    int rht_size = size_ / 2;
    memmove(rhs.pairs_, pairs_ + lft_size, rht_size * sizeof(KVPair));
    size_ = lft_size;
    rhs.size_ = rht_size;
    rhs.rht_ = rht_;
    rht_ = rhs_index;
    assert(rht_ != 25769836543);
    assert(rhs.rht_ != 25769836543);
  }

  void remove(int pos) {
    if(pos >= size_) throw debug_exception("removing in invalid place");
    memmove(pairs_ + pos, pairs_ + pos + 1, (size_ - pos - 1) * sizeof(KVPair));
    --size_;
  }

  void absorb_right(BptInternalNode &rhs) {
    memmove(pairs_ + size_, rhs.pairs_, rhs.size_ * sizeof(KVPair));
    size_ = size_ + rhs.size_;
    rhs.size_ = 0;
    rht_ = rhs.rht_;
  }

  void fetch_from_right(BptInternalNode &rhs) {
    int size_sum = size_ + rhs.size_;
    int lft_size = size_sum / 2;
    int rht_size = size_sum - lft_size;
    int diff = lft_size - size_;
    memmove(pairs_ + size_, rhs.pairs_, diff * sizeof(KVPair));
    memmove(rhs.pairs_, rhs.pairs_ + diff, diff * sizeof(KVPair));
    size_ = lft_size;
    rhs.size_ = rht_size;
  }

  void fetch_from_left(BptInternalNode &lhs) {
    int size_sum = lhs.size_ + size_;
    int lft_size = size_sum / 2;
    int rht_size = size_sum - lft_size;
    int diff = rht_size - size_;
    memmove(pairs_ + diff, pairs_, diff * sizeof(KVPair));
    memmove(pairs_, lhs.pairs_ + lft_size, diff * sizeof(KVPair));
    size_ = rht_size;
    lhs.size_ = lft_size;
  }

private:
  struct KVPair {
    KeyT key;
    ValueT value;
  };
  // KeyT high_key_;
  KVPair pairs_[CAPACITY];
  KeyCompare key_compare_;
  index_t rht_{nullpos};
  // parent_{nullpos};
};

template <Trivial KeyT, class KeyCompare = std::less<KeyT>>
class BptLeafSingleNode : public BptNodeBase {
public:
  static constexpr size_t SIZE_INFO = sizeof(KeyCompare) + sizeof(KeyT) + sizeof(index_t);
  static constexpr int CAPACITY = std::max(8ul,
    ((sizeof(KeyT) + SIZE_INFO + 4095) / 4096 * 4096 - SIZE_INFO) / sizeof(KeyT));
  static constexpr int MAX_SIZE = CAPACITY - 1, MIN_SIZE = CAPACITY * 0.40, MERGE_BOUND = CAPACITY * 0.90;
  BptLeafSingleNode() : BptNodeBase(NodeType::Leaf, MAX_SIZE, MIN_SIZE, MERGE_BOUND) {}

  void set_not_root() { min_size_ = MIN_SIZE; is_root_ = false; }

  KeyT& key(int index) { return key_[index]; }
  KeyT& highkey() { return key_[size_ - 1]; }

  const KeyT& key(int index) const { return key_[index]; }
  const KeyT& highkey() const { return key_[size_ - 1]; }

  int upper_bound(const KeyT &key) const {
    if(key_compare_(highkey(), key)) return size_;
    int l = 0, r = size_ - 1;
    while(l < r) {
      int mid = (l + r) / 2;
      if(key_compare_(key_[mid], key)) l = mid + 1;
      else r = mid;
    }
    return l;
  }

  // be sure pos <= size_.
  void insert(int pos, const KeyT &key) {
    if(pos > size_) throw debug_exception("inserting in invalid place");
    memmove(key_ + pos + 1, key_ + pos, (size_ - pos) * sizeof(KeyT));
    memcpy(key_ + pos, &key, sizeof(KeyT));
    ++size_;
    assert(rht_ != 25769836543);
  }

  void remove(int pos) {
    if(pos >= size_) throw debug_exception("removing in invalid place");
    memmove(key_ + pos, key_ + pos + 1, (size_ - pos - 1) * sizeof(KeyT));
    --size_;
    assert(rht_ != 25769836543);
  }

  void share(BptLeafSingleNode &rhs, index_t rhs_index) {
    // rhs should be empty.
    assert(rht_ != 25769836543);
    int lft_size = size_ / 2;
    int rht_size = size_ - lft_size;
    memmove(rhs.key_, key_ + lft_size, rht_size * sizeof(KeyT));
    size_ = lft_size;
    rhs.size_ = rht_size;
    rhs.rht_ = rht_;
    rht_ = rhs_index;
  }

  void absorb_right(BptLeafSingleNode &rhs) {
    memmove(key_ + size_, rhs.key_, rhs.size_ * sizeof(KeyT));
    size_ = size_ + rhs.size_;
    rhs.size_ = 0;
    rht_ = rhs.rht_;
  }

  void fetch_from_right(BptLeafSingleNode &rhs) {
    int size_sum = size_ + rhs.size_;
    int lft_size = size_sum / 2;
    int rht_size = size_sum - lft_size;
    int diff = lft_size - size_;
    memmove(key_ + size_, rhs.key_, diff * sizeof(KeyT));
    memmove(rhs.key_, rhs.key_ + diff, diff * sizeof(KeyT));
    size_ = lft_size;
    rhs.size_ = rht_size;
  }

  void fetch_from_left(BptLeafSingleNode &lhs) {
    int size_sum = lhs.size_ + size_;
    int lft_size = size_sum / 2;
    int rht_size = size_sum - lft_size;
    int diff = rht_size - size_;
    memmove(key_ + diff, key_, diff * sizeof(KeyT));
    memmove(key_, lhs.key_ + lft_size, diff * sizeof(KeyT));
    size_ = rht_size;
    lhs.size_ = lft_size;
  }

  // index_t lft_index() const { return lft_; }
  index_t rht_index() const { return rht_; }

private:
  KeyT key_[CAPACITY];
  // KeyT high_key_;
  KeyCompare key_compare_;
  // index_t lft_{nullpos};
  index_t rht_{nullpos};

};

/*
template <Trivial KeyT, Trivial ValueT, class KeyCompare>
class BptLeafPairNode : public BptNodeBase {
public:
  static constexpr size_t SIZE_KV = sizeof(KeyT) + sizeof(ValueT);
  static constexpr size_t SIZE_INFO = sizeof(KeyCompare) + sizeof(KeyT) + sizeof(index_t);
  static constexpr int CAPACITY = std::max(8ul,
    ((SIZE_KV + SIZE_INFO + 4095) / 4096 * 4096 - SIZE_INFO) / SIZE_KV);
  static constexpr int MAX_SIZE = CAPACITY - 1, MIN_SIZE = CAPACITY * 0.40, MERGE_BOUND = CAPACITY * 0.90;
  BptLeafPairNode(): BptNodeBase(NodeType::Leaf, MAX_SIZE, MIN_SIZE, MERGE_BOUND) {}

  void set_not_root() { min_size_ = MIN_SIZE; is_root_ = false; }

  KeyT& key(int index) { return pairs_[index].key; }
  ValueT& value(int index) { return pairs_[index].value; }
  KeyT& highkey() { return pairs_[size_ - 1].key; }

  const KeyT& key(int index) const { return pairs_[index].key; }
  const ValueT& value(int index) const { return pairs_[index].value; }
  const KeyT& highkey() const { return pairs_[size_ - 1].key; }

  int upper_bound(const KeyT &key) const {
    if(key_compare_(highkey(), key)) return size_;
    int l = 0, r = size_ - 1;
    while(l < r) {
      int mid = (l + r) / 2;
      if(key_compare_(pairs_[mid].key, key)) l = mid + 1;
      else r = mid;
    }
    return l;
  }

  // be sure pos <= size_.
  void insert(int pos, const KeyT &key, const ValueT &value) {
    if(pos > size_) throw debug_exception("inserting in invalid place");
    memmove(pairs_ + pos + 1, pairs_ + pos, (size_ - pos) * SIZE_KV);
    memcpy(&pairs_[pos].key, &key, sizeof(KeyT));
    memcpy(&pairs_[pos].value, &value, sizeof(ValueT));
    ++size_;
  }

  void remove(int pos) {
    if(pos >= size_) throw debug_exception("removing in invalid place");
    memmove(pairs_ + pos, pairs_ + pos + 1, (size_ - pos - 1) * sizeof(KVPair));
    --size_;
  }

  // index_t lft_index() const { return lft_; }
  index_t rht_index() const { return rht_; }

private:
  struct KVPair {
    KeyT key;
    ValueT value;
  };
  KVPair pairs_[CAPACITY];
  // KeyT high_key_;
  KeyCompare key_compare_;
  // index_t lft_{nullpos};
  index_t rht_{nullpos};
};
*/

}



#include "bpt_nodes.tcc"

#endif