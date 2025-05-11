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

  BptNodeBase() = default;
  void init(NodeType ntype, int max_size) { ntype_ = ntype; max_size_ = max_size; }

  NodeType type() const { return ntype_; }
  bool is_leaf() const { return ntype_ == NodeType::Leaf; }
  int size() const { return size_; }

  int max_size() const { return max_size_; }
  int min_size() const { return max_size_ * 0.40; }
  int merge_bound() const { return max_size_ * 0.90; }

  bool is_too_large() const { return size() > max_size(); }
  bool is_too_small() const { return size() < min_size(); }

protected:

  void set_size(int size) { size_ = size; }
  void change_size_by(int diff) { size_ = size_ + diff; }

private:
  NodeType ntype_{NodeType::Invalid};
  int max_size_{0};
  int size_{0};
};

template <Trivial KeyT, Trivial ValueT>
class BptInternalNode : public BptNodeBase {
  struct Storage {
    KeyT key;
    ValueT value;
  };

public:
  static constexpr int CAPACITY = std::max(8ul,
    ((sizeof(Storage) + 4095) / 4096 * 4096) / sizeof(Storage));

  BptInternalNode() = default;
  void init(int max_size = CAPACITY - 1) { BptNodeBase::init(NodeType::Internal, max_size); }

  const KeyT& key(int pos) const { return storage_[pos].key; }
  const ValueT& value(int pos) const { assert(pos < size());  return storage_[pos].value; }

  void write_key(int pos, const KeyT &key) { storage_[pos].key = key; }
  void write_value(int pos, const ValueT &value) { storage_[pos].value = value; }

  template <class KeyCompare>
  int locate_key(const KeyT &key, const KeyCompare &key_compare) const;
  template <class T, class Compare>
  requires requires(const T &t, const KeyT &key, const Compare &compare) {
    { compare(t, key) } -> std::convertible_to<bool>;
  }
  int locate_any(const T &t, const Compare &compare) const;

  void insert(int pos, const KeyT &key, const ValueT &value);
  void remove(int pos);
  void split(BptInternalNode *rhs);
  void merge(BptInternalNode *rhs);
  void redistribute(BptInternalNode *rhs);

  void self_check() {
    for(int i = 0; i < size(); ++i)
      assert(storage_[i].value != nullpos);
  }

private:
  Storage storage_[CAPACITY];
};

template <Trivial KeyT, Trivial ValueT>
class BptLeafNode : public BptNodeBase {

  struct Storage {
    KeyT key;
    ValueT value;
  };

public:
  static constexpr int CAPACITY = std::max(8ul,
    ((sizeof(Storage) + sizeof(index_t) + 4095) / 4096 * 4096 - sizeof(index_t)) / sizeof(Storage));

  BptLeafNode() = default;
  void init(int max_size = CAPACITY - 1) { BptNodeBase::init(NodeType::Leaf, max_size); rht_index_ = nullpos; }

  const KeyT& key(int pos) const { return storage_[pos].key; }
  const ValueT& value(int pos) const { return storage_[pos].value; }
  const index_t& rht_index() const { return rht_index_; }

  void write_key(int pos, const KeyT &key) { storage_[pos].key = key; }
  void write_value(int pos, const ValueT &value) { storage_[pos].value = value; }

  template <class KeyCompare>
  int locate_key(const KeyT &key, const KeyCompare &key_compare) const;
  template <class T, class Compare>
  requires requires(const T &t, const KeyT &key, const Compare &compare) {
    { compare(key, t) } -> std::convertible_to<bool>;
  }
  int locate_any(const T &t, const Compare &compare) const;

  void insert(int pos, const KeyT &key, const ValueT &value);
  void remove(int pos);
  void split(BptLeafNode *rhs, index_t rht_index);
  void merge(BptLeafNode *rhs);
  void redistribute(BptLeafNode *rhs);

private:
  Storage storage_[CAPACITY];
  index_t rht_index_;
};

}



#include "bpt_nodes.tcc"

#endif