#ifndef INSOMNIA_BPLUSTREE_H
#define INSOMNIA_BPLUSTREE_H

#include "bpt_nodes.h"
#include "buffer_pool.h"
#include "exception.h"

namespace insomnia::database {

namespace db = database;

template <
  Trivial KeyT, Trivial ValueT,
  class KeyCompare = std::less<KeyT>, class ValueCompare = std::less<ValueT>
>
class MultiBPlusTree {
  using index_t = BptNodeBase::index_t;
  static constexpr index_t nullpos = disk::IndexPool::nullpos;

  struct KVType {
    KeyT key;
    ValueT value;
  };

  struct KVCompare {
    KeyCompare key_compare;
    ValueCompare value_compare;
    bool operator()(const KVType &lhs, const KVType &rhs) const {
      if(key_compare(lhs.key, rhs.key)) return true;
      if(key_compare(rhs.key, lhs.key)) return false;
      return value_compare(lhs.value, rhs.value);
    }
  };
  struct KeyEqual {
    KeyCompare key_compare;
    bool operator()(const KeyT &lhs, const KeyT &rhs) const {
      return !key_compare(lhs, rhs) && !key_compare(rhs, lhs);
    }
  };
  struct KVEqual {
    KVCompare kv_compare;
    bool operator()(const KVType &lhs, const KVType &rhs) const {
      return !kv_compare(lhs, rhs) && !kv_compare(rhs, lhs);
    }
  };

  using Base = BptNodeBase;
  using Internal = BptInternalNode<KVType, index_t, KVCompare>;
  using Leaf = BptLeafSingleNode<KVType, KVCompare>;

  using BufferPool = conc::BufferPool<
    Base, index_t, std::max(sizeof(Internal), sizeof(Leaf))
  >;
  using Reader = typename BufferPool::Reader;
  using Writer = typename BufferPool::Writer;

public:

  MultiBPlusTree(const std::filesystem::path &name,
    size_t k_param, size_t buffer_capacity, size_t thread_num);
  ~MultiBPlusTree() { buffer_pool_.write_meta(&root_); }

  cntr::vector<ValueT> search(const KeyT &key);

  bool insert(const KeyT &key, const ValueT &value);

  bool remove(const KeyT &key, const ValueT &value);

private:


  // search for the leaf.
  Reader FindLeaf(Reader root_reader, const KeyT &key);

  // Optimistically find the leaf.
  Writer FindLeafOptim(Reader root_reader, const KVType &kv);

  // Pessimistically find the leaf.
  cntr::vector<Writer> FindLeafPessi(
    Writer root_writer, const KVType &kv, bool is_insert);

  BufferPool buffer_pool_;
  index_t root_{nullpos};
  KeyCompare key_compare_;
  KeyEqual key_equal_;
  KVCompare kv_compare_;
  KVEqual kv_equal_;
  std::shared_mutex root_latch_;
};

}


#include "bplustree.tcc"

#endif