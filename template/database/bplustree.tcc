#ifndef INSOMNIA_BPLUSTREE_TCC
#define INSOMNIA_BPLUSTREE_TCC

#include <cassert>

#include "bplustree.h"

namespace insomnia {

template <Trivial KeyT, Trivial ValueT, class KeyCompare, class ValueCompare>
MultiBPlusTree<KeyT, ValueT, KeyCompare, ValueCompare>::MultiBPlusTree(
  const std::filesystem::path &name,
  size_t k_param, size_t buffer_capacity, size_t thread_num)
    : buffer_pool_(name, k_param, buffer_capacity, thread_num) {
  if(!buffer_pool_.read_meta(&root_))
    root_ = nullpos;
}

template <Trivial KeyT, Trivial ValueT, class KeyCompare, class ValueCompare>
vector<ValueT> MultiBPlusTree<KeyT, ValueT, KeyCompare, ValueCompare>::search(
  const KeyT &key) {
  std::shared_lock root_lock(root_latch_);
  if(root_ == nullpos)
    return vector<ValueT>();
  vector<Reader> readers;
  readers.push_back(buffer_pool_.get_reader(root_));
  while(!readers.back().template as<Base>()->is_leaf()) {
    const Internal *internal = readers.back().template as<Internal>();
    int pos = internal->locate_any(key,
      [this] (const KeyT &key, const KVType &kv) { return !key_compare_(kv.key, key); });
    index_t index = internal->value(pos);
    readers.push_back(buffer_pool_.get_reader(index));
  }
  Reader reader = std::move(readers.back());
  readers.pop_back();
  const Leaf *leaf = reader.template as<Leaf>();
  int pos = leaf->locate_any(key,
    [this] (const KVType &kv, const KeyT &key) { return key_compare_(kv.key, key); });
  vector<ValueT> result;
  while(true) {
    if(pos == leaf->size()) {
      index_t rht_index = leaf->rht_index();
      if(rht_index == nullpos)
        return result;
      reader = buffer_pool_.get_reader(rht_index);
      leaf = reader.template as<Leaf>();
      pos = 0;
    }
    if(!key_equal_(leaf->key(pos).key, key))
      return result;
    result.push_back(leaf->value(pos));
    ++pos;
  }
}

template <Trivial KeyT, Trivial ValueT, class KeyCompare, class ValueCompare>
bool MultiBPlusTree<KeyT, ValueT, KeyCompare, ValueCompare>::insert(
  const KeyT &key, const ValueT &value) {
  KVType kv(key, value);
  std::unique_lock root_lock(root_latch_);
  if(root_ == nullpos) {
    root_ = buffer_pool_.alloc();
    Writer writer = buffer_pool_.get_writer(root_);
    Leaf *leaf = writer.template as<Leaf>();
    leaf->init();
    leaf->insert(0, kv, value);
    return true;
  }
  vector<Writer> writers;
  writers.push_back(buffer_pool_.get_writer(root_));
  while(!writers.back().template as<Base>()->is_leaf()) {
    Internal *internal = writers.back().template as<Internal>();
    int pos = internal->locate_key(kv, kv_compare_);
    index_t index = internal->value(pos);
    writers.push_back(buffer_pool_.get_writer(index));
  }
  Writer leaf_writer = std::move(writers.back());
  writers.pop_back();
  Leaf *leaf = leaf_writer.template as<Leaf>();
  {
    int pos = leaf->locate_key(kv, kv_compare_);
    if(pos != leaf->size() && kv_equal_(leaf->key(pos), kv))
      return false;
    leaf->insert(pos, kv, value);
  }
  if(!leaf->is_too_large())
    return true;
  {
    index_t rhs_index = buffer_pool_.alloc();
    Writer rhs_writer = buffer_pool_.get_writer(rhs_index);
    Leaf *rhs_leaf = rhs_writer.template as<Leaf>();
    rhs_leaf->init();
    leaf->split(rhs_leaf, rhs_index);
    if(writers.empty()) {
      index_t root_index = buffer_pool_.alloc();
      Writer root_writer = buffer_pool_.get_writer(root_index);
      Internal *root_internal = root_writer.template as<Internal>();
      root_internal->init();
      root_internal->insert(0, leaf->key(0), root_);
      root_internal->insert(1, rhs_leaf->key(0), rhs_index);
      root_ = root_index;
      return true;
    }
    Writer &parent_writer = writers.back();
    Internal *parent = parent_writer.template as<Internal>();
    int lft_pos = parent->locate_key(leaf->key(0), kv_compare_);
    parent->insert(lft_pos + 1, rhs_leaf->key(0), rhs_index);
  }
  leaf_writer.drop();
  while(writers.size() > 1) {
    Writer internal_writer = std::move(writers.back());
    writers.pop_back();
    Internal *internal = internal_writer.template as<Internal>();
    if(!internal->is_too_large())
      return true;
    index_t rhs_index = buffer_pool_.alloc();
    Writer rhs_writer = buffer_pool_.get_writer(rhs_index);
    Internal *rhs_internal = rhs_writer.template as<Internal>();
    rhs_internal->init();
    internal->split(rhs_internal);
    Writer &parent_writer = writers.back();
    Internal *parent = parent_writer.template as<Internal>();
    int lft_pos = parent->locate_key(internal->key(0), kv_compare_);
    parent->insert(lft_pos + 1, rhs_internal->key(0), rhs_index);
  }
  Writer root_writer = std::move(writers.back());
  writers.pop_back();
  Internal *root_internal = root_writer.template as<Internal>();
  if(!root_internal->is_too_large())
    return true;
  index_t rhs_index = buffer_pool_.alloc();
  Writer rhs_writer = buffer_pool_.get_writer(rhs_index);
  Internal *rhs_internal = rhs_writer.template as<Internal>();
  rhs_internal->init();
  root_internal->split(rhs_internal);
  index_t new_root_index = buffer_pool_.alloc();
  Writer new_root_writer = buffer_pool_.get_writer(new_root_index);
  Internal *new_root_internal = new_root_writer.template as<Internal>();
  new_root_internal->init();
  new_root_internal->insert(0, root_internal->key(0), root_);
  new_root_internal->insert(1, rhs_internal->key(0), rhs_index);
  root_ = new_root_index;
  return true;
}

template <Trivial KeyT, Trivial ValueT, class KeyCompare, class ValueCompare>
bool MultiBPlusTree<KeyT, ValueT, KeyCompare, ValueCompare>::remove(
  const KeyT &key, const ValueT &value) {
  KVType kv(key, value);
  std::unique_lock root_lock(root_latch_);
  if(root_ == nullpos)
    return false;
  vector<Writer> writers;
  writers.push_back(buffer_pool_.get_writer(root_));
  while(!writers.back().template as<Base>()->is_leaf()) {
    Internal *internal = writers.back().template as<Internal>();
    internal->self_check();
    int pos = internal->locate_key(kv, kv_compare_);
    index_t index = internal->value(pos);
    writers.push_back(buffer_pool_.get_writer(index));
  }
  Writer leaf_writer = std::move(writers.back());
  writers.pop_back();
  Leaf *leaf = leaf_writer.template as<Leaf>();
  {
    int pos = leaf->locate_key(kv, kv_compare_);
    if(pos == leaf->size() || !kv_equal_(leaf->key(pos), kv))
      return false;
    leaf->remove(pos);
  }
  if(!leaf->is_too_small())
    return true;

  {
    if(writers.empty()) {
      if(leaf->size() == 0) {
        leaf_writer.drop();
        buffer_pool_.dealloc(root_);
        root_ = nullpos;
      }
      return true;
    }
    Writer &parent_writer = writers.back();
    Internal *parent = parent_writer.template as<Internal>();
    parent->self_check();
    int pos = parent->locate_key(leaf->key(0), kv_compare_);
    if(pos > 0) {
      Writer lft_leaf_writer = buffer_pool_.get_writer(parent->value(pos - 1));
      Leaf *lft_leaf = lft_leaf_writer.template as<Leaf>();
      if(lft_leaf->size() + leaf->size() <= leaf->merge_bound()) {
        lft_leaf->merge(leaf);
        leaf_writer.drop();
        buffer_pool_.dealloc(parent->value(pos));
        parent->remove(pos);
      } else {
        lft_leaf->redistribute(leaf);
        parent->write_key(pos, leaf->key(0));
      }
    } else {
      Writer rht_leaf_writer = buffer_pool_.get_writer(parent->value(pos + 1));
      Leaf *rht_leaf = rht_leaf_writer.template as<Leaf>();
      if(leaf->size() + rht_leaf->size() <= leaf->merge_bound()) {
        leaf->merge(rht_leaf);
        rht_leaf_writer.drop();
        buffer_pool_.dealloc(parent->value(pos + 1));
        parent->remove(pos + 1);
      } else {
        leaf->redistribute(rht_leaf);
        parent->write_key(pos + 1, rht_leaf->key(0));
      }
    }
  }
  leaf_writer.drop();
  while(writers.size() > 1) {
    Writer internal_writer = std::move(writers.back());
    writers.pop_back();
    Internal *internal = internal_writer.template as<Internal>();
    if(!internal->is_too_small())
      return true;
    Writer &parent_writer = writers.back();
    Internal *parent = parent_writer.template as<Internal>();
    int pos = parent->locate_key(internal->key(0), kv_compare_);
    if(pos > 0) {
      Writer lft_writer = buffer_pool_.get_writer(parent->value(pos - 1));
      Internal *lft_internal = lft_writer.template as<Internal>();
      if(lft_internal->size() + internal->size() <= internal->merge_bound()) {
        lft_internal->merge(internal);
        internal_writer.drop();
        buffer_pool_.dealloc(parent->value(pos));
        parent->remove(pos);
      } else {
        lft_internal->redistribute(internal);
        parent->write_key(pos, internal->key(0));
      }
    } else {
      Writer rht_writer = buffer_pool_.get_writer(parent->value(pos + 1));
      Internal *rht_internal = rht_writer.template as<Internal>();
      if(internal->size() + rht_internal->size() <= internal->merge_bound()) {
        internal->merge(rht_internal);
        rht_writer.drop();
        buffer_pool_.dealloc(parent->value(pos + 1));
        parent->remove(pos + 1);
      } else {
        internal->redistribute(rht_internal);
        parent->write_key(pos + 1, rht_internal->key(0));
      }
    }
  }
  Writer root_writer = std::move(writers.back());
  writers.pop_back();
  Internal *root_internal = root_writer.template as<Internal>();
  if(root_internal->size() > 1)
    return true;
  index_t new_root = root_internal->value(0);
  root_internal->remove(0);
  root_writer.drop();
  buffer_pool_.dealloc(root_);
  root_ = new_root;
  return true;
}


}

#endif