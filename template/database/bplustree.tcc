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
vector<ValueT>
MultiBPlusTree<KeyT, ValueT, KeyCompare, ValueCompare>::search(const KeyT &key) {
  std::shared_lock root_lock(root_latch_);
  if(root_ == nullpos) return vector<ValueT>();
  Reader root_reader = buffer_pool_.get_reader(root_);
  root_lock.unlock();
  Reader leaf_reader = FindLeaf(std::move(root_reader), key);
  const Leaf *leaf = leaf_reader.template as<Leaf>();
  int pos;
  {
    int l = 0, r = leaf->size() - 1;
    while(l < r) {
      int mid = (l + r) / 2;
      if(key_compare_(leaf->key(mid).key, key)) l = mid + 1;
      else r = mid;
    }
    pos = r;
  }
  vector<ValueT> result;
  while(key_equal_(leaf->key(pos).key, key)) {
    result.push_back(leaf->key(pos++).value);
    if(pos == leaf->size()) {
      index_t rht_index = leaf->rht_index();
      if(rht_index == nullpos) break;
      leaf_reader = buffer_pool_.get_reader(rht_index);
      leaf = leaf_reader.template as<Leaf>();
      pos = 0;
    }
  }
  return result;
}

template <Trivial KeyT, Trivial ValueT, class KeyCompare, class ValueCompare>
bool MultiBPlusTree<KeyT, ValueT, KeyCompare, ValueCompare>::insert(
  const KeyT &key, const ValueT &value) {
  KVType kv(key, value);
  std::unique_lock root_lock(root_latch_);
  if(root_ == nullpos) {
    root_ = buffer_pool_.alloc();
    Writer root_writer = buffer_pool_.get_writer(root_);
    Leaf leaf_root;
    leaf_root.set_root();
    leaf_root.insert(0, KVType(key, value));
    root_writer.write(leaf_root);
    return true;
  }

  Writer root_writer = buffer_pool_.get_writer(root_);
  Writer leaf_writer;
  if(root_writer.template as<Base>()->is_leaf()) {
    leaf_writer = std::move(root_writer);
    root_lock.unlock();
  } else {
    root_writer.drop();
    Reader root_reader = buffer_pool_.get_reader(root_);
    root_lock.unlock();
    leaf_writer = FindLeafOptim(std::move(root_reader), kv);
  }
  Leaf *leaf = leaf_writer.template as<Leaf>();
  int pos = leaf->upper_bound(kv);
  if(kv_equal_(leaf->key(pos), kv))
    return false;
  if(leaf->is_insert_safe()) {
    // what if pos should be size_?
    leaf->insert(pos, kv);
    return true;
  }

  leaf_writer.drop();
  root_lock.lock();
  if(root_ == nullpos) {
    // what if root_ here is nullpos?
    root_ = buffer_pool_.alloc();
    root_writer = buffer_pool_.get_writer(root_);
    Leaf leaf_root;
    leaf_root.set_root();
    leaf_root.insert(0, KVType(key, value));
    root_writer.write(leaf_root);
    return true;
  }
  root_writer = buffer_pool_.get_writer(root_);
  root_lock.unlock();
  vector<Writer> writers = FindLeafPessi(std::move(root_writer), kv, true);

  leaf_writer = std::move(writers.back());
  writers.pop_back();
  leaf = leaf_writer.template as<Leaf>();
  pos = leaf->upper_bound(kv);
  if(kv_equal_(leaf->key(pos), kv))
    return false; // status may have been changed.
  leaf->insert(pos, kv);
  if(leaf->is_valid())
    return true;

  // leaf->size() > leaf->max_size()

  if(writers.empty()) {
    // the root node is full & the root node is the only node.
    // leaf - new root
    index_t rhs_index = buffer_pool_.alloc();
    Writer rhs_writer = buffer_pool_.get_writer(rhs_index);
    Leaf rhs_leaf;
    leaf->share(rhs_leaf, rhs_index);
    rhs_writer.write(rhs_leaf);
    assert(leaf->is_root());
    leaf->set_not_root();
    index_t new_root = buffer_pool_.alloc();
    Writer new_root_writer = buffer_pool_.get_writer(new_root);
    Internal root_internal;
    root_internal.insert(0, leaf->highkey(), root_);
    root_internal.insert(1, rhs_leaf.highkey(), rhs_index);
    root_internal.set_root();
    new_root_writer.write(root_internal);
    root_ = new_root;
    return true;
  }
  {
    // leaf - internal
    index_t rhs_index = buffer_pool_.alloc();
    Writer rhs_writer = buffer_pool_.get_writer(rhs_index);
    Leaf rhs_leaf;
    leaf->share(rhs_leaf, rhs_index);
    rhs_writer.write(rhs_leaf);
    Internal *parent = writers.back().template as<Internal>();
    pos = parent->upper_bound(rhs_leaf.highkey());
    parent->remove(pos);
    parent->insert(pos, rhs_leaf.highkey(), rhs_index);
    parent->insert(pos, leaf->highkey(), leaf_writer.id());
  }
  // internal - root
  while(writers.size() > 1) {
    Writer writer = std::move(writers.back());
    writers.pop_back();
    Internal *internal = writer.template as<Internal>();
    if(internal->is_valid())
      return true;
    index_t rhs_index = buffer_pool_.alloc();
    Writer rhs_writer = buffer_pool_.get_writer(rhs_index);
    Internal rhs_internal;
    internal->share(rhs_internal, rhs_index);
    rhs_writer.write(rhs_internal);
    Internal *parent = writers.back().template as<Internal>();
    pos = parent->upper_bound(rhs_internal.highkey());
    parent->remove(pos);
    parent->insert(pos, rhs_internal.highkey(), rhs_index);
    parent->insert(pos, internal->highkey(), writer.id());
  }
  {
    Writer &writer = writers.back();
    if(!writer.template as<Base>()->is_root())
      return true;

    // the root needs to be updated.
    // internal - new root

    Internal *internal = writer.template as<Internal>();
    if(internal->is_valid())
      return true;
    index_t rhs_index = buffer_pool_.alloc();
    Writer rhs_writer = buffer_pool_.get_writer(rhs_index);
    Internal rhs_internal;
    internal->share(rhs_internal, rhs_index);
    rhs_writer.write(rhs_internal);
    assert(internal->is_root());
    internal->set_not_root();
    index_t new_root = buffer_pool_.alloc();
    Writer new_root_writer = buffer_pool_.get_writer(new_root);
    Internal root_internal;
    root_internal.insert(0, internal->highkey(), root_);
    root_internal.insert(1, rhs_internal.highkey(), rhs_index);
    root_internal.set_root();
    new_root_writer.write(root_internal);
    root_ = new_root;
  }
  return true;
}


template <Trivial KeyT, Trivial ValueT, class KeyCompare, class ValueCompare>
bool MultiBPlusTree<KeyT, ValueT, KeyCompare, ValueCompare>::remove(const KeyT &key, const ValueT &value) {
  KVType kv(key, value);
  std::unique_lock root_lock(root_latch_);
  if(root_ == nullpos)
    return false;

  Writer root_writer = buffer_pool_.get_writer(root_);
  Writer leaf_writer;
  if(root_writer.template as<Base>()->is_leaf()) {
    leaf_writer = std::move(root_writer);
    root_lock.unlock();
  } else {
    root_writer.drop();
    Reader root_reader = buffer_pool_.get_reader(root_);
    root_lock.unlock();
    leaf_writer = FindLeafOptim(std::move(root_reader), kv);
  }
  Leaf *leaf = leaf_writer.template as<Leaf>();
  int pos = leaf->upper_bound(kv);
  if(!kv_equal_(leaf->key(pos), kv))
    return false;
  if(leaf->is_remove_safe()) {
    leaf->remove(pos);
    return true;
  }

  leaf_writer.drop();
  root_lock.lock();
  if(root_ == nullpos)
    return false;
  root_writer = buffer_pool_.get_writer(root_);
  root_lock.unlock();
  vector<Writer> writers = FindLeafPessi(std::move(root_writer), kv, false);

  leaf_writer = std::move(writers.back());
  writers.pop_back();
  leaf = leaf_writer.template as<Leaf>();
  pos = leaf->upper_bound(kv);
  if(!kv_equal_(leaf->key(pos), kv))
    return false; // status may have been changed.
  leaf->remove(pos);
  if(leaf->is_valid())
    return true;

  // leaf->size() < leaf->min_size().

  if(writers.empty()) {
    // the root node is the only node and node size <= 1.
    // delete it.
    if(leaf->size() == 1) return true;
    root_lock.lock();
    assert(leaf_writer.id() == root_);
    leaf_writer.drop();
    buffer_pool_.dealloc(root_); // while(!try()) {;}
    root_ = nullpos;
    return true;
  }
  {
    // leaf - internal
    Internal *parent = writers.back().template as<Internal>();
    pos = parent->upper_bound(leaf->highkey());
    assert(parent->value(pos) == leaf_writer.id());
    if(pos > 0) {
      // coalesce/redistribute with the one at its left.
      Writer left_writer = buffer_pool_.get_writer(parent->value(pos - 1));
      Leaf *left_leaf = left_writer.template as<Leaf>();
      if(left_leaf->size() + leaf->size() <= Leaf::MERGE_BOUND) {
        left_leaf->absorb_right(*leaf);
        index_t rht_index = leaf_writer.id();
        leaf_writer.drop();
        buffer_pool_.dealloc(rht_index);
        parent->remove(pos);
        memcpy(&parent->key(pos - 1), &left_leaf->highkey(), sizeof(KVType));
      } else {
        leaf->fetch_from_left(*left_leaf);
        memcpy(&parent->key(pos - 1), &left_leaf->highkey(), sizeof(KVType));
        return true; // parent size not modified
      }
    } else {
      // coalesce/redistribute with the one at its right.
      Writer right_writer = buffer_pool_.get_writer(parent->value(pos + 1));
      Leaf *right_leaf = right_writer.template as<Leaf>();
      if(leaf->size() + right_leaf->size() <= Leaf::MERGE_BOUND) {
        leaf->absorb_right(*right_leaf);
        index_t rht_index = right_writer.id();
        right_writer.drop();
        buffer_pool_.dealloc(rht_index);
        parent->remove(pos + 1);
        memcpy(&parent->key(pos), &leaf->highkey(), sizeof(KVType));
      } else {
        leaf->fetch_from_right(*right_leaf);
        memcpy(&parent->key(pos), &leaf->highkey(), sizeof(KVType));
        return true;
      }
    }
  }
  // internal - root
  while(writers.size() > 1) {
    Writer internal_writer = std::move(writers.back());
    writers.pop_back();
    Internal *internal = internal_writer.template as<Internal>();
    if(internal->is_valid())
      return true;

    Internal *parent = writers.back().template as<Internal>();
    pos = parent->upper_bound(internal->highkey());
    assert(parent->value(pos) == internal_writer.id());
    if(pos > 0) {
      // coalesce/redistribute with the one at its left.
      Writer left_writer = buffer_pool_.get_writer(parent->value(pos - 1));
      Internal *left_internal = left_writer.template as<Internal>();
      if(left_internal->size() + internal->size() <= Internal::MERGE_BOUND) {
        left_internal->absorb_right(*internal);
        index_t rht_index = internal_writer.id();
        internal_writer.drop();
        buffer_pool_.dealloc(rht_index);
        parent->remove(pos);
        memcpy(&parent->key(pos - 1), &left_internal->highkey(), sizeof(KVType));
      } else {
        internal->fetch_from_left(*left_internal);
        memcpy(&parent->key(pos - 1), &left_internal->highkey(), sizeof(KVType));
        return true; // parent size not modified
      }
    } else {
      // coalesce/redistribute with the one at its right.
      Writer right_writer = buffer_pool_.get_writer(parent->value(pos + 1));
      Internal *right_internal = right_writer.template as<Internal>();
      if(internal->size() + right_internal->size() <= Internal::MERGE_BOUND) {
        internal->absorb_right(*right_internal);
        index_t rht_index = right_writer.id();
        right_writer.drop();
        buffer_pool_.dealloc(rht_index);
        parent->remove(pos + 1);
        memcpy(&parent->key(pos), &internal->highkey(), sizeof(KVType));
      } else {
        internal->fetch_from_right(*right_internal);
        memcpy(&parent->key(pos), &internal->highkey(), sizeof(KVType));
        return true;
      }
    }
  }
  {
    Writer &writer = writers.back();
    if(!writer.template as<Base>()->is_root())
      return true;

    Internal *root_internal = writer.template as<Internal>();
    if(root_internal->is_valid())
      return true;

    // The internal root has only one child.
    // internal - delete root
    // The type of the new root is not sure. But... who cares?

    assert(root_internal->size() == 1);
    index_t root = root_internal->value(0);
    root_lock.lock();
    writer.drop();
    buffer_pool_.dealloc(root_);
    root_ = root;
    return true;
  }
}

template <Trivial KeyT, Trivial ValueT, class KeyCompare, class ValueCompare>
typename MultiBPlusTree<KeyT, ValueT, KeyCompare, ValueCompare>::Reader
MultiBPlusTree<KeyT, ValueT, KeyCompare, ValueCompare>::FindLeaf(
  Reader root_reader, const KeyT &key) {
  Reader reader = std::move(root_reader);
  while(!reader.template as<Base>()->is_leaf()) {
    const Internal *internal = reader.template as<Internal>();
    int pos;
    {
      int l = 0, r = internal->size() - 1;
      while(l < r) {
        int mid = (l + r) / 2;
        if(key_compare_(internal->key(mid).key, key)) l = mid + 1;
        else r = mid;
      }
      pos = r;
    }
    index_t child = internal->value(pos);
    reader = buffer_pool_.get_reader(child);
  }
  return reader;
}

template <Trivial KeyT, Trivial ValueT, class KeyCompare, class ValueCompare>
typename MultiBPlusTree<KeyT, ValueT, KeyCompare, ValueCompare>::Writer
MultiBPlusTree<KeyT, ValueT, KeyCompare, ValueCompare>::FindLeafOptim(
  Reader root_reader, const KVType &kv) {
  assert(!root_reader.template as<Base>()->is_leaf());
  Reader &reader = root_reader;
  while(true) {
    const Internal *internal = reader.template as<Internal>();
    int pos = internal->upper_bound(kv);
    index_t child = internal->value(pos);
    Reader child_reader = buffer_pool_.get_reader(child);
    if(child_reader.template as<Base>()->is_leaf()) {
      child_reader.drop();
      return buffer_pool_.get_writer(child);
    }
    reader = std::move(child_reader);
  }
}

template <Trivial KeyT, Trivial ValueT, class KeyCompare, class ValueCompare>
vector<typename MultiBPlusTree<KeyT, ValueT, KeyCompare, ValueCompare>::Writer>
MultiBPlusTree<KeyT, ValueT, KeyCompare, ValueCompare>::FindLeafPessi(
  Writer root_writer, const KVType &kv, bool is_insert) {
  vector<Writer> writers;
  writers.push_back(std::move(root_writer));
  while(!writers.back().template as<Base>()->is_leaf()) {
    Internal *internal = writers.back().template as<Internal>();
    int pos = internal->upper_bound(kv);
    index_t child = internal->value(pos);
    Writer writer = buffer_pool_.get_writer(child);
    // child is not root.
    if((is_insert && writer.template as<Base>()->is_insert_safe()) ||
      (!is_insert && writer.template as<Base>()->is_remove_safe())) {
      Writer parent = std::move(writers.back());
      writers.clear();
      writers.push_back(std::move(parent));
    }
    writers.push_back(std::move(writer));
  }
  return writers;
}

}

#endif