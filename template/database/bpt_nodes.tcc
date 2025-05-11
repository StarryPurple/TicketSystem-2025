#ifndef INSOMNIA_BPT_NODES_TCC
#define INSOMNIA_BPT_NODES_TCC

#include "bpt_nodes.h"

namespace insomnia {

template <Trivial KeyT, Trivial ValueT>
template <class KeyCompare>
int BptInternalNode<KeyT, ValueT>::locate_key(
  const KeyT &key, const KeyCompare &key_compare) const {
  int lft = 1, rht = size() - 1;
  if(key_compare(key, storage_[lft].key))
    return lft - 1;
  if(!key_compare(key, storage_[rht].key))
    return rht;
  while(lft < rht) {
    int mid = (lft + rht) / 2 + 1;
    if(key_compare(key, storage_[mid].key))
      rht = mid - 1;
    else
      lft = mid;
  }
  return rht;
}

template <Trivial KeyT, Trivial ValueT>
template <class T, class Compare>
requires requires(const T &t, const KeyT &key, const Compare &compare) {
  { compare(t, key) } -> std::convertible_to<bool>;
}
int BptInternalNode<KeyT, ValueT>::locate_any(const T &t, const Compare &compare) const {
  int lft = 1, rht = size() - 1;
  if(compare(t, storage_[lft].key))
    return lft - 1;
  if(!compare(t, storage_[rht].key))
    return rht;
  while(lft < rht) {
    int mid = (lft + rht) / 2 + 1;
    if(compare(t, storage_[mid].key))
      rht = mid - 1;
    else
      lft = mid;
  }
  return rht;
}

template <Trivial KeyT, Trivial ValueT>
void BptInternalNode<KeyT, ValueT>::insert(
  int pos, const KeyT &key, const ValueT &value) {
  for(int i = size(); i > pos; --i)
    storage_[i] = storage_[i - 1];
  storage_[pos] = Storage(key, value);
  change_size_by(1);
  self_check();
}

template <Trivial KeyT, Trivial ValueT>
void BptInternalNode<KeyT, ValueT>::remove(int pos) {
  for(int i = pos; i < size() - 1; ++i)
    storage_[i] = storage_[i + 1];
  change_size_by(-1);
  self_check();
}

template <Trivial KeyT, Trivial ValueT>
void BptInternalNode<KeyT, ValueT>::merge(BptInternalNode *rhs) {
  int lft_old_size = size(), rht_old_size = rhs->size();
  int tot_size = lft_old_size + rht_old_size;
  for(int i = 0; i < rht_old_size; ++i)
    storage_[i + lft_old_size] = rhs->storage_[i];
  set_size(tot_size);
  rhs->set_size(0);
}

template <Trivial KeyT, Trivial ValueT>
void BptInternalNode<KeyT, ValueT>::split(BptInternalNode *rhs) {
  int lft_size = size() / 2, rht_size = size() - lft_size;
  for(int i = 0; i < rht_size; ++i) {
    rhs->storage_[i].key = storage_[i + lft_size].key;
    rhs->storage_[i].value = storage_[i + lft_size].value;
  }
  set_size(lft_size);
  rhs->set_size(rht_size);
}

template <Trivial KeyT, Trivial ValueT>
void BptInternalNode<KeyT, ValueT>::redistribute(BptInternalNode *rhs) {
  int lft_old_size = size(), rht_old_size = rhs->size();
  int tot_size = lft_old_size + rht_old_size;
  int lft_size = tot_size / 2, rht_size = tot_size - lft_size;
  if(lft_old_size < lft_size) {
    int diff = lft_size - lft_old_size;
    for(int i = 0; i < diff; ++i)
      storage_[i + lft_old_size] = rhs->storage_[i];
    for(int i = 0; i < rht_size; ++i)
      rhs->storage_[i] = rhs->storage_[i + diff];
  } else {
    int diff = lft_old_size - lft_size;
    for(int i = rht_old_size - 1; i >= 0; --i)
      rhs->storage_[i + diff] = rhs->storage_[i];
    for(int i = 0; i < diff; ++i)
      rhs->storage_[i] = storage_[i + lft_size];
  }
  set_size(lft_size);
  rhs->set_size(rht_size);
}

/*****************************************************************************/

template <Trivial KeyT, Trivial ValueT>
void BptLeafNode<KeyT, ValueT>::insert(
  int pos, const KeyT &key, const ValueT &value) {
  for(int i = size(); i > pos; --i)
    storage_[i] = storage_[i - 1];
  storage_[pos] = Storage(key, value);
  change_size_by(1);
}

template <Trivial KeyT, Trivial ValueT>
void BptLeafNode<KeyT, ValueT>::remove(int pos) {
  for(int i = pos; i < size() - 1; ++i)
    storage_[i] = storage_[i + 1];
  change_size_by(-1);
}

template <Trivial KeyT, Trivial ValueT>
template <class KeyCompare>
int BptLeafNode<KeyT, ValueT>::locate_key(
  const KeyT &key, const KeyCompare &key_compare) const {
  int lft = 0, rht = size() - 1;
  if(key_compare(storage_[rht].key, key))
    return rht + 1;
  while(lft < rht) {
    int mid = (lft + rht) / 2;
    if(key_compare(storage_[mid].key, key))
      lft = mid + 1;
    else
      rht = mid;
  }
  return rht;
}

template <Trivial KeyT, Trivial ValueT>
template <class T, class Compare>
requires requires(const T &t, const KeyT &key, const Compare &compare) {
  { compare(key, t) } -> std::convertible_to<bool>;
}
int BptLeafNode<KeyT, ValueT>::locate_any(const T &t, const Compare &compare) const {
  int lft = 0, rht = size() - 1;
  if(compare(storage_[rht].key, t))
    return rht + 1;
  while(lft < rht) {
    int mid = (lft + rht) / 2;
    if(compare(storage_[mid].key, t))
      lft = mid + 1;
    else
      rht = mid;
  }
  return rht;
}


template <Trivial KeyT, Trivial ValueT>
void BptLeafNode<KeyT, ValueT>::merge(BptLeafNode *rhs) {
  int lft_old_size = size(), rht_old_size = rhs->size();
  int tot_size = lft_old_size + rht_old_size;
  for(int i = 0; i < rht_old_size; ++i)
    storage_[i + lft_old_size] = rhs->storage_[i];
  set_size(tot_size);
  rhs->set_size(0);
  rht_index_ = rhs->rht_index_;
  rhs->rht_index_ = nullpos;
}

template <Trivial KeyT, Trivial ValueT>
void BptLeafNode<KeyT, ValueT>::split(BptLeafNode *rhs, index_t rht_index) {
  int lft_size = size() / 2, rht_size = size() - lft_size;
  for(int i = 0; i < rht_size; ++i)
    rhs->storage_[i] = storage_[i + lft_size];
  set_size(lft_size);
  rhs->set_size(rht_size);
  rhs->rht_index_ = rht_index_;
  rht_index_ = rht_index;
}

template <Trivial KeyT, Trivial ValueT>
void BptLeafNode<KeyT, ValueT>::redistribute(BptLeafNode *rhs) {
  int lft_old_size = size(), rht_old_size = rhs->size();
  int tot_size = lft_old_size + rht_old_size;
  int lft_size = tot_size / 2, rht_size = tot_size - lft_size;
  if(lft_old_size < lft_size) {
    int diff = lft_size - lft_old_size;
    for(int i = 0; i < diff; ++i)
      storage_[i + lft_old_size] = rhs->storage_[i];
    for(int i = 0; i < rht_size; ++i)
      rhs->storage_[i] = rhs->storage_[i + diff];
  } else {
    int diff = lft_old_size - lft_size;
    for(int i = rht_old_size - 1; i >= 0; --i)
      rhs->storage_[i + diff] = rhs->storage_[i];
    for(int i = 0; i < diff; ++i)
      rhs->storage_[i] = storage_[i + lft_size];
  }
  set_size(lft_size);
  rhs->set_size(rht_size);
}

}

#endif