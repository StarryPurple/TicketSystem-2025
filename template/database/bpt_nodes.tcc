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
  memmove(storage_ + pos + 1, storage_ + pos, (size() - pos) * sizeof(Storage));
  memcpy(&storage_[pos].key, &key, sizeof(KeyT));
  memcpy(&storage_[pos].value, &value, sizeof(ValueT));
  change_size_by(1);
}

template <Trivial KeyT, Trivial ValueT>
void BptInternalNode<KeyT, ValueT>::remove(int pos) {
  memmove(storage_ + pos, storage_ + pos + 1, (size() - pos - 1) * sizeof(Storage));
  change_size_by(-1);
}

template <Trivial KeyT, Trivial ValueT>
void BptInternalNode<KeyT, ValueT>::merge(BptInternalNode *rhs) {
  int lft_old_size = size(), rht_old_size = rhs->size();
  int tot_size = lft_old_size + rht_old_size;
  memcpy(storage_ + lft_old_size, rhs->storage_, rht_old_size * sizeof(Storage));
  set_size(tot_size);
  rhs->set_size(0);
}

template <Trivial KeyT, Trivial ValueT>
void BptInternalNode<KeyT, ValueT>::split(BptInternalNode *rhs) {
  int lft_size = size() / 2, rht_size = size() - lft_size;
  memcpy(rhs->storage_, storage_ + lft_size, rht_size * sizeof(Storage));
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
    memcpy(storage_ + lft_old_size, rhs->storage_, diff * sizeof(Storage));
    memmove(rhs->storage_, rhs->storage_ + diff, rht_size * sizeof(Storage));
  } else {
    int diff = lft_old_size - lft_size;
    memmove(rhs->storage_ + diff, rhs->storage_, rht_old_size * sizeof(Storage));
    memcpy(rhs->storage_, storage_ + lft_size, diff * sizeof(Storage));
  }
  set_size(lft_size);
  rhs->set_size(rht_size);
}

/*****************************************************************************/

template <Trivial KeyT, Trivial ValueT>
void BptLeafNode<KeyT, ValueT>::insert(
  int pos, const KeyT &key, const ValueT &value) {
  memmove(storage_ + pos + 1, storage_ + pos, (size() - pos) * sizeof(Storage));
  memcpy(&storage_[pos].key, &key, sizeof(KeyT));
  memcpy(&storage_[pos].value, &value, sizeof(ValueT));
  change_size_by(1);
}

template <Trivial KeyT, Trivial ValueT>
void BptLeafNode<KeyT, ValueT>::remove(int pos) {
  memmove(storage_ + pos, storage_ + pos + 1, (size() - pos - 1) * sizeof(Storage));
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
  memcpy(storage_ + lft_old_size, rhs->storage_, rht_old_size * sizeof(Storage));
  set_size(tot_size);
  rhs->set_size(0);
  rht_index_ = rhs->rht_index_;
  rhs->rht_index_ = nullpos;
}

template <Trivial KeyT, Trivial ValueT>
void BptLeafNode<KeyT, ValueT>::split(BptLeafNode *rhs, index_t rht_index) {
  int lft_size = size() / 2, rht_size = size() - lft_size;
  memcpy(rhs->storage_, storage_ + lft_size, rht_size * sizeof(Storage));
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
    memcpy(storage_ + lft_old_size, rhs->storage_, diff * sizeof(Storage));
    memmove(rhs->storage_, rhs->storage_ + diff, rht_size * sizeof(Storage));
  } else {
    int diff = lft_old_size - lft_size;
    memmove(rhs->storage_ + diff, rhs->storage_, rht_old_size * sizeof(Storage));
    memcpy(rhs->storage_, storage_ + lft_size, diff * sizeof(Storage));
  }
  set_size(lft_size);
  rhs->set_size(rht_size);
}

}

#endif