#ifndef INSOMNIA_UNORDERED_MAP_TCC
#define INSOMNIA_UNORDERED_MAP_TCC

#include "unordered_map.h"

namespace insomnia::container {

template <class KeyT, class ValueT, class Hash, class KeyEqual>
unordered_map<KeyT, ValueT, Hash, KeyEqual>::unordered_map() {
  _bucket_size = 16;
  _size = 0;
  _header = new NodeBase;
  _node_list = new NodeBase*[_bucket_size];
  for(size_t i = 0; i < _bucket_size; ++i)
    _node_list[i] = new NodeBase;
  _header->t_prv = _header->t_nxt = _header;
}

/*
template <class KeyT, class ValueT, class Hash, class KeyEqual>
unordered_map<KeyT, ValueT, Hash, KeyEqual>::unordered_map(const unordered_map &other) {
  _bucket_size = other._bucket_size;
  _size = other._size;
  _node_list = new NodeBase*[_bucket_size];
  _header = new NodeBase;
  NodeBase *t_curr = _header;
  for(size_t i = 0; i < _bucket_size; ++i) {
    _node_list[i] = new NodeBase;
    for(NodeBase *here = _node_list[i], *there = other._node_list[i];
      there->b_nxt != nullptr;
      here = here->b_nxt, there = there->b_nxt) {
      here->b_nxt = there->b_nxt->clone();
      here->b_nxt->b_prv = here;
      t_curr->t_nxt = here->b_nxt;
      here->b_nxt->t_prv = t_curr;
      t_curr = t_curr->t_nxt;
    }
  }
  _header->t_prv = t_curr;
  t_curr->t_nxt = _header;
}
*/

template <class KeyT, class ValueT, class Hash, class KeyEqual>
unordered_map<KeyT, ValueT, Hash, KeyEqual>::unordered_map(unordered_map &&other) {
  _node_list = other._node_list;
  _header = other._header;
  _size = other._size;
  _bucket_size = other._bucket_size;
  other._node_list = nullptr;
  other._header = nullptr;
  other._size = 0;
  other._bucket_size = 0;
}

/*
template <class KeyT, class ValueT, class Hash, class KeyEqual>
unordered_map<KeyT, ValueT, Hash, KeyEqual>&
  unordered_map<KeyT, ValueT, Hash, KeyEqual>::operator=(const unordered_map &other) {
  if(this == &other) return *this;
  clear();
  for(size_t i = 0; i < _bucket_size; ++i)
    delete _node_list[i];
  delete[] _node_list;
  _bucket_size = other._bucket_size;
  _size = other._size;
  _node_list = new NodeBase*[_bucket_size];
  NodeBase *t_curr = _header;
  for(size_t i = 0; i < _bucket_size; ++i) {
    _node_list[i] = new NodeBase;
    for(NodeBase *here = _node_list[i], *there = other._node_list[i];
      there->b_nxt != nullptr;
      here = here->b_nxt, there = there->b_nxt) {
      here->b_nxt = there->b_nxt->clone();
      here->b_nxt->b_prv = here;
      t_curr->t_nxt = here->b_nxt;
      here->b_nxt->t_prv = t_curr;
      t_curr = t_curr->t_nxt;
      }
  }
  _header->t_prv = t_curr;
  t_curr->t_nxt = _header;
  return *this;
}
*/

template <class KeyT, class ValueT, class Hash, class KeyEqual>
unordered_map<KeyT, ValueT, Hash, KeyEqual>&
  unordered_map<KeyT, ValueT, Hash, KeyEqual>::operator=(unordered_map &&other) {
  if(this == &other) return *this;
  clear();
  for(size_t i = 0; i < _bucket_size; ++i)
    delete _node_list[i];
  delete[] _node_list;
  delete _header;
  _node_list = other._node_list;
  _header = other._header;
  _size = other._size;
  _bucket_size = other._bucket_size;
  other._node_list = nullptr;
  other._header = nullptr;
  other._size = 0;
  other._bucket_size = 0;
  return *this;
}

template <class KeyT, class ValueT, class Hash, class KeyEqual>
unordered_map<KeyT, ValueT, Hash, KeyEqual>::~unordered_map() {
  clear();
  delete[] _node_list;
  delete _header;
}

template <class KeyT, class ValueT, class Hash, class KeyEqual>
typename unordered_map<KeyT, ValueT, Hash, KeyEqual>::iterator
unordered_map<KeyT, ValueT, Hash, KeyEqual>::insert(const value_type &value) {
  if(auto it = find(value.first); it != end()) {
    it->second = value.second;
    return it;
  }
  size_t idx = hash_(value.first) % _bucket_size;
  NodeBase *node = new Node(value);
  return insert_node(node, idx);
}

template <class KeyT, class ValueT, class Hash, class KeyEqual>
typename unordered_map<KeyT, ValueT, Hash, KeyEqual>::iterator
unordered_map<KeyT, ValueT, Hash, KeyEqual>::insert(value_type &&value) {
  if(auto it = find(value.first); it != end()) {
    it->second = std::move(value.second);
    return it;
  }
  size_t idx = hash_(value.first) % _bucket_size;
  NodeBase *node = new Node(std::move(value));
  return insert_node(node, idx);
}

template <class KeyT, class ValueT, class Hash, class KeyEqual>
template <class... Args>
typename unordered_map<KeyT, ValueT, Hash, KeyEqual>::iterator
unordered_map<KeyT, ValueT, Hash, KeyEqual>::emplace(Args &&... args) {
  return insert(value_type(std::forward<Args>(args)...));
}


template <class KeyT, class ValueT, class Hash, class KeyEqual>
unordered_map<KeyT, ValueT, Hash, KeyEqual>::iterator
unordered_map<KeyT, ValueT, Hash, KeyEqual>::insert_node(NodeBase *node, size_t idx) {
  node->b_nxt = _node_list[idx]->b_nxt;
  node->b_prv = _node_list[idx];
  if(node->b_nxt) node->b_nxt->b_prv = node;
  _node_list[idx]->b_nxt = node;

  node->t_nxt = _header;
  node->t_prv = _header->t_prv;
  node->t_nxt->t_prv = node;
  node->t_prv->t_nxt = node;

  ++_size;
  if(overload()) rehash(_size);
  return iterator(this, node);
}

template <class KeyT, class ValueT, class Hash, class KeyEqual>
void unordered_map<KeyT, ValueT, Hash, KeyEqual>::clear() {
  for(size_t i = 0; i < _bucket_size; ++i) {
    for(NodeBase *curr = _node_list[i]->b_nxt, *del; curr != nullptr;) {
      del = curr;
      curr = curr->b_nxt;
      delete del;
    }
    _node_list[i]->b_nxt = nullptr;
  }
  _header->t_nxt = _header->t_prv = nullptr;
  _size = 0;
}

template <class KeyT, class ValueT, class Hash, class KeyEqual>
ValueT& unordered_map<KeyT, ValueT, Hash, KeyEqual>::operator[](const KeyT &key) {
  auto it = find(key);
  if(it == end()) it = insert(value_type{key, ValueT()});
  return it->second;
}

template <class KeyT, class ValueT, class Hash, class KeyEqual>
const ValueT& unordered_map<KeyT, ValueT, Hash, KeyEqual>::operator[](const KeyT &key) const {
  auto it = find(key);
  if(it == end()) it = insert(value_type{key, ValueT()});
  return it->second;
}

template <class KeyT, class ValueT, class Hash, class KeyEqual>
void unordered_map<KeyT, ValueT, Hash, KeyEqual>::erase(iterator it) {
  if(it._container != this || it._node == nullptr) throw invalid_iterator();
  if(it._node == _header) return;
  NodeBase *node = it._node;
  if(node->b_nxt) node->b_nxt->b_prv = node->b_prv;
  node->b_prv->b_nxt = node->b_nxt;
  node->t_nxt->t_prv = node->t_prv;
  node->t_prv->t_nxt = node->t_nxt;
  --_size;
  delete node;
}

template <class KeyT, class ValueT, class Hash, class KeyEqual>
typename unordered_map<KeyT, ValueT, Hash, KeyEqual>::iterator
unordered_map<KeyT, ValueT, Hash, KeyEqual>::find(const KeyT &key) {
  size_t idx = hash_(key) % _bucket_size;
  for(NodeBase *curr = _node_list[idx]->b_nxt; curr != nullptr; curr = curr->b_nxt)
    if(Node *node = static_cast<Node*>(curr); key_equal_(node->value_pair.first, key))
      return iterator(this, curr);
  return end();
}

template <class KeyT, class ValueT, class Hash, class KeyEqual>
void unordered_map<KeyT, ValueT, Hash, KeyEqual>::rehash(size_t bucket_size) {
  if(bucket_size <= _bucket_size) return;
  NodeBase **new_node_list = new NodeBase*[bucket_size];
  for(size_t i = 0; i < bucket_size; ++i)
    new_node_list[i] = new NodeBase;
  for(NodeBase *curr = _header->t_nxt; curr != _header;) {
    NodeBase *nxt = curr->t_nxt;

    size_t idx = hash_(static_cast<Node*>(curr)->value_pair.first) % bucket_size;
    curr->b_nxt = new_node_list[idx]->b_nxt;
    curr->b_prv = new_node_list[idx];
    if(curr->b_nxt) curr->b_nxt->b_prv = curr;
    new_node_list[idx]->b_nxt = curr;

    curr = nxt;
  }
  for(size_t i = 0; i < _bucket_size; ++i)
    delete _node_list[i];
  delete[] _node_list;
  _node_list = new_node_list;
  _bucket_size = bucket_size;
}










}

#endif