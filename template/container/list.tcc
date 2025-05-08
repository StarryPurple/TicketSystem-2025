#ifndef INSOMNIA_LIST_TCC
#define INSOMNIA_LIST_TCC

#include "list.h"

namespace insomnia {

template <class T>
list<T>::list() {
  _lft = new NodeBase;
  _rht = new NodeBase;
  _lft->nxt = _rht;
  _rht->prv = _lft;
}

template <class T>
list<T>::list(const list &other) : list() {
  _size = other._size;
  NodeBase *here = _lft;
  for(NodeBase *there = other._lft;
    there->nxt != other._rht;
    here = here->nxt, there = there->nxt) {
    here->nxt = new Node(static_cast<Node *>(there->nxt)->val);
    here->nxt->prv = here;
  }
  here->nxt = _rht;
  _rht->prv = here;
}

template <class T>
list<T>::list(list &&other) noexcept {
  _lft = other._lft;
  _rht = other._rht;
  _size = other._size;
  other._lft = nullptr;
  other._rht = nullptr;
  other._size = 0;
}

template <class T>
list<T>& list<T>::operator=(const list &other) {
  if(this == &other) return *this;
  clear();
  _size = other._size;
  NodeBase *here = _lft;
  for(NodeBase *there = other._lft;
    there->nxt != other._rht;
    here = here->nxt, there = there->nxt) {
    here->nxt = new Node(static_cast<Node *>(there->nxt)->val);
    here->nxt->prv = here;
    }
  here->nxt = _rht;
  _rht->prv = here;
  return *this;
}

template <class T>
list<T>& list<T>::operator=(list &&other) noexcept {
  if(this == &other) return *this;
  clear();
  _lft = other._lft;
  _rht = other._rht;
  _size = other._size;
  other._lft = nullptr;
  other._rht = nullptr;
  other._size = 0;
  return *this;
}

template <class T>
list<T>::~list() {
  clear();
  delete _lft;
  delete _rht;
}

template <class T>
void list<T>::insert_back(NodeBase *node) {
  ++_size;
  _rht->prv->nxt = node;
  node->prv = _rht->prv;
  _rht->prv = node;
  node->nxt = _rht;
}

template <class T>
void list<T>::insert_front(NodeBase *node) {
  ++_size;
  _lft->nxt->prv = node;
  node->nxt = _lft->nxt;
  _lft->nxt = node;
  node->prv = _lft;
}

template <class T>
T list<T>::pop_back() {
  if(empty()) throw container_is_empty();
  Node *del = _rht->prv;
  T res = std::move(del->val);
  --_size;
  _rht->prv = del->prv;
  _rht->prv->nxt = _rht;
  delete del;
  return res;
}

template <class T>
T list<T>::pop_front() {
  if(empty()) throw container_is_empty();
  Node *del = _lft->nxt;
  T res = std::move(del->val);
  --_size;
  _lft->nxt = del->nxt;
  _lft->nxt->prv = _lft;
  delete del;
  return res;
}

template <class T>
void list<T>::clear() {
  _size = 0;
  for(NodeBase *here = _lft->nxt, *del; here != _rht; ) {
    del = here;
    here = here->nxt;
    delete del;
  }
  _lft->nxt = _rht;
  _rht->prv = _lft;
}



}

#endif