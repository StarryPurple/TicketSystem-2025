#ifndef INSOMNIA_UNORDERED_MAP_H
#define INSOMNIA_UNORDERED_MAP_H

#include "exception.h"

namespace insomnia {

template <
  class KeyT,
  class ValueT,
  class Hash = std::hash<KeyT>,
  class KeyEqual = std::equal_to<KeyT>>
class unordered_map {
public:
  using value_type = std::pair<const KeyT, ValueT>;
  class iterator;
  friend iterator;

private:
  struct NodeBase {
    NodeBase *t_nxt{nullptr}, *t_prv{nullptr}, *b_nxt{nullptr}, *b_prv{nullptr};
    // virtual NodeBase* clone() { return new NodeBase; }
    virtual ~NodeBase() = default;
  };
  struct Node : NodeBase {
    value_type value_pair;
    Node(const value_type &_value_pair)
    : NodeBase(), value_pair(_value_pair) {}
    Node(value_type &&_value_pair)
      : NodeBase(), value_pair(std::move(_value_pair)) {}
    // virtual NodeBase* clone() override { return new Node(value_pair); }
    virtual ~Node() override = default;
  };

public:

  unordered_map();
  // unordered_map(const unordered_map &other);
  unordered_map(unordered_map &&other);
  // unordered_map& operator=(const unordered_map &other);
  unordered_map& operator=(unordered_map &&other);
  ~unordered_map();

  class iterator {
    friend unordered_map;
  public:
    iterator() : _container(nullptr), _node(nullptr) {}
    iterator(const unordered_map *container, NodeBase *node)
      : _container(container), _node(node) {}
    iterator& operator++() {
      if(!_container || !_node) throw invalid_iterator();
      if(_node == _container->_header) throw index_out_of_bound();
      _node = _node->t_nxt;
      return *this;
    }
    iterator& operator--() {
      if(!_container || !_node) throw invalid_iterator();
      if(_node == _container->_header->t_nxt) throw index_out_of_bound();
      _node = _node->t_prv;
      return *this;
    }
    iterator operator++(int) {
      auto tmp = *this;
      ++*this;
      return tmp;
    }
    iterator operator--(int) {
      auto tmp = *this;
      --*this;
      return tmp;
    }
    value_type& operator*() {
      if(!_container || !_node) throw invalid_iterator();
      Node *node = dynamic_cast<Node*>(_node);
      if(!node) throw invalid_iterator();
      return node->value_pair;
    }
    value_type* operator->() {
      return &**this;
    }
    bool operator==(const iterator &other) const {
      return _container == other._container && _node == other._node;
    }
    bool operator!=(const iterator &other) const {
      return _container != other._container || _node != other._node;
    }
  private:
    const unordered_map *_container;
    NodeBase *_node;
  };

  iterator begin() { return iterator{this, _header->t_nxt}; }
  iterator end() { return iterator{this, _header}; }

  iterator insert(const value_type &value);
  iterator insert(value_type &&value);
  template <class ...Args>
  iterator emplace(Args &&...args); // A fake implementation.

  ValueT& operator[](const KeyT &key);
  const ValueT& operator[](const KeyT &key) const;

  void erase(iterator it);
  void erase(const KeyT &key) { erase(find(key)); }

  iterator find(const KeyT &key);

  size_t size() const { return _size; }
  bool empty() const { return _size == 0; }
  void clear();
  void reserve(size_t bucket_size) { rehash(bucket_size); }

private:
  void rehash(size_t bucket_size);
  iterator insert_node(NodeBase *node, size_t idx);
  bool overload() const { return _size >= _bucket_size * 2; }

  NodeBase **_node_list, *_header;
  size_t _size, _bucket_size;
  Hash hash_;
  KeyEqual key_equal_;
};

}

#include "unordered_map.tcc"

#endif