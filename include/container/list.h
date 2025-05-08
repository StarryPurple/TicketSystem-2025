#ifndef INSOMNIA_LIST_H
#define INSOMNIA_LIST_H

namespace insomnia {

template <class T>
class list {
  struct NodeBase;
  struct Node;

public:
  list();
  list(const list &other);
  list(list &&other) noexcept;
  list& operator=(const list &other);
  list& operator=(list &&other) noexcept;
  ~list();

  void push_back(const T &val) { insert_back(new Node(val)); }
  void push_back(T &&val) { insert_back(new Node(std::move(val))); }
  void push_front(const T &val) { insert_front(new Node(val)); }
  void push_front(T &&val) { insert_front(new Node(std::move(val))); }
  template <class ...Args> requires std::is_constructible_v<T, Args...>
  void emplace_back(Args &&...args) { insert_back(new Node(std::forward<Args>(args)...)); }
  template <class ...Args> requires std::is_constructible_v<T, Args...>
  void emplace_front(Args &&...args) { insert_front(new Node(std::forward<Args>(args)...)); }
  T pop_back();
  T pop_front();
  size_t size() const { return _size; }
  bool empty() const { return _size == 0;}
  void clear();

private:
  struct NodeBase {
    NodeBase *prv{nullptr}, *nxt{nullptr};
    virtual ~NodeBase() = default;
  };
  struct Node : NodeBase {
    T val;
    Node(const T &val_) : NodeBase(), val(val_) {}
    Node(T &&val_) noexcept : NodeBase(), val(std::move(val_)) {}
    template <class ...Args>
    Node(Args &&...args) : NodeBase(), val(std::forward<Args>(args)...) {}
    ~Node() override = default;
  };
  NodeBase *_lft, *_rht; // rend and rbegin. Containing nothing.
  size_t _size;

  void insert_back(NodeBase *node);
  void insert_front(NodeBase *node);
};

}


#include "list.tcc"

#endif