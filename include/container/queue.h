#ifndef INSOMNIA_QUEUE_H
#define INSOMNIA_QUEUE_H

#include "config.h"
#include "list.h"

namespace insomnia::container {

template <class T>
class queue {
public:
  explicit queue(size_t capacity = 16);
  queue(const queue &other);
  queue(queue &&other) noexcept;
  queue& operator=(const queue &other);
  queue& operator=(queue &&other);
  ~queue() = default;

  void push(const T &val);
  void push(T &&val);
  template <class ...Args> requires std::is_constructible_v<T, Args...>
  void emplace(Args &&...args);
  const T& front() { return _data[_lft]; }
  T pop();
  size_t size() const { return (_data.capacity() + _rht - _lft) % _data.capacity(); }
  bool empty() const { return _lft == _rht; }
  void clear() { _data.clear(); _lft = 0; _rht = 0; }
  void reserve(size_t capacity);
private:
  vector<T> _data;
  size_t _lft, _rht;
};

}

#include "queue.tcc"

#endif