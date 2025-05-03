#ifndef INSOMNIA_QUEUE_TCC
#define INSOMNIA_QUEUE_TCC

#include "queue.h"

namespace insomnia::container {

template <class T>
queue<T>::queue(size_t capacity) : _data(capacity), _lft(0), _rht(0) {}

template <class T>
queue<T>::queue(const queue &other)
  : _data(other._data), _lft(other._lft), _rht(other._rht) {}

template <class T>
queue<T>::queue(queue &&other) noexcept
  : _data(std::move(other._data)), _lft(other._lft), _rht(other._rht) {
  other._lft = 0;
  other._rht = 0;
}

template <class T>
queue<T>& queue<T>::operator=(const queue &other) {
  if(this == &other) return *this;
  _data = other._data;
  _lft = other._lft;
  _rht = other._rht;
  return *this;
}

template <class T>
queue<T>& queue<T>::operator=(queue &&other) {
  if(this == &other) return *this;
  _data = std::move(other._data);
  _lft = other._lft;
  _rht = other._rht;
  other._lft = 0;
  other._rht = 0;
  return *this;
}

template <class T>
void queue<T>::push(const T &val) {
  _data.operator[](_rht) = val;
  _rht = (_rht + 1) % _data.capacity();
  if((_lft + _data.capacity() - _rht) % _data.capacity() == 1)
    reserve(_data.capacity() * 2);
}

template <class T>
void queue<T>::push(T &&val) {
  _data[_rht] = std::move(val);
  _rht = (_rht + 1) % _data.capacity();
  if((_lft + _data.capacity() - _rht) % _data.capacity() == 1)
    reserve(_data.capacity() * 2);
}

template <class T>
template <class... Args> requires std::is_constructible_v<T, Args...>
void queue<T>::emplace(Args &&... args) {
  _data.emplace(_rht, std::forward<Args>(args)...);
  _rht = (_rht + 1) % _data.capacity();
  if((_lft + _data.capacity() - _rht) % _data.capacity() == 1)
    reserve(_data.capacity() * 2);
}

template <class T>
void queue<T>::pop() {
  if(empty()) throw container_is_empty();
  _lft = (_lft + 1) % _data.capacity();
}

template <class T>
void queue<T>::reserve(size_t capacity) {
  if(capacity < _data.capacity()) return;
  size_t old_cap = _data.capacity();
  _data.resize(capacity);

  if(_rht < _lft) {
    for(size_t i = old_cap, j = capacity; i > _lft; --i, --j)
      _data[j - 1] = std::move(_data[i - 1]);
    _lft = _lft + capacity - old_cap;
  }
}

}

#endif