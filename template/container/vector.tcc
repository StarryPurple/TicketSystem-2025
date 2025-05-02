#ifndef INSOMNIA_VECTOR_TCC
#define INSOMNIA_VECTOR_TCC

#include "vector.h"

namespace insomnia::container {

template <class T>
vector<T>::vector(size_t size) {
  _beg = _end = new T[16];
  _lim = _beg + 16;
  resize(size);
}

template <class T>
vector<T>::vector(const vector &other) {
  _beg = new T[other.capacity()];
  _end = _beg + other.size();
  _lim = _beg + other.capacity();
  if constexpr(std::is_trivial_v<T> && std::is_move_constructible_v<T>)
    memcpy(_beg, other._beg, other.size() * sizeof(T));
  else
    for(T *here = _beg, there = other._beg; there != other._end; ++here, ++there)
      new (here) T(*there);
}

template <class T>
vector<T>::vector(vector &&other) : _beg(other._beg), _end(other._end), _lim(other._lim) {
  other._beg = other._end = other._lim = nullptr;
}

template <class T>
vector<T>::~vector() {
  clear();
  delete[] _beg;
}

template <class T>
vector<T>& vector<T>::operator=(const vector &other) {
  if(this == &other) return *this;
  clear();
  reserve(other.capacity());
  _end = _beg + other.size();
  if constexpr(std::is_trivial_v<T> && std::is_move_constructible_v<T>)
    memcpy(_beg, other._beg, other.size() * sizeof(T));
  else
    for(T *here = _beg, there = other._beg; there != other._end; ++here, ++there)
      new (here) T(*there);
  return *this;
}

template <class T>
vector<T>& vector<T>::operator=(vector &&other) {
  if(this == &other) return *this;
  clear();
  std::free(_beg);
  _beg = other._beg;
  _end = other._end;
  _lim = other._lim;
  other._beg = other._end = other._lim = nullptr;
  return *this;
}

template <class T>
typename vector<T>::iterator vector<T>::insert(size_t pos, const T &val) {
  if(pos > size()) throw index_out_of_bound();
  if(_end == _lim) reserve(capacity() * 2 + 1);
  T *ptr = _beg + pos;
  for(T *cur = _end, *prv = _end - 1; cur != ptr; --cur, --prv) {
    new (cur) T(std::move(*prv));
    prv->~T();
  }
  new (ptr) T(val);
  ++_end;
  return {this, ptr};
}

template <class T>
typename vector<T>::iterator vector<T>::insert(size_t pos, T &&val) {
  if(pos > size()) throw index_out_of_bound();
  if(_end == _lim) reserve(capacity() * 2 + 1);
  T *ptr = _beg + pos;
  for(T *cur = _end, *prv = _end - 1; cur != ptr; --cur, --prv) {
    new (cur) T(std::move(*prv));
    prv->~T();
  }
  new (ptr) T(std::move(val));
  ++_end;
  return {this, ptr};
}

template <class T>
template <class... Args> requires std::is_constructible_v<T, Args...>
typename vector<T>::iterator vector<T>::emplace(size_t pos, Args &&... args) {
  if(pos > size()) throw index_out_of_bound();
  if(_end == _lim) reserve(capacity() * 2 + 1);
  T *ptr = _beg + pos;
  for(T *cur = _end, *prv = _end - 1; cur != ptr; --cur, --prv) {
    new (cur) T(std::move(*prv));
    prv->~T();
  }
  new (ptr) T(std::forward<Args>(args)...);
  ++_end;
  return {this, ptr};
}


template <class T>
typename vector<T>::iterator vector<T>::erase(size_t pos) {
  if(pos >= size()) throw index_out_of_bound();
  T *ptr = _beg + pos;
  for(T *cur = ptr, *nxt = ptr + 1; nxt != _end; ++cur, ++nxt) {
    cur->~T();
    new (cur) T(std::move(*nxt));
  }
  --_end;
  _end->~T();
  return {this, ptr};
}



template <class T>
void vector<T>::reserve(size_t capacity) {
  if(capacity <= this->capacity()) return;
  T *new_beg = new T[capacity];
  T *new_end = new_beg + size();
  T *new_lim = new_beg + capacity;
  for(T *ptr = new_beg, *old_ptr = _beg; ptr != new_end; ++ptr, ++old_ptr) {
    new (ptr) T(std::move(*old_ptr));
    old_ptr->~T();
  }
  std::free(_beg);
  _beg = new_beg;
  _end = new_end;
  _lim = new_lim;
}

template <class T>
void vector<T>::resize(size_t size) {
  static_assert(std::is_default_constructible_v<T>,
    "resize should be called only when Type is default constructible.");
  if(size <= _end - _beg) {
    for(T *ptr = _beg + size; ptr != _end; ++ptr)
      ptr->~T();
  } else {
    reserve(size);
    for(T *ptr = _end; ptr != _beg + size; ++ptr)
      new (ptr) T();
  }
  _end = _beg + size;
}

template <class T>
T* vector<T>::data() {
  return _beg;
}



}




#endif