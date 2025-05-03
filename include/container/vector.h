#ifndef INSOMNIA_VECTOR_H
#define INSOMNIA_VECTOR_H

#include "config.h"
#include "exception.h"

namespace insomnia::container {
template <class T>
class vector {
public:
  class const_iterator;
  class iterator {
    friend const_iterator;
    friend vector;
  public:
    iterator(): _container(nullptr), _ptr(nullptr) {}
    iterator operator+(size_t n) const {
      return {_container, _ptr + n};
    }
    iterator operator-(size_t n) const {
      return {_container, _ptr - n};
    }
    iterator& operator+=(size_t n) {
      _ptr += n; return *this;
    }
    iterator& operator-=(size_t n) {
      _ptr -= n; return *this;
    }
    size_t operator-(const iterator& other) const {
      if(!_container || _container != other._container) throw invalid_iterator();
      return _ptr - other._ptr;
    }
    iterator operator++(int) {
      auto tmp = *this; ++_ptr; return tmp;
    }
    iterator operator--(int) {
      auto tmp = *this; --_ptr; return tmp;
    }
    iterator& operator++() {
      ++_ptr; return *this;
    }
    iterator& operator--() {
      --_ptr; return *this;
    }
    T& operator*() const {
      return *_ptr;
    }
    T* operator->() const noexcept {
      return _ptr;
    }
    bool operator==(const iterator &other) const {
      return _container == other._container && _ptr == other._ptr;
    }
    bool operator==(const const_iterator &other) const {
      return _container == other._container && _ptr == other._ptr;
    }
    bool operator!=(const iterator &other) const {
      return _container != other._container || _ptr != other._ptr;
    }
    bool operator!=(const const_iterator &other) const {
      return _container != other._container || _ptr != other._ptr;
    }
  private:
    const vector *_container;
    T* _ptr;
    iterator(const vector *container, T *ptr): _container(container), _ptr(ptr) {}
  };
  class const_iterator {
    friend iterator;
    friend vector;
  public:
    const_iterator(): _container(nullptr), _ptr(nullptr) {}
    const_iterator(const iterator &iter): _container(iter._container), _ptr(iter._ptr) {}
    iterator operator+(size_t n) const {
      return {_container, _ptr + n};
    }
    iterator operator-(size_t n) const {
      return {_container, _ptr - n};
    }
    iterator& operator+=(size_t n) {
      _ptr += n; return *this;
    }
    iterator& operator-=(size_t n) {
      _ptr -= n; return *this;
    }
    size_t operator-(const const_iterator& other) const {
      if(!_container || _container != other._container) throw invalid_iterator();
      return _ptr - other._ptr;
    }
    const_iterator operator++(int) {
      auto tmp = *this; ++_ptr; return tmp;
    }
    const_iterator operator--(int) {
      auto tmp = *this; --_ptr; return tmp;
    }
    const_iterator& operator++() {
      ++_ptr; return *this;
    }
    const_iterator& operator--() {
      --_ptr; return *this;
    }
    const T& operator*() const {
      return *_ptr;
    }
    const T* operator->() const noexcept {
      return _ptr;
    }
    bool operator==(const iterator &other) const {
      return _container == other._container && _ptr == other._ptr;
    }
    bool operator==(const const_iterator &other) const {
      return _container == other._container && _ptr == other._ptr;
    }
    bool operator!=(const iterator &other) const {
      return _container != other._container || _ptr != other._ptr;
    }
    bool operator!=(const const_iterator &other) const {
      return _container != other._container || _ptr != other._ptr;
    }
  private:
    const vector *_container;
    T* _ptr;
    const_iterator(const vector *container, T *ptr): _container(container), _ptr(ptr) {}
  };
  vector();
  explicit vector(size_t size) requires std::is_default_constructible_v<T>;
  vector(size_t size, const T &t) requires std::is_copy_constructible_v<T>;
  vector(const vector &other);
  vector(vector &&other);
  ~vector();

  vector& operator=(const vector &other);
  vector& operator=(vector &&other);

  size_t size() const {
    return _end - _beg;
  }
  size_t capacity() const {
    return _lim - _beg;
  }
  bool empty() const {
    return _beg == _end;
  }
  iterator begin() const {
    return {this, _beg};
  }
  iterator end() const {
    return {this, _end};
  }
  const_iterator cbegin() const {
    return {this, _beg};
  }
  const_iterator cend() const {
    return {this, _end};
  }

  T& at(const size_t &pos) {
    if(pos >= size()) throw index_out_of_bound();
    return *(_beg + pos);
  }
  const T& at(const size_t &pos) const {
    if(pos >= size()) throw index_out_of_bound();
    return *(_beg + pos);
  }
  T& operator[](const size_t &pos) {
    if(pos >= size()) throw index_out_of_bound();
    return *(_beg + pos);
  }
  const T& operator[](const size_t &pos) const {
    if(pos >= size()) throw index_out_of_bound();
    return *(_beg + pos);
  }
  T& front() {
    if(empty()) throw container_is_empty();
    return *_beg;
  }
  const T& front() const {
    if(empty()) throw container_is_empty();
    return *_beg;
  }
  T& back() {
    if(empty()) throw container_is_empty();
    return *(_end - 1);
  }
  const T& back() const {
    if(empty()) throw container_is_empty();
    return *(_end - 1);
  }

  iterator insert(size_t pos, const T &val);
  iterator insert(size_t pos, T &&val);
  template <class ...Args> requires std::is_constructible_v<T, Args...>
  iterator emplace(size_t pos, Args &&...args);
  iterator insert(iterator iter, const T &val) {
    if(this != iter._container) throw invalid_iterator();
    return insert(iter._ptr - _beg, val);
  }
  iterator insert(iterator iter, T &&val) {
    if(this != iter._container) throw invalid_iterator();
    return insert(iter._ptr - _beg, std::move(val));
  }
  template <class ...Args> requires std::is_constructible_v<T, Args...>
  iterator insert(iterator iter, Args &&...args) {
    if(this != iter._container) throw invalid_iterator();
    return insert(iter._ptr - _beg, std::forward<Args>(args)...);
  }
  iterator erase(size_t pos);
  iterator erase(iterator iter) {
    if(this != iter._container) throw invalid_iterator();
    return erase(iter._ptr - _beg);
  }

  void push_back(const T &val) {
    if(_end == _lim) reserve(capacity() * 2);
    new (_end) T(val);
    ++_end;
  }
  void push_back(T &&val) {
    if(_end == _lim) reserve(capacity() * 2);
    new (_end) T(std::move(val));
    ++_end;
  }
  template <class ...Args> requires std::is_constructible_v<T, Args...>
  void emplace_back(Args &&...args) {
    if(_end == _lim) reserve(capacity() * 2);
    new (_end) T(std::forward<Args>(args)...);
    ++_end;
  }
  void pop_back() {
    if(empty()) throw container_is_empty();
    --_end;
    _end->~T();
  }

  void clear() {
    for(T *ptr = _beg; ptr != _end; ++ptr)
      ptr->~T();
    _end = _beg;
  }
  void reserve(size_t capacity);
  void resize(size_t size) requires std::is_default_constructible_v<T>;
  void resize(size_t size, const T &t) requires std::is_copy_constructible_v<T>;
  T* data();

private:
  T *_beg, *_end, *_lim;
};
}

#include "vector.tcc"

#endif