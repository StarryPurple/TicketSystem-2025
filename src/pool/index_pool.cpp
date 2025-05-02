#include "index_pool.h"

namespace insomnia::disk {

IndexPool::IndexPool(const std::string &file) : pool_(file, std::ios::in | std::ios::out | std::ios::binary) {
  if(pool_.is_open()) {
    pool_.read(reinterpret_cast<char*>(&capacity_), sizeof(capacity_));
    int size = 0;
    pool_.read(reinterpret_cast<char*>(&size), sizeof(size));
    unallocated_.resize(size);
    pool_.read(reinterpret_cast<char*>(unallocated_.data()), size * sizeof(size_t));
  } else {
    pool_.close();
    pool_.open(file, std::ios::out | std::ios::binary);
    capacity_ = 0;
    int size = 0;
    pool_.write(reinterpret_cast<char*>(&capacity_), sizeof(capacity_));
    pool_.write(reinterpret_cast<char*>(&size), sizeof(size));
  }
}

IndexPool::~IndexPool() {
  pool_.seekp(0);
  pool_.write(reinterpret_cast<char*>(&capacity_), sizeof(capacity_));
  int size = unallocated_.size();
  pool_.write(reinterpret_cast<char*>(&size), sizeof(size));
  pool_.write(reinterpret_cast<char*>(unallocated_.data()), size * sizeof(size_t));
  pool_.close();
}

size_t IndexPool::allocate() {
  if(!unallocated_.empty()) {
    int res = unallocated_.back();
    unallocated_.pop_back();
    return res;
  }
  return ++capacity_;
}

void IndexPool::deallocate(size_t index) {
  unallocated_.push_back(index);
}






}