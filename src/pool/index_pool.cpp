#include "index_pool.h"

namespace insomnia::disk {

void IndexPool::open(const std::filesystem::path &file) {
  std::unique_lock lock(latch_);
  // treated always succeeded
  if(pool_.is_open()) close();
  pool_.open(file, std::ios::in | std::ios::out | std::ios::binary);
  if(pool_.is_open()) {
    pool_.seekg(0);
    pool_.read(reinterpret_cast<char*>(&capacity_), sizeof(capacity_));
    size_t size = 0;
    pool_.read(reinterpret_cast<char*>(&size), sizeof(size));
    unallocated_.resize(size);
    pool_.read(reinterpret_cast<char*>(unallocated_.data()), size * sizeof(size_t));
  } else {
    pool_.close();
    pool_.open(file, std::ios::out | std::ios::binary);
    pool_.close();
    pool_.open(file, std::ios::in | std::ios::out | std::ios::binary);
    capacity_ = 0;
    size_t size = 0;
    pool_.seekp(0);
    pool_.write(reinterpret_cast<char*>(&capacity_), sizeof(capacity_));
    pool_.write(reinterpret_cast<char*>(&size), sizeof(size));
    pool_.flush();
  }
}

void IndexPool::close() {
  std::unique_lock lock(latch_);
  if(!pool_.is_open()) return;
  pool_.seekp(0);
  pool_.write(reinterpret_cast<char*>(&capacity_), sizeof(capacity_));
  size_t size = unallocated_.size();
  pool_.write(reinterpret_cast<char*>(&size), sizeof(size));
  pool_.write(reinterpret_cast<char*>(unallocated_.data()), size * sizeof(size_t));
  pool_.flush();
  pool_.close();
  capacity_ = 0;
  unallocated_.resize(0);
}


IndexPool::index_t IndexPool::allocate() {
  std::unique_lock lock(latch_);
  if(!unallocated_.empty()) {
    index_t res = unallocated_.back();
    unallocated_.pop_back();
    return res;
  }
  return ++capacity_;
}

void IndexPool::deallocate(index_t index) {
  std::unique_lock lock(latch_);
  unallocated_.push_back(index);
}

}