#ifndef INSOMNIA_INDEX_POOL_H
#define INSOMNIA_INDEX_POOL_H

#include <fstream>
#include <filesystem>
#include <mutex>

#include "vector.h"

namespace insomnia::disk {

/**
 * @brief index allocator. Thread-safe. Only supports size_t as index type.
 */
class IndexPool {
public:
  using index_t = size_t;
  IndexPool() = default;
  explicit IndexPool(const std::filesystem::path &file) { open(file); };
  ~IndexPool() { close(); }
  void open(const std::filesystem::path &file);
  void close();
  bool is_open() const {
    std::unique_lock lock(latch_);
    return pool_.is_open();
  }
  index_t allocate();
  void deallocate(index_t index);
  index_t size() const {
    std::unique_lock lock(latch_);
    return capacity_;
  }

private:
  std::fstream pool_;
  index_t capacity_{0}; // 0 reserved for nullptr
  cntr::vector<index_t> unallocated_; // alignas(64) useful?
  alignas(64) mutable std::mutex latch_;
};

}


#endif