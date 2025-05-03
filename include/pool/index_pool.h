#ifndef INSOMNIA_INDEX_POOL_H
#define INSOMNIA_INDEX_POOL_H

#include <fstream>
#include <filesystem>

#include "vector.h"

namespace insomnia::disk {

class IndexPool {
public:
  using index_t = size_t;
  IndexPool() = default;
  explicit IndexPool(const std::filesystem::path &file) { open(file); };
  ~IndexPool() { close(); }
  void open(const std::filesystem::path &file);
  void close();
  bool is_open() const { return pool_.is_open(); }
  index_t allocate();
  void deallocate(index_t index);
  index_t size() const { return capacity_; }

private:
  std::fstream pool_;
  index_t capacity_{0};
  cntr::vector<index_t> unallocated_;
};

}


#endif