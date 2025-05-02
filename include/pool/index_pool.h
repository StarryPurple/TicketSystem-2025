#ifndef INSOMNIA_INDEX_POOL_H
#define INSOMNIA_INDEX_POOL_H

#include <fstream>

#include "vector.h"

namespace insomnia::disk {

class IndexPool {
public:
  IndexPool(const std::string &file);
  ~IndexPool();
  size_t allocate();
  void deallocate(size_t index);
  size_t size() const { return capacity_; }

private:
  std::fstream pool_;
  size_t capacity_;
  cntr::vector<size_t> unallocated_;
};

}


#endif