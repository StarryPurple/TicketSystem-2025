#ifndef INSOMNIA_BUFFER_TYPES_H
#define INSOMNIA_BUFFER_TYPES_H

#include <fstream>
#include <optional>
#include "vector.h"

namespace insomnia::buffer {

namespace cntr = container;

using PhysicalPageId = size_t;
using LogicalPageId = size_t;
using FrameId = size_t;
using TimestampId = size_t;

class buffer_exception : std::runtime_error {
public:
  using std::runtime_error::runtime_error;
  using std::runtime_error::operator=;
  using std::runtime_error::what;
};

class PhysicalPage {
public:
  static constexpr size_t PAGE_SIZE = 4096;
private:
  char _data[PAGE_SIZE];
  const PhysicalPageId _page_id;
  const std::string _filename;
  //  for sake of thread safety, we choose to create a std::fstream every time a read/write task is called.
  //  maybe a filestream pool can be implemented.
  // std::fstream _fstream;
public:
  PhysicalPage(const std::string &filename, const PhysicalPageId &page_id);
  void read();
  void write();
  char* data();
  const char* data() const;
};

template <class T>
class LogicalPage {
public:
  static constexpr size_t PAGE_COUNT = (sizeof(T) - 1) / PhysicalPage::PAGE_SIZE + 1;
  static constexpr size_t PAGE_SIZE = PAGE_COUNT * PhysicalPage::PAGE_SIZE;
private:
  char _data[PAGE_SIZE];
  const LogicalPageId _page_id;
  const std::string _filename;
public:
  LogicalPage(const std::string &filename, const PhysicalPageId &page_id);
  void concatenate(const cntr::vector<PhysicalPage> &physical_pages);
  cntr::vector<PhysicalPage> deconcatenate();
  void read(); // needed?
  void write(); // needed?
  char* c_data();
  const char* c_data() const;
  T* data();
  const T* data() const;
};

template <class T>
class Frame {
public:
  static constexpr size_t FRAME_SIZE = PhysicalPage::PAGE_SIZE;
private:
  char _data[FRAME_SIZE];
  std::optional<PhysicalPageId> _page_id;
  bool _is_dirty = false;
  bool _is_pinned = false;
public:
  void load(const PhysicalPage &page);
  PhysicalPage get_physical_page() const;
  void set_dirty();
  void set_clean();
  void pin();
  void unpin();
  void reset();
};

}

#endif