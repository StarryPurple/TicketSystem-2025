#ifndef INSOMNIA_FSTREAM_H
#define INSOMNIA_FSTREAM_H

#include <fstream>
#include <filesystem>
#include <mutex>

#include "index_pool.h"

namespace insomnia::disk {

/** @brief specialized for one-type disk recording.
 *  @warning This fstream enormously varies from std::fstream. Be careful.
 *    aware, bare, care.
 */
template <class T>
class fstream {
  static constexpr size_t SIZE_T = sizeof(T);
public:
  using index_t = IndexPool::index_t;

  fstream() = default;
  fstream(const std::filesystem::path &file) { open(file); };
  void open(const std::filesystem::path &file);
  void close();
  bool is_open() const {
    std::unique_lock lock(disk_io_latch_);
    return basic_fstream_.is_open();
  };
  ~fstream() { close(); }

  fstream(const fstream&) = delete;
  fstream(fstream&&) = delete;
  fstream& operator=(const fstream&) = delete;
  fstream& operator=(fstream&&) = delete;

  index_t alloc() { return index_pool_.allocate(); }
  void dealloc(index_t index) { index_pool_.deallocate(index); }
  void write(index_t index, const T *data);
  void read(index_t index, T *data);
  void reserve(size_t file_size);

private:
  bool is_open_locked() const { return basic_fstream_.is_open(); }
  void close_locked();
  void reserve_locked(size_t file_size);

  std::fstream basic_fstream_;
  mutable std::mutex disk_io_latch_;
  IndexPool index_pool_;
  size_t file_size_{0};
};


}


#include "fstream.tcc"

#endif