#ifndef INSOMNIA_FSTREAM_H
#define INSOMNIA_FSTREAM_H

#include <fstream>
#include <filesystem>
#include <mutex>

#include "index_pool.h"

namespace insomnia {
/**
 * @brief specialized for metadata placeholder for disk::fstream
 */
class monometa {};

/** @brief specialized for one-type disk recording.
 *  @warning This fstream enormously varies from std::fstream. Be careful.
 *    aware, bare, care.
 */
template <class T, class Meta = monometa>
class fstream {
  static constexpr size_t SIZE_T = sizeof(T);
  static constexpr size_t SIZE_META = (std::is_same_v<Meta, monometa> ? 0 : sizeof(Meta));
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
  void write_meta(const Meta *meta) requires (!std::is_same_v<Meta, monometa>);
  // fails if meta data was not initialized.
  // You can use it to see whether the db file is newly created.
  bool read_meta(Meta *meta) requires (!std::is_same_v<Meta, monometa>);
  void reserve(size_t file_size);

private:
  bool is_open_locked() const { return basic_fstream_.is_open(); }
  void close_locked();
  void reserve_locked(size_t data_capacity);

  std::fstream basic_fstream_;
  mutable std::mutex disk_io_latch_;
  IndexPool index_pool_;
  size_t file_size_{0};
};


}


#include "fstream.tcc"

#endif