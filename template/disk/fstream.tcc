#ifndef INSOMNIA_FSTREAM_TCC
#define INSOMNIA_FSTREAM_TCC

#include <cassert>

#include "fstream.h"

namespace insomnia {

template <class T, class Meta>
void fstream<T, Meta>::open(const std::filesystem::path &file) {
  std::unique_lock lock(disk_io_latch_);
  if(is_open_locked()) close_locked();
  std::filesystem::path index_file = file.parent_path() / (file.filename().string() + ".idx");
  basic_fstream_.open(file, std::ios::in | std::ios::out | std::ios::binary);
  index_pool_.open(index_file);
  if(!basic_fstream_.is_open()) {
    basic_fstream_.close();
    index_pool_.close();
    std::filesystem::remove(index_file);
    basic_fstream_.open(file, std::ios::out | std::ios::binary);
    basic_fstream_.close();
    basic_fstream_.open(file, std::ios::in | std::ios::out | std::ios::binary);
    index_pool_.open(index_file);
  }
  file_size_ = std::filesystem::file_size(file);
}

template <class T, class Meta>
void fstream<T, Meta>::close() {
  std::unique_lock lock(disk_io_latch_);
  if(!is_open_locked()) return;
  basic_fstream_.close();
  index_pool_.close();
  file_size_ = 0;
}

template <class T, class Meta>
void fstream<T, Meta>::write(index_t index, const T *data) {
  if(index == IndexPool::nullpos)
    throw segmentation_fault("Writing nullpos / nullptr");
  std::unique_lock lock(disk_io_latch_);
  size_t offset = SIZE_META + index * SIZE_T;
  // in accordance with the codes of read().
  if(offset + SIZE_T > file_size_) reserve_locked(offset * 2 + SIZE_T + SIZE_META);
  basic_fstream_.seekp(offset, std::ios::beg);
  basic_fstream_.write(reinterpret_cast<const char*>(data), SIZE_T);
}

template <class T, class Meta>
void fstream<T, Meta>::read(index_t index, T *data) {
  if(index == IndexPool::nullpos)
    throw segmentation_fault("Reading nullpos / nullptr");
  std::unique_lock lock(disk_io_latch_);
  size_t offset = SIZE_META + index * SIZE_T;
  // read at the end of file will lead to a std::fstream bad status.
  // consider file_size_ = offset = 0 here.
  if(offset + SIZE_T > file_size_) reserve_locked(offset * 2 + SIZE_T + SIZE_META);
  basic_fstream_.seekg(offset, std::ios::beg);
  basic_fstream_.read(reinterpret_cast<char*>(data), SIZE_T);
}

template <class T, class Meta>
void fstream<T, Meta>::reserve(size_t file_size) {
  std::unique_lock lock(disk_io_latch_);
  if(file_size < file_size_) return;
  basic_fstream_.seekp(file_size, std::ios::beg);
  basic_fstream_.write("\0", 1);
  file_size_ = file_size;
}

template <class T, class Meta>
void fstream<T, Meta>::close_locked() {
  if(!is_open()) return;
  basic_fstream_.close();
  index_pool_.close();
  file_size_ = 0;
}

template <class T, class Meta>
void fstream<T, Meta>::reserve_locked(size_t file_size) {
  if(file_size < file_size_) return;
  basic_fstream_.seekp(file_size, std::ios::beg);
  basic_fstream_.write("\0", 1);
  file_size_ = file_size;
}

template <class T, class Meta>
bool fstream<T, Meta>::read_meta(Meta *meta) requires (!std::is_same_v<Meta, monometa>) {
  std::unique_lock lock(disk_io_latch_);
  if(file_size_ < SIZE_META) { reserve_locked(SIZE_META); return false; }
  basic_fstream_.seekg(std::ios::beg);
  basic_fstream_.read(reinterpret_cast<char*>(meta), SIZE_META);
  return true;
}

template <class T, class Meta>
void fstream<T, Meta>::write_meta(const Meta *meta) requires (!std::is_same_v<Meta, monometa>) {
  std::unique_lock lock(disk_io_latch_);
  if(file_size_ < SIZE_META) reserve_locked(SIZE_META);
  basic_fstream_.seekp(std::ios::beg);
  basic_fstream_.write(reinterpret_cast<const char*>(meta), SIZE_META);
}

}


#endif