#ifndef INSOMNIA_FSTREAM_TCC
#define INSOMNIA_FSTREAM_TCC

#include "fstream.h"

namespace insomnia::disk {

template <class T>
void fstream<T>::open(const std::filesystem::path &file) {
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

template <class T>
void fstream<T>::close() {
  std::unique_lock lock(disk_io_latch_);
  if(!is_open_locked()) return;
  basic_fstream_.close();
  index_pool_.close();
  file_size_ = 0;
}

template <class T>
void fstream<T>::write(index_t index, const T *data) {
  std::unique_lock lock(disk_io_latch_);
  size_t offset = index * SIZE_T;
  // in accordance with the codes of read().
  if(offset >= file_size_) reserve_locked(offset * 2 + SIZE_T);
  basic_fstream_.seekp(offset, std::ios::beg);
  basic_fstream_.write(reinterpret_cast<const char*>(data), SIZE_T);
}

template <class T>
void fstream<T>::read(index_t index, T *data) {
  std::unique_lock lock(disk_io_latch_);
  size_t offset = index * SIZE_T;
  // read at the end of file will lead to a std::fstream bad status.
  // consider file_size_ = offset = 0 here.
  if(offset >= file_size_) reserve_locked(offset * 2 + SIZE_T);
  basic_fstream_.seekg(offset, std::ios::beg);
  basic_fstream_.read(reinterpret_cast<char*>(data), SIZE_T);
}

template <class T>
void fstream<T>::reserve(size_t file_size) {
  std::unique_lock lock(disk_io_latch_);
  if(file_size < file_size_) return;
  basic_fstream_.seekp(file_size, std::ios::beg);
  basic_fstream_.write("\0", 1);
  file_size_ = file_size;
}

template <class T>
void fstream<T>::close_locked() {
  if(!is_open()) return;
  basic_fstream_.close();
  index_pool_.close();
  file_size_ = 0;
}

template <class T>
void fstream<T>::reserve_locked(size_t file_size) {
  if(file_size < file_size_) return;
  basic_fstream_.seekp(file_size, std::ios::beg);
  basic_fstream_.write("\0", 1);
  file_size_ = file_size;
}



}


#endif