#ifndef INSOMNIA_BUFFER_POOL_H
#define INSOMNIA_BUFFER_POOL_H

#include <shared_mutex>

#include "fstream.h"
#include "unordered_map.h"
#include "index_pool.h"
#include "lru_k_replacer.h"
#include "task_scheduler.h"


namespace insomnia::concurrent {
/**
 * @brief disk read/write buffer pool.
 * @tparam T The storage type of the disk.
 * @tparam align The maximum size of derived types of T.
 * @warning remember to check the @align param everytime you derive a type from T.
 * @warning Make sure all Writers/Readers are deconstructed before buffer pool deconstructs.
 */
template <class T, size_t align = sizeof(T)>
class BufferPool {
public:
  static constexpr size_t PAGE_SIZE = (align + 4095) / 4096 * 4096;
  using page_id_t = disk::IndexPool::index_t;
  using frame_id_t = disk::IndexPool::index_t;
  class Writer;
  class Reader;

private:

  struct alignas(PAGE_SIZE) AlignedPage {
    char data_[PAGE_SIZE];
    bool operator==(const AlignedPage &other) const { return strcmp(data_, other.data_) == 0; }
    bool operator!=(const AlignedPage &other) const { return strcmp(data_, other.data_) != 0; }
  };
  static_assert(sizeof(AlignedPage) == PAGE_SIZE);

  class Frame {
    friend Writer;
    friend Reader;
    friend BufferPool;

    alignas(PAGE_SIZE) AlignedPage page_;

    const frame_id_t frame_id_;
    std::atomic<size_t> pin_count_{0};
    bool is_dirty_{false};
    bool is_valid_{false};

    alignas(64) std::shared_mutex page_latch_; // false sharing stuff..
    size_t page_id_;

  public:
    explicit Frame(frame_id_t frame_id) : frame_id_(frame_id) {}
    Frame(Frame &&other) : frame_id_(other.frame_id_), pin_count_(other.pin_count_.load()),
        is_dirty_(other.is_dirty_), is_valid_(other.is_valid_), page_id_(other.page_id_) {
      other.pin_count_.store(0);
      other.is_dirty_ = false;
      other.is_valid_ = false;
    }

    char* data() { return page_.data_; }

    void drop() {
      pin_count_.store(0);
      is_valid_ = false;
      is_dirty_ = false;
    }
  };

public:
  class Writer {
  public:
    Writer() = default;
    explicit Writer(
      page_id_t page_id, Frame *frame, policy::LruKReplacer *replacer,
      std::mutex *bp_latch, TaskScheduler *scheduler, disk::fstream<AlignedPage> *fstream,
      std::condition_variable *replacer_cv, std::unique_lock<std::mutex> lock);
    Writer(const Writer&) = delete;
    Writer& operator=(const Writer&) = delete;
    Writer(Writer &&other) noexcept;
    Writer& operator=(Writer &&other) noexcept;
    ~Writer() { drop(); }

    page_id_t page_id() const { return page_id_; }
    char* data() {
      frame_->is_dirty_ = true;
      return frame_->data();
    }
    template <class Derived>
    Derived* as() {
      static_assert(std::is_base_of_v<T, Derived>);
      frame_->is_dirty_ = true;
      return reinterpret_cast<Derived*>(frame_->data());
    }
    bool is_dirty() const { return frame_->is_dirty_; }
    bool is_valid() const { return is_valid_; }
    void flush();
    void drop();

  private:
    page_id_t page_id_;
    Frame *frame_;
    policy::LruKReplacer *replacer_;
    std::mutex *bp_latch_;
    TaskScheduler *scheduler_;
    disk::fstream<AlignedPage> *fstream_;
    std::condition_variable *replacer_cv_;
    bool is_valid_{false};
  };
  class Reader {
  public:
    Reader() = default;
    explicit Reader(
      page_id_t page_id, Frame *frame, policy::LruKReplacer *replacer,
      std::mutex *bp_latch, TaskScheduler *scheduler, disk::fstream<AlignedPage> *fstream,
      std::condition_variable *replacer_cv, std::unique_lock<std::mutex> lock);
    Reader(const Reader&) = delete;
    Reader& operator=(const Reader&) = delete;
    Reader(Reader &&other) noexcept;
    Reader& operator=(Reader &&other) noexcept;
    ~Reader() { drop(); }

    page_id_t page_id() const { return page_id_; }
    const char* data() const { return frame_->data(); }
    template <class Derived>
    const Derived* as() const {
      static_assert(std::is_base_of_v<T, Derived>);
      return reinterpret_cast<Derived*>(frame_->data());
    }

    bool is_dirty() const { return frame_->is_dirty_; }
    bool is_valid() const { return is_valid_; }
    void flush();
    void drop();

  private:
    page_id_t page_id_;
    Frame *frame_;
    policy::LruKReplacer *replacer_;
    std::mutex *bp_latch_;
    TaskScheduler *scheduler_;
    disk::fstream<AlignedPage> *fstream_;
    std::condition_variable *replacer_cv_;
    bool is_valid_{false};
  };

  BufferPool(const std::string &file_prefix, size_t k_param, size_t frame_num, size_t thread_num);
  BufferPool(const BufferPool&) = delete;
  BufferPool(BufferPool&&) = delete;
  BufferPool& operator=(const BufferPool&) = delete;
  BufferPool& operator=(BufferPool&&) = delete;
  ~BufferPool() { flush_all(); }
  size_t frame_capacity() const { return frame_num_; }
  page_id_t alloc() { return fstream_.alloc(); }
  // fails if this page is still in use by writer/reader.
  bool dealloc(page_id_t page_id);
  Writer get_writer(page_id_t page_id);
  Reader get_reader(page_id_t page_id);
  // void flush(frame_id_t frame_id);
  void flush_all();

  // if page not in buffer, returns SIZE_MAX.
  size_t get_pin_count(page_id_t page_id) {
    std::unique_lock lock(bp_latch_);
    if (auto it = page_map_.find(page_id); it == page_map_.end())
      return SIZE_MAX;
    else
      return frames_[it->second].pin_count_.load();
  }

private:
  alignas(64) std::mutex bp_latch_;
  alignas(64) std::condition_variable replacer_cv_;
  cntr::unordered_map<page_id_t, frame_id_t> page_map_;

  const size_t frame_num_;
  policy::LruKReplacer replacer_;
  TaskScheduler scheduler_;
  // disk::IndexPool page_id_pool_; Duplicated with the pool in fstream_.
  static_assert(std::is_same_v<page_id_t, disk::IndexPool::index_t>);
  disk::fstream<AlignedPage> fstream_;

  cntr::vector<Frame> frames_;
  cntr::vector<frame_id_t> free_frames_;

};

}

#include "buffer_pool.tcc"

#endif