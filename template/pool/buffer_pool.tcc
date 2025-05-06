#ifndef INSOMNIA_BUFFER_POOL_TCC
#define INSOMNIA_BUFFER_POOL_TCC

#include "buffer_pool.h"

namespace insomnia::concurrent {

template <class T, size_t align>
BufferPool<T, align>::Writer::Writer(page_id_t page_id, Frame *frame,
  policy::LruKReplacer *replacer, std::mutex *bp_latch, TaskScheduler *scheduler,
  disk::fstream<AlignedPage> *fstream, std::condition_variable *replacer_cv,
  std::unique_lock<std::mutex> lock)
    : is_valid_(true),
      page_id_(page_id),
      frame_(frame),
      replacer_(replacer),
      bp_latch_(bp_latch),
      scheduler_(scheduler),
      fstream_(fstream),
      replacer_cv_(replacer_cv) {
  if (frame_->pin_count_.fetch_add(1) == 0) {
    replacer_->pin(frame_->frame_id_);
  }
  replacer_->access(frame_->frame_id_);
  lock.unlock();              // locked at get_writer.
  frame_->page_latch_.lock(); // should it be outside the buffer pool latch?
}

template <class T, size_t align>
BufferPool<T, align>::Writer::Writer(Writer &&other) noexcept
    : is_valid_(true),
      page_id_(other.page_id_),
      frame_(other.frame_),
      replacer_(other.replacer_),
      bp_latch_(other.bp_latch_),
      scheduler_(other.scheduler_),
      fstream_(other.fstream_),
      replacer_cv_(other.replacer_cv_) {
  other.is_valid_ = false;
  other.frame_ = nullptr;
  other.replacer_ = nullptr;
  other.bp_latch_ = nullptr;
  other.scheduler_ = nullptr;
  other.fstream_ = nullptr;
  other.replacer_cv_ = nullptr;
}

template <class T, size_t align>
typename BufferPool<T, align>::Writer&
  BufferPool<T, align>::Writer::operator=(Writer &&other) noexcept {
  if(this == &other) return *this;
  drop();
  is_valid_ = other.is_valid_;
  page_id_ = other.page_id_;
  frame_ = other.frame_;
  replacer_ = other.replacer_;
  bp_latch_ = other.bp_latch_;
  scheduler_ = other.scheduler_;
  fstream_ = other.fstream_;
  replacer_cv_ = other.replacer_cv_;;

  other.is_valid_ = false;
  other.frame_ = nullptr;
  other.replacer_ = nullptr;
  other.bp_latch_ = nullptr;
  other.scheduler_ = nullptr;
  other.fstream_ = nullptr;
  other.replacer_cv_ = nullptr;
  return *this;
}

template <class T, size_t align>
void BufferPool<T, align>::Writer::flush() {
  if (!frame_->is_dirty_) return;
  auto future = scheduler_->schedule(page_id_, fstream_->write, page_id_, &frame_->page_);
  future.get();  // optimize later
  frame_->is_dirty_ = false;
}


template <class T, size_t align>
void BufferPool<T, align>::Writer::drop() {
  if(!is_valid_) return;
  {
    std::unique_lock lock(*bp_latch_);
    frame_->page_latch_.unlock();  // should it be outside the bpm latch?
    if (frame_->pin_count_.fetch_sub(1) == 1) {
      replacer_->unpin(frame_->frame_id_);
      replacer_cv_->notify_one();
    }
  }
  frame_ = nullptr;
  replacer_ = nullptr;
  bp_latch_ = nullptr;
  scheduler_ = nullptr;
  fstream_ = nullptr;
  replacer_cv_ = nullptr;
  is_valid_ = false;
}

/*******************************************************************************************************************/

template <class T, size_t align>
BufferPool<T, align>::Reader::Reader(page_id_t page_id, Frame *frame,
  policy::LruKReplacer *replacer, std::mutex *bp_latch, TaskScheduler *scheduler,
  disk::fstream<AlignedPage> *fstream, std::condition_variable *replacer_cv,
  std::unique_lock<std::mutex> lock)
    : is_valid_(true),
      page_id_(page_id),
      frame_(frame),
      replacer_(replacer),
      bp_latch_(bp_latch),
      scheduler_(scheduler),
      fstream_(fstream),
      replacer_cv_(replacer_cv) {
  if (frame_->pin_count_.fetch_add(1) == 0) {
    replacer_->pin(frame_->frame_id_);
  }
  replacer_->access(frame_->frame_id_);
  lock.unlock();                      // locked at get_reader.
  frame_->page_latch_.lock_shared();  // should it be outside the buffer pool latch?
}

template <class T, size_t align>
BufferPool<T, align>::Reader::Reader(Reader &&other) noexcept
    : is_valid_(true),
      page_id_(other.page_id_),
      frame_(other.frame_),
      replacer_(other.replacer_),
      bp_latch_(other.bp_latch_),
      scheduler_(other.scheduler_),
      fstream_(other.fstream_),
      replacer_cv_(other.replacer_cv_) {
  other.is_valid_ = false;
  other.frame_ = nullptr;
  other.replacer_ = nullptr;
  other.bp_latch_ = nullptr;
  other.scheduler_ = nullptr;
  other.fstream_ = nullptr;
  other.replacer_cv_ = nullptr;
}

template <class T, size_t align>
typename BufferPool<T, align>::Reader&
  BufferPool<T, align>::Reader::operator=(Reader &&other) noexcept {
  if(this == &other) return *this;
  drop();
  is_valid_ = other.is_valid_;
  page_id_ = other.page_id_;
  frame_ = other.frame_;
  replacer_ = other.replacer_;
  bp_latch_ = other.bp_latch_;
  scheduler_ = other.scheduler_;
  fstream_ = other.fstream_;
  replacer_cv_ = other.replacer_cv_;

  other.is_valid_ = false;
  other.frame_ = nullptr;
  other.replacer_ = nullptr;
  other.bp_latch_ = nullptr;
  other.scheduler_ = nullptr;
  other.fstream_ = nullptr;
  other.replacer_cv_ = nullptr;
  return *this;
}

template <class T, size_t align>
void BufferPool<T, align>::Reader::flush() {
  if (!frame_->is_dirty_) return;
  auto future = scheduler_->schedule(page_id_, fstream_->write, page_id_, &frame_->page_);
  future.get();  // optimize later
  frame_->is_dirty_ = false;
}


template <class T, size_t align>
void BufferPool<T, align>::Reader::drop() {
  if (!is_valid_) return;
  {
    std::unique_lock lock(*bp_latch_);
    frame_->page_latch_.unlock_shared();  // should it be outside the bpm latch?
    if (frame_->pin_count_.fetch_sub(1) == 1) {
      replacer_->unpin(frame_->frame_id_);
      replacer_cv_->notify_one();
    }
  }
  frame_ = nullptr;
  replacer_ = nullptr;
  bp_latch_ = nullptr;
  scheduler_ = nullptr;
  fstream_ = nullptr;
  replacer_cv_ = nullptr;
  is_valid_ = false;
}

/*******************************************************************************************************************/

template <class T, size_t align>
BufferPool<T, align>::BufferPool(const std::string &file_prefix, size_t k_param, size_t frame_num, size_t thread_num)
    : frame_num_(frame_num),
      replacer_(k_param, frame_num),
      scheduler_(thread_num),
      fstream_(file_prefix + ".dat") {
  frames_.reserve(frame_num);
  free_frames_.reserve(frame_num);
  page_map_.reserve(frame_num);
  for (frame_id_t i = 0; i < frame_num; i++) {
    frames_.push_back(Frame(i));
    free_frames_.push_back(i);
  }
}

template <class T, size_t align>
bool BufferPool<T, align>::dealloc(page_id_t page_id) {
  std::unique_lock lock(bp_latch_);
  if (auto it = page_map_.find(page_id); it != page_map_.end()) {
    if (frames_[it->second].pin_count_.load() > 0)
      return false;
    // memory erasure / eviction
    // no need to write the data back.
    frames_[it->second].drop();
    free_frames_.push_back(it->second);
    replacer_.remove(it->second);
    page_map_.erase(it);
  }
  // disk erasure
  fstream_.dealloc(page_id);
  return true;
}

template <class T, size_t align>
typename BufferPool<T, align>::Reader
BufferPool<T, align>::get_reader(page_id_t page_id) {
  frame_id_t frame_id;
  std::unique_lock lock(bp_latch_);
  if (auto it = page_map_.find(page_id); it != page_map_.end()) {
    frame_id = it->second;
  } else {
    if (!free_frames_.empty()) {
      frame_id = free_frames_.back();
      free_frames_.pop_back();
    } else {
      // HDD time delay

      if(!replacer_cv_.wait_for(lock, std::chrono::milliseconds(20), [this] { return replacer_.has_evictable_frame(); }))
        // return Reader(); // empty vessel.
        throw pool_overflow("Buffer pool frames full for 20ms."); // Yes I prefer exception more than optional right

      // replacer_cv_.wait(lock, [this] { return replacer_.has_evictable_frame(); });
      frame_id = replacer_.evict();

      // flush old data
      if (frames_[frame_id].is_dirty_) {
        auto future = scheduler_.schedule(
          frames_[frame_id].page_id_,
          [this, frame_id] { fstream_.write(frames_[frame_id].page_id_, &frames_[frame_id].page_); });
        future.get();  // optimize later
      }

      page_map_.erase(frames_[frame_id].page_id_);
      frames_[frame_id].drop();  // redundant
    }
    page_map_.emplace(page_id, frame_id);
    frames_[frame_id].page_id_ = page_id;
    frames_[frame_id].is_valid_ = true;

    // fetch new data
    auto future = scheduler_.schedule(page_id,
      [this, page_id, frame_id] {
        // might be ignored if page_id is new
        fstream_.read(page_id, &frames_[frame_id].page_);
      });
    future.get();  // optimize later
  }
  // bp_latch_ unlock in Reader page constructor.
  return Reader(page_id, &frames_[frame_id], &replacer_, &bp_latch_, &scheduler_, &fstream_, &replacer_cv_,
    std::move(lock));
}

template <class T, size_t align>
typename BufferPool<T, align>::Writer BufferPool<T, align>::get_writer(page_id_t page_id) {
  frame_id_t frame_id;
  std::unique_lock lock(bp_latch_);
  if (auto it = page_map_.find(page_id); it != page_map_.end()) {
    frame_id = it->second;
  } else {
    if (!free_frames_.empty()) {
      frame_id = free_frames_.back();
      free_frames_.pop_back();
    } else {
      // HDD time delay

      if(!replacer_cv_.wait_for(lock, std::chrono::milliseconds(20), [this] { return replacer_.has_evictable_frame(); }))
        // return Writer(); // empty vessel
        throw pool_overflow("Buffer pool frames full for 20ms."); // Yes I prefer exception more than optional right

      // replacer_cv_.wait(lock, [this] { return replacer_.has_evictable_frame(); });
      frame_id = replacer_.evict();

      // flush old data
      if (frames_[frame_id].is_dirty_) {
        auto future = scheduler_.schedule(
          frames_[frame_id].page_id_,
          [this, frame_id] { fstream_.write(frames_[frame_id].page_id_, &frames_[frame_id].page_); });
        future.get();  // optimize later
      }

      page_map_.erase(frames_[frame_id].page_id_);
      frames_[frame_id].drop();  // redundant
    }
    page_map_.emplace(page_id, frame_id);
    frames_[frame_id].page_id_ = page_id;
    frames_[frame_id].is_valid_ = true;

    // fetch new data
    auto future = scheduler_.schedule(page_id,
      [this, page_id, frame_id] {
        // might be ignored if page_id is new
        fstream_.read(page_id, &frames_[frame_id].page_);
      });
    future.get();  // optimize later
  }
  // bp_latch_ unlock in Writer page constructor.
  return Writer(page_id, &frames_[frame_id], &replacer_, &bp_latch_, &scheduler_, &fstream_, &replacer_cv_,
    std::move(lock));
}

template <class T, size_t align>
void BufferPool<T, align>::flush_all() {
  for (auto &frame : frames_) {
    if (!frame.is_valid_ || !frame.is_dirty_)
      continue;
    std::unique_lock bp_lock(bp_latch_);
    std::unique_lock frame_lock(frame.page_latch_);
    auto future = scheduler_.schedule(frame.page_id_,
      [this, &frame] { fstream_.write(frame.page_id_, &frame.page_); });
    future.get();
    frame.is_dirty_ = false;
  }
}

}

#endif