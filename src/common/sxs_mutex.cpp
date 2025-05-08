#include "sxs_mutex.h"

namespace insomnia {

void sxs_mutex::lock_S() {
  std::unique_lock lock(mutex_);
  auto tid = std::this_thread::get_id();
  uint32_t state = state_.load(std::memory_order_relaxed);

  // 检查是否已经持有任何锁
  if ((state & X_MASK) && writer_thread_.load(std::memory_order_relaxed) == tid) {
    throw std::runtime_error("Already holding X lock");
  }
  if ((state & SX_MASK) && writer_thread_.load(std::memory_order_relaxed) == tid) {
    throw std::runtime_error("Already holding SX lock");
  }
  if ((state & S_COUNT_MASK) && writer_thread_.load(std::memory_order_relaxed) == tid) {
    throw std::runtime_error("Already holding S lock");
  }

  // 等待没有X/SX锁
  cond_.wait(lock, [this, tid] {
      uint32_t s = state_.load(std::memory_order_relaxed);
      return !(s & (X_MASK | SX_MASK)) ||
             (writer_thread_.load(std::memory_order_relaxed) == tid);
  });

  // 检查是否可以获取S锁
  state = state_.load(std::memory_order_relaxed);
  if (state & (X_MASK | SX_MASK)) {
    if (writer_thread_.load(std::memory_order_relaxed) != tid) {
      throw std::runtime_error("Cannot acquire S lock while X/SX is held by others");
    }
  }

  // 增加S计数
  uint32_t new_state = state + 1;
  if ((new_state & S_COUNT_MASK) == 0) {
    throw std::overflow_error("Too many S locks");
  }
  state_.store(new_state, std::memory_order_release);
}

void sxs_mutex::lock_X() {
  std::unique_lock lock(mutex_);
  std::thread::id current_thread = std::this_thread::get_id();
  cond_.wait(lock, [this, current_thread] {
      uint32_t state = state_.load(std::memory_order_relaxed);
      return state == 0 ||
            ((state & SX_MASK) && writer_thread_ == current_thread &&
             (state & S_COUNT_MASK) == 0);
  });

  state_.store(X_MASK, std::memory_order_release);
  writer_thread_ = current_thread;
}






}