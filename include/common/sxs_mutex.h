#ifndef INSOMNIA_SXS_MUTEX_H
#define INSOMNIA_SXS_MUTEX_H

#include <atomic>
#include <mutex>
#include <condition_variable>
#include <thread>

namespace insomnia {

class sxs_mutex {
public:
  enum class Mode { None, S, SX, X };

  sxs_mutex() : state_(0), writer_thread_() {}
  ~sxs_mutex() = default;

  void lock_S();
  void lock_X();
  void lock_SX();
  void lock(Mode mode) {
    switch(mode) {
    case Mode::S: lock_S(); break;
    case Mode::SX: lock_SX(); break;
    case Mode::X: lock_X(); break;
    default: throw std::invalid_argument("Invalid lock mode");
    }
  }

  bool try_lock_S();
  bool try_lock_X();
  bool try_lock_SX();
  bool try_lock(Mode mode) {
    switch (mode) {
    case Mode::S: return try_lock_S();
    case Mode::SX: return try_lock_SX();
    case Mode::X: return try_lock_X();
    default: throw std::invalid_argument("Invalid lock mode");
    }
  }

  void unlock_S();
  void unlock_X();
  void unlock_SX();

  void unlock();

  bool try_upgrade_X();
  bool try_upgrade_SX();
  void upgrade_X();
  void upgrade_SX();
  void downgrade_S();
  void downgrade_SX();

  // every type of lock
  bool transfer_lock(std::thread::id new_thread);

  Mode get_current_mode() const {
    uint32_t state = state_.load(std::memory_order_relaxed);
    if (state & X_MASK) return Mode::X;
    if (state & SX_MASK) return Mode::SX;
    if (state & S_COUNT_MASK) return Mode::S;
    return Mode::None;
  }
  std::thread::id get_writer_thread() const {
    return writer_thread_.load(std::memory_order_relaxed);
  }

private:
  static constexpr uint32_t S_COUNT_MASK = 0xFFFF;  // S计数掩码（低16位）
  static constexpr uint32_t SX_MASK      = 0x10000; // SX标志位（第17位）
  static constexpr uint32_t X_MASK       = 0x20000; // X标志位（第18位）

  std::atomic<uint32_t> state_;                // S count + SX * SX_MASK + X count * X_MASK
  std::atomic<std::thread::id> writer_thread_; // X/SX carrier
  std::mutex mutex_;
  std::condition_variable cond_;

};

}


#endif