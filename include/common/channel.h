#ifndef INSOMNIA_CHANNEL_H
#define INSOMNIA_CHANNEL_H

#include <mutex>
#include <condition_variable>
#include <optional>

#include "queue.h"

namespace insomnia::concurrent {

// ?
template <class T>
class Channel {
public:
  void enqueue(T &&t) {
    {
      std::unique_lock lock(latch_);
      queue_.push(std::move(t));
    }
    cv_.notify_one();
  }
  void enqueue(const T &t) {
    {
      std::unique_lock lock(latch_);
      queue_.push(t);
    }
    cv_.notify_one();
  }
  template <class ...Args>
  void enqueue(Args &&...args) {
    {
      std::unique_lock lock(latch_);
      queue_.emplace(std::forward<Args>(args)...);
    }
    cv_.notify_one();
  }
  T get() {
    std::unique_lock lock(latch_);
    cv_.wait(lock, [this] { return !this->queue_.empty(); });
    T res = std::move(queue_.front());
    queue_.pop();
    return res;
  }
  // When to close?
  // std::optional<T> try_get() ...
private:
  std::mutex latch_;
  std::condition_variable cv_;
  cntr::queue<T> queue_;
  // bool closed;
};

}


#include "channel.tcc"

#endif