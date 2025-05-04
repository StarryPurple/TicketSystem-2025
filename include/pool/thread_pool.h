#ifndef INSOMNIA_THREAD_POOL_H
#define INSOMNIA_THREAD_POOL_H

#include <thread>
#include <mutex>
#include <condition_variable>
#include <future>
#include <functional>

#include "vector.h"
#include "queue.h"


namespace insomnia::concurrent {

namespace cntr = container;

/**
 * @brief a thread pool that can manage multithread tasks.
 *
 * @TODO Change the queue to deque to enable task stealing.
 *
 * @warning doesn't support id-queued task scheduling. The execution order has no guarantee.
 */
class ThreadPool {
public:
  explicit ThreadPool(size_t thread_count = std::thread::hardware_concurrency());
  template <class Func, class... Args>
  auto enqueue(Func &&func, Args &&...args) -> std::future<std::result_of_t<Func(Args...)>>;
  ~ThreadPool();

private:
  cntr::vector<std::thread> threads_;
  cntr::queue<std::function<void()>> tasks_;
  std::mutex global_mutex_;
  std::condition_variable global_cv_;
  bool closed;
};

}

#include "thread_pool.tcc"

#endif