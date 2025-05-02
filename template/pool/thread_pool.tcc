#ifndef INSOMNIA_THREAD_POOL_TCC
#define INSOMNIA_THREAD_POOL_TCC

#include "thread_pool.h"

namespace insomnia::concurrent {

template <class Func, class... Args>
auto ThreadPool::enqueue(Func &&func, Args &&... args) -> std::future<std::result_of_t<Func(Args...)>> {
  using result_t = std::result_of_t<Func(Args...)>;
  auto *task = new std::packaged_task<result_t()>(
    std::bind(std::forward<Func>(func), std::forward<Args>(args)...));
  auto future = task->get_future();
  {
    std::unique_lock lock(global_mutex_);
    if(closed) { throw invalid_pool(); }
    tasks_.emplace([task] { (*task)(); delete task; });
  }
  global_cv_.notify_one();
  return future;
}


}

#endif