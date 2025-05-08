#ifndef INSOMNIA_TASK_SCHEDULER_TCC
#define INSOMNIA_TASK_SCHEDULER_TCC

#include "task_scheduler.h"

namespace insomnia {

template <class F, class... Args>
auto TaskScheduler::schedule(size_t id, F &&f, Args &&... args)
-> std::future<std::result_of_t<F(Args...)>> {
  if(closed) throw invalid_pool("Task scheduler queue has closed.");
  using result_t = std::result_of_t<F(Args...)>;
  auto *task = new std::packaged_task<result_t()>(
    std::bind(std::forward<F>(f), std::forward<Args>(args)...));
  auto future = task->get_future();
  int idx = static_cast<int>(id % task_queues_.size());
  auto &queue = task_queues_[idx];
  {
    std::unique_lock lock(queue.latch);
    queue.task_queue.emplace([task] { (*task)(); delete task; });
  }
  queue.cv.notify_one();
  return future;
}



}

#endif