#ifndef INSOMNIA_TASK_SCHEDULER_H
#define INSOMNIA_TASK_SCHEDULER_H

#include <functional>
#include <future>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <atomic>

#include "vector.h"
#include "queue.h"
#include "thread_pool.h"

namespace insomnia {
/**
 * @brief thread pool that supports id-queue.
 */
class TaskScheduler {
public:
  explicit TaskScheduler(size_t thread_num = 16);
  ~TaskScheduler();
  TaskScheduler(const TaskScheduler&) = delete;
  TaskScheduler(TaskScheduler&&) = delete;
  TaskScheduler& operator=(const TaskScheduler&) = delete;
  TaskScheduler& operator=(TaskScheduler&&) = delete;

  template <class F, class ...Args>
  auto schedule(size_t id, F &&f, Args &&...args) -> std::future<std::result_of_t<F(Args...)>>;
  // No more task enqueue is allowed.
  // The existing tasks will go on executing and finish earlier than the destruction of TaskScheduler.
  void close_queue();
  // close queues and wait for all tasks to be finished.
  void close();

private:
  struct TaskQueue {
    queue<std::function<void()>> task_queue;
    std::mutex latch;
    std::condition_variable cv;
  };
  void loop(TaskQueue &queue);

  vector<TaskQueue> task_queues_;
  vector<std::thread> workers_;
  std::atomic<bool> closed;
};


}


#include "task_scheduler.tcc"

#endif