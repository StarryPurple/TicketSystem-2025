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

namespace insomnia::concurrent {
/**
 * @brief thread pool specified for id-queue and read/write relationship.
 */
class TaskScheduler {
  static constexpr auto GROUP_NUM = 16;
  static constexpr auto MAX_ACTIVE_TASK_COUNT = 3 * std::thread::hardware_concurrency();
  struct TaskGroup {
    std::mutex latch;
    std::condition_variable cv;
    cntr::queue<std::function<void()>> readers, writers;
    std::atomic<size_t> last_write_id{0}, next_write_id{1};
    bool is_writing{false}; // might be false positive
  };
public:
  explicit TaskScheduler(size_t thread_num = std::thread::hardware_concurrency());
  ~TaskScheduler() { stop(); }
  TaskScheduler(const TaskScheduler&) = delete;
  TaskScheduler(TaskScheduler&&) = delete;
  TaskScheduler& operator=(const TaskScheduler&) = delete;
  TaskScheduler& operator=(TaskScheduler&&) = delete;

  template <class F, class ...Args>
  auto schedule(size_t id, bool is_write, F &&f, Args &&...args) -> std::future<std::result_of_t<F(Args...)>>;
  void stop();

private:

  void work();
  bool process_group(TaskGroup &group);

  static void atomic_max(std::atomic<size_t> &cnt, size_t val);

  std::atomic<bool> stopped_;
  cntr::vector<std::thread> workers_; // handle write requests
  TaskGroup groups_[GROUP_NUM];
  std::atomic<int> last_group_idx_{0};
  ThreadPool reader_pool_;
};


}


#include "task_scheduler.tcc"

#endif