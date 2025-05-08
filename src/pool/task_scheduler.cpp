#include "task_scheduler.h"

namespace insomnia {

TaskScheduler::TaskScheduler(size_t thread_num) : closed(false), task_queues_(thread_num) {
  workers_.reserve(thread_num);
  for(size_t i = 0; i < thread_num; ++i) {
    workers_.emplace_back([this, i] { loop(task_queues_[i]); });
  }
}

TaskScheduler::~TaskScheduler() {
  close();
}

void TaskScheduler::loop(TaskQueue &queue) {
  while(true) {
    bool has_task = false;
    std::function<void()> task;
    {
      std::unique_lock lock(queue.latch);
      queue.cv.wait(lock, [this, &queue] { return closed.load() || !queue.task_queue.empty(); });
      if(!queue.task_queue.empty()) {
        task = std::move(queue.task_queue.front());
        queue.task_queue.pop();
        has_task = true;
      }
      if(!has_task && closed.load())
        return;
    }
    if(has_task)
      task();
  }
}

void TaskScheduler::close_queue() {
  if(closed.exchange(true)) return;
  for(auto &queue : task_queues_)
    queue.cv.notify_all();
}

void TaskScheduler::close() {
  if(closed.exchange(true)) return;
  for(auto &queue : task_queues_)
    queue.cv.notify_all();
  for(auto &worker : workers_)
    worker.join();
}

}