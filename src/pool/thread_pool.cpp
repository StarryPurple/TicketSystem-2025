#include "thread_pool.h"

namespace insomnia::concurrent {

ThreadPool::ThreadPool(size_t thread_count) : closed(false) {
  for(size_t i = 0; i < thread_count; ++i)
    threads_.emplace_back([this] {
      while(true) {
        std::function<void()> task;
        {
          std::unique_lock lock(this->global_mutex_);
          this->global_cv_.wait(lock,
            [this] { return this->closed || !this->tasks_.empty(); });
          if(this->closed && this->tasks_.empty()) return;
          if(this->tasks_.empty()) continue;
          task = std::move(tasks_.front());
          tasks_.pop();
        }
        task();
      }
    });
}

ThreadPool::~ThreadPool() {
  {
    std::unique_lock lock(global_mutex_);
    closed = true;
  }
  global_cv_.notify_all();
  for(auto &thread : threads_)
    thread.join();
}



}