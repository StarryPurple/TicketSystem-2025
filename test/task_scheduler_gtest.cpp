#include <iostream>
#include <gtest/gtest.h>
#include "thread_pool.h"
#include "task_scheduler.h"

using ism::conc::TaskScheduler;

TEST(TaskSchedulerTest, SequentialExecution) {
  TaskScheduler pool(4);
  std::vector<int> execution_order;
  std::mutex order_mutex;

  const size_t target_id = 123; // 固定ID
  for (int i = 0; i < 1000; ++i) {
    pool.schedule(target_id, [i, &execution_order, &order_mutex] {
        std::lock_guard lock(order_mutex);
        execution_order.push_back(i);
    });
  }

  pool.close();
  ASSERT_EQ(execution_order.size(), 1000);
  for (int i = 0; i < 1000; ++i) {
    EXPECT_EQ(execution_order[i], i); // 必须严格按0-999顺序执行
  }
}

TEST(TaskSchedulerTest, ThreadSafety) {
  TaskScheduler pool(8);
  std::atomic<int> counter(0);
  constexpr int total_tasks = 10000;

  // 多个ID并行递增计数器
  for (int i = 0; i < total_tasks; ++i) {
    pool.schedule(i % 16, [&counter] { ++counter; });
  }

  pool.close();
  EXPECT_EQ(counter.load(), total_tasks); // 必须无丢失更新
}

TEST(TaskSchedulerTest, HighLoad) {
  TaskScheduler pool(std::thread::hardware_concurrency());
  std::atomic<int> completed(0);
  constexpr int total_tasks = 100000;

  auto heavy_task = [&completed] {
    volatile double x = 1.0;
    for (int i = 0; i < 1000; ++i) x *= 1.001; // 模拟计算密集型任务
    completed++;
  };

  for (int i = 0; i < total_tasks; ++i) {
    pool.schedule(i % 32, heavy_task); // 分散到多个队列
  }

  pool.close();
  EXPECT_EQ(completed.load(), total_tasks);
}

TEST(TaskSchedulerTest, TaskDependency) {
  TaskScheduler pool(4);
  std::future<int> final_result;

  // 链式任务：A -> B -> C，必须按序执行
  final_result = pool.schedule(1, [] { return 1; });
  final_result = pool.schedule(1, [](int x) { return x + 2; }, final_result.get());
  final_result = pool.schedule(1, [](int x) { return x * 3; }, final_result.get());

  pool.close();
  EXPECT_EQ(final_result.get(), 9); // (1 + 2) * 3 = 9
}


TEST(TaskSchedulerBenchmark, Throughput) {
  TaskScheduler pool(std::thread::hardware_concurrency());
  constexpr int tasks = 100000;
  auto start = std::chrono::high_resolution_clock::now();

  for (int i = 0; i < tasks; ++i) {
    pool.schedule(i % 16, [] { /* 空任务测试调度开销 */ });
  }

  pool.close();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::high_resolution_clock::now() - start
  );
  std::cout << "ThreadPool throughput: " << tasks / duration.count() << "K tasks/sec\n";
}