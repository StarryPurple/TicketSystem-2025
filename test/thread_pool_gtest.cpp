#include <iostream>
#include <gtest/gtest.h>
#include "thread_pool.h"

namespace ism = insomnia;
namespace insomnia {
namespace conc = concurrent;
namespace cntr = container;
}

TEST(ThreadPoolEnqueue, ThreadPoolTest) {
  ism::conc::ThreadPool pool(4);

  auto result1 = pool.enqueue([](int answer) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
      return answer;
  }, 42);

  auto result2 = pool.enqueue([]() {
      std::this_thread::sleep_for(std::chrono::seconds(2));
      return std::string("Hello, World!");
  });
  ASSERT_EQ(result1.get(), 42);
  ASSERT_EQ(result2.get(), "Hello, World!");
}