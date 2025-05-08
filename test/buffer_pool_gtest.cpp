// The main body of this file originally comes from a CMU15-445 2024fall project1 test file.
// I've adjusted it in accord with my implementation.
// The expected action of get_reader/get_writer varies in tests.
// Check what they require (exception, invalid result, infinite waiting)
// and change the functions before use.
#include <gtest/gtest.h>
#include "buffer_pool.h"
#include "array.h"

using namespace insomnia;
namespace fs = std::filesystem;

class BufferPoolTestFixture : public ::testing::Test {
protected:
  using str_t = array<char, 60>;
  using BufferPool = BufferPool<str_t>;
  using Writer = BufferPool::Writer;
  using Reader = BufferPool::Reader;
  const fs::path test_dir{"db_data"};
  const fs::path base_fname{test_dir / "bp_test"};
  const size_t frame_cnt{10}, k_dist{5}, thread_cnt{4};

  void SetUp() override {

    // Create clean test directory
    fs::remove_all(test_dir);  // Clear any previous runs
    fs::create_directories(test_dir);

    // Create initial empty file if needed by BufferPool
    std::ofstream(base_fname).close();
  }

  void TearDown() override {
    // Remove EVERYTHING in the test directory
    fs::remove_all(test_dir);
  }
};

TEST_F(BufferPoolTestFixture, BasicTest) {
  BufferPool bp(base_fname, k_dist, frame_cnt, thread_cnt);
  auto pid = bp.alloc();
  str_t str = "Hello, world!";
  {
    auto writer = bp.get_writer(pid);
    strcpy(writer.data(), str.c_str());
    EXPECT_STREQ(writer.data(), str.c_str());  // Use EXPECT_STREQ for C strings
  }
  {
    const auto reader = bp.get_reader(pid);
    EXPECT_STREQ(reader.data(), str.c_str());
  }
  {
    const auto reader = bp.get_reader(pid);
    EXPECT_STREQ(reader.data(), str.c_str());
  }
  ASSERT_TRUE(bp.dealloc(pid));
}

TEST_F(BufferPoolTestFixture, PagePinEasyTest) {
  BufferPool bp(base_fname, k_dist, 2, thread_cnt);
  const auto pid0 = bp.alloc();
  const auto pid1 = bp.alloc();

  const str_t str0 = "page0";
  const str_t str1 = "page1";
  const str_t str0updated = "page0updated";
  const str_t str1updated = "page1updated";

  {
    auto p0_writer = bp.get_writer(pid0);
    strcpy(p0_writer.data(), str0.c_str());
    auto p1_writer = bp.get_writer(pid1);
    strcpy(p1_writer.data(), str1.c_str());
    ASSERT_EQ(1, bp.get_pin_count(pid0));
    ASSERT_EQ(1, bp.get_pin_count(pid1));
    auto pid_t1 = bp.alloc();
    ASSERT_THROW(bp.get_reader(pid_t1), conc::pool_overflow);
    auto pid_t2 = bp.alloc();
    ASSERT_THROW(bp.get_writer(pid_t2), conc::pool_overflow);
    ASSERT_EQ(1, bp.get_pin_count(pid0));
    p0_writer.drop();
    ASSERT_EQ(0, bp.get_pin_count(pid0));
    ASSERT_EQ(1, bp.get_pin_count(pid1));
    p1_writer.drop();
    ASSERT_EQ(0, bp.get_pin_count(pid0));
    ASSERT_EQ(0, bp.get_pin_count(pid1));
  }
  {
    auto pidt1 = bp.alloc();
    auto writer1 = bp.get_reader(pidt1);

    auto pidt2 = bp.alloc();
    auto writer2 = bp.get_writer(pidt2);

    // failed to get returns SIZE_MAX
    ASSERT_EQ(SIZE_MAX, bp.get_pin_count(pid0));
    ASSERT_EQ(SIZE_MAX, bp.get_pin_count(pid1));
  }
  {
    auto writer0 = bp.get_writer(pid0);
    EXPECT_STREQ(writer0.data(), str0.c_str());
    strcpy(writer0.data(), str0updated.c_str());

    auto writer1 = bp.get_writer(pid1);
    EXPECT_STREQ(writer1.data(), str1.c_str());
    strcpy(writer1.data(), str1updated.c_str());

    ASSERT_EQ(1, bp.get_pin_count(pid0));
    ASSERT_EQ(1, bp.get_pin_count(pid1));
  }
  ASSERT_EQ(0, bp.get_pin_count(pid0));
  ASSERT_EQ(0, bp.get_pin_count(pid1));
  {
    auto reader0 = bp.get_reader(pid0);
    EXPECT_STREQ(reader0.data(), str0updated.c_str());

    auto reader1 = bp.get_reader(pid1);
    EXPECT_STREQ(reader1.data(), str1updated.c_str());

    ASSERT_EQ(1, bp.get_pin_count(pid0));
    ASSERT_EQ(1, bp.get_pin_count(pid1));
  }
  ASSERT_EQ(0, bp.get_pin_count(pid0));
  ASSERT_EQ(0, bp.get_pin_count(pid1));
}

TEST_F(BufferPoolTestFixture, PagePinMediumTest) {
  BufferPool bp(base_fname, k_dist, frame_cnt, thread_cnt);
  auto pid0 = bp.alloc();
  auto writer0 = bp.get_writer(pid0);

  str_t hello = "Hello";
  strcpy(writer0.data(), hello.c_str());
  EXPECT_STREQ(writer0.data(), hello.c_str());

  writer0.drop();

  cntr::vector<Writer> writers;

  for(size_t i = 0; i < frame_cnt; ++i) {
    auto pid = bp.alloc();
    auto writer = bp.get_writer(pid);
    writers.push_back(std::move(writer));
  }

  for(const auto &writer : writers) {
    auto pid = writer.id();
    EXPECT_EQ(1, bp.get_pin_count(pid));
  }

  for(size_t i = 0; i < frame_cnt; ++i) {
    auto pid = bp.alloc();
    ASSERT_THROW(bp.get_writer(pid), conc::pool_overflow);
  }

  for(size_t i = 0; i < frame_cnt / 2; ++i) {
    auto pid = writers.front().id();
    EXPECT_EQ(1, bp.get_pin_count(pid));
    writers.erase(writers.begin());
    EXPECT_EQ(0, bp.get_pin_count(pid));
  }

  for(const auto &writer : writers) {
    auto pid = writer.id();
    EXPECT_EQ(1, bp.get_pin_count(pid));
  }

  for(size_t i = 0; i < frame_cnt / 2 - 1; ++i) {
    auto pid = bp.alloc();
    auto page = bp.get_writer(pid);
    writers.push_back(std::move(page));
  }

  {
    auto p_ori = bp.get_reader(pid0);
    EXPECT_STREQ(p_ori.data(), hello.c_str());
  }

  auto pid_last = bp.alloc();
  auto reader_last = bp.get_reader(pid_last);
  EXPECT_THROW(bp.get_reader(pid0), conc::pool_overflow);
}

TEST_F(BufferPoolTestFixture, PageAccessTest) {
  const size_t rounds = 50;
  BufferPool bp(base_fname, k_dist, 1, thread_cnt);

  auto pid = bp.alloc();
  char buf[4096];
  std::thread thread([&] {
    for(size_t i = 0; i < rounds; ++i) {
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
      auto writer = bp.get_writer(pid);
      strcpy(writer.data(), std::to_string(i).c_str());
    }
  });

  for(size_t i = 0; i < rounds; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    auto writer = bp.get_reader(pid);
    memcpy(buf, writer.data(), 4096);
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    EXPECT_STREQ(writer.data(), buf);
  }
  thread.join();
}

TEST_F(BufferPoolTestFixture, ContentionTest) {
  BufferPool bp(base_fname, k_dist, frame_cnt, thread_cnt);
  const size_t rounds = 100000;
  auto pid = bp.alloc();
  auto workload = [&] {
    for(size_t i = 0; i < rounds; ++i) {
      auto writer = bp.get_writer(pid);
      strcpy(writer.data(), std::to_string(i).c_str());
    }
  };
  std::thread thread1(workload), thread2(workload), thread3(workload), thread4(workload);

  thread3.join();
  thread2.join();
  thread4.join();
  thread1.join();
}

TEST_F(BufferPoolTestFixture, DeadlockTest) {
  BufferPool bp(base_fname, k_dist, frame_cnt, thread_cnt);
  auto pid0 = bp.alloc();
  auto pid1 = bp.alloc();
  auto writer0 = bp.get_writer(pid0);
  std::atomic<bool> start(false);
  std::thread child([&] {
    start.store(true);
    auto writer_t0 = bp.get_writer(pid0);
  });
  while(!start.load()) {}
  std::this_thread::sleep_for(std::chrono::seconds(1));
  auto writer1 = bp.get_writer(pid1);
  writer0.drop();
  child.join();
}

TEST_F(BufferPoolTestFixture, EvictableTest) {
  const size_t rounds = 100, num_readers = 8;
  BufferPool bp(base_fname, k_dist, 1, thread_cnt);
  for(size_t i = 0; i < rounds; ++i) {
    std::mutex mutex;
    std::condition_variable cv;
    bool signal = false;
    auto winner_pid = bp.alloc();
    auto loser_pid = bp.alloc();
    cntr::vector<std::thread> readers;
    for(size_t j = 0; j < num_readers; ++j) {
      readers.emplace_back([&] {
        std::unique_lock lock(mutex);
        cv.wait(lock, [&signal] { return signal; });
        auto reader = bp.get_reader(winner_pid);
        ASSERT_THROW(bp.get_reader(loser_pid), conc::pool_overflow);
      });
    }
    std::unique_lock lock(mutex);
    if(i % 2 == 0) {
      auto reader = bp.get_reader(winner_pid);
      signal = true;
      cv.notify_all();
      lock.unlock();
      reader.drop();
    } else {
      auto writer = bp.get_writer(winner_pid);
      signal = true;
      cv.notify_all();
      lock.unlock();
      writer.drop();
    }
    for(auto &reader : readers)
      reader.join();
  }
}

TEST_F(BufferPoolTestFixture, ConcurrentReaderWriterTest) {
  const size_t num_threads = 9, num_pages = 1000;
  BufferPool bp(base_fname, k_dist, frame_cnt, thread_cnt);
  cntr::vector<size_t> pid_list;
  cntr::vector<str_t> data(num_pages);
  for(size_t i = 0; i < num_pages; ++i) {
    auto pid = bp.alloc();
    pid_list.emplace_back(pid);
    data[pid] = "Page " + std::to_string(i);
    auto writer = bp.get_writer(pid);
    strcpy(writer.data(), data[pid].c_str());
  }
  std::mutex io_latch;
  cntr::vector<std::thread> threads;
  for(size_t tid = 0; tid < num_threads; ++tid)
    threads.emplace_back([&] {
      for(size_t iter = 0; iter < num_pages; ++iter) {
        auto pid = pid_list[iter];
        if(iter % 2 == 0) {
          Reader reader = bp.get_reader(pid);
          ASSERT_EQ(pid, reader.id());
          auto ans = data[pid].c_str();
          auto out = reader.data();
          if(strcmp(ans, out) != 0) {
            std::unique_lock lock(io_latch);
            std::cerr << "ans is" << ans << ", out is " << out << std::endl;
          }
        } else {
          Writer writer = bp.get_writer(pid);
          ASSERT_EQ(pid, writer.id());
          auto ans = data[pid].c_str();
          auto out = writer.data();
          if(strcmp(ans, out) != 0) {
            std::unique_lock lock(io_latch);
            std::cerr << "ans is" << ans << ", out is " << out << std::endl;
          }
        }
      }
    });
  for(auto &thread : threads)
    thread.join();
}

struct T {
  char a[10];
  T(const char _a[10]) { memcpy(a, _a, 10); }
  bool operator==(const T &other) const { return memcmp(a, other.a, 10) == 0; }
};

struct U : T {
  char b[10];
  U(const char _a[10], const char _b[10]) : T(_a) { memcpy(b, _b, 10); }
  bool operator==(const U &other) const {
    return memcmp(a, other.a, 10) == 0 && memcmp(b, other.b, 10) == 0;
  }
};

TEST_F(BufferPoolTestFixture, CutOffTest) {
  size_t pid1, pid2;
  T t("t"), out_t("");
  U u("uu", "vvv"), out_u("", "");

  {
    conc::BufferPool<T, sizeof(U)> bp(base_fname, k_dist, frame_cnt, thread_cnt);
    pid1 = bp.alloc();
    auto writer1 = bp.get_writer(pid1);
    writer1.write(u);
    pid2 = bp.alloc();
    auto writer2 = bp.get_writer(pid2);
    writer2.write(t);
  }
  {
    conc::BufferPool<T, sizeof(U)> bp(base_fname, k_dist, frame_cnt, thread_cnt);
    auto reader1 = bp.get_reader(pid1);
    reader1.read(out_u);
    ASSERT_EQ(u, out_u);
    auto reader2 = bp.get_reader(pid2);
    reader2.read(out_t);
    ASSERT_EQ(t, out_t);
  }
}




