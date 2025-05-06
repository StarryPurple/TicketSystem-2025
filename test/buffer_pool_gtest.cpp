// The main body of this file originally comes from a CMU15-445 2024fall project1 test file.
// I've adjusted it in accord with my implementation.
#include <gtest/gtest.h>
#include "buffer_pool.h"
#include "array.h"

using namespace ism;
namespace fs = std::filesystem;

class BufferPoolTestFixture : public ::testing::Test {
protected:
  using str_t = cntr::array<char, 60>;
  using BufferPool = concurrent::BufferPool<str_t>;
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
  auto pid = bp.allocate();
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
  ASSERT_TRUE(bp.deallocate(pid));
}

TEST_F(BufferPoolTestFixture, PagePinEasyTest) {
  BufferPool bp(base_fname, k_dist, 2, thread_cnt);
  const auto pid0 = bp.allocate();
  const auto pid1 = bp.allocate();

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
    auto pid_t1 = bp.allocate();
    ASSERT_THROW(bp.get_reader(pid_t1), conc::pool_overflow);
    auto pid_t2 = bp.allocate();
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
    auto pidt1 = bp.allocate();
    auto writer1 = bp.get_reader(pidt1);

    auto pidt2 = bp.allocate();
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
  BufferPool bp(base_fname, k_dist, frame_cnt);
  auto pid0 = bp.allocate();
  auto writer0 = bp.get_writer(pid0);

  str_t hello = "Hello";
  strcpy(writer0.data(), hello.c_str());
  EXPECT_STREQ(writer0.data(), hello.c_str());

  writer0.drop();

  cntr::vector<Writer> writers;

  for(size_t i = 0; i < frame_cnt; ++i) {
    auto pid = bp.allocate();
    auto writer = bp.get_writer(pid);
    writers.push_back(std::move(writer));
  }

  for(const auto &writer : writers) {
    auto pid = writer.page_id();
    EXPECT_EQ(1, bp.get_pin_count(pid));
  }

  for(size_t i = 0; i < frame_cnt; ++i) {
    auto pid = bp.allocate();
    ASSERT_THROW(bp.get_writer(pid), conc::pool_overflow);
  }

  for(size_t i = 0; i < frame_cnt / 2; ++i) {
    auto pid = writers.front().page_id();
    EXPECT_EQ(1, bp.get_pin_count(pid));
    writers.erase(writers.begin());
    EXPECT_EQ(0, bp.get_pin_count(pid));
  }

  for(const auto &writer : writers) {
    auto pid = writer.page_id();
    EXPECT_EQ(1, bp.get_pin_count(pid));
  }

  for(size_t i = 0; i < frame_cnt / 2 - 1; ++i) {
    auto pid = bp.allocate();
    auto page = bp.get_writer(pid);
    writers.push_back(std::move(page));
  }

  {
    auto p_ori = bp.get_reader(pid0);
    EXPECT_STREQ(p_ori.data(), hello.c_str());
  }

  auto pid_last = bp.allocate();
  auto reader_last = bp.get_reader(pid_last);
  EXPECT_THROW(bp.get_reader(pid0), conc::pool_overflow);
}















