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
  const fs::path test_dir{"db_data"};
  const fs::path base_fname{test_dir / "bp_test"};
  const size_t frame_cnt{10}, k_dist{5}, thread_cnt{8};

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
  conc::BufferPool<str_t> bp(base_fname, k_dist, frame_cnt, thread_cnt);
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
  conc::BufferPool<str_t> bp(base_fname, k_dist, 2, thread_cnt);
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
    strcpy(p1_writer.data(), str0.c_str());
    ASSERT_EQ((std::pair<bool, size_t>{true, 1}), bp.test_get_pin_count(pid0));
    ASSERT_EQ((std::pair<bool, size_t>{true, 1}), bp.test_get_pin_count(pid1));
  }
}
