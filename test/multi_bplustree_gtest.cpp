#include <gtest/gtest.h>

#include "array.h"
#include "bplustree.h"


using namespace ism;
namespace fs = std::filesystem;

class MultiBptFixture : public ::testing::Test {
protected:
  using str_t = cntr::array<char, 64>;
  using MultiBpt = database::MultiBPlusTree<str_t, int>;
  const fs::path test_dir{"db_data"};
  const fs::path base_fname{test_dir / "multi_bpt_test"};
  const size_t buffer_capa{10}, k_dist{5}, thread_cnt{4};

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

TEST_F(MultiBptFixture, ExampleTest) {
  MultiBpt bpt(base_fname, k_dist, buffer_capa, thread_cnt);
  bpt.insert("FlowersForAlgernon", 1966);
  bpt.insert("CppPrimer", 2012);
  bpt.insert("Dune", 2021);
  bpt.insert("CppPrimer", 2001);
  auto list1 = bpt.search("CppPrimer");
  ASSERT_EQ(list1.size(), 2);
  ASSERT_EQ(list1[0], 2001);
  ASSERT_EQ(list1[1], 2012);
  auto list2 = bpt.search("Java");
  ASSERT_EQ(list2.size(), 0);
}

TEST_F(MultiBptFixture, MassInsertTest) {
  MultiBpt bpt(base_fname, k_dist, buffer_capa, thread_cnt);
  const int range = 1000;
  for(int i = 1; i <= range; ++i)
    bpt.insert(std::to_string(i), i);
  for(int i = 1; i <= range; ++i) {
    auto list = bpt.search(std::to_string(i));
    ASSERT_EQ(list.size(), 1);
    ASSERT_EQ(list[0], i);
  }
  for(int i = 1; i <= range; ++i)
    bpt.insert("0", i);
  auto list = bpt.search("0");
  ASSERT_EQ(list.size(), range);
}