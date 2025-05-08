// The main body of this file originally comes from a CMU15-445 2024fall project1 test file.
// I've adjusted it in accord with my implementation.
#include <gtest/gtest.h>
#include "lru_k_replacer.h"

using insomnia::LruKReplacer;

TEST(LRUKReplacerTest, SampleTest) {
  size_t frame;

  // Initialize the replacer.
  LruKReplacer lru_replacer(2, 7);

  // Add six frames to the replacer. We now have frames [1, 2, 3, 4, 5]. We set frame 6 as non-evictable.
  lru_replacer.access(1);
  lru_replacer.access(2);
  lru_replacer.access(3);
  lru_replacer.access(4);
  lru_replacer.access(5);
  lru_replacer.access(6);
  lru_replacer.unpin(1);
  lru_replacer.unpin(2);
  lru_replacer.unpin(3);
  lru_replacer.unpin(4);
  lru_replacer.unpin(5);
  lru_replacer.pin(6);

  // The size of the replacer is the number of frames that can be evicted, _not_ the total number of frames entered.
  ASSERT_EQ(5, lru_replacer.free_cnt());

  // Record an access for frame 1. Now frame 1 has two accesses total.
  lru_replacer.access(1);
  // All other frames now share the maximum backward k-distance. Since we use timestamps to break ties, where the first
  // to be evicted is the frame with the oldest timestamp, the order of eviction should be [2, 3, 4, 5, 1].

  // Evict three pages from the replacer.
  // To break ties, we use LRU with respect to the oldest timestamp, or the least recently used frame.
  ASSERT_EQ(2, lru_replacer.evict());
  ASSERT_EQ(3, lru_replacer.evict());
  ASSERT_EQ(4, lru_replacer.evict());
  ASSERT_EQ(2, lru_replacer.free_cnt());
  // Now the replacer has the frames [5, 1].

  // Insert new frames [3, 4], and update the access history for 5. Now, the ordering is [3, 1, 5, 4].
  lru_replacer.access(3);
  lru_replacer.access(4);
  lru_replacer.access(5);
  lru_replacer.access(4);
  lru_replacer.unpin(3);
  lru_replacer.unpin(4);
  ASSERT_EQ(4, lru_replacer.free_cnt());

  // Look for a frame to evict. We expect frame 3 to be evicted next.
  ASSERT_EQ(3, lru_replacer.evict());
  ASSERT_EQ(3, lru_replacer.free_cnt());

  // Set 6 to be evictable. 6 Should be evicted next since it has the maximum backward k-distance.
  lru_replacer.unpin(6);
  ASSERT_EQ(4, lru_replacer.free_cnt());
  ASSERT_EQ(6, lru_replacer.evict());
  ASSERT_EQ(3, lru_replacer.free_cnt());

  // Mark frame 1 as non-evictable. We now have [5, 4].
  lru_replacer.pin(1);

  // We expect frame 5 to be evicted next.
  ASSERT_EQ(2, lru_replacer.free_cnt());
  ASSERT_EQ(5, lru_replacer.evict());
  ASSERT_EQ(1, lru_replacer.free_cnt());

  // Update the access history for frame 1 and make it evictable. Now we have [4, 1].
  lru_replacer.access(1);
  lru_replacer.access(1);
  lru_replacer.unpin(1);
  ASSERT_EQ(2, lru_replacer.free_cnt());

  // Evict the last two frames.
  ASSERT_EQ(4, lru_replacer.evict());
  ASSERT_EQ(1, lru_replacer.free_cnt());
  ASSERT_EQ(1, lru_replacer.evict());
  ASSERT_EQ(0, lru_replacer.free_cnt());

  // Insert frame 1 again and mark it as non-evictable.
  lru_replacer.access(1);
  lru_replacer.pin(1);
  ASSERT_EQ(0, lru_replacer.free_cnt());

  // A failed eviction should not change the size of the replacer.
  frame = lru_replacer.evict();
  ASSERT_EQ(lru_replacer.npos, frame);

  // Mark frame 1 as evictable again and evict it.
  lru_replacer.unpin(1);
  ASSERT_EQ(1, lru_replacer.free_cnt());
  ASSERT_EQ(1, lru_replacer.evict());
  ASSERT_EQ(0, lru_replacer.free_cnt());

  // There is nothing left in the replacer, so make sure this doesn't do something strange.
  frame = lru_replacer.evict();
  ASSERT_EQ(lru_replacer.npos, frame);
  ASSERT_EQ(0, lru_replacer.free_cnt());

  // Make sure that setting a non-existent frame as evictable or non-evictable doesn't do something strange.
  lru_replacer.pin(6);
  lru_replacer.unpin(6);
}