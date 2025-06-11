/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "gtest/gtest.h"

#include "common/lang/filesystem.h"
#include "oblsm/include/ob_lsm.h"
#include "oblsm/ob_lsm_impl.h"
#include "unittest/oblsm/ob_lsm_test_base.h"
#include "oblsm/include/ob_lsm_options.h"
#include <cstdio> // Required for printf

using namespace oceanbase;

class ObLsmCompactionTest : public ObLsmTestBase {
};

bool check_compaction(ObLsm* lsm)
{
  printf("--- Starting check_compaction ---\n"); fflush(stdout);
  ObLsmImpl *lsm_impl = dynamic_cast<ObLsmImpl*>(lsm);
  if (nullptr == lsm_impl) {
    printf("[FAIL] check_compaction: lsm_impl is nullptr.\n"); fflush(stdout);
    return false;
  }
  auto sstables = lsm_impl->get_sstables();
  ObLsmOptions options; // Moved options to the top for consistent access

  printf("check_compaction: Number of levels in sstables: %zu, Expected default_levels: %zu\n",
         sstables->size(), options.default_levels); fflush(stdout);
  if (sstables->size() != options.default_levels) {
    printf("[FAIL] check_compaction: Number of levels mismatch. Actual: %zu, Expected: %zu\n",
           sstables->size(), options.default_levels); fflush(stdout);
    return false;
  }

  // It's possible sstables->size() is 0 if default_levels is 0, handle this.
  if (options.default_levels == 0) {
      printf("check_compaction: default_levels is 0, skipping further checks.\n"); fflush(stdout);
      return true; // Or specific logic if 0 levels is a valid end state for some tests
  }


  const auto& level_0 = sstables->at(0); // Level 0 files
  printf("check_compaction: Level 0 file count: %zu, Expected max default_l0_file_num: %zu\n",
         level_0.size(), options.default_l0_file_num); fflush(stdout);
  if (level_0.size() > options.default_l0_file_num) {
    printf("[FAIL] check_compaction: Level 0 file count exceeds limit. Actual: %zu, Expected max: %zu\n",
           level_0.size(), options.default_l0_file_num); fflush(stdout);
    return false;
  }

  // check level_i size
  size_t current_level_max_bytes = options.default_l1_level_size;
  for (size_t i = 1; i < options.default_levels; ++i) {
    // Ensure level i exists before trying to access it
    if (i >= sstables->size()) {
        printf("[FAIL] check_compaction: Trying to access level %zu, but sstables only has %zu levels.\n", i, sstables->size()); fflush(stdout);
        return false; // Should not happen if sstables->size() == options.default_levels and default_levels > i
    }
    const auto& level_i_files = sstables->at(i);
    long long level_i_total_bytes = 0; // Use long long to avoid overflow if sstable->size() is large
    for (const auto& sstable : level_i_files) {
      if (!sstable) {
          printf("[WARN] check_compaction: Null sstable found in level %zu.\n", i); fflush(stdout);
          continue;
      }
      level_i_total_bytes += sstable->size(); // Assuming get_file_size() returns bytes
      printf("  Level %zu, SSTable file_id=%u, file_size=%u, first_key='%.*s', last_key='%.*s'\n",
             i, sstable->sst_id(), sstable->size(),
             static_cast<int>(sstable->first_key().size()), sstable->first_key().data(),
             static_cast<int>(sstable->last_key().size()), sstable->last_key().data()); fflush(stdout);
    }
    printf("check_compaction: Level %zu total bytes: %lld, Expected max (with 10%% tolerance): %.0f\n",
           i, level_i_total_bytes, static_cast<double>(current_level_max_bytes * 1.1)); fflush(stdout);
    if (level_i_total_bytes > static_cast<long long>(current_level_max_bytes * 1.1)) {
      printf("[FAIL] check_compaction: Level %zu total bytes %lld exceeds limit %.0f\n",
             i, level_i_total_bytes, static_cast<double>(current_level_max_bytes * 1.1)); fflush(stdout);
      return false;
    }
    current_level_max_bytes *= options.default_level_ratio;
  }

  // check level_i overlap (for levels >= 1)
  for (size_t i = 1; i < options.default_levels; ++i) {
    const auto& level_files = sstables->at(i);
    if (level_files.size() <= 1) {
      continue;
    }

    // Use std::string to avoid dangling string_views
    std::vector<std::pair<std::string, std::string>> key_ranges;
    key_ranges.reserve(level_files.size()); // Optional: pre-allocate
    for (const auto& sst : level_files) {
      // sst->first_key() and sst->last_key() return std::string by value.
      // These will be copied into the pair, ensuring lifetime.
      key_ranges.emplace_back(sst->first_key(), sst->last_key());
    }

    // Sort by first key using ObDefaultComparator for user keys
    ObDefaultComparator user_key_comp;
    std::sort(key_ranges.begin(), key_ranges.end(),
              [&](const auto& a, const auto& b) {
                // Now a.first and b.first are std::string,
                // user_key_comp.compare takes string_view, implicit conversion is fine.
                return user_key_comp.compare(a.first, b.first) < 0;
              });

    // Debug: Print sorted ranges
    printf("check_compaction: Sorted key ranges for Level %zu (using DefaultComparator):\n", i); fflush(stdout);
    for (size_t k = 0; k < key_ranges.size(); ++k) {
        printf("  SSTable %zu: First='%.*s', Last='%.*s'\n",
               k,
               static_cast<int>(key_ranges[k].first.size()), key_ranges[k].first.data(),
               static_cast<int>(key_ranges[k].second.size()), key_ranges[k].second.data()); fflush(stdout);
    }

    for (size_t j = 1; j < key_ranges.size(); ++j) {
      if (user_key_comp.compare(key_ranges[j].first, key_ranges[j-1].second) <= 0) { // Overlap if current.first <= prev.last
        printf("[FAIL] check_compaction: Overlap detected in Level %zu between SSTable %zu (first='%.*s', last='%.*s') and SSTable %zu (first='%.*s', last='%.*s').\n",
               i,
               j, static_cast<int>(key_ranges[j].first.size()), key_ranges[j].first.data(),
                  static_cast<int>(key_ranges[j].second.size()), key_ranges[j].second.data(),
               j-1, static_cast<int>(key_ranges[j-1].first.size()), key_ranges[j-1].first.data(),
                    static_cast<int>(key_ranges[j-1].second.size()), key_ranges[j-1].second.data()); fflush(stdout);
        return false;
      }
    }
  }
  printf("--- check_compaction finished: PASSED ---\n"); fflush(stdout);
  return true;
}

// TEST_P(ObLsmCompactionTest, DISABLED_oblsm_compaction_test_basic1)
TEST_P(ObLsmCompactionTest, oblsm_compaction_test_basic1)
{
  size_t num_entries = GetParam();
  auto data = KeyValueGenerator::generate_data(num_entries);

  for (const auto& [key, value] : data) {
    ASSERT_EQ(db->put(key, value), RC::SUCCESS);
  }
  sleep(1);

  ObLsmIterator* it = db->new_iterator(ObLsmReadOptions());
  it->seek_to_first();
  size_t count = 0;
  while (it->valid()) {
      it->next();
      ++count;
  }
  EXPECT_EQ(count, num_entries);
  delete it;
  ASSERT_TRUE(check_compaction(db));
}

void thread_put(ObLsm *db, int start, int end) {
  for (int i = start; i < end; ++i) {
    const std::string key = "key" + std::to_string(i);
    RC rc = db->put(key, key);
    ASSERT_EQ(rc, RC::SUCCESS);
  }
}

// TEST_P(ObLsmCompactionTest, DISABLED_ConcurrentPutAndGetTest) {
TEST_P(ObLsmCompactionTest, ConcurrentPutAndGetTest) {
  const int num_entries = GetParam();
  const int num_threads = 4;
  const int batch_size = num_entries / num_threads;

  std::vector<std::thread> threads;
  for (int i = 0; i < num_threads; ++i) {
    int start = i * batch_size;
    int end = 0;
    if (i == num_threads - 1) {
      end = num_entries;
    } else {
      end = start + batch_size;
    }
    threads.emplace_back(thread_put, db, start, end);
  }

  for (auto &thread : threads) {
    thread.join();
  }
  // wait for compaction
  sleep(1);

  // Verify all data using iterator
  ObLsmReadOptions options;
  ObLsmIterator *iterator = db->new_iterator(options);

  iterator->seek_to_first();
  int count = 0;
  while (iterator->valid()) {
    iterator->next();
    ++count;
  }

  EXPECT_EQ(count, num_entries);

  // Clean up
  delete iterator;

  ASSERT_TRUE(check_compaction(db));
}

INSTANTIATE_TEST_SUITE_P(
    ObLsmCompactionTests,
    ObLsmCompactionTest,
    // ::testing::Values(1, 10, 1000, 10000, 100000)
    ::testing::Values(100000)
);

int main(int argc, char **argv)
{
  LOG_PANIC("Logger test: Application is starting up!");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}