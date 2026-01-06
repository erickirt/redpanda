#include "cloud_storage_clients/util.h"

#include <gtest/gtest.h>

TEST(Util, AllPathsToFile) {
    using namespace cloud_storage_clients;

    auto result1 = util::all_paths_to_file(object_key{"a/b/c/log.txt"});
    auto expected1 = std::vector<object_key>{
      object_key{"a"},
      object_key{"a/b"},
      object_key{"a/b/c"},
      object_key{"a/b/c/log.txt"}};
    EXPECT_EQ(result1, expected1);

    auto result2 = util::all_paths_to_file(object_key{"a/b/c/"});
    EXPECT_EQ(result2, std::vector<object_key>{});

    auto result3 = util::all_paths_to_file(object_key{""});
    EXPECT_EQ(result3, std::vector<object_key>{});

    auto result4 = util::all_paths_to_file(object_key{"foo"});
    EXPECT_EQ(result4, std::vector<object_key>{object_key{"foo"}});
}
