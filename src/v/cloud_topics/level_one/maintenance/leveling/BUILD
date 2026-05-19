load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = ["//src/v/cloud_topics/level_one:__subpackages__"])

redpanda_cc_library(
    name = "leveling_source",
    srcs = [
        "leveling_source.cc",
    ],
    hdrs = [
        "leveling_source.h",
    ],
    include_prefix = "cloud_topics/level_one/maintenance/leveling",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cloud_topics:log_reader_config",
        "//src/v/cloud_topics/level_one/common:abstract_io",
        "//src/v/cloud_topics/level_one/frontend_reader:reader",
        "//src/v/cloud_topics/level_one/maintenance:meta",
        "//src/v/cloud_topics/level_one/maintenance:worker_probe",
        "//src/v/cloud_topics/level_one/metastore",
        "//src/v/cloud_topics/level_one/metastore:leveling_range_builder",
        "//src/v/compaction:reducer",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "//src/v/utils:prefix_logger",
        "@seastar",
    ],
)
