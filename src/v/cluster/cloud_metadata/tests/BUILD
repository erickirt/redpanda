load("//bazel:test.bzl", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "manual_mixin",
    hdrs = [
        "manual_mixin.h",
    ],
    implementation_deps = [
    ],
    include_prefix = "cluster/cloud_metadata/tests",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/test_utils:scoped_config",
    ],
)

redpanda_cc_gtest(
    name = "cluster_manifest_test",
    timeout = "short",
    srcs = [
        "cluster_manifest_test.cc",
    ],
    deps = [
        "//src/v/bytes:iostream",
        "//src/v/cluster",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)
