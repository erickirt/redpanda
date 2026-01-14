load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = ["//src/v/cloud_topics/level_one:__subpackages__"])

redpanda_cc_library(
    name = "domain_manager",
    hdrs = [
        "domain_manager.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cloud_topics/level_one/metastore:rpc_types",
    ],
)

redpanda_cc_library(
    name = "domain_supervisor",
    srcs = [
        "domain_supervisor.cc",
    ],
    hdrs = [
        "domain_supervisor.h",
    ],
    implementation_deps = [
        ":simple_domain_manager",
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics/level_one/common:abstract_io",
        "//src/v/cluster",
        "//src/v/model",
        "//src/v/ssx:when_all",
        "//src/v/ssx:work_queue",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":domain_manager",
        "//src/v/base",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "simple_domain_manager",
    srcs = [
        "simple_domain_manager.cc",
    ],
    hdrs = [
        "simple_domain_manager.h",
    ],
    implementation_deps = [
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics/level_one/metastore:garbage_collector",
        "//src/v/cloud_topics/level_one/metastore:rpc_types",
        "//src/v/config",
        "//src/v/container:chunked_hash_map",
        "//src/v/model:batch_builder",
        "//src/v/ssx:future_util",
        "//src/v/ssx:sleep_abortable",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":domain_manager",
        "//src/v/cloud_topics/level_one/metastore:simple",
        "//src/v/cloud_topics/level_one/metastore:simple_stm",
        "@seastar",
    ],
)
