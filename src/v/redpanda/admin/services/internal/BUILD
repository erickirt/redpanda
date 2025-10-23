load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = [
    "//src/v/redpanda:__subpackages__",
])

redpanda_cc_library(
    name = "debug",
    srcs = ["debug.cc"],
    hdrs = ["debug.h"],
    deps = [
        "//proto/redpanda/core/admin/v2/internal:debug_redpanda_proto",
        "//src/v/base",
        "//src/v/finjector",
        "//src/v/redpanda/admin/proxy:client",
        "//src/v/serde/protobuf:rpc",
        "//src/v/utils:to_string",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "breakglass",
    srcs = ["breakglass.cc"],
    hdrs = ["breakglass.h"],
    deps = [
        "//proto/redpanda/core/admin/v2/internal:breakglass_redpanda_proto",
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/finjector",
        "//src/v/model",
        "//src/v/redpanda/admin/proxy:client",
        "//src/v/serde/protobuf:rpc",
        "//src/v/utils:to_string",
        "@fmt",
        "@seastar",
    ],
)
