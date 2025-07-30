/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/seastarx.h"
#include "container/fragmented_vector.h"

#include <seastar/core/sstring.hh>

namespace serde::pb {

struct field_mask {
    chunked_vector<ss::sstring> paths;

    bool operator==(const field_mask& other) const = default;
};

} // namespace serde::pb
