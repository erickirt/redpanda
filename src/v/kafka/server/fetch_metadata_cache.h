/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "container/chunked_hash_map.h"
#include "model/fundamental.h"
#include "model/ktp.h"

#include <seastar/core/lowres_clock.hh>

#include <optional>

namespace kafka {

struct partition_metadata {
    model::offset start_offset;
    model::offset high_watermark;
    model::offset last_stable_offset;
};

class fetch_metadata_cache {
public:
    explicit fetch_metadata_cache()
      : _eviction_timer([this] { evict(); }) {
        _eviction_timer.arm_periodic(eviction_timeout);
    }

    fetch_metadata_cache(const fetch_metadata_cache&) = delete;
    fetch_metadata_cache(fetch_metadata_cache&&) = default;

    fetch_metadata_cache& operator=(const fetch_metadata_cache&) = delete;
    fetch_metadata_cache& operator=(fetch_metadata_cache&&) = delete;
    ~fetch_metadata_cache() = default;

    void insert_or_assign(
      model::ktp_with_hash ktp,
      model::offset start_offset,
      model::offset hw,
      model::offset lso) {
        _cache.insert_or_assign(std::move(ktp), entry(start_offset, hw, lso));
    }

    std::optional<partition_metadata> get(const model::ktp_with_hash& ktp) {
        auto it = _cache.find(ktp);
        return it != _cache.end()
                 ? std::make_optional<partition_metadata>(it->second.md)
                 : std::nullopt;
    }

    /**
     * @brief Return the number of items currently cached.
     */
    size_t size() const { return _cache.size(); }

private:
    struct entry {
        entry(model::offset start_offset, model::offset hw, model::offset lso)
          : md(start_offset, hw, lso)
          , timestamp(ss::lowres_clock::now()) {}

        partition_metadata md;
        ss::lowres_clock::time_point timestamp;
    };

    void evict() {
        const auto now = ss::lowres_clock::now();
        std::erase_if(_cache, [&now](const auto& e) {
            return (e.second.timestamp + eviction_timeout) < now;
        });
    }

    constexpr static std::chrono::seconds eviction_timeout{60};
    chunked_hash_map<model::ktp_with_hash, entry> _cache;
    ss::timer<> _eviction_timer;
};
} // namespace kafka
