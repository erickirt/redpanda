/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "cloud_topics/level_one/maintenance/keyed_priority_queue.h"
#include "cloud_topics/level_one/maintenance/meta.h"
#include "model/fundamental.h"

namespace cloud_topics::l1 {

/// \brief A scheduling queue of CTPs awaiting compaction, ordered best-first by
/// a caller-supplied `cmp_t`.
///
/// Keyed on `topic_id_partition`, so each CTP has at most one entry: re-queuing
/// a CTP updates its position in place rather than appending a duplicate, and
/// `clear(tidp)` evicts a CTP's entry when it is unmanaged.
class compaction_queue {
public:
    explicit compaction_queue(compaction_cmp_t cmp)
      : _queue(std::move(cmp)) {}

    /// Enqueue `meta`, or update its position in place if its CTP is already
    /// queued. The owning CTP is taken from the meta.
    void push(log_compaction_meta_ptr meta) {
        const auto tidp = meta->tidp;
        _queue.upsert(tidp, std::move(meta));
    }

    /// The highest-priority queued log. Precondition: non-empty.
    const log_compaction_meta_ptr& top() const { return _queue.top().second; }

    /// Remove the highest-priority queued log. Precondition: non-empty.
    void pop() {
        auto tidp = _queue.top().first;
        _queue.erase(tidp);
    }

    /// Drop the queued entry for `tidp`, if any (e.g. when a CTP is unmanaged).
    /// No-op if the CTP has nothing queued.
    void clear(const model::topic_id_partition& tidp) {
        if (_queue.contains(tidp)) {
            _queue.erase(tidp);
        }
    }

    bool empty() const { return _queue.empty(); }
    size_t size() const { return _queue.size(); }
    bool contains(const model::topic_id_partition& tidp) const {
        return _queue.contains(tidp);
    }

private:
    keyed_priority_queue<
      model::topic_id_partition,
      log_compaction_meta_ptr,
      compaction_cmp_t>
      _queue;
};

} // namespace cloud_topics::l1
