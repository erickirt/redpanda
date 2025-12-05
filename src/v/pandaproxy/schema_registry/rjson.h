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

#include "json/iobuf_writer.h"
#include "pandaproxy/json/rjson_util.h"
#include "pandaproxy/schema_registry/types.h"

namespace json {

template<typename Buffer>
void rjson_serialize(
  json::iobuf_writer<Buffer>& w,
  const pandaproxy::schema_registry::schema_definition::raw_string& def) {
    w.String(def());
}

} // namespace json
