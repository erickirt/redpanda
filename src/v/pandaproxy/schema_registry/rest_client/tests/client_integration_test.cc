/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "bytes/iobuf.h"
#include "http/client.h"
#include "pandaproxy/schema_registry/rest_client/client.h"
#include "pandaproxy/schema_registry/rest_client/error.h"
#include "pandaproxy/schema_registry/types.h"
#include "pandaproxy/test/pandaproxy_fixture.h"
#include "pandaproxy/test/utils.h"
#include "test_utils/boost_fixture.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>

#include <boost/beast/http/status.hpp>
#include <boost/beast/http/verb.hpp>
#include <boost/test/tools/old/interface.hpp>

#include <algorithm>
#include <chrono>
#include <memory>
#include <string_view>
#include <variant>

namespace rc = pandaproxy::schema_registry::rest_client;
namespace pps = pandaproxy::schema_registry;
namespace bh = boost::beast::http;
using namespace std::chrono_literals;

namespace {

// A backward-compatible Avro record evolution: v2 adds a field with a default,
// so it registers under the default (backward) compatibility after v1.
constexpr std::string_view schema_v1
  = R"({"schema": "{\"type\": \"record\", \"name\": \"r1\", \"fields\": [{\"name\": \"f1\", \"type\": \"string\"}]}", "schemaType": "AVRO"})";
constexpr std::string_view schema_v2
  = R"({"schema": "{\"type\": \"record\", \"name\": \"r1\", \"fields\": [{\"name\": \"f1\", \"type\": \"string\"}, {\"name\": \"f2\", \"type\": \"string\", \"default\": \"\"}]}", "schemaType": "AVRO"})";

rc::client make_rest_client(uint16_t port) {
    net::base_transport::configuration cfg;
    cfg.server_addr = net::unresolved_address{"localhost", port};
    return rc::client{
      std::make_unique<http::client>(cfg),
      fmt::format("http://localhost:{}", port),
      std::nullopt,
      pps::qualified_subjects_enabled::yes};
}

// POST a schema under subject_path (the wire form, raw) via the seed client.
void register_schema(
  http::client& client, std::string_view subject_path, std::string_view body) {
    auto res = http_request(
      client,
      fmt::format("/subjects/{}/versions", subject_path),
      iobuf::from(body),
      bh::verb::post,
      serialization_format::schema_registry_v1_json,
      serialization_format::schema_registry_v1_json);
    BOOST_REQUIRE_EQUAL(res.headers.result(), bh::status::ok);
}

} // namespace

// Drives the rest_client against the in-tree Schema Registry server: seeds
// schemas over the real REST API, then exercises all three read calls plus the
// real not-found (40401/40402) responses through a real http::client. This is
// the fidelity counterpart to the mock-based client_test — it proves the wire
// shape, qualified-subject %3A path encoding, and error-code classification
// against actual server responses. (References are covered by the parser unit
// tests; this test keeps to default- and context-qualified subjects.)
FIXTURE_TEST(sr_rest_client_integration, pandaproxy_test_fixture) {
    info("Seeding subjects via the real REST API");
    auto seed = make_schema_reg_client();
    register_schema(seed, "multi", schema_v1);
    register_schema(seed, "multi", schema_v2);
    register_schema(seed, "solo", schema_v1);
    // Qualified subjects are enabled by default; this exercises the client's
    // %3A path encoding end-to-end against the real server.
    register_schema(seed, ":.myctx:ctx-sub", schema_v1);

    auto sut = make_rest_client(*schema_reg_port);
    ss::abort_source as;
    retry_chain_node rtc(as, 10s, 100ms);

    const auto multi = pps::context_subject::unqualified("multi");
    const auto solo = pps::context_subject::unqualified("solo");
    const auto ctx_sub = pps::context_subject{
      pps::context{".myctx"}, pps::subject{"ctx-sub"}};

    info("list_subjects returns the seeded subjects");
    {
        auto res = sut.list_subjects(rtc).get();
        BOOST_REQUIRE(res.has_value());
        const auto& subs = res.value();
        auto contains = [&subs](const pps::context_subject& s) {
            return std::ranges::find(subs, s) != subs.end();
        };
        BOOST_REQUIRE(contains(multi));
        BOOST_REQUIRE(contains(solo));
        BOOST_REQUIRE(contains(ctx_sub));
    }

    info("list_subject_versions returns [1, 2]");
    {
        auto res = sut.list_subject_versions(multi, rtc).get();
        BOOST_REQUIRE(res.has_value());
        BOOST_REQUIRE_EQUAL(res->size(), 2U);
        BOOST_REQUIRE_EQUAL((*res)[0], pps::schema_version{1});
        BOOST_REQUIRE_EQUAL((*res)[1], pps::schema_version{2});
    }

    info("get_schema_by_version returns the stored schema");
    {
        auto res
          = sut.get_schema_by_version(multi, pps::schema_version{2}, rtc).get();
        BOOST_REQUIRE(res.has_value());
        BOOST_REQUIRE_EQUAL(res->schema.sub(), multi);
        BOOST_REQUIRE_EQUAL(res->version, pps::schema_version{2});
        BOOST_REQUIRE_GE(res->id(), 1);
        BOOST_REQUIRE(res->schema.def().type() == pps::schema_type::avro);
        BOOST_REQUIRE(!res->schema.def().raw()().linearize_to_string().empty());
    }

    info("get_schema_by_version reaches a context-qualified subject (%3A)");
    {
        auto res
          = sut.get_schema_by_version(ctx_sub, pps::schema_version{1}, rtc)
              .get();
        BOOST_REQUIRE(res.has_value());
        BOOST_REQUIRE_EQUAL(res->schema.sub(), ctx_sub);
        BOOST_REQUIRE_EQUAL(res->version, pps::schema_version{1});
    }

    info("a missing subject yields subject_not_found (real 40401)");
    {
        const auto missing = pps::context_subject::unqualified("missing");
        auto versions = sut.list_subject_versions(missing, rtc).get();
        BOOST_REQUIRE(!versions.has_value());
        BOOST_REQUIRE(
          std::holds_alternative<rc::subject_not_found>(versions.error()));

        auto schema
          = sut.get_schema_by_version(missing, pps::schema_version{1}, rtc)
              .get();
        BOOST_REQUIRE(!schema.has_value());
        BOOST_REQUIRE(
          std::holds_alternative<rc::subject_not_found>(schema.error()));
    }

    info("a missing version yields version_not_found (real 40402)");
    {
        auto res = sut
                     .get_schema_by_version(multi, pps::schema_version{99}, rtc)
                     .get();
        BOOST_REQUIRE(!res.has_value());
        BOOST_REQUIRE(
          std::holds_alternative<rc::version_not_found>(res.error()));
    }

    sut.shutdown().get();
}
