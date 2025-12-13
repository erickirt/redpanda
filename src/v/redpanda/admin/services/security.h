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

#include "cluster/controller.h"
#include "proto/redpanda/core/admin/v2/security.proto.h"

namespace admin {

class security_service_impl : public proto::admin::security_service {
public:
    explicit security_service_impl(cluster::controller* controller);

    seastar::future<proto::admin::resolve_oidc_identity_response>
      resolve_oidc_identity(
        serde::pb::rpc::context,
        proto::admin::resolve_oidc_identity_request) override;
    seastar::future<proto::admin::refresh_oidc_keys_response> refresh_oidc_keys(
      serde::pb::rpc::context,
      proto::admin::refresh_oidc_keys_request) override;
    seastar::future<proto::admin::revoke_oidc_sessions_response>
      revoke_oidc_sessions(
        serde::pb::rpc::context,
        proto::admin::revoke_oidc_sessions_request) override;

private:
    cluster::controller* _controller;
};

} // namespace admin
