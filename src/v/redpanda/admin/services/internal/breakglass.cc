
#include "redpanda/admin/services/internal/breakglass.h"

#include <seastar/core/coroutine.hh>

namespace proto {
using namespace proto::admin::internal;
}

namespace admin::internal {

seastar::future<proto::controller_forced_reconfiguration_response>
breakglass_service_impl::controller_forced_reconfiguration(
  serde::pb::rpc::context, proto::controller_forced_reconfiguration_request) {
    co_return proto::controller_forced_reconfiguration_response{};
}
} // namespace admin::internal
