# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

"""Shared helpers for Controller Forced Reconfiguration (CFR) ducktape tests.

Extracted from controller_forced_reconfiguration_test.py so that both the
API-level CFR tests and the CLI-script CFR tests can share the same cluster
lifecycle helpers.
"""

import socket
from dataclasses import dataclass
from typing import Any, Optional

from ducktape.cluster.cluster import ClusterNode
from ducktape.services.service import Service
from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until
from random import shuffle

from rptest.services import tls as tls_mod
from rptest.services.admin import PartitionDetails, Replica
from rptest.services.redpanda import TLSProvider
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import wait_until_result

# Sentinel returned by the admin API when no leader exists for a partition.
NO_LEADER = -1


@dataclass
class NTP:
    namespace: str = "kafka"
    topic: str = "topic"
    partition: int = 0


@dataclass
class TimeoutConfig:
    timeout_s: int
    backoff_s: int


REALLY_SHORT_TIMEOUT = TimeoutConfig(timeout_s=5, backoff_s=1)
SHORT_TIMEOUT = TimeoutConfig(timeout_s=30, backoff_s=2)
MEDIUM_TIMEOUT = TimeoutConfig(timeout_s=60, backoff_s=2)
LONG_TIMEOUT = TimeoutConfig(timeout_s=120, backoff_s=2)
REALLY_LONG_TIMEOUT = TimeoutConfig(timeout_s=300, backoff_s=10)


class SimpleTLSProvider(TLSProvider):
    """Minimal TLS provider that creates broker and service-client certs
    from a shared TLSCertManager.

    Equivalent to the MTLSProvider found in acls_test.py / audit_log_test.py,
    consolidated here so CFR tests (and potentially others) can share it.
    """

    def __init__(self, tls_cert_manager: tls_mod.TLSCertManager) -> None:
        self.tls = tls_cert_manager

    @property
    def ca(self) -> tls_mod.CertificateAuthority:
        return self.tls.ca

    def create_broker_cert(
        self,
        service: Service,
        node: ClusterNode,
    ) -> tls_mod.Certificate:
        assert node in service.nodes
        return self.tls.create_cert(node.name)

    def create_service_client_cert(
        self,
        service: Service,
        name: str,
    ) -> tls_mod.Certificate:
        return self.tls.create_cert(socket.gethostname(), name=name, common_name=name)


class ControllerForcedReconfigurationTestBase(RedpandaTest):
    def __init__(
        self, test_context: TestContext, cluster_size: int, *args: Any, **kwargs: Any
    ):
        self.cluster_size = cluster_size
        self.majority_to_kill = cluster_size // 2 + 1
        self._next_node_id_counter = cluster_size + 1

        super().__init__(
            test_context,
            num_brokers=cluster_size,
            *args,
            **kwargs,
        )

    def _next_node_id(self) -> int:
        """this test kills nodes, cleans them, then reboots with a new node_id, keep track of the node id"""
        nid = self._next_node_id_counter
        self._next_node_id_counter += 1
        return nid

    def setUp(self):
        """rp will be custom started in each test"""
        pass

    def _start_redpanda_base(self, cluster_size: int) -> list[ClusterNode]:
        """start redpanda with a specific cluster size"""
        seed_nodes = self.redpanda.nodes[0:cluster_size]
        joiner_nodes = self.redpanda.nodes[cluster_size:]

        self.redpanda.set_seed_servers(seed_nodes)

        """Controller force reconfiguration does not guarantee that internal topics are safe from data loss
           but data loss on these topics does make it really hard to create any produce/consume test that
           passes. We are enforcing no data loss on internal topics for ease of testing."""
        self.redpanda.add_extra_rp_conf(
            {"internal_topic_replication_factor": cluster_size}
        )
        self.redpanda.start(nodes=seed_nodes, omit_seeds_on_idx_one=False)
        return joiner_nodes

    def _setup_topic(self, topic_spec: TopicSpec, timeout: TimeoutConfig):
        """start a topic given the spec"""
        self.client().create_topic(topic_spec)
        # Wait for initial leader
        self.redpanda._admin.await_stable_leader(
            topic=topic_spec.name,
            replication=topic_spec.replication_factor,
            timeout_s=timeout.timeout_s,
            backoff_s=timeout.backoff_s,
        )

    def _living_nodes(self) -> list[ClusterNode]:
        return self.redpanda.started_nodes()

    def _living_hostnames(self) -> list[str]:
        hostnames: list[str] = []
        node: ClusterNode
        for node in self.redpanda.started_nodes():
            hostname = node.account.hostname
            assert hostname is not None
            hostnames.append(hostname)
        return hostnames

    def _wait_until_no_leader(self, ntp: NTP, timeout: TimeoutConfig):
        """Scrapes the debug endpoints of all replicas and checks if any of the replicas think they are the leader"""

        def no_leader():
            living_nodes = self._living_nodes()
            for living_node in living_nodes:
                state = self.redpanda._admin.get_partition_state(
                    ntp.namespace, ntp.topic, ntp.partition, node=living_node
                )
                if "replicas" not in state.keys() or len(state["replicas"]) == 0:
                    continue
                for r in state["replicas"]:
                    assert "raft_state" in r.keys()
                    if r["raft_state"]["is_leader"]:
                        return False
            return True

        wait_until(
            no_leader,
            timeout_sec=timeout.timeout_s,
            backoff_sec=timeout.backoff_s,
            err_msg="Partition has a leader",
        )

    def _controller_recovered(self, killed_ids: list[int]) -> bool:
        """True when a controller leader exists that is NOT one of the
        killed nodes.  Uses the authenticated admin client."""
        admin = self.redpanda._admin
        for node in self.redpanda.started_nodes():
            try:
                info = admin.get_partitions(
                    namespace="redpanda",
                    topic="controller",
                    partition=0,
                    node=node,
                )
                leader_id = info.get("leader_id", NO_LEADER)
                if leader_id != NO_LEADER and leader_id not in killed_ids:
                    return True
            except Exception:
                continue
        return False

    def _split_cluster(
        self, ntp: NTP, timeout: TimeoutConfig, replication: int = 5
    ) -> tuple[list[Replica], list[Replica]]:
        """
        Splits the cluster into nodes to kill and nodes to survive
        """
        assert self.redpanda

        def _get_details() -> tuple[bool, Optional[PartitionDetails]]:
            d = self.redpanda._admin._get_stable_configuration(
                hosts=self._living_hostnames(),
                namespace=ntp.namespace,
                topic=ntp.topic,
                partition=ntp.partition,
                replication=replication,
            )
            if d is None:
                return (False, None)
            return (True, d)

        partition_details: PartitionDetails = wait_until_result(
            _get_details, timeout_sec=timeout.timeout_s, backoff_sec=timeout.backoff_s
        )

        replicas = partition_details.replicas
        shuffle(replicas)
        mid = len(replicas) // 2 + 1
        (to_kill, to_survive) = (replicas[0:mid], replicas[mid:])
        return (to_kill, to_survive)

    def _do_stop_nodes(self, ntp: NTP, to_kill: list[Replica], timeout: TimeoutConfig):
        """ingests the output of _split_cluster, actually stops those nodes"""
        for replica in to_kill:
            node = self.redpanda.get_node_by_id(replica.node_id)
            assert node
            self.logger.debug(f"Stopping node with node_id: {replica.node_id}")
            self.redpanda.stop_node(node)
        # The partition should be leaderless.
        self._wait_until_no_leader(ntp=ntp, timeout=timeout)

    def _stop_majority_nodes(
        self, ntp: NTP, timeout: TimeoutConfig, replication: int = 5
    ) -> tuple[list[Replica], list[Replica]]:
        """chains together the above two, split the cluster then kill the majority"""
        killed, alive = self._split_cluster(
            ntp=ntp, timeout=timeout, replication=replication
        )
        self._do_stop_nodes(ntp=ntp, to_kill=killed, timeout=timeout)
        return (killed, alive)

    def _toggle_recovery_mode(
        self, node: ClusterNode, timeout: TimeoutConfig, recovery_mode_enabled: bool
    ):
        """reboot a node with recovery mode set accordingly"""
        self.logger.info(f"stopping node: {node.name}")
        self.redpanda.stop_node(node, timeout=timeout.timeout_s)

        self.logger.info(f"restarting node: {node.name}")
        self.redpanda.start_node(
            node,
            timeout=timeout.timeout_s,
            auto_assign_node_id=True,
            override_cfg_params={"recovery_mode_enabled": recovery_mode_enabled},
        )

    def _bulk_toggle_recovery_mode(
        self,
        nodes: list[ClusterNode],
        timeout: TimeoutConfig,
        recovery_mode_enabled: bool,
    ):
        """toggle recovery mode on all provided nodes"""
        for node in nodes:
            self._toggle_recovery_mode(node, timeout, recovery_mode_enabled)

    def _join_new_node(self, joiner_node: ClusterNode) -> int:
        """joins a given cluster node with a new node id"""
        self.logger.debug(f"joining {joiner_node.name=}")
        self.redpanda.clean_node(
            joiner_node, preserve_logs=True, preserve_current_install=True
        )
        joiner_node_id = self._next_node_id()
        self.logger.debug(f"assigned {joiner_node_id=} to {joiner_node.name=}")
        self.redpanda.start_node(
            joiner_node,
            auto_assign_node_id=False,
            node_id_override=joiner_node_id,
            omit_seeds_on_idx_one=True,
        )
        wait_until(
            lambda: self.redpanda.registered(joiner_node),
            timeout_sec=LONG_TIMEOUT.timeout_s,
            backoff_sec=LONG_TIMEOUT.backoff_s,
        )
        return joiner_node_id
