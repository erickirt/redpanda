# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

"""Ducktape tests that exercise the invoke_controller_reconfiguration CLI
script against a real cluster where the controller has lost quorum."""

import subprocess

from ducktape.tests.test import TestContext

from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest

# Name of the script on PATH, installed via the script_package bazel rule.
CFR_SCRIPT = "invoke_controller_reconfiguration"

# Substring from the script's --help output used to verify correct packaging.
CFR_HELP_DESCRIPTION = \
    "Helper script to orchestrate invoking Controller Forced Reconfiguration"

# ── CFR script CLI flags ─────────────────────────────────────────────────
CFR_FLAG_HELP = "--help"


class ControllerForcedReconfigurationScriptTest(RedpandaTest):
    """
    Validates that the invoke_controller_reconfiguration helper script
    is packaged correctly and accessible on PATH in the test environment.
    """

    def __init__(self, test_context: TestContext) -> None:
        super().__init__(test_context=test_context, num_brokers=1)

    @cluster(num_nodes=1)
    def test_cfr_script_on_path(self) -> None:
        result = subprocess.run(
            [CFR_SCRIPT, CFR_FLAG_HELP],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, \
            f"Script exited with {result.returncode}: {result.stderr}"

        assert CFR_HELP_DESCRIPTION in result.stdout, \
            f"Expected description not found in help output:\n{result.stdout}"

        assert "baremetal" in result.stdout, \
            f"Expected 'baremetal' subcommand not found in help output:\n{result.stdout}"

        assert "kubernetes" in result.stdout, \
            f"Expected 'kubernetes' subcommand not found in help output:\n{result.stdout}"
