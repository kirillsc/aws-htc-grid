# Copyright 2024 Amazon.com, Inc. or its affiliates.
# SPDX-License-Identifier: Apache-2.0
# Licensed under the Apache License, Version 2.0 https://aws.amazon.com/apache-2-0/

"""Unit tests for the EC2 capacity controller backlog read + desired-count math.

Runnable with plain stdlib (no pytest): `python3 -m unittest test_ec2_capacity_controller`.
The controller imports boto3 / aws_lambda_powertools and the api/drain/orb_client modules at
load and constructs the queue + state-table singletons at import; none of those AWS deps are
installed in dev, so we stub them in sys.modules (and via env vars) before importing.
"""

from __future__ import annotations

import os
import sys
import types
import unittest
from unittest import mock


FAKE_BACKLOG = {"n": 0}


def _install_stub_modules() -> None:
    """Stub the deps the controller imports/instantiates at module load."""
    if "boto3" not in sys.modules:
        sys.modules["boto3"] = mock.MagicMock(name="boto3")

    if "aws_lambda_powertools" not in sys.modules:
        powertools = types.ModuleType("aws_lambda_powertools")

        class _Logger:
            def __init__(self, *a, **k):
                pass

            def __getattr__(self, _name):  # info/debug/warning/exception -> no-ops
                return lambda *a, **k: None

            def inject_lambda_context(self, *a, **k):
                def _decorator(func):
                    return func

                return _decorator

        powertools.Logger = _Logger
        sys.modules["aws_lambda_powertools"] = powertools

    # api.queue_manager.queue_manager -> a fake queue whose length tracks FAKE_BACKLOG.
    if "api" not in sys.modules:
        sys.modules["api"] = types.ModuleType("api")

    qm_mod = types.ModuleType("api.queue_manager")

    class _FakeQueue:
        def get_queue_length(self):
            return FAKE_BACKLOG["n"]

    qm_mod.queue_manager = lambda *a, **k: _FakeQueue()
    sys.modules["api.queue_manager"] = qm_mod

    stm_mod = types.ModuleType("api.state_table_manager")
    stm_mod.state_table_manager = lambda *a, **k: mock.MagicMock(name="state_table")
    sys.modules["api.state_table_manager"] = stm_mod

    # drain / orb_client are imported but only called inside the handler; stub as MagicMocks.
    drain_mod = types.ModuleType("drain")
    drain_mod.LIFECYCLE_DRAINING = "draining"
    drain_mod.read_drain_state = lambda ids: {}
    drain_mod.busy_instance_ids = lambda st: set()
    drain_mod.cordon = mock.MagicMock(name="cordon")
    drain_mod.uncordon = mock.MagicMock(name="uncordon")
    drain_mod.resend_stop = mock.MagicMock(name="resend_stop")
    sys.modules["drain"] = drain_mod

    orb_mod = types.ModuleType("orb_client")
    orb_mod.list_live = lambda: []
    orb_mod.create = mock.MagicMock(name="create", return_value={})
    orb_mod.terminate = mock.MagicMock(name="terminate", return_value={})
    sys.modules["orb_client"] = orb_mod


os.environ.update(
    {
        "REGION": "eu-west-1",
        "TASK_QUEUE_SERVICE": "SQS",
        "TASK_QUEUE_CONFIG": "{}",
        "TASKS_QUEUE_NAME": "htc_task_queue_aws__0",
        "STATE_TABLE_NAME": "tasks_state_table",
        "MIN_INSTANCES": "0",
        "MAX_INSTANCES": "5",
        "TARGET_PENDING_PER_INSTANCE": "4",
    }
)
_install_stub_modules()

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ec2_capacity_controller as cc  # noqa: E402


class ReadBacklogTest(unittest.TestCase):
    def test_reads_queue_length_as_float(self):
        FAKE_BACKLOG["n"] = 7
        self.assertEqual(cc._read_backlog(), 7.0)
        self.assertIsInstance(cc._read_backlog(), float)

    def test_empty_queue(self):
        FAKE_BACKLOG["n"] = 0
        self.assertEqual(cc._read_backlog(), 0.0)


class DesiredCountTest(unittest.TestCase):
    """The handler's desired math: clamp(ceil(backlog / target), MIN, MAX)."""

    def _run(self, backlog):
        FAKE_BACKLOG["n"] = backlog
        res = cc.handler({}, None)
        return res["desired"]

    def test_zero_backlog_floors_to_min(self):
        self.assertEqual(self._run(0), cc.MIN_INSTANCES)

    def test_ceil_division(self):
        # target=4: 5 pending -> ceil(5/4)=2
        self.assertEqual(self._run(5), 2)

    def test_clamped_to_max(self):
        # 100 pending -> ceil(100/4)=25, clamped to MAX_INSTANCES=5
        self.assertEqual(self._run(100), cc.MAX_INSTANCES)


if __name__ == "__main__":
    unittest.main()
