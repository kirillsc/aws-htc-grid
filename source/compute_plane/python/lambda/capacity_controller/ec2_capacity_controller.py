# Copyright 2024 Amazon.com, Inc. or its affiliates.
# SPDX-License-Identifier: Apache-2.0
# Licensed under the Apache License, Version 2.0 https://aws.amazon.com/apache-2-0/

"""HTC-Grid EC2 capacity controller.

EventBridge invokes this on a fixed interval. Each tick it:
  1. reads the backlog metric (pending_tasks_ddb, emitted by scaling_metrics);
  2. reads current worker capacity from the ORB orchestrator ({"action":"status"}),
     including each machine's drain tags (lifecycle / drain_deadline);
  3. computes desired instance count = clamp(ceil(backlog / target_per_instance), MIN, MAX);
  4. scale-up   -> orchestrator create  (or uncordon a draining instance to reclaim it);
     scale-down -> graceful, task-aware drain (cordon -> sweep -> terminate).

Graceful scale-down (ADR-003) is task-aware and never breaks running work:
  * To scale down we first CORDON the chosen instances (orchestrator `cordon`: tag
    `draining` + SSM `docker compose stop`). The agent's SIGTERM handler finishes the
    in-flight task and stops claiming new ones. The instance keeps running while it drains.
  * On a later tick we SWEEP draining instances: an instance with no in-flight task (per the
    live-task heartbeat query) is terminated; one past its drain_deadline is terminated anyway
    (stragglers re-queued by ttl_checker); if backlog rebounded we uncordon it instead.

"Busy" is read from the SAME heartbeat signal ttl_checker uses: a state-table row that is
`processing*` with heartbeat_expiration_timestamp > now is a pair working right now. The
task_owner "<instance-id>-pair-N" maps that back to the EC2 instance. No new index, no new
writes. If the state table is throttling we skip scale-down for the tick (keep capacity).

Single-flight is enforced at the infrastructure level by reserved_concurrent_executions = 1
(ADR-001), so overlapping/duplicate ticks cannot double-issue ORB's non-idempotent create.
"""

from __future__ import annotations

import json
import math
import os
import time

import boto3
from aws_lambda_powertools import Logger

from api.state_table_manager import state_table_manager
from utils.state_table_common import StateTableException

logger = Logger(service=os.environ.get("POWERTOOLS_SERVICE_NAME", "capacity_controller"))

REGION = os.environ["REGION"]
ORCHESTRATOR_FUNCTION = os.environ["ORCHESTRATOR_FUNCTION_NAME"]
TEMPLATE_ID = os.environ.get("ORB_TEMPLATE_ID", "RunInstances-OnDemand")
METRIC_NAMESPACE = os.environ["METRIC_NAMESPACE"]
METRIC_NAME = os.environ["METRIC_NAME"]
METRIC_DIMENSION_NAME = os.environ["METRIC_DIMENSION_NAME"]
METRIC_DIMENSION_VALUE = os.environ["METRIC_DIMENSION_VALUE"]
MIN_INSTANCES = int(os.environ.get("MIN_INSTANCES", "0"))
MAX_INSTANCES = int(os.environ.get("MAX_INSTANCES", "5"))
TARGET_PER_INSTANCE = max(1, int(os.environ.get("TARGET_PENDING_PER_INSTANCE", "4")))

# State table, used to detect which workers are busy (heartbeat-based, same as ttl_checker).
STATE_TABLE_NAME = os.environ["STATE_TABLE_NAME"]
STATE_TABLE_SERVICE = os.environ.get("STATE_TABLE_SERVICE", "DynamoDB")
STATE_TABLE_CONFIG = os.environ.get("STATE_TABLE_CONFIG", "{}")

lambda_client = boto3.client("lambda", region_name=REGION)
cloudwatch = boto3.client("cloudwatch", region_name=REGION)
state_table = state_table_manager(
    STATE_TABLE_SERVICE, STATE_TABLE_CONFIG, STATE_TABLE_NAME, REGION
)


def _read_backlog() -> float:
    """Most recent value of the backlog metric (pending tasks)."""
    end = int(time.time())
    resp = cloudwatch.get_metric_data(
        MetricDataQueries=[
            {
                "Id": "backlog",
                "MetricStat": {
                    "Metric": {
                        "Namespace": METRIC_NAMESPACE,
                        "MetricName": METRIC_NAME,
                        "Dimensions": [
                            {"Name": METRIC_DIMENSION_NAME, "Value": METRIC_DIMENSION_VALUE}
                        ],
                    },
                    "Period": 60,
                    "Stat": "Average",
                },
                "ReturnData": True,
            }
        ],
        StartTime=end - 600,
        EndTime=end,
        ScanBy="TimestampDescending",
    )
    # get_metric_data omits empty buckets, so values[0] is the most recent real datapoint
    # (the scaling_metrics Lambda emits pending_tasks_ddb every minute).
    values = resp["MetricDataResults"][0].get("Values", [])
    return float(values[0]) if values else 0.0


def _invoke_orchestrator(payload: dict) -> dict:
    resp = lambda_client.invoke(
        FunctionName=ORCHESTRATOR_FUNCTION,
        InvocationType="RequestResponse",
        Payload=json.dumps(payload).encode(),
    )
    body = json.loads(resp["Payload"].read() or b"{}")
    return body


def _live_machines() -> list[dict]:
    body = _invoke_orchestrator({"action": "status"})
    return body.get("body", {}).get("result", {}).get("machines", [])


def _busy_instance_ids():
    """Set of EC2 instance ids with at least one in-flight task right now.

    Reads the live-task heartbeat (processing* AND heartbeat_expiration_timestamp > now) and
    maps each task_owner "<instance-id>-pair-N" back to its instance. Returns None if the
    state table is throttling, so the caller can defer scale-down (fail safe = keep capacity).
    """
    busy: set[str] = set()
    try:
        for live_tasks in state_table.query_live_tasks():
            for item in live_tasks:
                owner = item.get("task_owner") or ""
                instance_id = owner.split("-pair-")[0]
                if instance_id and instance_id != "None":
                    busy.add(instance_id)
    except StateTableException as exc:
        if getattr(exc, "caused_by_throttling", False):
            logger.warning("state table throttling: deferring scale-down this tick")
            return None
        raise
    return busy


def _machine_age_key(m: dict):
    """Oldest-first sort key: ORB machines carry a created timestamp; fall back to id."""
    return m.get("created_at") or m.get("launch_time") or m.get("machine_id", "")


@logger.inject_lambda_context(log_event=False)
def handler(event, context):  # noqa: ANN001
    # Single-flight is guaranteed by reserved_concurrent_executions = 1 (ADR-001); no
    # application-level lock is needed.
    now = int(time.time())
    backlog = _read_backlog()
    machines = _live_machines()
    live = len(machines)

    draining = [m for m in machines if m.get("lifecycle") == "draining"]
    active = [m for m in machines if m.get("lifecycle") != "draining"]

    desired = math.ceil(backlog / TARGET_PER_INSTANCE) if backlog > 0 else 0
    desired = max(MIN_INSTANCES, min(MAX_INSTANCES, desired))

    logger.info(
        "capacity reconcile",
        backlog=backlog,
        live=live,
        active=len(active),
        draining=len(draining),
        target_per_instance=TARGET_PER_INSTANCE,
        desired=desired,
        min_instances=MIN_INSTANCES,
        max_instances=MAX_INSTANCES,
    )

    busy = _busy_instance_ids()  # None if state table is throttling
    actions: list[dict] = []

    # deficit > 0 -> we need more capacity; deficit < 0 -> we have surplus to drain.
    deficit = desired - len(active)

    # --- 1. Sweep draining instances (uncordon to reclaim, else terminate when safe) -------
    to_uncordon: list[str] = []
    to_terminate: list[str] = []
    for m in sorted(draining, key=_machine_age_key):
        iid = m.get("machine_id")
        if not iid:
            continue
        if deficit > 0:
            # Backlog rebounded: reclaim a draining instance instead of launching a new one.
            to_uncordon.append(iid)
            deficit -= 1
            continue
        deadline = int(m.get("drain_deadline") or 0)
        drained = busy is not None and iid not in busy
        if drained or now >= deadline:
            # Drained cleanly, or the drain deadline passed (force-terminate; ttl_checker
            # re-queues any straggler that did not finish in time).
            to_terminate.append(iid)
        # else: still draining, leave it for a later tick.

    if to_uncordon:
        res = _invoke_orchestrator({"action": "uncordon", "machine_ids": to_uncordon})
        logger.info("uncordon", machine_ids=to_uncordon, count=len(to_uncordon))
        actions.append({"action": "uncordon", "machine_ids": to_uncordon, "orchestrator": res})
    if to_terminate:
        res = _invoke_orchestrator({"action": "terminate", "machine_ids": to_terminate})
        logger.info("terminate", machine_ids=to_terminate, count=len(to_terminate))
        actions.append({"action": "terminate", "machine_ids": to_terminate, "orchestrator": res})

    # --- 2. Scale up: any remaining deficit after uncordoning -> create new instances ------
    if deficit > 0:
        res = _invoke_orchestrator(
            {"action": "create", "template_id": TEMPLATE_ID, "count": deficit}
        )
        logger.info("scale_up", count=deficit, template_id=TEMPLATE_ID)
        actions.append({"action": "create", "count": deficit, "orchestrator": res})

    # --- 3. Scale down: surplus active instances -> cordon (graceful drain) -----------------
    surplus = len(active) - desired
    if surplus > 0:
        if busy is None:
            # State table throttling: cannot tell which instances are idle. Defer cordoning
            # to keep capacity rather than risk draining a busy worker.
            logger.warning(
                "surplus but busy-set unknown (throttling); skipping cordon", surplus=surplus
            )
        else:
            # Victim order: idle instances first, then oldest, so we drain the cheapest first.
            def _victim_key(m: dict):
                iid = m.get("machine_id", "")
                return (1 if iid in busy else 0, _machine_age_key(m))

            victims = [
                m["machine_id"]
                for m in sorted(active, key=_victim_key)[:surplus]
                if m.get("machine_id")
            ]
            if victims:
                res = _invoke_orchestrator({"action": "cordon", "machine_ids": victims})
                logger.info("cordon", machine_ids=victims, count=len(victims))
                actions.append({"action": "cordon", "machine_ids": victims, "orchestrator": res})

    if not actions:
        logger.info("noop", live=live, desired=desired)
        return {"statusCode": 200, "action": "noop", "live": live, "desired": desired}
    logger.info("reconcile actions", action_count=len(actions))
    return {"statusCode": 200, "actions": actions, "live": live, "desired": desired}
