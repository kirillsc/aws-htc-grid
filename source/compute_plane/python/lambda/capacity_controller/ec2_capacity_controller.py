# Copyright 2024 Amazon.com, Inc. or its affiliates.
# SPDX-License-Identifier: Apache-2.0
# Licensed under the Apache License, Version 2.0 https://aws.amazon.com/apache-2-0/

"""HTC-Grid EC2 capacity controller.

EventBridge invokes this on a fixed interval. Each tick it:
  1. reads the backlog directly from the task queue (SQS ApproximateNumberOfMessages);
  2. reads live capacity from ORB (orb_client.list_live) + drain tags from EC2;
  3. computes desired instance count = clamp(ceil(backlog / target_per_instance), MIN, MAX);
  4. reconciles: sweep draining instances, scale up (orb_client.create), or scale down (cordon).

Two responsibilities, two homes (ADR-005):
  * DRAIN is EC2-level and owned HERE (drain.py): cordon = SSM `compose stop` + `draining`
    tags; idle-detect = the task heartbeat; sweep = terminate-when-idle/expired or uncordon.
    EC2-level, so it works no matter which AWS API provisioned the instance.
  * The KILL goes through ORB (orb_client.terminate): ORB decrements the right request's desired
    count for self-healing APIs (ASG / Fleet maintain) instead of letting a replacement relaunch.
    ORB is the capacity abstraction — it picks RunInstances / EC2Fleet / ASG per request; the
    controller just hands it the idle ids.

The controller is a STATELESS RECONCILER: it keeps no state between ticks and re-derives the
world each tick from observed truth (orb_client.list_live, EC2 drain tags, the heartbeat busy
set). With reserved_concurrent_executions = 1 (ADR-001) ticks never overlap, so a crash at a
random point is healed by the next tick re-converging. Cordon's tag-then-stop is non-atomic, so
the sweep RE-ISSUES `compose stop` to any still-busy draining instance (ADR-005 crash-recovery).
"""

from __future__ import annotations

import math
import os
import time

from aws_lambda_powertools import Logger

from api.queue_manager import queue_manager
from api.state_table_manager import state_table_manager

import drain
import orb_client

logger = Logger(service=os.environ.get("POWERTOOLS_SERVICE_NAME", "capacity_controller"))

REGION = os.environ["REGION"]
MIN_INSTANCES = int(os.environ.get("MIN_INSTANCES", "0"))
MAX_INSTANCES = int(os.environ.get("MAX_INSTANCES", "5"))
TARGET_PER_INSTANCE = max(1, int(os.environ.get("TARGET_PENDING_PER_INSTANCE", "4")))

# Task queue, read directly for the backlog (SQS ApproximateNumberOfMessages). This is the
# same number scaling_metrics used to republish to CloudWatch — read here without the hop.
TASK_QUEUE_SERVICE = os.environ["TASK_QUEUE_SERVICE"]
TASK_QUEUE_CONFIG = os.environ.get("TASK_QUEUE_CONFIG", "{}")
TASKS_QUEUE_NAME = os.environ["TASKS_QUEUE_NAME"]

# State table, used to detect which workers are busy (heartbeat-based, same as ttl_checker).
STATE_TABLE_NAME = os.environ["STATE_TABLE_NAME"]
STATE_TABLE_SERVICE = os.environ.get("STATE_TABLE_SERVICE", "DynamoDB")
STATE_TABLE_CONFIG = os.environ.get("STATE_TABLE_CONFIG", "{}")

task_queue = queue_manager(
    TASK_QUEUE_SERVICE, TASK_QUEUE_CONFIG, TASKS_QUEUE_NAME, REGION
)
state_table = state_table_manager(
    STATE_TABLE_SERVICE, STATE_TABLE_CONFIG, STATE_TABLE_NAME, REGION
)


def _read_backlog() -> float:
    """Pending tasks, read straight from the task queue (SQS ApproximateNumberOfMessages).

    For PrioritySQS, queue_manager returns QueuePrioritySQS whose get_queue_length() sums
    the backlog across every priority queue, so this works unchanged for both backends.
    """
    return float(task_queue.get_queue_length())


def _machine_age_key(m: dict):
    """Oldest-first sort key: machines carry a created timestamp; fall back to id."""
    return m.get("created_at") or m.get("launch_time") or m.get("machine_id", "")


def _sweep_draining(draining, deficit, busy, now, actions):
    """Stage 1 — sweep draining instances: reclaim, terminate-when-safe, or re-stop.

    For each draining instance (oldest first):
      * deficit > 0   -> uncordon (reclaim instead of launching new); consumes one of the deficit.
      * drained / past deadline -> terminate via ORB.
      * still busy    -> re-issue compose stop (idempotent) to heal a cordon whose original stop
                         never landed (ADR-005 crash-recovery).
      * busy unknown (throttling) and not past deadline -> leave for a later tick.

    Returns the deficit remaining after any uncordons (input for the scale-up stage).
    """
    logger.debug(
        "stage sweep: begin",
        draining=len(draining),
        deficit_in=deficit,
        busy_known=busy is not None,
    )
    to_uncordon: list[str] = []
    to_terminate: list[str] = []
    to_resend_stop: list[str] = []
    for m in sorted(draining, key=_machine_age_key):
        iid = m.get("machine_id")
        if not iid:
            continue
        if deficit > 0:
            # Backlog rebounded: reclaim a draining instance instead of launching a new one.
            to_uncordon.append(iid)
            deficit -= 1
            logger.debug("stage sweep: reclaim draining instance", machine_id=iid, deficit_left=deficit)
            continue
        # Fail-safe deadline: a missing/unreadable drain_deadline tag means "unknown", so we
        # do NOT force-terminate this tick (a transient DescribeInstances failure must not kill
        # draining instances mid-task). Only a real, past deadline forces termination.
        raw_deadline = m.get("drain_deadline")
        deadline_passed = raw_deadline is not None and now >= int(raw_deadline)
        is_busy = busy is not None and iid in busy
        drained = busy is not None and not is_busy
        logger.debug(
            "stage sweep: evaluate draining instance",
            machine_id=iid,
            drain_deadline=raw_deadline,
            deadline_passed=deadline_passed,
            is_busy=is_busy,
            drained=drained,
        )
        if drained or deadline_passed:
            to_terminate.append(iid)
        elif is_busy:
            # Still busy: re-issue compose stop (idempotent) to heal a cordon that tagged the
            # instance but whose original stop never landed (ADR-005 crash-recovery).
            to_resend_stop.append(iid)
        # else: busy unknown (throttling) and not past deadline -> leave it for a later tick.

    if to_uncordon:
        drain.uncordon(to_uncordon)
        actions.append({"action": "uncordon", "machine_ids": to_uncordon})
    if to_resend_stop:
        drain.resend_stop(to_resend_stop)
        actions.append({"action": "resend_stop", "machine_ids": to_resend_stop})
    if to_terminate:
        res = orb_client.terminate(to_terminate)
        actions.append({"action": "terminate", "machine_ids": to_terminate, "orb": res})

    logger.debug(
        "stage sweep: done",
        uncordon=len(to_uncordon),
        resend_stop=len(to_resend_stop),
        terminate=len(to_terminate),
        deficit_out=deficit,
    )
    return deficit


def _scale_up(deficit, actions):
    """Stage 2 — scale up: any deficit remaining after reclaiming draining instances -> create."""
    logger.debug("stage scale_up: begin", deficit=deficit)
    if deficit > 0:
        res = orb_client.create(deficit)
        actions.append({"action": "create", "count": deficit, "orb": res})
        logger.debug("stage scale_up: created capacity", count=deficit)
    else:
        logger.debug("stage scale_up: no deficit, skip")


def _scale_down(active, desired, busy, actions):
    """Stage 3 — scale down: cordon surplus active instances for graceful drain.

    Cordon is non-destructive (the worker finishes its in-flight task then stops); the next
    tick's sweep terminates it once idle. Victim order is idle-first then oldest.
    """
    surplus = len(active) - desired
    logger.debug("stage scale_down: begin", active=len(active), desired=desired, surplus=surplus, busy_known=busy is not None)
    if surplus <= 0:
        logger.debug("stage scale_down: no surplus, skip")
        return
    if busy is None:
        # Throttling: cannot tell which instances are idle. Defer cordoning to keep
        # capacity rather than risk draining a busy worker.
        logger.warning("surplus but busy-set unknown (throttling); skipping cordon", surplus=surplus)
        return

    # Victim order: idle instances first, then oldest, so we drain the cheapest first.
    def _victim_key(m: dict):
        iid = m.get("machine_id", "")
        return (1 if iid in busy else 0, _machine_age_key(m))

    victims = [
        m["machine_id"]
        for m in sorted(active, key=_victim_key)[:surplus]
        if m.get("machine_id")
    ]
    logger.debug("stage scale_down: selected victims", victims=victims, count=len(victims))
    if victims:
        drain.cordon(victims)
        actions.append({"action": "cordon", "machine_ids": victims})


@logger.inject_lambda_context(log_event=False)
def handler(event, context):  # noqa: ANN001
    # Single-flight is guaranteed by reserved_concurrent_executions = 1 (ADR-001).
    now = int(time.time())
    backlog = _read_backlog()
    machines = orb_client.list_live()
    live = len(machines)

    # Drain state is controller-owned EC2 tags, read directly (not via the provider).
    drain_state = drain.read_drain_state([m.get("machine_id") for m in machines])
    for m in machines:
        st = drain_state.get(m.get("machine_id"), {})
        m["lifecycle"] = st.get("lifecycle")
        m["drain_deadline"] = st.get("drain_deadline")

    draining = [m for m in machines if m.get("lifecycle") == drain.LIFECYCLE_DRAINING]
    active = [m for m in machines if m.get("lifecycle") != drain.LIFECYCLE_DRAINING]

    desired = math.ceil(backlog / TARGET_PER_INSTANCE) if backlog > 0 else 0  # TODO: Each instance has a different number of workers, 1to1 mapping is not correct.
    desired = max(MIN_INSTANCES, min(MAX_INSTANCES, desired))

    busy = drain.busy_instance_ids(state_table)  # None if state table is throttling

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
        busy_known=busy is not None,
    )

    actions: list[dict] = []
    deficit = desired - len(active)  # >0 need more capacity; surplus handled separately

    # The three reconcile stages (ADR-005). Sweep returns the deficit left after reclaiming
    # draining instances, which scale-up then satisfies; scale-down handles surplus.
    deficit = _sweep_draining(draining, deficit, busy, now, actions)
    _scale_up(deficit, actions)
    _scale_down(active, desired, busy, actions)

    if not actions:
        logger.info("noop", live=live, desired=desired)
        return {"statusCode": 200, "action": "noop", "live": live, "desired": desired}
    logger.info("reconcile actions", action_count=len(actions))
    return {"statusCode": 200, "actions": actions, "live": live, "desired": desired}
