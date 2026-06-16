# Copyright 2024 Amazon.com, Inc. or its affiliates.
# SPDX-License-Identifier: Apache-2.0
# Licensed under the Apache License, Version 2.0 https://aws.amazon.com/apache-2-0/

"""HTC-Grid EC2 capacity controller.

EventBridge invokes this on a fixed interval. Each tick it:
  1. reads the backlog metric (pending_tasks_ddb, emitted by scaling_metrics);
  2. reads current LIVE worker capacity from the ORB orchestrator ({"action":"status"});
  3. computes desired instance count = clamp(ceil(backlog / target_per_instance), MIN, MAX);
  4. scale-up  -> orchestrator create (count = desired-live);
     scale-down -> orchestrator terminate (oldest live machine ids).

A DynamoDB lock item provides single-flight so overlapping ticks cannot double-issue
create/terminate (ORB request_machines is NOT idempotent). Scale-down terminates by
explicit ids and accepts the v1 no-drain caveat (in-flight tasks re-queued by ttl_checker).
"""

from __future__ import annotations

import json
import math
import os
import time

import boto3

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
LOCK_TABLE = os.environ["LOCK_TABLE"]
LOCK_TTL_SEC = int(os.environ.get("LOCK_TTL_SEC", "300"))

lambda_client = boto3.client("lambda", region_name=REGION)
cloudwatch = boto3.client("cloudwatch", region_name=REGION)
dynamodb = boto3.client("dynamodb", region_name=REGION)

LOCK_KEY = "capacity-controller-lock"


def _acquire_lock(now: int) -> bool:
    """Single-flight: only one controller tick may act at a time."""
    try:
        dynamodb.put_item(
            TableName=LOCK_TABLE,
            Item={"id": {"S": LOCK_KEY}, "expires_at": {"N": str(now + LOCK_TTL_SEC)}},
            ConditionExpression="attribute_not_exists(id) OR expires_at < :now",
            ExpressionAttributeValues={":now": {"N": str(now)}},
        )
        return True
    except dynamodb.exceptions.ConditionalCheckFailedException:
        return False


def _release_lock() -> None:
    try:
        dynamodb.delete_item(TableName=LOCK_TABLE, Key={"id": {"S": LOCK_KEY}})
    except Exception as exc:  # noqa: BLE001
        print(f"warning: failed to release lock: {exc}")


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


def handler(event, context):  # noqa: ANN001
    now = int(time.time())
    if not _acquire_lock(now):
        print("another controller tick holds the lock; skipping")
        return {"statusCode": 200, "skipped": "locked"}

    try:
        backlog = _read_backlog()
        machines = _live_machines()
        live = len(machines)

        desired = math.ceil(backlog / TARGET_PER_INSTANCE) if backlog > 0 else 0
        desired = max(MIN_INSTANCES, min(MAX_INSTANCES, desired))

        print(
            f"backlog={backlog} live={live} target/inst={TARGET_PER_INSTANCE} "
            f"desired={desired} (min={MIN_INSTANCES} max={MAX_INSTANCES})"
        )

        if desired > live:
            count = desired - live
            res = _invoke_orchestrator(
                {"action": "create", "template_id": TEMPLATE_ID, "count": count}
            )
            return {"statusCode": 200, "action": "scale_up", "count": count, "orchestrator": res}

        if desired < live:
            # oldest-first: ORB machines carry a launch/created timestamp; fall back to id order.
            def _ts(m: dict):
                return m.get("created_at") or m.get("launch_time") or m.get("machine_id", "")

            ordered = sorted(machines, key=_ts)
            to_remove = [m["machine_id"] for m in ordered[: (live - desired)] if m.get("machine_id")]
            if to_remove:
                res = _invoke_orchestrator({"action": "terminate", "machine_ids": to_remove})
                return {
                    "statusCode": 200,
                    "action": "scale_down",
                    "machine_ids": to_remove,
                    "orchestrator": res,
                }

        return {"statusCode": 200, "action": "noop", "live": live, "desired": desired}
    finally:
        _release_lock()
