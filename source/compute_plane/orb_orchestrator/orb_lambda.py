"""Lambda handler: drive ORB create/status/terminate via its async SDK.

Invoked synchronously (e.g. `aws lambda invoke`) with an event of the shape:

    {"action": "create",    "template_id": "RunInstances-OnDemand", "count": 1}
    {"action": "status",    "request_id": "req-..."}     # request-scoped
    {"action": "status"}                                  # live managed machines
    {"action": "status",    "include_terminated": true}  # full history
    {"action": "terminate", "machine_ids": ["i-..."]}     # explicit ids
    {"action": "terminate", "all": true}                  # every LIVE machine (gated)

ORB state lives in DynamoDB (tables created/used per the bundled config). The
handler is stateless: it opens a fresh ORB SDK client per invocation.

Two safety behaviours matter for the HTC-Grid integration, where an automated
controller (not just a human operator) drives this handler:

  * `status` and `terminate {"all": true}` count only LIVE machines by default.
    ORB's `list_machines()` returns every machine it ever managed, including
    terminated ones, so a naive "count machines" over-reports capacity and a
    naive "terminate all" re-issues terminate against already-dead instances.
    We filter to LIVE_STATES so the controller reasons over real capacity.
  * `terminate {"all": true}` is a fleet-wide kill switch that BYPASSES the
    graceful drain path. It is gated behind ORB_ALLOW_TERMINATE_ALL=1, left
    unset in the HTC-Grid deployment, so a stray invocation cannot wipe a live
    worker fleet mid-task.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import subprocess
import sys
import time
from typing import Any

import boto3
from aws_lambda_powertools import Logger

logger = Logger(service=os.environ.get("POWERTOOLS_SERVICE_NAME", "orb_orchestrator"))


def _patch_orb_at_cold_start() -> None:
    """Apply the 4 mandatory orb-py DynamoDB-backend patches at cold start.

    orb-py is installed unmodified by `pip_requirements` (so its native wheels match the
    runtime). We cannot patch /var/task (read-only in Lambda), so copy the installed `orb`
    package to a writable /tmp dir, patch that copy, and prepend it to sys.path BEFORE any
    `import orb`. Idempotent (the patch script skips already-applied edits) and fast (string
    replaces), so the per-cold-start cost is negligible.
    """
    if os.environ.get("ORB_SKIP_RUNTIME_PATCH") == "1":
        return
    import importlib.util

    spec = importlib.util.find_spec("orb")
    if spec is None or not spec.submodule_search_locations:
        logger.warning("orb package not found; cannot apply runtime patches")
        return
    installed_orb = spec.submodule_search_locations[0]  # .../orb
    dst_root = "/tmp/orb-patched"
    dst_orb = os.path.join(dst_root, "orb")
    if not os.path.isdir(dst_orb):
        os.makedirs(dst_root, exist_ok=True)
        shutil.copytree(installed_orb, dst_orb)
        patch_script = os.path.join(os.environ["LAMBDA_TASK_ROOT"], "patches", "apply_orb_patches.py")
        subprocess.run([sys.executable, patch_script, dst_root], check=True)
    # Ensure the patched copy wins over the /var/task site-packages one.
    if dst_root not in sys.path:
        sys.path.insert(0, dst_root)


_patch_orb_at_cold_start()

# Machine statuses that count as live capacity. ORB persists terminated machines
# in its state table, so anything outside this set is historical and must not be
# counted as capacity or re-terminated.
LIVE_STATES = {"pending", "running", "stopping", "shutting-down"}

# Graceful-drain tags written on a worker instance when it is cordoned (scale-down candidate).
# The capacity controller reads them back via the enriched `status` action so it never needs
# EC2 permissions of its own. drain_deadline bounds the drain: past it the controller
# terminates the instance regardless of remaining work (stragglers re-queued by ttl_checker).
TAG_LIFECYCLE = "htc:lifecycle"
TAG_DRAIN_DEADLINE = "htc:drain_deadline"
LIFECYCLE_DRAINING = "draining"

# Seconds a cordoned instance is allowed to finish in-flight work before it is force-terminated.
# Defaults to the worker compose stop_grace_period (1500s) so a clean drain normally completes.
DRAIN_DEADLINE_SEC = int(os.environ.get("ORB_DRAIN_DEADLINE_SEC", "1500"))

# Stop / start the worker compose project. `stop` sends SIGTERM to the agent containers, whose
# GracefulKiller finishes the in-flight task and stops claiming new ones (cordon + drain);
# `start` resumes them (uncordon). Project name matches the worker user-data.
_COMPOSE_STOP_CMD = "docker compose -p htc-workers stop"
_COMPOSE_START_CMD = "docker compose -p htc-workers start"

_REGION = os.environ.get("ORB_REGION") or os.environ.get("AWS_REGION", "eu-west-1")


def _ec2_client():
    return boto3.client("ec2", region_name=_REGION)


def _ssm_client():
    return boto3.client("ssm", region_name=_REGION)


def _live_machines(machines: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Filter ORB's machine list down to live (non-terminated) machines."""
    return [m for m in machines if m.get("status") in LIVE_STATES]


def _instance_drain_tags(machine_ids: list[str]) -> dict[str, dict[str, str]]:
    """Return {instance_id: {lifecycle, drain_deadline}} for the given instances.

    Reads the htc:* tags directly from EC2 so the controller can see drain state without
    its own EC2 permissions. Best-effort: on any error returns an empty mapping (the
    controller then treats instances as not-draining, which is safe).
    """
    ids = [m for m in machine_ids if m]
    if not ids:
        return {}
    try:
        resp = _ec2_client().describe_instances(InstanceIds=ids)
    except Exception:  # noqa: BLE001
        logger.exception("describe_instances for drain tags failed", machine_ids=ids)
        return {}
    out: dict[str, dict[str, str]] = {}
    for reservation in resp.get("Reservations", []):
        for inst in reservation.get("Instances", []):
            iid = inst.get("InstanceId")
            tags = {t["Key"]: t["Value"] for t in inst.get("Tags", [])}
            if iid:
                out[iid] = {
                    "lifecycle": tags.get(TAG_LIFECYCLE),
                    "drain_deadline": tags.get(TAG_DRAIN_DEADLINE),
                }
    return out


def _send_compose_command(machine_ids: list[str], command: str) -> dict[str, Any]:
    """Run a shell command on the given instances over SSM (async, best-effort).

    SSM failures are logged, not raised: cordon must not fail the tick, and the
    drain_deadline tag still forces termination even if the stop command never lands.
    """
    ids = [m for m in machine_ids if m]
    if not ids:
        return {}
    try:
        resp = _ssm_client().send_command(
            InstanceIds=ids,
            DocumentName="AWS-RunShellScript",
            Parameters={"commands": [command]},
        )
        return {"command_id": resp.get("Command", {}).get("CommandId")}
    except Exception as exc:  # noqa: BLE001
        logger.exception("SSM send_command failed", command=command, machine_ids=ids)
        return {"error": str(exc)}

# ORB wants writable work/log/cache/scripts/health dirs. In Lambda only /tmp is
# writable, so the env points there (see CDK / Dockerfile); ensure they exist
# before ORB initializes.
for _var in (
    "ORB_WORK_DIR",
    "ORB_LOG_DIR",
    "ORB_CACHE_DIR",
    "ORB_SCRIPTS_DIR",
    "ORB_HEALTH_DIR",
):
    _path = os.environ.get(_var)
    if _path:
        os.makedirs(_path, exist_ok=True)


def _materialize_grid_config() -> None:
    """Render the bundled ORB config with this grid's values and point ORB at it.

    The image bundles a read-only config (/var/task/orb-config) with placeholder table
    prefix / subnet / SG / instance-profile / AMI. Terraform passes the real values via env;
    we copy the config to a writable /tmp dir, substitute them, and repoint ORB_CONFIG_DIR.
    This keeps ONE image usable for any grid (no per-grid image build).
    """
    src = os.environ.get("ORB_CONFIG_DIR")
    if not src:
        return  # no bundled config dir (e.g. local/PoC use of the baked config)

    # Fail loud if the grid's table prefix is missing: silently falling back to the bundled
    # "orb-poc" placeholder would point ORB at the WRONG DynamoDB tables. In the Terraform
    # deployment the orb_orchestrator module always sets ORB_TABLE_PREFIX.
    table_prefix = os.environ.get("ORB_TABLE_PREFIX")
    if not table_prefix:
        raise RuntimeError(
            "ORB_TABLE_PREFIX is unset; refusing to use the bundled placeholder table prefix. "
            "Set ORB_TABLE_PREFIX (the orb_orchestrator Terraform module sets it)."
        )

    import json

    dst = "/tmp/orb-config"
    shutil.rmtree(dst, ignore_errors=True)
    shutil.copytree(src, dst)

    region = os.environ.get("ORB_REGION", "eu-west-1")
    subnet_ids = [s for s in os.environ.get("ORB_SUBNET_IDS", "").split(",") if s]
    sg_ids = [s for s in os.environ.get("ORB_SECURITY_GROUP_IDS", "").split(",") if s]
    instance_profile = os.environ.get("ORB_INSTANCE_PROFILE_ARN", "")
    image_id = os.environ.get("ORB_IMAGE_ID", "")
    instance_type = os.environ.get("ORB_INSTANCE_TYPE", "")
    template_id = os.environ.get("ORB_TEMPLATE_ID", "RunInstances-OnDemand")

    # The worker cloud-init is large and lives in SSM; fetch it (plain text — ORB
    # base64-encodes user_data itself when building the launch template).
    user_data = ""
    ud_param = os.environ.get("ORB_USER_DATA_SSM_PARAM")
    if ud_param:
        import boto3

        try:
            user_data = (
                boto3.client("ssm", region_name=region)
                .get_parameter(Name=ud_param)["Parameter"]["Value"]
            )
        except Exception:  # noqa: BLE001
            logger.exception("could not load worker user_data from SSM", ssm_param=ud_param)

    # config.json: table prefix (both places) + provider template_defaults subnet.
    cfg_path = os.path.join(dst, "config.json")
    with open(cfg_path) as f:
        cfg = json.load(f)
    cfg["storage"]["dynamodb_strategy"]["table_prefix"] = table_prefix
    cfg["storage"]["dynamodb_strategy"]["region"] = region
    for prov in cfg.get("provider", {}).get("providers", []):
        pc = prov.get("config", {})
        pc.setdefault("storage", {}).setdefault("dynamodb", {})
        pc["storage"]["dynamodb"]["table_prefix"] = table_prefix
        pc["storage"]["dynamodb"]["region"] = region
        if subnet_ids:
            prov.setdefault("template_defaults", {})["subnet_ids"] = subnet_ids
    with open(cfg_path, "w") as f:
        json.dump(cfg, f, indent=2)

    # aws_templates.json: fill the active RunInstances template with grid values.
    tpl_path = os.path.join(dst, "aws_templates.json")
    with open(tpl_path) as f:
        tpls = json.load(f)
    for t in tpls.get("templates", []):
        if t.get("template_id") != template_id:
            continue
        if subnet_ids:
            t["subnet_ids"] = subnet_ids
        if sg_ids:
            t["security_group_ids"] = sg_ids
        if instance_profile:
            t["instance_profile"] = instance_profile
        if image_id:
            t["image_id"] = image_id
        if instance_type:
            t["machine_types"] = {instance_type: 1}
        if user_data:
            t["user_data"] = user_data
    with open(tpl_path, "w") as f:
        json.dump(tpls, f, indent=2)

    os.environ["ORB_CONFIG_DIR"] = dst


_materialize_grid_config()


class BadRequest(Exception):
    """Raised for malformed invocation payloads."""


async def _dispatch(event: dict[str, Any]) -> dict[str, Any]:
    from orb import orb  # imported lazily so cold-start dir setup runs first

    action = (event or {}).get("action")
    if action not in {"create", "status", "terminate", "cordon", "uncordon"}:
        raise BadRequest(
            "action must be one of create|status|terminate|cordon|uncordon, "
            f"got {action!r}"
        )

    # cordon / uncordon are pure EC2 tag + SSM operations; they do not touch ORB state, so
    # handle them without spinning up the ORB SDK client.
    if action == "cordon":
        machine_ids = event.get("machine_ids") or []
        if not machine_ids:
            raise BadRequest("cordon requires machine_ids[]")
        deadline = int(time.time()) + DRAIN_DEADLINE_SEC
        _ec2_client().create_tags(
            Resources=machine_ids,
            Tags=[
                {"Key": TAG_LIFECYCLE, "Value": LIFECYCLE_DRAINING},
                {"Key": TAG_DRAIN_DEADLINE, "Value": str(deadline)},
            ],
        )
        ssm = _send_compose_command(machine_ids, _COMPOSE_STOP_CMD)
        return {
            "action": "cordon",
            "machine_ids": machine_ids,
            "drain_deadline": deadline,
            "ssm": ssm,
        }

    if action == "uncordon":
        machine_ids = event.get("machine_ids") or []
        if not machine_ids:
            raise BadRequest("uncordon requires machine_ids[]")
        ssm = _send_compose_command(machine_ids, _COMPOSE_START_CMD)
        _ec2_client().delete_tags(
            Resources=machine_ids,
            Tags=[{"Key": TAG_LIFECYCLE}, {"Key": TAG_DRAIN_DEADLINE}],
        )
        return {"action": "uncordon", "machine_ids": machine_ids, "ssm": ssm}

    async with orb(provider="aws") as client:
        if action == "create":
            template_id = event.get("template_id", "RunInstances-OnDemand")
            count = int(event.get("count", 1))
            result = await client.request_machines(
                template_id=template_id, count=count
            )
            return {"action": "create", "result": result}

        if action == "status":
            request_id = event.get("request_id")
            if request_id:
                result = await client.get_request_status([request_id])
                return {"action": "status", "result": result}
            # Machine list. Default to live machines only so a controller's
            # capacity count is accurate; include_terminated=true returns the
            # full history.
            result = await client.list_machines()
            machines = result.get("machines", [])
            if not event.get("include_terminated"):
                machines = _live_machines(machines)
            # Enrich live machines with their drain tags so the controller can see which
            # instances are draining (and until when) without needing EC2 permissions.
            drain_tags = _instance_drain_tags(
                [m.get("machine_id") for m in machines]
            )
            for m in machines:
                tags = drain_tags.get(m.get("machine_id"), {})
                m["lifecycle"] = tags.get("lifecycle")
                m["drain_deadline"] = tags.get("drain_deadline")
            return {
                "action": "status",
                "result": {"machines": machines, "count": len(machines)},
            }

        # terminate
        if event.get("all"):
            # Fleet-wide kill switch that BYPASSES drain: gated off by default.
            if os.environ.get("ORB_ALLOW_TERMINATE_ALL") != "1":
                raise BadRequest(
                    "terminate all is disabled (set ORB_ALLOW_TERMINATE_ALL=1 to enable). "
                    "It bypasses graceful drain and must not be the scale-down path; "
                    "pass explicit machine_ids instead."
                )
            listed = await client.list_machines()
            machine_ids = [
                m["machine_id"]
                for m in _live_machines(listed.get("machines", []))
                if m.get("machine_id")
            ]
        else:
            machine_ids = event.get("machine_ids") or []
        if not machine_ids:
            raise BadRequest("terminate requires machine_ids[] or all=true")
        result = await client.return_machines(machine_ids)
        return {"action": "terminate", "requested_ids": machine_ids, "result": result}


@logger.inject_lambda_context(log_event=False)
def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Lambda entrypoint. Wraps the async ORB calls in a fresh event loop."""
    action = (event or {}).get("action")
    logger.append_keys(action=action)
    try:
        body = asyncio.run(_dispatch(event))
        logger.info("orb dispatch ok")
        return {"statusCode": 200, "body": body}
    except BadRequest as exc:
        # Client error (malformed payload / gated kill-switch): warn, do not stacktrace.
        logger.warning("bad request", error=str(exc))
        return {"statusCode": 400, "error": str(exc)}
    except Exception as exc:  # noqa: BLE001 - surface any ORB/AWS error to caller
        logger.exception("orb dispatch failed")
        return {"statusCode": 500, "error": f"{type(exc).__name__}: {exc}"}
