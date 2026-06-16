# HTC-Grid EC2 Backend — Architecture Design Decisions (ADR)

Running log of notable design decisions for the EC2 worker-plane backend. Newest first.

---

## ADR-002: Keep capacity_controller and orb_orchestrator as separate Lambdas

**Status:** Decided — **two separate Lambdas** (controller invokes orchestrator via
Lambda-to-Lambda). Revisit only if ORB init becomes cheap or the cross-invoke latency proves
material.

**Context.**
Both run on the ec2 backend. Could they be one Lambda? `capacity_controller` is the *brain*
(EventBridge `rate(1 min)` → read backlog + live count → decide), `orb_orchestrator` is the
*actuator* (ORB → EC2 create/status/terminate). Each controller tick invokes the orchestrator
at least once (`status`), plus `create`/`terminate` when scaling.

**Why separate.**
- **Cold-start weight.** The orchestrator pulls orb-py + its tree (pydantic, cryptography,
  sqlalchemy, boto3) and on every cold start runs `_patch_orb_at_cold_start()` (copy orb → /tmp,
  apply 4 patches, build a fresh ORB SDK client = seconds). The controller is boto3-only and
  fires every minute. Merging would put that heavy ORB init on the high-frequency metric path
  even on no-op/status-only ticks. (ORB doc B.2: keep the actuator a separate Lambda; do not
  fold ORB init into the high-frequency metric path; keep each invocation single-purpose.)
- **Roles / reuse.** Controller = decider, orchestrator = actuator (mirrors KEDA vs autoscaler
  on EKS). The orchestrator can be invoked by other callers (manual ops, future controllers),
  not just this one.
- **IAM blast radius.** The orchestrator owns the 3 DynamoDB state tables, EC2
  RunInstances/TerminateInstances, `iam:PassRole`, and a KMS key. The controller needs only
  `lambda:InvokeFunction` + `cloudwatch:GetMetricData`. Merging would grant the metric path the
  full EC2-launch IAM.
- **Concurrency model.** The controller is pinned to `reserved_concurrent_executions = 1`
  (ADR-001). The orchestrator must not be — it may be invoked concurrently (e.g. a status read
  while a create/terminate is in flight). One merged function cannot hold both policies.

**Consequences / trade-off.**
- Each tick pays a Lambda-to-Lambda invoke (and the orchestrator's cold-start latency on a cold
  invoke). Accepted. If it becomes material, the mitigation is **provisioned concurrency on
  `orb_orchestrator`** (keep ORB warm), not merging.

**Revisit if:** ORB initialization becomes cheap (e.g. patches upstreamed, lighter client), or
the per-tick invoke latency/cost is shown to matter and provisioned concurrency is insufficient.

---

## ADR-001: Single-flight for the capacity controller — DynamoDB lock vs Lambda reserved concurrency

**Status:** Decided — use **`reserved_concurrent_executions = 1`**; remove the DynamoDB lock. Revisit if requirements change.

**Context.**
The `capacity_controller` Lambda runs on an EventBridge `rate(1 min)` schedule and reconciles
worker capacity by invoking the ORB orchestrator. Ticks can overlap (a slow tick — ORB cold-start
is several seconds — may still run when the next fires; EventBridge can also deliver more than
once). ORB's `create` (`request_machines`) is **not idempotent**, so two concurrent ticks could
each issue a create and double-launch capacity. We need at-most-one reconcile in flight.

**Options.**

| | DynamoDB lock (one-row conditional put + TTL) | `reserved_concurrent_executions = 1` |
|---|---|---|
| Overlapping tick | cleanly skips (no-op) | throttled, async-retried (runs slightly later) |
| Crashed tick | holds lock until ~300s TTL (stuck-state risk) | concurrency frees on exit — no stuck state |
| Cost / code | extra table + acquire/release + TTL handling | one attribute, zero code |
| Guards manual `aws lambda invoke` | yes (skips) | throttled/retried, not skipped |

Note: neither mechanism alone prevents *sequential* over-creation; that is handled separately by
ORB `status` listing freshly-launched instances as `pending` on the next tick, so the loop
converges instead of overshooting.

**Decision.**
Revert to `reserved_concurrent_executions = 1` for **simplicity**: it gives the same
no-two-ticks-at-once guarantee for our single (EventBridge) invoker, needs no extra resources or
code, and eliminates the stuck-lock failure mode we hit during live testing (the controller was
briefly VPC-attached, timed out, and left the lock held until its TTL expired).

**Consequences.**
- Remove `aws_dynamodb_table.lock`, its IAM statement, and the `_acquire_lock`/`_release_lock`
  logic in `ec2_capacity_controller.py`.
- An overlapping scheduled tick is throttled and async-retried (deferred re-run) rather than
  cleanly skipped — harmless at 1/min with sub-300s ticks.
- Manual concurrent invocations during testing are throttled rather than no-op'd.

**Revisit if:** the controller gains multiple legitimate concurrent invokers, ticks routinely
approach the Lambda timeout, or we need an explicit "skip" (vs retry) semantic — at which point the
DynamoDB lock (or a Step Functions state machine) becomes the better fit.
