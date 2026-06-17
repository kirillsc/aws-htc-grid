# HTC-Grid EC2 Backend — Architecture Design Decisions (ADR)

Running log of notable design decisions for the EC2 worker-plane backend. Newest first.

---

## ADR-003: Graceful, task-aware scale-down via cordon + heartbeat-detected idleness

**Status:** Decided — scale-down is a two-phase **cordon → sweep → terminate** loop driven by
the capacity controller, using the *existing* task heartbeat as the busy signal. No new
DynamoDB index, no new table, no agent change, no Step Functions.

**Context.**
v1 scale-down picked the oldest live machines and terminated them immediately. Any task
in-flight on a terminated instance was killed and only recovered when `ttl_checker` re-queued
it after its heartbeat lapsed — lossy, and dependent on every task being idempotent. We want to
terminate only instances that have no in-flight work (or have exceeded a drain deadline),
without blocking the 1-minute control tick.

**What we reuse (nothing new on the hot path).**
- **Busy signal = the existing heartbeat.** Each agent, on claim and every
  `task_ttl_refresh_interval_sec`, writes `heartbeat_expiration_timestamp = now + offset` on
  its task row while it is `processing*`. A row that is `processing*` with
  `heartbeat_expiration_timestamp > now` is a pair working *right now*.
- **Index = the existing `gsi_ttl_index`.** It already projects `task_owner`. `ttl_checker`
  already queries it across all 32 state partitions with `heartbeat < now`; we add the mirror
  (`query_live_tasks`, `heartbeat > now`) and reuse the same throttle-skip guard.
- **Instance identity = the existing `task_owner`.** On EC2 `task_owner = "<instance-id>-pair-N"`
  (the instance id comes from IMDS in user-data), so `task_owner.split("-pair-")[0]` is the
  EC2 instance — the same id ORB uses as `machine_id`. No lookup, no join.
- **Drain = the agent's existing SIGTERM behaviour.** `docker compose -p htc-workers stop`
  (sent over SSM by the orchestrator) makes each agent's `GracefulKiller` finish its in-flight
  task and stop claiming new ones, within the compose `stop_grace_period` (1500s).

**Loop (each tick; single-flight by ADR-001).**
1. Read backlog + ORB `status` (now enriched with each machine's `htc:lifecycle` /
   `htc:drain_deadline` tags). `active` = live minus `draining`.
2. Compute the busy-instance set from `query_live_tasks`. If the state table is throttling,
   defer scale-down this tick (fail safe = keep capacity).
3. **Sweep `draining` instances:** not-busy → `terminate`; past `drain_deadline` → `terminate`
   anyway (stragglers re-queued by `ttl_checker`); backlog rebounded → `uncordon` (reclaim).
4. **Reconcile `active`:** surplus → **`cordon`** the victims (idle-first, then oldest); they
   become `draining` and a later tick's sweep terminates them once idle. Cordon ≠ terminate.

Because cordon stops new claims, once an instance leaves the busy set it stays out — no
terminate/claim race. An idle instance is cordoned on tick N and terminated on tick N+1.

**Decision rationale.** This is the minimal change that makes scale-down task-aware: it adds
no schema, no write amplification, and no new service, and it reuses a proven, throttle-aware
access pattern (`ttl_checker`'s 32-partition `gsi_ttl_index` fan-out). ORB stays the actuator
(ADR-002): the controller decides, the orchestrator performs the EC2 tag + SSM + terminate.

**Consequences / trade-offs.**
- Safety is **best-effort drain + `ttl_checker` backstop**: a task exceeding `drain_deadline` is
  still killed and re-queued (tasks are already assumed idempotent in v1).
- "Idle" means "idle within ~`heartbeat offset` (≈30s)"; fine for scale-down granularity.
- Two-tick latency to actually terminate (cordon, then sweep) — intentional, never blocks a tick.
- SSM cordon is best-effort; the `drain_deadline` tag guarantees eventual termination even if the
  SSM command never lands, so no instance can pin capacity forever.
- The controller now bundles the shared state-table DAL (`api-v0.1` + `utils`, boto3-only) and
  gains `dynamodb:Query` on the state table + its indexes; the orchestrator gains
  `ec2:DeleteTags` + `ssm:SendCommand`.

**Revisit if:** the 32-partition fan-out per tick becomes material at very large fleet sizes
(then a sparse instance-scoped registry/GSI keyed on idleness is the next step), or a hard
no-re-queue guarantee is required (then block termination while any task is in flight).

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
