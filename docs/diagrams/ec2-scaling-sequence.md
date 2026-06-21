# HTC-Grid EC2 Backend — Scaling Sequence

The control loop for `worker_backend = "ec2"`: every `rate(1 minute)` the capacity
controller reconciles demand (SQS backlog) against supply (ORB live machine count) and
drives ORB to create/terminate worker instances. This is the EC2 analogue of KEDA +
Cluster Autoscaler on the EKS backend.

## High-level (the core loop)

The essential decision loop: read backlog, read live capacity, reconcile, create or
terminate workers.

```mermaid
sequenceDiagram
    autonumber
    box rgb(232,245,233) Scaling control
        participant CTL as capacity_controller<br/>Lambda (concurrency=1)
    end
    box rgb(225,245,254) Task dataplane
        participant SQSQ as SQS task queue(s)
    end
    box rgb(255,243,224) ORB
        participant ORB as orb_orchestrator<br/>Lambda (ORB)
    end
    box rgb(252,228,236) Worker plane
        participant EC2 as EC2 / worker instance
    end

    CTL->>SQSQ: read backlog (GetQueueAttributes)
    SQSQ-->>CTL: ApproximateNumberOfMessages
    CTL->>ORB: status (how many live?)
    ORB-->>CTL: live count
    Note over CTL: desired = clamp(ceil(backlog / target_per_instance), min, max)
    alt desired > live
        CTL->>ORB: create (desired - live)
        ORB->>EC2: launch worker(s)
    else desired < live
        CTL->>ORB: terminate (oldest)
        ORB->>EC2: terminate worker(s)
    else desired == live
        Note over CTL: no-op
    end
```

## Detailed

Same loop with the trigger, ORB state store, worker boot, and task dataplane shown.

```mermaid
sequenceDiagram
    autonumber
    box rgb(225,245,254) Trigger
        participant EB as EventBridge<br/>rate(1 min)
    end
    box rgb(232,245,233) Scaling control
        participant CTL as capacity_controller<br/>Lambda (concurrency=1)
    end
    box rgb(255,243,224) ORB
        participant ORB as orb_orchestrator<br/>Lambda (ORB)
        participant DDB as DynamoDB<br/>orb-* state
    end
    box rgb(252,228,236) Worker plane
        participant EC2 as EC2 / worker instance
        participant SQS as SQS + DDB state
    end

    Note over EB,CTL: reserved_concurrent_executions = 1 →<br/>at most one tick runs at a time (overlap is throttled + retried)

    EB->>CTL: invoke tick
    CTL->>SQS: GetQueueAttributes (ApproximateNumberOfMessages)
    SQS-->>CTL: backlog
    CTL->>ORB: invoke {"action":"status"}
    ORB->>DDB: list machines (filter live)
    DDB-->>ORB: live machines
    ORB-->>CTL: live count
    Note over CTL: desired = clamp(ceil(backlog / target_per_instance), min, max)

    alt desired > live  (scale up)
        CTL->>ORB: invoke {"action":"create","count":Δ}
        ORB->>DDB: record request
        ORB->>EC2: RunInstances (worker template)
        Note over EC2: cloud-init: SSM config → ECR login →<br/>NUM_PAIRS = min(vCPU/pair_cpu, mem/pair_mem) →<br/>docker compose up -d (N agent+RIE pairs)
        EC2->>SQS: long-poll, claim, run, write results
    else desired < live  (scale down — graceful, ADR-003)
        Note over CTL: pick victims (idle-first via live-task heartbeat, then oldest)
        CTL->>ORB: invoke {"action":"cordon","machine_ids":[...]}
        ORB->>EC2: CreateTags(draining, drain_deadline) + SSM `compose stop`
        Note over EC2,SQS: agent finishes in-flight task, stops claiming (SIGTERM)
        Note over CTL: NEXT tick sweeps: query live-task heartbeat (gsi_ttl_index)
        CTL->>DDB: Query processing* AND heartbeat > now (per partition)
        DDB-->>CTL: task_owner -> busy instance ids
        CTL->>ORB: invoke {"action":"terminate"} once idle (or past deadline)
        ORB->>EC2: TerminateInstances
        Note over EC2,SQS: stragglers past deadline re-queued by ttl_checker
    else desired == live
        Note over CTL: no-op
    end
```

## Notes

- **Backlog read directly from SQS (no CloudWatch hop).** The controller reads the demand
  signal straight from the task queue — `queue_manager(...).get_queue_length()`, i.e. SQS
  `ApproximateNumberOfMessages` (summed across all priority queues for PrioritySQS). This is
  the *same* number the EKS-only `scaling_metrics` Lambda republishes to CloudWatch as
  `pending_tasks_ddb`; reading the queue directly drops a Lambda and a CloudWatch round-trip
  from the EC2 scaling path, so backlog changes are seen within one tick instead of stacking
  two ~1-min schedules plus CloudWatch ingestion lag. `scaling_metrics` / `pending_tasks_ddb`
  remain **EKS-only** (KEDA consumes the metric there); they are not deployed on the ec2 backend.
- **Demand vs supply.** The SQS backlog is the demand signal; ORB's live machine count
  is supply. The controller reconciles to
  `desired = clamp(ceil(backlog / target_pending_per_instance), min, max)`.
- **Single-flight via `reserved_concurrent_executions = 1`** (ADR-001). At most one tick
  runs at a time, so overlapping/duplicate invocations cannot double-issue ORB's
  non-idempotent `create`. An overlapping scheduled tick is throttled and async-retried
  (deferred re-run) rather than skipped; concurrency frees on exit (no stuck state). See
  `docs/architecture_design_decisions.md`.
- **Eventually consistent.** `create` returns before instances exist; the next tick sees
  them via `status`, so the loop self-corrects rather than over-launching.
- **Two scaling levels.** ORB scales the number of instances; each instance computes its
  own pair count (`NUM_PAIRS`) at boot. Per-instance worker count is static.
- **Graceful, task-aware scale-down (ADR-003).** Scale-down is a two-phase **cordon → sweep →
  terminate** loop. The controller cordons a victim (orchestrator `cordon`: tag `draining` +
  SSM `docker compose stop`), the agent finishes its in-flight task and stops claiming, and a
  later tick terminates the instance once the **live-task heartbeat** (`query_live_tasks` over
  the same `gsi_ttl_index` the `ttl_checker` uses, `heartbeat > now`) shows it idle — or once
  the `drain_deadline` passes (stragglers re-queued by `ttl_checker`; needs idempotent tasks).
  No new index/table/agent change. See `docs/architecture_design_decisions.md`.
- **Deferred (v1).** On-demand RunInstances only (EC2 Fleet/Spot is a later phase); no Step
  Functions drain (the cordon/heartbeat loop replaces the need for it).
