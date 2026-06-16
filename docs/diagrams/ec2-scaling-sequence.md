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
        participant CW as CloudWatch<br/>(pending_tasks_ddb)
    end
    box rgb(255,243,224) ORB
        participant ORB as orb_orchestrator<br/>Lambda (ORB)
    end
    box rgb(252,228,236) Worker plane
        participant EC2 as EC2 / worker instance
    end

    CTL->>CW: read backlog (pending_tasks_ddb)
    CW-->>CTL: backlog
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
        participant CW as CloudWatch<br/>(pending_tasks_ddb)
    end
    box rgb(255,243,224) ORB
        participant ORB as orb_orchestrator<br/>Lambda (ORB)
        participant DDB as DynamoDB<br/>orb-* state
    end
    box rgb(252,228,236) Worker plane
        participant EC2 as EC2 / worker instance
        participant SQS as SQS + DDB state
    end

    Note over CW: scaling_metrics Lambda (unchanged)<br/>publishes backlog every minute

    Note over EB,CTL: reserved_concurrent_executions = 1 →<br/>at most one tick runs at a time (overlap is throttled + retried)

    EB->>CTL: invoke tick
    CTL->>CW: GetMetricData pending_tasks_ddb
    CW-->>CTL: backlog
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
    else desired < live  (scale down)
        Note over CTL: pick oldest live machine ids
        CTL->>ORB: invoke {"action":"terminate","machine_ids":[...]}
        ORB->>EC2: TerminateInstances
        Note over EC2,SQS: v1: no graceful drain —<br/>in-flight tasks re-queued by ttl_checker
    else desired == live
        Note over CTL: no-op
    end
```

## Notes

- **`scaling_metrics` Lambda (demand signal, unchanged from EKS).** A pre-existing
  control-plane Lambda fired by its own EventBridge `rate(1 min)`. Each tick it reads the
  queue length (count of PENDING tasks via the queue_manager over the SQS/DDB state) and
  `put_metric_data` publishes it as the CloudWatch metric `pending_tasks_ddb`
  (namespace/dimension from env). It only *produces* the metric and makes no scaling
  decision: on EKS, KEDA consumes it; on EC2, the `capacity_controller` does. Shared by
  both backends, so it is reused as-is.
- **Demand vs supply.** `pending_tasks_ddb` is the demand signal; ORB's live machine count
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
- **Deferred (v1).** No graceful drain on scale-down (ttl_checker re-queues; needs
  idempotent tasks); on-demand RunInstances only (EC2 Fleet/Spot is a later phase).
