#!/usr/bin/env bash
# Submit ONE mock_computation job to an EC2-backend HTC-Grid from OUTSIDE the VPC.
#
# A host outside the grid VPC cannot reach the private API Gateway or ElastiCache Redis, so we
# do NOT submit from here. Instead we run the submitter image ON a worker instance (which is
# inside the VPC) via SSM RunShellScript, and only orchestrate from this machine. This machine
# needs AWS creds with: lambda:InvokeFunction (orchestrator), ssm:SendCommand + GetCommandInvocation,
# ec2:DescribeInstances, events:DisableRule/EnableRule, and (to verify) dynamodb:Scan.
#
# Usage:  TAG=mygrid REGION=eu-west-1 ./ec2_submit_one.sh
#
# Reconstructed after the work instance was terminated with this file uncommitted; rebuilt from
# the session transcript (header, step logic, and the exact intra-VPC submitter invocation that
# was used live). Re-test before relying on it.
set -euo pipefail

TAG="${TAG:?set TAG to the grid/project tag, e.g. main-orb-t1}"
REGION="${REGION:-eu-west-1}"
ORCHESTRATOR="orb-orchestrator-${TAG}"
TICK_RULE="capacity-controller-tick-${TAG}"
ECR="$(aws sts get-caller-identity --query Account --output text).dkr.ecr.${REGION}.amazonaws.com"
SUBMITTER_IMAGE="${ECR}/submitter:${TAG}"

# Job shape (small single batch by default; override via env).
NUM_TASKS="${NUM_TASKS:-1}"
WORKER_ARGS="${WORKER_ARGS:-1000 1 1}"   # "<duration_ms> <memory> <output>" for mock_computation
JOB_SIZE="${JOB_SIZE:-10}"
JOB_BATCH_SIZE="${JOB_BATCH_SIZE:-5}"

log() { echo "[$(date -u +%H:%M:%S)] $*"; }

# --- 1. Pause the controller so it can't terminate our worker mid-test ------------------------
# Re-enabled on exit no matter how the script ends.
log "disabling controller tick rule ${TICK_RULE}"
aws events disable-rule --name "${TICK_RULE}" --region "${REGION}" 2>/dev/null || \
  log "WARN: could not disable ${TICK_RULE} (continuing)"
restore_rule() {
  log "re-enabling controller tick rule ${TICK_RULE}"
  aws events enable-rule --name "${TICK_RULE}" --region "${REGION}" 2>/dev/null || true
}
trap restore_rule EXIT

orb_invoke() {  # $1 = JSON payload -> writes response to /tmp/orb_status.json, echoes it
  aws lambda invoke --function-name "${ORCHESTRATOR}" --region "${REGION}" \
    --cli-binary-format raw-in-base64-out --payload "$1" /tmp/orb_status.json >/dev/null
  cat /tmp/orb_status.json
}

# --- 2. Find a running worker, else launch one and wait -----------------------------------------
running_worker() {
  orb_invoke '{"action":"status"}' >/dev/null
  python3 - <<'PY'
import json
d = json.load(open("/tmp/orb_status.json"))
machines = d.get("body", {}).get("result", {}).get("machines", [])
ids = [m["machine_id"] for m in machines if m.get("status") == "running"]
print(ids[0] if ids else "")
PY
}

INSTANCE_ID="$(running_worker)"
if [ -z "${INSTANCE_ID}" ]; then
  log "no running worker; launching one via ORB"
  orb_invoke '{"action":"create","template_id":"RunInstances-OnDemand","count":1}' >/dev/null
  log "waiting for a worker to reach running (up to ~3 min)"
  for _ in $(seq 1 18); do
    sleep 10
    INSTANCE_ID="$(running_worker)"
    [ -n "${INSTANCE_ID}" ] && break
  done
fi
[ -n "${INSTANCE_ID}" ] || { log "ERROR: no worker became running"; exit 1; }
log "using worker ${INSTANCE_ID}"

# --- 3. Wait for bootstrap to finish (SSM agent Online + 'bootstrap complete' in the log) -------
log "waiting for SSM + bootstrap complete on ${INSTANCE_ID}"
# Pass commands via a JSON file to avoid inline --parameters quoting pitfalls.
cat > /tmp/ssm_check.json <<JSON
{"commands":["grep -c 'bootstrap complete' /var/log/htc-bootstrap.log 2>/dev/null || echo 0"]}
JSON
for _ in $(seq 1 30); do
  CMD_ID="$(aws ssm send-command --region "${REGION}" --instance-ids "${INSTANCE_ID}" \
    --document-name AWS-RunShellScript --parameters file:///tmp/ssm_check.json \
    --query 'Command.CommandId' --output text 2>/dev/null || true)"
  if [ -n "${CMD_ID:-}" ]; then
    sleep 3
    OUT="$(aws ssm get-command-invocation --region "${REGION}" \
      --command-id "${CMD_ID}" --instance-id "${INSTANCE_ID}" \
      --query 'StandardOutputContent' --output text 2>/dev/null || echo 0)"
    [ "${OUT//[$'\t\r\n ']/}" != "0" ] && { log "bootstrap complete"; break; }
  fi
  sleep 7
done

# --- 4. Submit one job by running the submitter image ON the worker (intra-VPC) -----------------
# INTRA_VPC=1 makes the client use the private API Gateway and skip Cognito; the worker is in-VPC.
log "submitting workload via submitter image on ${INSTANCE_ID}"
cat > /tmp/ssm_submit.json <<JSON
{"commands":["docker run --rm --network host -e INTRA_VPC=1 -e AWS_REGION=${REGION} -v /opt/htc/agent-config:/etc/agent:ro ${SUBMITTER_IMAGE} python3 ./client.py -n ${NUM_TASKS} --worker_arguments \"${WORKER_ARGS}\" --job_size ${JOB_SIZE} --job_batch_size ${JOB_BATCH_SIZE} --log info"]}
JSON
SUBMIT_CMD_ID="$(aws ssm send-command --region "${REGION}" --instance-ids "${INSTANCE_ID}" \
  --document-name AWS-RunShellScript --parameters file:///tmp/ssm_submit.json \
  --query 'Command.CommandId' --output text)"
log "submit command ${SUBMIT_CMD_ID}; waiting for completion"
aws ssm wait command-executed --region "${REGION}" \
  --command-id "${SUBMIT_CMD_ID}" --instance-id "${INSTANCE_ID}" 2>/dev/null || true
aws ssm get-command-invocation --region "${REGION}" \
  --command-id "${SUBMIT_CMD_ID}" --instance-id "${INSTANCE_ID}" \
  --query 'StandardOutputContent' --output text || true

log "done (controller rule will be re-enabled on exit)"
