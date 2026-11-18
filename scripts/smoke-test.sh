#!/usr/bin/env bash
# End-to-end check of the Compose stack: starts it, runs a batch of jobs, then kills a
# worker container mid-job and verifies another worker finishes the job.
#
#   scripts/smoke-test.sh          # leaves nothing behind
#   KEEP=1 scripts/smoke-test.sh   # leave the stack running afterwards
set -euo pipefail
cd "$(dirname "$0")/.."

API=http://localhost:8080
BATCH=200

sql() { docker compose exec -T postgres psql -U scheduler -d scheduler -tA -c "$1"; }
step() { printf '\n==> %s\n' "$*"; }
fail() { printf 'FAILED: %s\n' "$*" >&2; exit 1; }

# wait_for <seconds> <description> <command...>: retry until the command succeeds
wait_for() {
  local deadline=$((SECONDS + $1)) what=$2; shift 2
  until "$@" >/dev/null 2>&1; do
    (( SECONDS < deadline )) || fail "timed out waiting for: $what"
    sleep 1
  done
}

cleanup() {
  if [[ "${KEEP:-0}" != 1 ]]; then
    step "Tearing down"
    docker compose down -v --remove-orphans >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

step "Starting postgres, 1 api, 3 workers, prometheus, grafana"
docker compose up -d --build --quiet-pull
wait_for 120 "API health" curl -fsS "$API/actuator/health"
alive() { [[ "$(sql "SELECT count(*) FROM workers WHERE status = 'ALIVE'")" == "$1" ]]; }
wait_for 120 "3 workers registered" alive 3
echo "API is up; 3 workers alive"

step "Admin UI, Prometheus and Grafana"
curl -fsS "$API/" | grep -q "<title>Task Scheduler Admin</title>" || fail "admin UI not served at /"
curl -fsS "$API/api/v1/stats" | grep -q '"workersAlive":3' || fail "stats endpoint does not report 3 live workers"
targets_up() { [[ "$(curl -fsS localhost:9090/api/v1/targets?state=active | grep -o '"health":"up"' | wc -l | tr -d ' ')" == 4 ]]; }
wait_for 60 "Prometheus to scrape the api and 3 workers" targets_up
echo "Prometheus: 4 targets up (api + 3 workers found by DNS)"
dashboard() { curl -fsS localhost:3000/api/dashboards/uid/task-scheduler | grep -q '"title":"Task Scheduler"'; }
wait_for 60 "Grafana to serve the provisioned dashboard anonymously" dashboard
# The dashboard's queries must return data, not just exist: ask Grafana's datasource proxy.
queue_series() {
  curl -fsS -G localhost:3000/api/datasources/proxy/uid/prometheus/api/v1/query \
    --data-urlencode 'query=max by (state) (jobs_queue_depth)' | grep -q '"state":"ready"'
}
wait_for 60 "Grafana to read queue depth through Prometheus" queue_series
echo "Grafana: dashboard provisioned, Prometheus datasource returns data"

step "Submitting $BATCH demo.noop jobs over HTTP"
for i in $(seq 1 "$BATCH"); do
  curl -fsS -o /dev/null -X POST "$API/api/v1/jobs" -H 'Content-Type: application/json' \
    -d "{\"type\": \"demo.noop\", \"idempotencyKey\": \"smoke-$i\"}"
done
succeeded() { [[ "$(sql "SELECT count(*) FROM jobs WHERE type = '$1' AND status = 'SUCCEEDED'")" == "$2" ]]; }
wait_for 60 "$BATCH jobs to succeed" succeeded demo.noop "$BATCH"
[[ "$(sql "SELECT count(*) FROM job_attempts a JOIN jobs j ON j.id = a.job_id WHERE j.type = 'demo.noop'")" == "$BATCH" ]] \
  || fail "expected exactly one attempt per job"
sql "SELECT 'worker ' || left(worker_id::text, 8) || ' ran ' || count(*) || ' jobs' FROM job_attempts GROUP BY worker_id ORDER BY count(*) DESC"

step "Retries: a job that fails twice, then succeeds"
curl -fsS -o /dev/null -X POST "$API/api/v1/jobs" -H 'Content-Type: application/json' \
  -d '{"type": "demo.flaky", "payload": {"succeedOnAttempt": 3}, "maxRetries": 3}'
wait_for 120 "flaky job to succeed on attempt 3" succeeded demo.flaky 1
sql "SELECT 'attempt ' || a.attempt || ': ' || a.outcome FROM job_attempts a JOIN jobs j ON j.id = a.job_id WHERE j.type = 'demo.flaky' ORDER BY a.attempt"

step "Crash recovery: kill -9 the worker that is running a 15 s job"
curl -fsS -o /dev/null -X POST "$API/api/v1/jobs" -H 'Content-Type: application/json' \
  -d '{"type": "demo.sleep", "payload": {"millis": 15000}, "idempotencyKey": "smoke-crash"}'
running() { [[ -n "$(sql "SELECT 1 FROM jobs WHERE idempotency_key = 'smoke-crash' AND status = 'RUNNING'")" ]]; }
wait_for 30 "the sleep job to start" running
# A worker's hostname is its container id.
victim=$(sql "SELECT w.hostname FROM jobs j JOIN workers w ON w.id = j.locked_by WHERE j.idempotency_key = 'smoke-crash'")
echo "Job is running on container $victim; killing it"
docker kill "$victim" >/dev/null
succeeded_key() { [[ "$(sql "SELECT status FROM jobs WHERE idempotency_key = 'smoke-crash'")" == SUCCEEDED ]]; }
wait_for 90 "another worker to finish the job" succeeded_key
history=$(sql "SELECT string_agg(a.outcome, ',' ORDER BY a.attempt) FROM job_attempts a JOIN jobs j ON j.id = a.job_id WHERE j.idempotency_key = 'smoke-crash'")
[[ "$history" == "ABANDONED,SUCCEEDED" ]] || fail "expected attempts ABANDONED,SUCCEEDED but got $history"
[[ "$(sql "SELECT count(DISTINCT worker_id) FROM job_attempts a JOIN jobs j ON j.id = a.job_id WHERE j.idempotency_key = 'smoke-crash'")" == 2 ]] \
  || fail "expected the retry to run on a different worker"
echo "Attempts: $history (on two different workers)"
sql "SELECT 'worker ' || left(id::text, 8) || ' on ' || hostname || ': ' || status FROM workers ORDER BY started_at"

step "Graceful shutdown: docker stop the worker that is running a 2 min job (takes ~30 s)"
curl -fsS -o /dev/null -X POST "$API/api/v1/jobs" -H 'Content-Type: application/json' \
  -d '{"type": "demo.sleep", "payload": {"millis": 120000}, "idempotencyKey": "smoke-graceful", "maxRetries": 0}'
job_sql() { sql "SELECT $1 FROM jobs j WHERE j.idempotency_key = 'smoke-graceful'"; }
graceful_running() { [[ "$(job_sql status)" == RUNNING ]]; }
wait_for 30 "the 2 min job to start" graceful_running
victim=$(sql "SELECT w.hostname FROM jobs j JOIN workers w ON w.id = j.locked_by WHERE j.idempotency_key = 'smoke-graceful'")
echo "Job is running on container $victim; stopping it (SIGTERM)"
docker stop "$victim" >/dev/null
[[ "$(sql "SELECT status FROM workers WHERE hostname = '$victim'")" == STOPPED ]] \
  || fail "expected the stopped worker to be STOPPED, not reaped as DEAD"
first=$(sql "SELECT a.outcome FROM job_attempts a JOIN jobs j ON j.id = a.job_id WHERE j.idempotency_key = 'smoke-graceful' AND a.attempt = 1")
[[ "$first" == RELEASED ]] || fail "expected attempt 1 to be RELEASED but got '$first'"
# maxRetries is 0, so the job only survives because the released attempt was not charged.
[[ "$(job_sql attempts_excused)" == 1 ]] || fail "expected the released attempt to be excused"
resumed() { [[ "$(job_sql "status || ' ' || attempts")" == "RUNNING 2" ]]; }
wait_for 30 "another worker to pick the released job up as attempt 2" resumed
echo "Attempt 1 RELEASED without using retry budget; attempt 2 is running on another worker"
curl -fsS -o /dev/null "$API/actuator/health"

step "Metrics endpoint"
metrics=$(curl -fsS "$API/actuator/prometheus")
for name in jobs_submitted_total jobs_queue_depth jobs_oldest_pending_age_seconds workers_alive; do
  grep -q "^$name" <<<"$metrics" || fail "metric $name missing from /actuator/prometheus"
done
grep -E '^(jobs_submitted_total|jobs_queue_depth|workers_alive)' <<<"$metrics"

printf '\nSMOKE TEST PASSED\n'
