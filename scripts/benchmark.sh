#!/usr/bin/env bash
# Rough throughput measurement against the Compose stack: enqueues N no-op jobs directly
# in SQL (so submission is not the bottleneck) and times how long the workers take to
# drain them. Measures scheduler overhead per job: claim + ack, with an empty handler.
#
#   scripts/benchmark.sh [jobs=20000]     # stack must already be up: docker compose up -d --build
set -euo pipefail
cd "$(dirname "$0")/.."

N=${1:-20000}
sql() { docker compose exec -T postgres psql -U scheduler -d scheduler -tA -c "$1"; }

workers=$(sql "SELECT count(*) FROM workers WHERE status = 'ALIVE'")
[[ "$workers" -gt 0 ]] || { echo "No live workers. Start the stack first: docker compose up -d --build" >&2; exit 1; }

sql "DELETE FROM jobs WHERE type = 'demo.noop' AND payload @> '{\"bench\": true}'" >/dev/null
echo "Enqueuing $N demo.noop jobs for $workers workers..."
sql "INSERT INTO jobs (type, payload) SELECT 'demo.noop', '{\"bench\": true}' FROM generate_series(1, $N)" >/dev/null

while [[ "$(sql "SELECT count(*) FROM jobs WHERE payload @> '{\"bench\": true}' AND status <> 'SUCCEEDED'")" != 0 ]]; do
  sleep 0.5
done

sql "
  SELECT format(
    E'jobs:        %s\nworkers:     %s\nelapsed:     %s s (first claim to last ack, database clock)\nthroughput:  %s jobs/s',
    count(*), $workers,
    round(extract(epoch FROM max(a.finished_at) - min(a.started_at))::numeric, 2),
    round(count(*) / extract(epoch FROM max(a.finished_at) - min(a.started_at))::numeric))
  FROM job_attempts a JOIN jobs j ON j.id = a.job_id
  WHERE j.payload @> '{\"bench\": true}'"
