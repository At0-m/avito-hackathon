#!/usr/bin/env sh
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
STAMP=${STAMP:-$(date -u +%Y%m%dT%H%M%SZ)}
RESULT_DIR=${RESULT_DIR:-"bench/results/$STAMP-faults"}
PROFILES=${PROFILES:-80}
EVENTS_PER_PROFILE=${EVENTS_PER_PROFILE:-50}
REQUESTS=${REQUESTS:-80}
CONCURRENCY=${CONCURRENCY:-20}

cd "$ROOT_DIR"
mkdir -p "$RESULT_DIR"

if [ "${RESET:-1}" = "1" ]; then
  docker compose down -v --remove-orphans
fi
WORKER_COMMAND_SOURCE=broker docker compose up -d --build --wait --scale worker=3

run_fault_scenario() {
  name=$1
  service=$2
  seed=$3
  outage=$4

  (
    cd backend
    go run ./cmd/bench-data \
      --profiles "$PROFILES" \
      --events-per-profile "$EVENTS_PER_PROFILE" \
      --seed "$seed"
  )

  (
    cd backend
    go run ./cmd/bench-api \
      --scenario "$name" \
      --requests "$REQUESTS" \
      --concurrency "$CONCURRENCY" \
      --seed "$seed" \
      --output "../$RESULT_DIR/$name-api.json"
  ) &
  load_pid=$!

  (
    cd backend
    go run ./cmd/bench-fault \
      --service "$service" \
      --action stop-start \
      --delay 1s \
      --outage "$outage" \
      --compose-file ../docker-compose.yml \
      --output "../$RESULT_DIR/$name-fault.json"
  )

  wait "$load_pid"
}

(
  cd backend
  go run ./cmd/bench-environment --output "../$RESULT_DIR/environment.json"
)

run_fault_scenario worker-outage worker 43 5s
run_fault_scenario redpanda-outage redpanda 44 5s
run_fault_scenario clickhouse-outage clickhouse 45 3s

docker compose ps > "$RESULT_DIR/compose-ps.txt"
docker compose logs --no-color backend worker outbox redpanda > "$RESULT_DIR/services.log"

(
  cd backend
  go run ./cmd/bench-report \
    --output "../$RESULT_DIR/summary.md" \
    ../"$RESULT_DIR"/*-api.json
)

echo "Fault benchmark results: $RESULT_DIR"
