#!/usr/bin/env sh
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
STAMP=${STAMP:-$(date -u +%Y%m%dT%H%M%SZ)}
RESULT_DIR=${RESULT_DIR:-"bench/results/$STAMP"}
PROFILES=${PROFILES:-100}
EVENTS_PER_PROFILE=${EVENTS_PER_PROFILE:-50}
REQUESTS=${REQUESTS:-100}
CONCURRENCY=${CONCURRENCY:-20}

cd "$ROOT_DIR"
mkdir -p "$RESULT_DIR"

if [ "${RESET:-1}" = "1" ]; then
  docker compose down -v --remove-orphans
fi

WORKER_COMMAND_SOURCE=broker docker compose up -d --build --wait --scale worker=3

(
  cd backend
  go run ./cmd/bench-environment --output "../$RESULT_DIR/environment.json"
  go run ./cmd/bench-data \
    --profiles "$PROFILES" \
    --events-per-profile "$EVENTS_PER_PROFILE" \
    --seed 42
  go run ./cmd/bench-api \
    --scenario burst \
    --requests "$REQUESTS" \
    --concurrency "$CONCURRENCY" \
    --seed 42 \
    --output "../$RESULT_DIR/api-burst.json"
)

echo "Benchmark results: $RESULT_DIR"
