#!/usr/bin/env sh
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
STAMP=${STAMP:-$(date -u +%Y%m%dT%H%M%SZ)}
RESULT_DIR=${RESULT_DIR:-"bench/results/$STAMP-paths"}
PROFILES=${PROFILES:-300}
EVENTS_PER_PROFILE=${EVENTS_PER_PROFILE:-50}
REQUESTS=${REQUESTS:-300}
CONCURRENCY=${CONCURRENCY:-30}
SEED=${SEED:-42}

cd "$ROOT_DIR"
mkdir -p "$RESULT_DIR"

run_path() {
  source=$1
  output_name=$2

  docker compose down -v --remove-orphans
  WORKER_COMMAND_SOURCE="$source" docker compose up -d --build --wait --scale worker=3

  (
    cd backend
    go run ./cmd/bench-environment --output "../$RESULT_DIR/$output_name-environment.json"
    go run ./cmd/bench-data \
      --profiles "$PROFILES" \
      --events-per-profile "$EVENTS_PER_PROFILE" \
      --seed "$SEED"
    go run ./cmd/bench-api \
      --scenario "$output_name" \
      --command-source "$source" \
      --requests "$REQUESTS" \
      --concurrency "$CONCURRENCY" \
      --seed "$SEED" \
      --output "../$RESULT_DIR/$output_name-api.json"
  )
}

run_path database postgres-queue
run_path broker redpanda-command

(
  cd backend
  go run ./cmd/bench-report \
    --output "../$RESULT_DIR/summary.md" \
    ../"$RESULT_DIR"/*-api.json
)

docker compose down -v --remove-orphans

echo "Command-path comparison results: $RESULT_DIR"
