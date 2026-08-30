#!/usr/bin/env sh
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
STAMP=${STAMP:-$(date -u +%Y%m%dT%H%M%SZ)}
RESULT_DIR=${RESULT_DIR:-"bench/results/$STAMP-matrix"}
EVENTS_PER_PROFILE=${EVENTS_PER_PROFILE:-50}
CONCURRENCY=${CONCURRENCY:-50}

cd "$ROOT_DIR"
mkdir -p "$RESULT_DIR"

if [ "${RESET:-1}" = "1" ]; then
  docker compose down -v --remove-orphans
fi
WORKER_COMMAND_SOURCE=broker docker compose up -d --build --wait --scale worker=3

(
  cd backend
  go run ./cmd/bench-environment --output "../$RESULT_DIR/environment.json"
)

run_case() {
  name=$1
  seed=$2
  profiles=$3
  requests=$4
  rate=$5

  (
    cd backend
    go run ./cmd/bench-data \
      --profiles "$profiles" \
      --events-per-profile "$EVENTS_PER_PROFILE" \
      --seed "$seed"
    go run ./cmd/bench-api \
      --scenario "$name" \
      --requests "$requests" \
      --concurrency "$CONCURRENCY" \
      --rate "$rate" \
      --seed "$seed" \
      --output "../$RESULT_DIR/$name-api.json"
  )
}

run_case steady-20-rps 50 300 300 20
run_case ramp-50-rps 51 500 500 50
run_case ramp-100-rps 52 1000 1000 100
run_case burst-1000 53 1000 1000 0

(
  cd backend
  go run ./cmd/bench-report \
    --output "../$RESULT_DIR/summary.md" \
    ../"$RESULT_DIR"/*-api.json
)

echo "Benchmark matrix results: $RESULT_DIR"
