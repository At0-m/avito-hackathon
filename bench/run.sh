#!/usr/bin/env bash
# Avito Recap bench stand orchestrator. Run from the repository root (or via
# make bench-*). Requires Docker Engine with the Compose v2 plugin, bash,
# GNU date and awk on a Linux host. Go is not required on the host: seeding,
# sampling, verification and reports run in containers inside the bench
# network.
#
#   bench/run.sh up | down | clean | status | grafana
#   bench/run.sh smoke | steady | hotkey | backlog | burst | soak
#
# All load parameters are environment variables; see bench/README.md.
set -euo pipefail

ROOT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT_DIR"

PROJECT=${BENCH_PROJECT:-avito-recap-bench}
COMPOSE_FILE=bench/compose.bench.yml
export BENCH_UID=${BENCH_UID:-$(id -u)}
export BENCH_GID=${BENCH_GID:-$(id -g)}
export BENCH_COMMAND_SOURCE=${BENCH_COMMAND_SOURCE:-broker}
export BENCH_WORKER_RETRY_BASE=${BENCH_WORKER_RETRY_BASE:-250ms}
export BENCH_WORKER_RETRY_MAX=${BENCH_WORKER_RETRY_MAX:-1s}
export BENCH_WORKER_MAX_ATTEMPTS=${BENCH_WORKER_MAX_ATTEMPTS:-3}
export BENCH_WORKER_LEASE_DURATION=${BENCH_WORKER_LEASE_DURATION:-30s}
export BENCH_WORKER_HEARTBEAT_INTERVAL=${BENCH_WORKER_HEARTBEAT_INTERVAL:-10s}
WORKERS=${WORKERS:-3}
YEAR=${YEAR:-2026}
EVENTS_PER_PROFILE=${EVENTS_PER_PROFILE:-50}
DRAIN_DEADLINE=${DRAIN_DEADLINE:-600s}
GRAFANA_PORT=${BENCH_GRAFANA_PORT:-13000}
VM_PORT=${BENCH_VM_PORT:-18428}

DC=(docker compose -p "$PROJECT" -f "$COMPOSE_FILE")
RESULT_DIR=""
RUN_ID=""
CONTAINER_DIR=""
STATS_PID=""

log() { printf '[bench %s] %s\n' "$(date -u +%H:%M:%S)" "$*" >&2; }
now() { date -u +%Y-%m-%dT%H:%M:%S.%3NZ; }
calc_round() { awk "BEGIN { printf \"%d\", ($1) + 0.5 }"; }
calc_floor() { awk "BEGIN { v = ($1); printf \"%d\", (v == int(v) ? v : int(v)) }"; }

mark() {
  local at
  at=$(now)
  echo "$1 $at" >>"$RESULT_DIR/phases.log"
  printf '%s' "$at"
}

# step runs a command, records a failure and continues; the report decides
# the verdict from the files that were (or were not) written.
step() {
  local code=0
  "$@" || code=$?
  if [ "$code" -ne 0 ]; then
    echo "$(now) exit=$code $*" >>"$RESULT_DIR/steps-failed.log"
    log "step failed (exit $code): $*"
  fi
  return 0
}

tools() { "${DC[@]}" --profile tools run --rm --no-deps -T tools "$@"; }

stand_up() {
  log "building and starting the bench stand (project $PROJECT, workers=$WORKERS, source=$BENCH_COMMAND_SOURCE)"
  "${DC[@]}" build
  "${DC[@]}" up -d --wait --scale worker="$WORKERS"
}

stand_down() { "${DC[@]}" --profile tools down --remove-orphans; }

stand_clean() {
  # Only resources of the bench project: its containers, network, named
  # volumes and the two locally built bench images. bench/results is kept.
  "${DC[@]}" --profile tools down -v --remove-orphans
  docker image rm -f avito-recap-bench-app:local avito-recap-bench-tools:local 2>/dev/null || true
}

start_docker_stats() {
  (
    while true; do
      local ts ids
      ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)
      ids=$(docker ps -q --filter "label=com.docker.compose.project=$PROJECT" 2>/dev/null || true)
      if [ -n "$ids" ]; then
        # shellcheck disable=SC2086
        docker stats --no-stream --format '{{json .}}' $ids 2>/dev/null |
          sed "s/^/{\"at\":\"$ts\",\"stats\":/; s/\$/}/" >>"$RESULT_DIR/docker-stats.jsonl" || true
      fi
      sleep "${BENCH_STATS_INTERVAL:-5}"
    done
  ) &
  STATS_PID=$!
}

stop_docker_stats() {
  if [ -n "$STATS_PID" ]; then
    kill "$STATS_PID" 2>/dev/null || true
    wait "$STATS_PID" 2>/dev/null || true
    STATS_PID=""
  fi
}
trap stop_docker_stats EXIT

capture_host() {
  mkdir -p "$RESULT_DIR/host"
  local sha=unknown dirty=unknown
  if git rev-parse --git-dir >/dev/null 2>&1; then
    sha=$(git rev-parse HEAD 2>/dev/null || echo unknown)
    dirty=$(git status --porcelain 2>/dev/null | wc -l | tr -d ' ')
  fi
  printf '{"git_sha":"%s","git_uncommitted_files":"%s","docker":"%s","compose":"%s","kernel":"%s","host_cpus":"%s"}\n' \
    "$sha" "$dirty" "$(docker version --format '{{.Server.Version}}' 2>/dev/null || true)" \
    "$(docker compose version --short 2>/dev/null || true)" "$(uname -r)" "$(nproc 2>/dev/null || echo unknown)" \
    >"$RESULT_DIR/host/host.json"
  echo "GIT_SHA=$sha" >>"$RESULT_DIR/run.env"

  # Effective container environment, allow-list only, secrets redacted.
  local allow='^(WORKER_|OUTBOX_|NARRATIVE_PROVIDER|MISTRAL_|RECAP_POLL_AFTER|SSE_|REDPANDA_|METRICS_ADDR|DB_HOST|CLICKHOUSE_URL|GIN_MODE)'
  local container name
  for container in $(docker ps -q --filter "label=com.docker.compose.project=$PROJECT"); do
    name=$(docker inspect -f '{{.Name}} image={{.Image}} restarts={{.RestartCount}} cpus={{.HostConfig.NanoCpus}} memory={{.HostConfig.Memory}}' "$container")
    echo "[$name]"
    docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "$container" | { grep -E "$allow" || true; } |
      while IFS= read -r line; do
        local key=${line%%=*} value=${line#*=}
        if [[ "$key" =~ (KEY|PASSWORD|TOKEN|SECRET) ]]; then
          if [ -n "$value" ]; then value='<set, redacted>'; else value='<empty>'; fi
        fi
        echo "  $key=$value"
      done
  done >"$RESULT_DIR/host/container-env.txt"
  "${DC[@]}" images >"$RESULT_DIR/host/images.txt" 2>/dev/null || true
}

# prepare_run <scenario> <profiles>: stand up, check targets, seed data,
# preflight and write run.env.
prepare_run() {
  local scenario=$1 profiles=$2
  RUN_ID=${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-$scenario}
  RESULT_DIR="bench/results/$RUN_ID"
  CONTAINER_DIR="/results/$RUN_ID"
  TESTID=${TESTID:-$RUN_ID}
  SEED=${SEED:-$(date +%s)}
  mkdir -p "$RESULT_DIR/logs"
  : >"$RESULT_DIR/phases.log"
  {
    echo "RUN_ID=$RUN_ID"
    echo "SCENARIO=$scenario"
    echo "TESTID=$TESTID"
    echo "SEED=$SEED"
    echo "PROFILES=$profiles"
    echo "EVENTS_PER_PROFILE=$EVENTS_PER_PROFILE"
    echo "YEAR=$YEAR"
    echo "WORKERS=$WORKERS"
    echo "COMMAND_SOURCE=$BENCH_COMMAND_SOURCE"
    echo "NARRATIVE_PROVIDER=disabled"
    echo "NARRATIVE_EXPECT=disabled"
    echo "WORKER_RETRY_BASE=$BENCH_WORKER_RETRY_BASE"
    echo "WORKER_RETRY_MAX=$BENCH_WORKER_RETRY_MAX"
    echo "WORKER_MAX_ATTEMPTS=$BENCH_WORKER_MAX_ATTEMPTS"
    echo "WORKER_LEASE_DURATION=$BENCH_WORKER_LEASE_DURATION"
    echo "WORKER_HEARTBEAT_INTERVAL=$BENCH_WORKER_HEARTBEAT_INTERVAL"
  } >"$RESULT_DIR/run.env"
  log "run $RUN_ID: results in $RESULT_DIR"

  if [ "${BENCH_RESET:-0}" = "1" ]; then
    log "BENCH_RESET=1: removing bench volumes before the run"
    "${DC[@]}" --profile tools down -v --remove-orphans
  fi
  stand_up
  mark stand_ready >/dev/null
  start_docker_stats

  local consumers=-1
  if [ "$BENCH_COMMAND_SOURCE" != "database" ]; then consumers=$WORKERS; fi
  step tools bench-verify metrics --expect "api=1,outbox=1,worker=$WORKERS,sampler=1" \
    --expect-consumers "$consumers" --wait 180s --output "$CONTAINER_DIR/metrics-start.json"

  capture_host
  step tools bench-environment --output "$CONTAINER_DIR/environment.json" \
    --vm-url http://victoriametrics:8428 \
    --host-file "host=$CONTAINER_DIR/host/host.json" \
    --host-file "container_env=$CONTAINER_DIR/host/container-env.txt" \
    --host-file "images=$CONTAINER_DIR/host/images.txt"

  log "seeding $profiles profiles x $EVENTS_PER_PROFILE events (seed $SEED)"
  mark seed_start >/dev/null
  tools bench-data --profiles "$profiles" --events-per-profile "$EVENTS_PER_PROFILE" \
    --year "$YEAR" --seed "$SEED" --ids-output "$CONTAINER_DIR/profiles.json"
  mark seed_end >/dev/null
  step tools bench-verify preflight --ids "$CONTAINER_DIR/profiles.json" --year "$YEAR" \
    --events-per-profile "$EVENTS_PER_PROFILE" --output "$CONTAINER_DIR/preflight.json"
  RUN_START=$(mark run_start)
  echo "RUN_START=$RUN_START" >>"$RESULT_DIR/run.env"
}

# k6_run <label> KEY=VALUE...: one k6 phase; manifest goes to manifest-<label>.log.
k6_run() {
  local label=$1
  shift
  local args=()
  local pair
  for pair in "$@"; do args+=(-e "$pair"); done
  log "k6 phase $label: $*"
  local code=0
  "${DC[@]}" --profile tools run --rm --no-deps -T \
    -e LABEL="$label" -e RESULTS_DIR="$CONTAINER_DIR" -e IDS_FILE="$CONTAINER_DIR/profiles.json" \
    -e YEAR="$YEAR" -e POST_P95_MS="${POST_P95_MS:-150}" -e POST_ERROR_RATE="${POST_ERROR_RATE:-0.01}" \
    ${args[@]+"${args[@]}"} \
    k6 run --quiet --tag "testid=$TESTID" -o experimental-prometheus-rw \
    --log-format json --log-output "file=$CONTAINER_DIR/manifest-$label.log" \
    /scripts/recap.js || code=$?
  echo "$code" >"$RESULT_DIR/k6-$label.exit"
  if [ "$code" -ne 0 ]; then
    log "k6 phase $label exited with $code (99 = k6 thresholds failed); continuing to reconciliation"
  fi
}

wait_drain() {
  local label=$1 since=$2
  shift 2
  step tools bench-verify wait-drain --manifest "$CONTAINER_DIR/manifest-$label.log" --since "$since" \
    --deadline "$DRAIN_DEADLINE" --output "$CONTAINER_DIR/drain-${DRAIN_LABEL:-$label}.json" "$@"
}

reconcile() {
  local label=$1 manifest=$2
  shift 2
  local summaries=()
  if [ -f "$RESULT_DIR/k6-$manifest-summary.json" ]; then
    summaries=(--k6-summary "$CONTAINER_DIR/k6-$manifest-summary.json")
  fi
  step tools bench-verify reconcile --manifest "$CONTAINER_DIR/manifest-$manifest.log" ${summaries[@]+"${summaries[@]}"} \
    --year "$YEAR" --run-start "$RUN_START" --output "$CONTAINER_DIR/reconcile-$label.json" "$@"
}

finish_run() {
  local load_start=$1 load_end=$2
  local consumers=-1
  if [ "$BENCH_COMMAND_SOURCE" != "database" ]; then consumers=$WORKERS; fi
  sleep 6 # let VictoriaMetrics receive the last scrape and remote-write batch
  step tools bench-verify metrics --expect "api=1,outbox=1,worker=$WORKERS,sampler=1" \
    --expect-consumers "$consumers" --testid "$TESTID" --start "$load_start" --end "$load_end" \
    --output "$CONTAINER_DIR/metrics-end.json"
  mark run_end >/dev/null
  stop_docker_stats
  "${DC[@]}" ps -a >"$RESULT_DIR/compose-ps.txt" 2>&1 || true
  "${DC[@]}" logs --no-color --timestamps backend outbox worker sampler >"$RESULT_DIR/logs/services.log" 2>&1 || true
  "${DC[@]}" logs --no-color --timestamps redpanda clickhouse postgres >"$RESULT_DIR/logs/infrastructure.log" 2>&1 || true
  docker ps -a --filter "label=com.docker.compose.project=$PROJECT" \
    --format '{{.Names}}' | while read -r name; do
      docker inspect -f '{{.Name}} restarts={{.RestartCount}} status={{.State.Status}} exit={{.State.ExitCode}}' "$name"
    done >"$RESULT_DIR/restarts.txt" 2>/dev/null || true

  local code=0
  tools bench-verify report --run-dir "$CONTAINER_DIR" \
    --p95 "${POST_P95_MS:-150}ms" --error-rate "${POST_ERROR_RATE:-0.01}" \
    --drain-gate "${BACKLOG_DRAIN_GATE:-60s}" --min-ingress-ratio "${MIN_INGRESS_RATIO:-0.98}" || code=$?
  log "summary:     $RESULT_DIR/summary.md"
  log "verdict:     $RESULT_DIR/verdict.json"
  log "environment: $RESULT_DIR/environment.json"
  log "grafana:     http://127.0.0.1:$GRAFANA_PORT (dashboards in folder 'Avito Recap bench', testid=$TESTID)"
  if [ "$code" -ne 0 ]; then
    log "RESULT: FAILED (report exit $code); the report is saved"
  else
    log "RESULT: PASSED"
  fi
  return "$code"
}

# ---- scenarios -----------------------------------------------------------------

scenario_smoke() {
  local count=${SMOKE_COUNT:-100} rate=${SMOKE_RATE:-5}
  prepare_run smoke "$count"
  { echo "SMOKE_COUNT=$count"; echo "SMOKE_RATE=$rate"; echo "SMOKE_POLL_EVERY_NTH=${SMOKE_POLL_EVERY_NTH:-10}"; } >>"$RESULT_DIR/run.env"
  local start end
  start=$(mark load_start)
  k6_run smoke PHASE=smoke COUNT="$count" RATE="$rate" OFFSET=0 POLL_EVERY_NTH="${SMOKE_POLL_EVERY_NTH:-10}"
  end=$(mark load_end)
  wait_drain smoke "$end"
  mark drained >/dev/null
  reconcile smoke smoke
  finish_run "$start" "$end"
}

scenario_steady() {
  local rate=${STEADY_RATE:-20} warmup=${STEADY_WARMUP_S:-60} duration=${STEADY_DURATION_S:-300}
  local every=${STEADY_USER_EVERY_S:-5}
  local warm_count meas_count users=0
  warm_count=$(calc_round "$rate * $warmup")
  meas_count=$(calc_round "$rate * $duration")
  if awk "BEGIN { exit !($every > 0) }"; then users=$(calc_floor "$duration / $every"); fi
  prepare_run steady $((warm_count + meas_count + users))
  {
    echo "STEADY_RATE=$rate"; echo "STEADY_WARMUP_S=$warmup"; echo "STEADY_DURATION_S=$duration"
    echo "STEADY_USER_EVERY_S=$every"; echo "STEADY_USER_COHORT=$users"
  } >>"$RESULT_DIR/run.env"

  mark warmup_start >/dev/null
  k6_run warmup PHASE=steady-warmup RATE="$rate" DURATION_S="$warmup" OFFSET=0
  local warm_end
  warm_end=$(mark warmup_end)
  wait_drain warmup "$warm_end"
  mark warmup_drained >/dev/null
  reconcile warmup warmup

  local start end
  start=$(mark measure_start)
  k6_run measure PHASE=steady-measure RATE="$rate" DURATION_S="$duration" OFFSET="$warm_count" \
    USER_EVERY_S="$every" USER_OFFSET=$((warm_count + meas_count))
  end=$(mark measure_end)
  wait_drain measure "$end"
  mark drained >/dev/null
  reconcile measure measure
  finish_run "$start" "$end"
}

scenario_hotkey() {
  local vus=${HOTKEY_VUS:-100}
  prepare_run hotkey 1
  echo "HOTKEY_VUS=$vus" >>"$RESULT_DIR/run.env"
  local start
  start=$(mark workers_stop)
  "${DC[@]}" stop worker
  mark workers_stopped >/dev/null
  k6_run hotkey-wave1 PHASE=hotkey VUS="$vus" OFFSET=0
  mark wave1_end >/dev/null
  # Reconcile before the workers return: exactly one request and one command.
  reconcile hotkey-wave1 hotkey-wave1
  local t0
  t0=$(mark workers_start)
  "${DC[@]}" start worker
  DRAIN_LABEL=hotkey wait_drain hotkey-wave1 "$t0"
  mark ready >/dev/null
  k6_run hotkey-wave2 PHASE=hotkey VUS="$vus" OFFSET=0
  local end
  end=$(mark wave2_end)
  reconcile hotkey-wave2 hotkey-wave2
  finish_run "$start" "$end"
}

scenario_backlog() {
  local count=${BACKLOG_COUNT:-1000}
  prepare_run backlog "$count"
  { echo "BACKLOG_COUNT=$count"; echo "BACKLOG_VUS=${BACKLOG_VUS:-50}"; echo "BACKLOG_DRAIN_GATE=${BACKLOG_DRAIN_GATE:-60s}"; } >>"$RESULT_DIR/run.env"
  local start
  start=$(mark workers_stop)
  "${DC[@]}" stop worker
  mark workers_stopped >/dev/null
  k6_run backlog-fill PHASE=backlog-fill COUNT="$count" VUS="${BACKLOG_VUS:-50}" OFFSET=0
  mark fill_end >/dev/null
  reconcile backlog-fill backlog-fill
  step tools bench-verify wait-queued --manifest "$CONTAINER_DIR/manifest-backlog-fill.log" \
    --expect "$count" --timeout 120s --output "$CONTAINER_DIR/queued.json"
  mark backlog_confirmed >/dev/null
  local t0
  t0=$(mark workers_start)
  "${DC[@]}" start worker
  DRAIN_LABEL=backlog wait_drain backlog-fill "$t0" --interval 250ms
  local end
  end=$(mark drained)
  reconcile backlog backlog-fill
  finish_run "$start" "$end"
}

scenario_burst() {
  local base=${BURST_BASE_RATE:-10} peak=${BURST_PEAK_RATE:-100}
  local pre=${BURST_PRE_S:-120} spike=${BURST_SPIKE_S:-10} post=${BURST_POST_S:-180}
  local base_count extra_count=0
  base_count=$(calc_round "$base * ($pre + $spike + $post)")
  if awk "BEGIN { exit !($peak > $base) }"; then extra_count=$(calc_round "($peak - $base) * $spike"); fi
  prepare_run burst $((base_count + extra_count))
  {
    echo "BURST_BASE_RATE=$base"; echo "BURST_PEAK_RATE=$peak"; echo "BURST_PRE_S=$pre"
    echo "BURST_SPIKE_S=$spike"; echo "BURST_POST_S=$post"; echo "BURST_HOLD_S=${BURST_HOLD_S:-30}"
  } >>"$RESULT_DIR/run.env"
  local start end
  start=$(mark load_start)
  k6_run burst PHASE=burst BASE_RATE="$base" PEAK_RATE="$peak" PRE_S="$pre" SPIKE_S="$spike" POST_S="$post" OFFSET=0
  end=$(mark load_end)
  sleep 10 # VictoriaMetrics latency offset + last scrape
  step tools bench-verify burst --manifest "$CONTAINER_DIR/manifest-burst.log" --testid "$TESTID" \
    --pre "${pre}s" --spike "${spike}s" --post "${post}s" --hold "${BURST_HOLD_S:-30}s" \
    --backlog-tolerance "${BURST_BACKLOG_TOLERANCE:-0}" --age-tolerance "${BURST_AGE_TOLERANCE:-0}" \
    --max-baseline-slope "${BURST_MAX_BASELINE_SLOPE:-0.1}" --output "$CONTAINER_DIR/burst.json"
  wait_drain burst "$end"
  mark drained >/dev/null
  reconcile burst burst
  finish_run "$start" "$end"
}

scenario_soak() {
  local rate=${SOAK_RATE:-5} duration=${SOAK_DURATION_S:-3600} warmup=${SOAK_WARMUP_S:-120}
  local every=${SOAK_USER_EVERY_S:-30}
  local warm_count meas_count users=0
  warm_count=$(calc_round "$rate * $warmup")
  meas_count=$(calc_round "$rate * $duration")
  if awk "BEGIN { exit !($every > 0) }"; then users=$(calc_floor "$duration / $every"); fi
  prepare_run soak $((warm_count + meas_count + users))
  {
    echo "SOAK_RATE=$rate"; echo "SOAK_DURATION_S=$duration"; echo "SOAK_WARMUP_S=$warmup"
    echo "SOAK_USER_EVERY_S=$every"; echo "SOAK_WINDOW=${SOAK_WINDOW:-5m}"; echo "SOAK_DRAIN_DEADLINE=${SOAK_DRAIN_DEADLINE:-1800s}"
  } >>"$RESULT_DIR/run.env"

  mark warmup_start >/dev/null
  k6_run soak-warmup PHASE=soak-warmup RATE="$rate" DURATION_S="$warmup" OFFSET=0
  local warm_end
  warm_end=$(mark warmup_end)
  wait_drain soak-warmup "$warm_end"
  reconcile soak-warmup soak-warmup

  local start end
  start=$(mark measure_start)
  k6_run soak-measure PHASE=soak-measure RATE="$rate" DURATION_S="$duration" OFFSET="$warm_count" \
    USER_EVERY_S="$every" USER_OFFSET=$((warm_count + meas_count))
  end=$(mark measure_end)
  DRAIN_DEADLINE=${SOAK_DRAIN_DEADLINE:-1800s} DRAIN_LABEL=soak wait_drain soak-measure "$end" --interval 1s --sample-every 5s
  mark drained >/dev/null
  reconcile soak soak-measure
  sleep 6
  step tools bench-verify soak --manifest "$CONTAINER_DIR/manifest-soak-measure.log" --testid "$TESTID" \
    --duration "${duration}s" --window "${SOAK_WINDOW:-5m}" --compare "${SOAK_COMPARE:-15m}" \
    --output-dir "$CONTAINER_DIR"
  finish_run "$start" "$end"
}

usage() {
  sed -n '2,12p' "$0" | sed 's/^# \{0,1\}//'
}

command=${1:-}
case "$command" in
  up) stand_up ;;
  down) stand_down ;;
  clean) stand_clean ;;
  status) "${DC[@]}" ps ;;
  grafana)
    echo "Grafana:         http://127.0.0.1:$GRAFANA_PORT  (anonymous viewer; admin/\${BENCH_GRAFANA_PASSWORD:-bench})"
    echo "VictoriaMetrics: http://127.0.0.1:$VM_PORT/vmui"
    echo "Remote host:     ssh -L $GRAFANA_PORT:127.0.0.1:$GRAFANA_PORT -L $VM_PORT:127.0.0.1:$VM_PORT <bench-host>"
    ;;
  smoke) scenario_smoke ;;
  steady) scenario_steady ;;
  hotkey) scenario_hotkey ;;
  backlog)
    # Spec: broker mode, three workers, all stopped first.
    export BENCH_COMMAND_SOURCE=${BACKLOG_COMMAND_SOURCE:-broker}
    WORKERS=${BACKLOG_WORKERS:-3}
    scenario_backlog
    ;;
  burst) scenario_burst ;;
  soak) scenario_soak ;;
  *)
    usage
    exit 2
    ;;
esac
