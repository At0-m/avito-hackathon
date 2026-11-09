.PHONY: up down reset check bench-smoke bench-paths bench-matrix bench-faults \
	bench-up bench-down bench-clean bench-status bench-grafana \
	bench-mvp bench-mvp-smoke bench-steady bench-hotkey bench-backlog bench-burst bench-soak

up:
	docker compose up -d --build

down:
	docker compose down

reset:
	docker compose down -v
	docker compose up -d --build

check:
	cd backend && gofmt -w ./cmd ./internal ./pkg && go vet ./... && go test ./...
	cd frontend && npm run check

# Legacy benchmark scripts (dev docker-compose.yml, host Go toolchain).
bench-smoke:
	./scripts/benchmark-smoke.sh

bench-paths:
	./scripts/benchmark-paths.sh

bench-matrix:
	./scripts/benchmark-matrix.sh

bench-faults:
	./scripts/benchmark-faults.sh

# Isolated bench stand (bench/compose.bench.yml, project avito-recap-bench).
# See bench/README.md. Parameters are environment variables, for example
#   STEADY_RATE=10 make bench-steady
bench-up:
	bash bench/run.sh up

bench-down:
	bash bench/run.sh down

bench-clean:
	bash bench/run.sh clean

bench-status:
	bash bench/run.sh status

bench-grafana:
	bash bench/run.sh grafana

bench-mvp-smoke:
	bash bench/run.sh smoke

bench-steady:
	bash bench/run.sh steady

bench-hotkey:
	bash bench/run.sh hotkey

bench-backlog:
	bash bench/run.sh backlog

# The four MVP scenarios one after another; stops at the first failure.
bench-mvp: bench-mvp-smoke bench-steady bench-hotkey bench-backlog

# Manual-only scenarios: not part of bench-mvp, CI or any default target.
bench-burst:
	bash bench/run.sh burst

bench-soak:
	bash bench/run.sh soak
