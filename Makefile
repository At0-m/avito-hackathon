.PHONY: up down reset check bench-smoke bench-paths bench-matrix bench-faults

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

bench-smoke:
	./scripts/benchmark-smoke.sh

bench-paths:
	./scripts/benchmark-paths.sh

bench-matrix:
	./scripts/benchmark-matrix.sh

bench-faults:
	./scripts/benchmark-faults.sh
