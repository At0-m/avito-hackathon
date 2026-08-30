.PHONY: fmt test vet race integration check run worker outbox example bench-data bench-api bench-env bench-fault bench-report

fmt:
	gofmt -w ./cmd ./internal ./pkg

test:
	go test ./...

vet:
	go vet ./...

race:
	go test -race ./...

check: vet test

run:
	go run ./cmd/server

example:
	go run ./cmd/example

worker:
	go run ./cmd/worker

outbox:
	go run ./cmd/outbox

integration:
	go test -race ./internal/repository -run '^TestIntegration' -count=1 -v

bench-data:
	go run ./cmd/bench-data --profiles=100 --events-per-profile=50 --seed=42

bench-api:
	go run ./cmd/bench-api --scenario=burst --requests=100 --concurrency=20 --seed=42

bench-env:
	go run ./cmd/bench-environment --output=../bench/results/environment.json

bench-fault:
	go run ./cmd/bench-fault --service=worker --action=stop-start --compose-file=../docker-compose.yml

bench-report:
	go run ./cmd/bench-report --output=../bench/results/summary.md ../bench/results/*-api.json
