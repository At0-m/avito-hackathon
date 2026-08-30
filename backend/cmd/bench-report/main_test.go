package main

import (
	"strings"
	"testing"
	"time"
)

func TestEvaluateAppliesDecisionGates(t *testing.T) {
	value := evaluate(inputReport{
		Scenario: "burst", Requests: 100, Failures: 0, LostRequests: 0,
		DuplicateIDs: 0, DuplicateDBRows: 0, DrainTime: 10 * time.Second,
		Acceptance: latency{P95: 100 * time.Millisecond},
	}, 150*time.Millisecond, 0.01, 60*time.Second)
	if !passed(value) {
		t.Fatalf("expected passing evaluation: %+v", value)
	}

	value = evaluate(inputReport{
		Scenario: "outage", Requests: 100, Failures: 2, LostRequests: 1,
		DuplicateIDs: 1, DuplicateDBRows: 1, DrainTime: 90 * time.Second,
		Acceptance: latency{P95: 200 * time.Millisecond},
	}, 150*time.Millisecond, 0.01, 60*time.Second)
	if passed(value) {
		t.Fatalf("expected failing evaluation: %+v", value)
	}
	if !strings.Contains(render([]evaluation{value}, 150*time.Millisecond, 0.01, 60*time.Second), "ПРОВЕРИТЬ") {
		t.Fatal("в отчёте нет решения ПРОВЕРИТЬ")
	}
}
