package main

import (
	"strings"
	"testing"
	"time"
)

func TestBuildReportCapturesFailuresDuplicatesAndDrainTime(t *testing.T) {
	started := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
	finished := started.Add(5 * time.Second)
	results := []result{
		{RequestID: "one", FinalStatus: "ready", AcceptLatency: 10 * time.Millisecond, ReadyLatency: time.Second, AcceptedAt: started.Add(time.Second)},
		{RequestID: "one", FinalStatus: "ready", AcceptLatency: 20 * time.Millisecond, ReadyLatency: 2 * time.Second, AcceptedAt: started.Add(2 * time.Second)},
		{FinalStatus: "timeout", AcceptLatency: 30 * time.Millisecond, AcceptedAt: started.Add(3 * time.Second)},
	}

	value := buildReport("burst", "broker", started, finished, "http://api", 2, 0, 2, queueMetrics{}, results)
	if value.Successes != 2 || value.Failures != 1 {
		t.Fatalf("successes=%d failures=%d", value.Successes, value.Failures)
	}
	if value.LostRequests != 1 {
		t.Fatalf("lost=%d, want 1", value.LostRequests)
	}
	if value.DuplicateIDs != 1 {
		t.Fatalf("duplicates=%d, want 1", value.DuplicateIDs)
	}
	if value.DrainTime != 2*time.Second {
		t.Fatalf("drain=%s, want 2s", value.DrainTime)
	}
	if value.Acceptance.P95 != 30*time.Millisecond {
		t.Fatalf("acceptance p95=%s", value.Acceptance.P95)
	}
	if !strings.Contains(renderMarkdown(value), "Повторяющихся request ID: 1") {
		t.Fatal("в Markdown нет числа повторов")
	}
}
