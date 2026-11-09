package main

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"
)

var runStart = time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)

func ptr(value time.Time) *time.Time { return &value }

func readyRow(id, profile string, created time.Time, ttr time.Duration) RequestRow {
	return RequestRow{
		ID: id, ProfileID: profile, Year: 2026, Status: "ready", AlgorithmVersion: "v1", RecapID: id,
		Attempts: 1, MaxAttempts: 3, CreatedAt: created, StartedAt: ptr(created.Add(100 * time.Millisecond)),
		FinishedAt: ptr(created.Add(ttr)),
	}
}

func stateWith(rows []RequestRow, snapshots ...SnapshotRow) DBState {
	state := DBState{
		ByID: map[string]RequestRow{}, ByProfile: map[string][]RequestRow{}, Snapshots: map[string]SnapshotRow{},
		SnapshotsByKey: map[string]int{}, OutboxCommands: map[string]int{},
	}
	for _, row := range rows {
		state.ByID[row.ID] = row
		state.ByProfile[row.ProfileID] = append(state.ByProfile[row.ProfileID], row)
		state.OutboxCommands[row.ID] = 1
	}
	for _, snapshot := range snapshots {
		state.Snapshots[snapshot.ID] = snapshot
		state.SnapshotsByKey[snapshot.ProfileID+":2026"]++
	}
	return state
}

func TestParseManifestLineFromK6JSONLog(t *testing.T) {
	line := `{"level":"info","msg":"MANIFEST {\"s\":\"measure\",\"ph\":\"steady-measure\",\"it\":7,\"p\":\"p1\",\"y\":2026,\"t\":1,\"c\":202,\"id\":\"r1\",\"k\":\"accepted\"}","source":"console","time":"2026-11-01T10:00:00Z"}`
	entry, ok, err := parseManifestLine(line)
	if err != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	if entry.Scenario != "measure" || entry.Iteration != 7 || entry.ID != "r1" || entry.Kind != KindAccepted || entry.Status != 202 {
		t.Fatalf("unexpected entry %+v", entry)
	}
	if _, ok, _ := parseManifestLine(`{"level":"warning","msg":"Request Failed"}`); ok {
		t.Fatal("non-manifest line must be ignored")
	}
	if _, ok, err := parseManifestLine(`MANIFEST {broken`); !ok || err == nil {
		t.Fatal("broken manifest payload must be reported as a parse error")
	}
}

func TestReconcileClassifiesNew202Ready200AndTimeoutResolvedLater(t *testing.T) {
	created := runStart.Add(time.Second)
	entries := []Entry{
		{Scenario: "measure", Phase: "m", ProfileID: "p1", Year: 2026, StartMS: 1, Status: 202, ID: "r1", Kind: KindAccepted},
		// A client timeout: the request still exists in PG and is ready.
		{Scenario: "measure", Phase: "m", ProfileID: "p2", Year: 2026, StartMS: 2, Status: 0, Kind: KindTransport},
		// A ready snapshot returned with 200 (warm hit).
		{Scenario: "measure", Phase: "m", ProfileID: "p3", Year: 2026, StartMS: 3, Status: 200, ID: "r3", Kind: KindSnapshot},
		{Scenario: "measure", Phase: "m", Kind: KindExhausted},
	}
	state := stateWith(
		[]RequestRow{readyRow("r1", "p1", created, 2*time.Second), readyRow("r2", "p2", created, time.Second), readyRow("r3", "p3", runStart.Add(-time.Hour), time.Second)},
		SnapshotRow{ID: "r1", ProfileID: "p1", Year: 2026, NarrativeSource: "template"},
		SnapshotRow{ID: "r2", ProfileID: "p2", Year: 2026, NarrativeSource: "template"},
		SnapshotRow{ID: "r3", ProfileID: "p3", Year: 2026, NarrativeSource: "template"},
	)
	result := Reconcile(entries, state, ReconcileOptions{RunStart: runStart})

	if result.PostsSent != 3 || result.GeneratorExhausted != 1 {
		t.Fatalf("posts=%d exhausted=%d", result.PostsSent, result.GeneratorExhausted)
	}
	if result.New202 != 1 || result.Repeat202 != 0 || result.Ready200 != 1 {
		t.Fatalf("new=%d repeat=%d ready200=%d", result.New202, result.Repeat202, result.Ready200)
	}
	if result.Ambiguous != 1 || result.AmbiguousResolution["resolved_ready"] != 1 {
		t.Fatalf("timeout must resolve to ready by business key: %+v", result.AmbiguousResolution)
	}
	if result.ConfirmedMissing != 0 || result.ReadyWithSnap != 1 || result.AcceptedIDs != 1 {
		t.Fatalf("missing=%d ready=%d accepted=%d", result.ConfirmedMissing, result.ReadyWithSnap, result.AcceptedIDs)
	}
	if result.SnapshotResponseBad != 0 {
		t.Fatal("200 response with an existing snapshot must not be flagged")
	}
	if result.ServerTimeToReady["m"].P95 != 2*time.Second {
		t.Fatalf("server time to ready p95=%s", result.ServerTimeToReady["m"].P95)
	}
}

func TestReconcileTimeoutWithoutRowIsAbsentNotLost(t *testing.T) {
	entries := []Entry{{ProfileID: "p1", Year: 2026, Status: 0, Kind: KindTransport}}
	result := Reconcile(entries, stateWith(nil), ReconcileOptions{RunStart: runStart})
	if result.ConfirmedMissing != 0 || result.AmbiguousResolution["absent_in_pg"] != 1 {
		t.Fatalf("missing=%d resolution=%v", result.ConfirmedMissing, result.AmbiguousResolution)
	}
}

func TestReconcileHotKeyCountsOneNewAndRepeats(t *testing.T) {
	var entries []Entry
	for index := 0; index < 100; index++ {
		entries = append(entries, Entry{Scenario: "hotkey", ProfileID: "hot", Year: 2026, StartMS: int64(index), Status: 202, ID: "r-hot", Kind: KindAccepted})
	}
	row := RequestRow{ID: "r-hot", ProfileID: "hot", Year: 2026, Status: "queued", AlgorithmVersion: "v1", CreatedAt: runStart.Add(time.Second)}
	result := Reconcile(entries, stateWith([]RequestRow{row}), ReconcileOptions{RunStart: runStart})
	if result.AcceptedIDs != 1 || result.New202 != 1 || result.Repeat202 != 99 {
		t.Fatalf("accepted=%d new=%d repeat=%d", result.AcceptedIDs, result.New202, result.Repeat202)
	}
	if result.RequestsForProfiles != 1 || result.OutboxCommands != 1 {
		t.Fatalf("requests=%d commands=%d", result.RequestsForProfiles, result.OutboxCommands)
	}
	if result.Pending["queued"] != 1 {
		t.Fatalf("pending=%v", result.Pending)
	}
}

func TestReconcileFlagsMissingFailedAndPreexistingWork(t *testing.T) {
	entries := []Entry{
		{ProfileID: "p1", Year: 2026, StartMS: 1, Status: 202, ID: "lost", Kind: KindAccepted},
		{ProfileID: "p2", Year: 2026, StartMS: 2, Status: 202, ID: "r2", Kind: KindAccepted},
		{ProfileID: "p3", Year: 2026, StartMS: 3, Status: 202, ID: "r3", Kind: KindAccepted},
	}
	failed := RequestRow{ID: "r2", ProfileID: "p2", Year: 2026, Status: "failed", ErrorCode: "dependency_unavailable", CreatedAt: runStart.Add(time.Second)}
	old := RequestRow{ID: "r3", ProfileID: "p3", Year: 2026, Status: "processing", CreatedAt: runStart.Add(-time.Hour)}
	result := Reconcile(entries, stateWith([]RequestRow{failed, old}), ReconcileOptions{RunStart: runStart})
	if result.ConfirmedMissing != 1 || result.MissingSample[0] != "lost" {
		t.Fatalf("missing=%d sample=%v", result.ConfirmedMissing, result.MissingSample)
	}
	if result.UnexpectedFailed != 1 || result.Failed["dependency_unavailable"] != 1 {
		t.Fatalf("unexpected failed=%d failed=%v", result.UnexpectedFailed, result.Failed)
	}
	if result.Repeat202 != 1 {
		t.Fatalf("202 for work created before the run must count as repeat, got %d", result.Repeat202)
	}
	allowed := Reconcile(entries, stateWith([]RequestRow{failed, old}), ReconcileOptions{
		RunStart: runStart, AllowedFailures: map[string]bool{"dependency_unavailable": true},
	})
	if allowed.UnexpectedFailed != 0 {
		t.Fatal("allowed failure codes must not be counted as unexpected")
	}
}

func TestReconcileDetectsReadyWithoutSnapshot(t *testing.T) {
	entries := []Entry{{ProfileID: "p1", Year: 2026, StartMS: 1, Status: 202, ID: "r1", Kind: KindAccepted}}
	result := Reconcile(entries, stateWith([]RequestRow{readyRow("r1", "p1", runStart, time.Second)}), ReconcileOptions{RunStart: runStart})
	if result.ReadyBrokenLink != 1 || result.ReadyWithSnap != 0 {
		t.Fatalf("broken=%d ready=%d", result.ReadyBrokenLink, result.ReadyWithSnap)
	}
}

func sampleSeries(start time.Time, step time.Duration, backlog []float64) []queueSample {
	values := make([]queueSample, len(backlog))
	for index, value := range backlog {
		values[index] = queueSample{At: start.Add(time.Duration(index) * step), Backlog: value, Age: value}
	}
	return values
}

func TestFindRecoveryRequiresHold(t *testing.T) {
	step := 5 * time.Second
	// Spike ends at index 2; a short dip at index 4 must not count.
	series := sampleSeries(runStart, step, []float64{50, 40, 30, 20, 3, 9, 3, 3, 3, 3, 3, 3, 3, 3})
	value := findRecovery(series, runStart.Add(2*step), runStart.Add(13*step), 30*time.Second, 5, 5)
	if !value.Recovered {
		t.Fatalf("expected recovery: %+v", value)
	}
	if want := 4 * step.Seconds(); float64(value.Seconds) != want {
		t.Fatalf("recovery after %.0fs, want %.0fs", float64(value.Seconds), want)
	}
}

func TestFindRecoveryReportsNoRecovery(t *testing.T) {
	step := 5 * time.Second
	series := sampleSeries(runStart, step, []float64{50, 40, 30, 20, 10, 9, 8, 3, 3, 3})
	value := findRecovery(series, runStart, runStart.Add(9*step), 30*time.Second, 5, 5)
	if value.Recovered || !math.IsNaN(float64(value.Seconds)) {
		t.Fatalf("hold interval does not fit in the window, got %+v", value)
	}
}

func TestNarrativeVerdictSeparatesDisabledFromFallback(t *testing.T) {
	disabled := narrativeVerdict(map[string]int{"worker:disabled": 3}, map[string]float64{"provider_disabled": 100}, 0)
	if disabled != "template (provider disabled by configuration, no provider calls)" {
		t.Fatalf("got %q", disabled)
	}
	fallback := narrativeVerdict(map[string]int{"worker:mistral": 3}, map[string]float64{"fallback_provider_error": 4}, 4)
	if fallback == disabled || fallback[:17] != "template fallback" {
		t.Fatalf("got %q", fallback)
	}
}

func TestTrendMarksSustainedGrowthOnly(t *testing.T) {
	rising := trendOf("rss", []float64{100, 110, 120, 130, 140, 150, 160}, 3)
	if !rising.Sustained {
		t.Fatalf("expected sustained growth: %+v", rising)
	}
	spike := trendOf("rss", []float64{100, 300, 100, 100, 100, 100, 100}, 3)
	if spike.Sustained {
		t.Fatalf("a single peak is not sustained growth: %+v", spike)
	}
}

func TestReportFailsWhenMeasurementIsMissing(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "run.env"), []byte("SCENARIO=smoke\nRUN_ID=test\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	err := runReport([]string{"--run-dir", dir})
	var failed checkFailed
	if err == nil || !asCheckFailed(err, &failed) {
		t.Fatalf("missing inputs must fail the report, got %v", err)
	}
	var verdict reportData
	data, readErr := os.ReadFile(filepath.Join(dir, "verdict.json"))
	if readErr != nil {
		t.Fatalf("verdict.json must be written even on failure: %v", readErr)
	}
	if err := json.Unmarshal(data, &verdict); err != nil || verdict.Passed || len(verdict.Missing) == 0 {
		t.Fatalf("verdict=%+v err=%v", verdict, err)
	}
	if _, err := os.Stat(filepath.Join(dir, "summary.md")); err != nil {
		t.Fatalf("summary.md must be written even on failure: %v", err)
	}
}

func TestJSONFloatKeepsMissingAsNull(t *testing.T) {
	encoded, err := json.Marshal(map[string]jsonFloat{"missing": jsonFloat(math.NaN()), "value": 1.5})
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != `{"missing":null,"value":1.5}` {
		t.Fatalf("got %s", encoded)
	}
}
