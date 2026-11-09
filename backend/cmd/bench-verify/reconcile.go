package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/lib/pq"
	"recap-personalization/internal/benchmark"
)

// RequestRow is the reconciliation view of recap_requests.
type RequestRow struct {
	ID               string
	ProfileID        string
	Year             int
	Status           string
	AlgorithmVersion string
	RecapID          string
	ErrorCode        string
	Attempts         int
	MaxAttempts      int
	CreatedAt        time.Time
	StartedAt        *time.Time
	FinishedAt       *time.Time
}

// SnapshotRow is the reconciliation view of recaps.
type SnapshotRow struct {
	ID              string
	ProfileID       string
	Year            int
	NarrativeSource string
}

// DBState holds the rows relevant for one manifest.
type DBState struct {
	ByID           map[string]RequestRow
	ByProfile      map[string][]RequestRow
	Snapshots      map[string]SnapshotRow
	SnapshotsByKey map[string]int // profile_id:year -> count
	OutboxCommands map[string]int // aggregate_id -> generation commands
}

type ReconcileOptions struct {
	RunStart        time.Time
	AllowedFailures map[string]bool
}

type UserSummary struct {
	Observed       int                      `json:"observed"`
	Ready          int                      `json:"ready"`
	Failed         int                      `json:"failed"`
	Timeout        int                      `json:"timeout"`
	ContractErrors int                      `json:"contract_errors"`
	PostErrors     int                      `json:"post_errors"`
	Polls          int                      `json:"polls"`
	TimeToReady    benchmark.LatencySummary `json:"time_to_ready"`
}

type ReconcileResult struct {
	GeneratedAt        time.Time      `json:"generated_at"`
	Manifest           manifestStats  `json:"manifest"`
	K6PostsStarted     int            `json:"k6_posts_started"`
	ManifestComplete   bool           `json:"manifest_complete"`
	PostsSent          int            `json:"posts_sent"`
	PostsByScenario    map[string]int `json:"posts_by_scenario"`
	PostsByPhase       map[string]int `json:"posts_by_phase"`
	GeneratorExhausted int            `json:"generator_exhausted"`
	// Iterations started after the quota was complete; no POST, not an error.
	IterationsOverQuota int `json:"iterations_over_quota"`
	Kinds              map[string]int `json:"kinds"`
	HTTPStatuses       map[string]int `json:"http_statuses"`

	// Client outcomes of POST.
	New202        int `json:"new_202"`
	Repeat202     int `json:"repeat_202"`
	Ready200      int `json:"ready_200"`
	Failed202     int `json:"failed_202"`
	Rejected4xx   int `json:"rejected_4xx"`
	Ambiguous     int `json:"ambiguous_transport_or_5xx"`
	ContractError int `json:"contract_errors"`

	// Confirmed work (unique IDs from valid 202 responses).
	AcceptedIDs      int            `json:"accepted_ids"`
	ReadyWithSnap    int            `json:"ready_with_snapshot"`
	ReadyBrokenLink  int            `json:"ready_without_valid_snapshot"`
	Failed           map[string]int `json:"failed_by_code"`
	UnexpectedFailed int            `json:"unexpected_failed"`
	Pending          map[string]int `json:"pending_at_deadline"`
	ConfirmedMissing int            `json:"confirmed_but_missing_in_pg"`
	MissingSample    []string       `json:"missing_sample,omitempty"`
	IdentityMismatch int            `json:"identity_mismatch"`
	Retried          int            `json:"requests_with_retries"`

	// Ambiguous outcomes resolved by business key (profile_id, year).
	AmbiguousResolution map[string]int `json:"ambiguous_resolution"`

	// Snapshot (200) responses.
	SnapshotIDs          int      `json:"snapshot_response_ids"`
	SnapshotIDSample     []string `json:"snapshot_id_sample,omitempty"`
	SnapshotResponseBad  int      `json:"snapshot_responses_without_matching_row"`
	AcceptedIDSample     []string `json:"accepted_id_sample,omitempty"`
	RequestsForProfiles  int      `json:"requests_for_cohort_profiles"`
	SnapshotsForProfiles int      `json:"snapshots_for_cohort_profiles"`
	OutboxCommands       int      `json:"generation_commands_for_accepted_ids"`

	DuplicateRequestGroups  int `json:"duplicate_request_groups"`
	DuplicateSnapshotGroups int `json:"duplicate_snapshot_groups"`

	ServerTimeToReady map[string]benchmark.LatencySummary `json:"server_time_to_ready"`
	QueueWait         map[string]benchmark.LatencySummary `json:"queue_wait_first_claim"`
	NarrativeSources  map[string]int                      `json:"narrative_sources"`
	User              UserSummary                         `json:"user_cohort"`
}

const sampleLimit = 20

// Reconcile classifies manifest entries against database state. It is pure so
// that the classification rules are unit-testable.
func Reconcile(entries []Entry, state DBState, options ReconcileOptions) ReconcileResult {
	result := ReconcileResult{
		GeneratedAt:         time.Now().UTC(),
		PostsByScenario:     map[string]int{},
		PostsByPhase:        map[string]int{},
		Kinds:               map[string]int{},
		HTTPStatuses:        map[string]int{},
		Failed:              map[string]int{},
		Pending:             map[string]int{},
		AmbiguousResolution: map[string]int{},
		ServerTimeToReady:   map[string]benchmark.LatencySummary{},
		QueueWait:           map[string]benchmark.LatencySummary{},
		NarrativeSources:    map[string]int{},
	}
	byID := map[string][]Entry{}
	snapshotIDs := map[string]struct{}{}
	var userReady []time.Duration

	for _, entry := range entries {
		if entry.Kind == KindExhausted {
			result.GeneratorExhausted++
			continue
		}
		if entry.Kind == KindOverQuota {
			result.IterationsOverQuota++
			continue
		}
		result.PostsSent++
		result.PostsByScenario[entry.Scenario]++
		result.PostsByPhase[entry.Phase]++
		result.Kinds[entry.Kind]++
		result.HTTPStatuses[strconv.Itoa(entry.Status)]++
		switch entry.Kind {
		case KindAccepted, KindFailed202:
			if entry.Kind == KindFailed202 {
				result.Failed202++
			}
			if entry.ID != "" {
				byID[entry.ID] = append(byID[entry.ID], entry)
			}
		case KindSnapshot:
			result.Ready200++
			if _, seen := snapshotIDs[entry.ID]; !seen {
				snapshotIDs[entry.ID] = struct{}{}
				if len(result.SnapshotIDSample) < sampleLimit {
					result.SnapshotIDSample = append(result.SnapshotIDSample, entry.ID)
				}
			}
			snapshot, ok := state.Snapshots[entry.ID]
			if !ok || snapshot.ProfileID != entry.ProfileID || snapshot.Year != entry.Year {
				result.SnapshotResponseBad++
			}
		case KindContractError:
			result.ContractError++
		case KindRejected:
			if entry.Status >= 500 {
				result.Ambiguous++
				result.AmbiguousResolution[resolveByKey(state, entry)]++
			} else {
				result.Rejected4xx++
			}
		case KindTransport:
			result.Ambiguous++
			result.AmbiguousResolution[resolveByKey(state, entry)]++
		}
		if entry.User {
			result.User.Observed++
			result.User.Polls += entry.Polls
			switch entry.Outcome {
			case "ready":
				result.User.Ready++
				userReady = append(userReady, time.Duration(entry.ReadyMS*float64(time.Millisecond)))
			case "failed":
				result.User.Failed++
			case "timeout":
				result.User.Timeout++
			case "contract_error":
				result.User.ContractErrors++
			default:
				result.User.PostErrors++
			}
		}
	}
	result.SnapshotIDs = len(snapshotIDs)
	result.User.TimeToReady = benchmark.Summarize(userReady)

	ttr := map[string][]time.Duration{}
	wait := map[string][]time.Duration{}
	ids := make([]string, 0, len(byID))
	for id := range byID {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	result.AcceptedIDs = len(ids)
	for _, id := range ids {
		group := byID[id]
		sort.Slice(group, func(i, j int) bool { return group[i].StartMS < group[j].StartMS })
		first := group[0]
		if len(result.AcceptedIDSample) < sampleLimit {
			result.AcceptedIDSample = append(result.AcceptedIDSample, id)
		}
		result.OutboxCommands += state.OutboxCommands[id]
		row, ok := state.ByID[id]
		if !ok {
			result.ConfirmedMissing++
			result.Repeat202 += len(group) - 1
			result.New202++
			if len(result.MissingSample) < sampleLimit {
				result.MissingSample = append(result.MissingSample, id)
			}
			continue
		}
		if row.ProfileID != first.ProfileID || row.Year != first.Year {
			result.IdentityMismatch++
		}
		// The first 202 for an ID is "new" only when the row was created
		// during this run; otherwise every 202 reused older work.
		if !options.RunStart.IsZero() && row.CreatedAt.Before(options.RunStart.Add(-2*time.Second)) {
			result.Repeat202 += len(group)
		} else {
			result.New202++
			result.Repeat202 += len(group) - 1
		}
		if row.Attempts > 1 {
			result.Retried++
		}
		phase := first.Phase
		if phase == "" {
			phase = "all"
		}
		switch row.Status {
		case "ready":
			snapshot, exists := state.Snapshots[row.RecapID]
			if row.RecapID == "" || !exists || snapshot.ProfileID != row.ProfileID || snapshot.Year != row.Year {
				result.ReadyBrokenLink++
				continue
			}
			result.ReadyWithSnap++
			result.NarrativeSources[snapshot.NarrativeSource]++
			if row.FinishedAt != nil {
				value := row.FinishedAt.Sub(row.CreatedAt)
				ttr[phase] = append(ttr[phase], value)
				ttr["all"] = append(ttr["all"], value)
			}
			if row.StartedAt != nil {
				value := row.StartedAt.Sub(row.CreatedAt)
				wait[phase] = append(wait[phase], value)
				wait["all"] = append(wait["all"], value)
			}
		case "failed":
			result.Failed[row.ErrorCode]++
			if !options.AllowedFailures[row.ErrorCode] {
				result.UnexpectedFailed++
			}
		default:
			result.Pending[row.Status]++
		}
	}
	for phase, values := range ttr {
		result.ServerTimeToReady[phase] = benchmark.Summarize(values)
	}
	for phase, values := range wait {
		result.QueueWait[phase] = benchmark.Summarize(values)
	}

	for _, rows := range state.ByProfile {
		result.RequestsForProfiles += len(rows)
		versions := map[string]int{}
		for _, row := range rows {
			versions[strconv.Itoa(row.Year)+":"+row.AlgorithmVersion]++
		}
		for _, count := range versions {
			if count > 1 {
				result.DuplicateRequestGroups++
			}
		}
	}
	for _, count := range state.SnapshotsByKey {
		result.SnapshotsForProfiles += count
		if count > 1 {
			result.DuplicateSnapshotGroups++
		}
	}
	return result
}

// resolveByKey looks up an ambiguous POST by its business key. A client
// timeout is not a loss by itself: the work may exist and become ready.
func resolveByKey(state DBState, entry Entry) string {
	for _, row := range state.ByProfile[entry.ProfileID] {
		if row.Year == entry.Year {
			return "resolved_" + row.Status
		}
	}
	return "absent_in_pg"
}

func runReconcile(args []string) error {
	flags := flag.NewFlagSet("reconcile", flag.ExitOnError)
	databaseURL := flags.String("database-url", defaultDatabaseURL(), "PostgreSQL URL")
	var manifests, summaries, scenarioList, allowed multiFlag
	flags.Var(&manifests, "manifest", "k6 manifest log (repeatable or comma separated)")
	flags.Var(&summaries, "k6-summary", "k6 summary JSON for the manifest completeness check (repeatable)")
	flags.Var(&scenarioList, "scenario", "only reconcile these k6 scenarios")
	flags.Var(&allowed, "allow-failure", "error codes that are expected for this scenario")
	year := flags.Int("year", 2026, "recap year")
	runStartValue := flags.String("run-start", "", "RFC3339 start of the run; rows created earlier count as reused")
	output := flags.String("output", "", "JSON output path")
	_ = flags.Parse(args)

	scenarios := map[string]bool{}
	for _, name := range scenarioList {
		scenarios[name] = true
	}
	entries, stats, err := readManifests(manifests, scenarios)
	if err != nil {
		return err
	}
	runStart, err := parseTime(*runStartValue)
	if err != nil {
		return err
	}
	db, err := openDB(*databaseURL)
	if err != nil {
		return err
	}
	defer db.Close()

	state, err := loadDBState(db, entries, *year)
	if err != nil {
		return err
	}
	options := ReconcileOptions{RunStart: runStart, AllowedFailures: map[string]bool{}}
	for _, code := range allowed {
		options.AllowedFailures[code] = true
	}
	result := Reconcile(entries, state, options)
	result.Manifest = stats

	result.ManifestComplete = stats.ParseErrors == 0
	if len(summaries) > 0 {
		for _, path := range summaries {
			summary, err := readK6Summary(path)
			if err != nil {
				return err
			}
			result.K6PostsStarted += int(summary.value("posts_started", "count"))
		}
		result.ManifestComplete = result.ManifestComplete && result.K6PostsStarted == result.PostsSent
	}
	return writeJSON(*output, result)
}

func loadDBState(db *sql.DB, entries []Entry, year int) (DBState, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	state := DBState{
		ByID: map[string]RequestRow{}, ByProfile: map[string][]RequestRow{},
		Snapshots: map[string]SnapshotRow{}, SnapshotsByKey: map[string]int{}, OutboxCommands: map[string]int{},
	}
	profiles := profileIDs(entries)
	ids := acceptedIDs(entries)

	const columns = `id::text, profile_id::text, year, status::text, algorithm_version,
		COALESCE(recap_id::text, ''), COALESCE(error_code, ''), attempt_count, max_attempts,
		created_at, started_at, finished_at`
	scanRows := func(rows *sql.Rows) error {
		defer rows.Close()
		for rows.Next() {
			var row RequestRow
			var started, finished sql.NullTime
			if err := rows.Scan(&row.ID, &row.ProfileID, &row.Year, &row.Status, &row.AlgorithmVersion,
				&row.RecapID, &row.ErrorCode, &row.Attempts, &row.MaxAttempts,
				&row.CreatedAt, &started, &finished); err != nil {
				return err
			}
			if started.Valid {
				value := started.Time.UTC()
				row.StartedAt = &value
			}
			if finished.Valid {
				value := finished.Time.UTC()
				row.FinishedAt = &value
			}
			row.CreatedAt = row.CreatedAt.UTC()
			if _, seen := state.ByID[row.ID]; !seen {
				state.ByID[row.ID] = row
				if row.Year == year {
					state.ByProfile[row.ProfileID] = append(state.ByProfile[row.ProfileID], row)
				}
			}
		}
		return rows.Err()
	}

	rows, err := db.QueryContext(ctx, `SELECT `+columns+` FROM recap_requests WHERE profile_id = ANY($1::uuid[]) AND year = $2`, pq.Array(profiles), year)
	if err != nil {
		return state, fmt.Errorf("load requests by profile: %w", err)
	}
	if err := scanRows(rows); err != nil {
		return state, err
	}
	// IDs whose row does not belong to the requested profile/year.
	var unresolved []string
	for _, id := range ids {
		if _, ok := state.ByID[id]; !ok {
			unresolved = append(unresolved, id)
		}
	}
	if len(unresolved) > 0 {
		rows, err := db.QueryContext(ctx, `SELECT `+columns+` FROM recap_requests WHERE id = ANY($1::uuid[])`, pq.Array(unresolved))
		if err != nil {
			return state, fmt.Errorf("load requests by id: %w", err)
		}
		if err := scanRows(rows); err != nil {
			return state, err
		}
	}

	snapshotRows, err := db.QueryContext(ctx, `
		SELECT id::text, profile_id::text, year, narrative_source
		FROM recaps
		WHERE (profile_id = ANY($1::uuid[]) AND year = $2) OR id = ANY($3::uuid[])
	`, pq.Array(profiles), year, pq.Array(ids))
	if err != nil {
		return state, fmt.Errorf("load snapshots: %w", err)
	}
	defer snapshotRows.Close()
	for snapshotRows.Next() {
		var row SnapshotRow
		if err := snapshotRows.Scan(&row.ID, &row.ProfileID, &row.Year, &row.NarrativeSource); err != nil {
			return state, err
		}
		state.Snapshots[row.ID] = row
		if row.Year == year {
			state.SnapshotsByKey[row.ProfileID+":"+strconv.Itoa(row.Year)]++
		}
	}
	if err := snapshotRows.Err(); err != nil {
		return state, err
	}

	outboxRows, err := db.QueryContext(ctx, `
		SELECT aggregate_id::text, count(*)
		FROM outbox_events
		WHERE aggregate_id = ANY($1::uuid[]) AND topic = 'recap.generation-commands.v1'
		GROUP BY aggregate_id
	`, pq.Array(ids))
	if err != nil {
		return state, fmt.Errorf("load outbox commands: %w", err)
	}
	defer outboxRows.Close()
	for outboxRows.Next() {
		var id string
		var count int
		if err := outboxRows.Scan(&id, &count); err != nil {
			return state, err
		}
		state.OutboxCommands[id] = count
	}
	return state, outboxRows.Err()
}

func describeCounts(values map[string]int) string {
	if len(values) == 0 {
		return "0"
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", key, values[key]))
	}
	return strings.Join(parts, ", ")
}
