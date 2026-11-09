package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"time"

	"github.com/lib/pq"
)

type queuedResult struct {
	StartedAt       time.Time `json:"started_at"`
	ConfirmedAt     time.Time `json:"confirmed_at,omitempty"`
	Expected        int       `json:"expected"`
	AcceptedIDs     int       `json:"accepted_ids_in_manifest"`
	Queued          int       `json:"queued"`
	Processing      int       `json:"processing"`
	Terminal        int       `json:"terminal"`
	Found           int       `json:"found_in_pg"`
	SystemBacklog   int       `json:"system_backlog"`
	OutboxPending   int       `json:"outbox_unpublished_for_cohort"`
	Confirmed       bool      `json:"confirmed"`
	ManifestLines   int       `json:"manifest_lines"`
	ManifestProblem string    `json:"manifest_problem,omitempty"`
}

// wait-queued confirms that exactly N accepted cohort requests sit in the
// queue (status queued, nothing processing) and that the system has no
// other backlog. Workers must be stopped.
func runWaitQueued(args []string) error {
	flags := flag.NewFlagSet("wait-queued", flag.ExitOnError)
	databaseURL := flags.String("database-url", defaultDatabaseURL(), "PostgreSQL URL")
	var manifests multiFlag
	flags.Var(&manifests, "manifest", "k6 manifest log (repeatable or comma separated)")
	expected := flags.Int("expect", 1000, "expected queued requests")
	timeout := flags.Duration("timeout", 2*time.Minute, "maximum wait")
	interval := flags.Duration("interval", 500*time.Millisecond, "poll interval")
	output := flags.String("output", "", "JSON output path")
	_ = flags.Parse(args)

	entries, stats, err := readManifests(manifests, nil)
	if err != nil {
		return err
	}
	ids := acceptedIDs(entries)
	result := queuedResult{StartedAt: time.Now().UTC(), Expected: *expected, AcceptedIDs: len(ids), ManifestLines: stats.Lines}
	if len(ids) != *expected {
		result.ManifestProblem = fmt.Sprintf("manifest has %d accepted IDs, expected %d", len(ids), *expected)
	}
	db, err := openDB(*databaseURL)
	if err != nil {
		return err
	}
	defer db.Close()

	deadline := time.Now().Add(*timeout)
	for {
		if err := sampleQueued(db, ids, &result); err != nil {
			return err
		}
		if result.ManifestProblem == "" && result.Found == *expected && result.Queued == *expected &&
			result.Processing == 0 && result.SystemBacklog == *expected {
			result.Confirmed = true
			result.ConfirmedAt = time.Now().UTC()
			break
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(*interval)
	}
	if err := writeJSON(*output, result); err != nil {
		return err
	}
	if !result.Confirmed {
		return checkFailed{reason: fmt.Sprintf("backlog not confirmed: found=%d queued=%d processing=%d system_backlog=%d expected=%d %s",
			result.Found, result.Queued, result.Processing, result.SystemBacklog, *expected, result.ManifestProblem)}
	}
	return nil
}

func sampleQueued(db *sql.DB, ids []string, result *queuedResult) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return db.QueryRowContext(ctx, `
		SELECT
			count(*),
			count(*) FILTER (WHERE status = 'queued'),
			count(*) FILTER (WHERE status = 'processing'),
			count(*) FILTER (WHERE status IN ('ready', 'failed')),
			(SELECT count(*) FROM recap_requests WHERE status IN ('queued', 'processing')),
			(SELECT count(*) FROM outbox_events WHERE aggregate_id = ANY($1::uuid[]) AND status <> 'published')
		FROM recap_requests
		WHERE id = ANY($1::uuid[])
	`, pq.Array(ids)).Scan(&result.Found, &result.Queued, &result.Processing, &result.Terminal, &result.SystemBacklog, &result.OutboxPending)
}

type drainSample struct {
	At       time.Time `json:"at"`
	Ready    int       `json:"ready"`
	Failed   int       `json:"failed"`
	Pending  int       `json:"pending"`
	Outbox   int       `json:"outbox_unpublished"`
	Elapsed  float64   `json:"elapsed_seconds"`
	FoundInP int       `json:"found"`
}

type drainResult struct {
	Since              time.Time     `json:"since"`
	Interval           string        `json:"poll_interval"`
	Deadline           string        `json:"deadline"`
	Expected           int           `json:"expected_ids"`
	FirstReadyAt       *time.Time    `json:"first_ready_at,omitempty"`
	AllTerminalAt      *time.Time    `json:"all_terminal_at,omitempty"`
	OutboxEmptyAt      *time.Time    `json:"outbox_empty_at,omitempty"`
	FirstReadySeconds  float64       `json:"first_ready_seconds,omitempty"`
	DrainSeconds       float64       `json:"drain_seconds,omitempty"`
	OutboxEmptySeconds float64       `json:"outbox_empty_seconds,omitempty"`
	Completed          bool          `json:"completed"`
	TimedOut           bool          `json:"timed_out"`
	Final              drainSample   `json:"final"`
	Samples            []drainSample `json:"samples,omitempty"`
	ResolutionSeconds  float64       `json:"observation_resolution_seconds"`
}

// wait-drain polls until every accepted ID from the manifests is ready or
// failed. Missing IDs never become terminal and keep the wait open until the
// deadline; reconciliation reports them as lost.
func runWaitDrain(args []string) error {
	flags := flag.NewFlagSet("wait-drain", flag.ExitOnError)
	databaseURL := flags.String("database-url", defaultDatabaseURL(), "PostgreSQL URL")
	var manifests multiFlag
	flags.Var(&manifests, "manifest", "k6 manifest log (repeatable or comma separated)")
	sinceValue := flags.String("since", "", "t0 in RFC3339; default is the start of waiting")
	deadline := flags.Duration("deadline", 10*time.Minute, "maximum wait")
	interval := flags.Duration("interval", 250*time.Millisecond, "poll interval")
	sampleEvery := flags.Duration("sample-every", time.Second, "keep one sample per period in the output")
	output := flags.String("output", "", "JSON output path")
	_ = flags.Parse(args)

	entries, _, err := readManifests(manifests, nil)
	if err != nil {
		return err
	}
	ids := acceptedIDs(entries)
	since, err := parseTime(*sinceValue)
	if err != nil {
		return err
	}
	if since.IsZero() {
		since = time.Now().UTC()
	}
	db, err := openDB(*databaseURL)
	if err != nil {
		return err
	}
	defer db.Close()

	result := drainResult{
		Since: since, Interval: interval.String(), Deadline: deadline.String(), Expected: len(ids),
		ResolutionSeconds: interval.Seconds(),
	}
	stop := time.Now().Add(*deadline)
	var lastKept time.Time
	for {
		current, err := sampleDrain(db, ids)
		if err != nil {
			return err
		}
		now := current.At
		current.Elapsed = now.Sub(since).Seconds()
		if result.FirstReadyAt == nil && current.Ready > 0 {
			at := now
			result.FirstReadyAt = &at
			result.FirstReadySeconds = now.Sub(since).Seconds()
		}
		if result.OutboxEmptyAt == nil && current.Outbox == 0 {
			at := now
			result.OutboxEmptyAt = &at
			result.OutboxEmptySeconds = now.Sub(since).Seconds()
		}
		if now.Sub(lastKept) >= *sampleEvery && len(result.Samples) < 20000 {
			result.Samples = append(result.Samples, current)
			lastKept = now
		}
		result.Final = current
		if current.Pending == 0 && current.FoundInP == len(ids) {
			at := now
			result.AllTerminalAt = &at
			result.DrainSeconds = now.Sub(since).Seconds()
			result.Completed = true
			break
		}
		if time.Now().After(stop) {
			result.TimedOut = true
			break
		}
		time.Sleep(*interval)
	}
	if err := writeJSON(*output, result); err != nil {
		return err
	}
	if !result.Completed {
		return checkFailed{reason: fmt.Sprintf("drain not completed within %s: ready=%d failed=%d pending=%d found=%d of %d",
			*deadline, result.Final.Ready, result.Final.Failed, result.Final.Pending, result.Final.FoundInP, len(ids))}
	}
	return nil
}

func sampleDrain(db *sql.DB, ids []string) (drainSample, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var value drainSample
	err := db.QueryRowContext(ctx, `
		SELECT
			count(*),
			count(*) FILTER (WHERE status = 'ready'),
			count(*) FILTER (WHERE status = 'failed'),
			count(*) FILTER (WHERE status IN ('queued', 'processing')),
			(SELECT count(*) FROM outbox_events WHERE aggregate_id = ANY($1::uuid[]) AND status <> 'published')
		FROM recap_requests
		WHERE id = ANY($1::uuid[])
	`, pq.Array(ids)).Scan(&value.FoundInP, &value.Ready, &value.Failed, &value.Pending, &value.Outbox)
	value.At = time.Now().UTC()
	return value, err
}
