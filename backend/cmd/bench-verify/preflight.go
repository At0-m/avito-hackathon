package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/lib/pq"
)

type preflightResult struct {
	CheckedAt         time.Time `json:"checked_at"`
	Profiles          int       `json:"profiles_expected"`
	ProfilesInPG      int       `json:"profiles_with_year_in_pg"`
	ExistingRequests  int       `json:"existing_requests_for_cohort"`
	ExistingSnapshots int       `json:"existing_snapshots_for_cohort"`
	SystemBacklog     int       `json:"system_backlog"`
	EventsTotal       int64     `json:"clickhouse_events_total"`
	ProfilesWithEvent int64     `json:"clickhouse_profiles_with_events"`
	MinEvents         int64     `json:"clickhouse_min_events_per_profile"`
	MaxEvents         int64     `json:"clickhouse_max_events_per_profile"`
	ExpectedEvents    int       `json:"expected_events_per_profile"`
	Passed            bool      `json:"passed"`
	Problems          []string  `json:"problems,omitempty"`
}

func runPreflight(args []string) error {
	flags := flag.NewFlagSet("preflight", flag.ExitOnError)
	databaseURL := flags.String("database-url", defaultDatabaseURL(), "PostgreSQL URL")
	clickhouseURL := flags.String("clickhouse-url", env("CLICKHOUSE_URL", "http://clickhouse:8123"), "ClickHouse HTTP URL")
	idsFile := flags.String("ids", "", "profile ID JSON file from bench-data --ids-output")
	offset := flags.Int("offset", 0, "first profile index")
	count := flags.Int("count", 0, "number of profiles (0 = all)")
	year := flags.Int("year", 2026, "recap year")
	events := flags.Int("events-per-profile", 50, "expected events per profile")
	allowBacklog := flags.Bool("allow-backlog", false, "do not fail when other requests are still queued/processing")
	output := flags.String("output", "", "JSON output path")
	_ = flags.Parse(args)

	ids, err := loadProfileIDs(*idsFile, *offset, *count)
	if err != nil {
		return err
	}
	db, err := openDB(*databaseURL)
	if err != nil {
		return err
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	result := preflightResult{CheckedAt: time.Now().UTC(), Profiles: len(ids), ExpectedEvents: *events}
	err = db.QueryRowContext(ctx, `
		SELECT
			(SELECT count(*) FROM profile_available_years WHERE profile_id = ANY($1::uuid[]) AND year = $2),
			(SELECT count(*) FROM recap_requests WHERE profile_id = ANY($1::uuid[]) AND year = $2),
			(SELECT count(*) FROM recaps WHERE profile_id = ANY($1::uuid[]) AND year = $2),
			(SELECT count(*) FROM recap_requests WHERE status IN ('queued', 'processing'))
	`, pq.Array(ids), *year).Scan(&result.ProfilesInPG, &result.ExistingRequests, &result.ExistingSnapshots, &result.SystemBacklog)
	if err != nil {
		return fmt.Errorf("query PostgreSQL: %w", err)
	}

	if err := clickhouseEventStats(ctx, *clickhouseURL, ids, *year, &result); err != nil {
		result.Problems = append(result.Problems, "ClickHouse check failed: "+err.Error())
	}

	if result.ProfilesInPG != len(ids) {
		result.Problems = append(result.Problems, fmt.Sprintf("profiles with year %d in PostgreSQL: %d of %d", *year, result.ProfilesInPG, len(ids)))
	}
	if result.ExistingRequests != 0 || result.ExistingSnapshots != 0 {
		result.Problems = append(result.Problems, fmt.Sprintf("cohort is not cold: %d requests and %d snapshots already exist (use a new SEED)", result.ExistingRequests, result.ExistingSnapshots))
	}
	if result.SystemBacklog != 0 && !*allowBacklog {
		result.Problems = append(result.Problems, fmt.Sprintf("system backlog is %d before the run (previous work not drained)", result.SystemBacklog))
	}
	if result.ProfilesWithEvent != int64(len(ids)) {
		result.Problems = append(result.Problems, fmt.Sprintf("profiles with ClickHouse events: %d of %d", result.ProfilesWithEvent, len(ids)))
	}
	if result.MinEvents != int64(*events) || result.MaxEvents != int64(*events) {
		result.Problems = append(result.Problems, fmt.Sprintf("events per profile min=%d max=%d, expected %d", result.MinEvents, result.MaxEvents, *events))
	}
	result.Passed = len(result.Problems) == 0
	if err := writeJSON(*output, result); err != nil {
		return err
	}
	if !result.Passed {
		return checkFailed{reason: strings.Join(result.Problems, "; ")}
	}
	return nil
}

func clickhouseEventStats(ctx context.Context, endpoint string, ids []string, year int, result *preflightResult) error {
	if len(ids) == 0 {
		return nil
	}
	quoted := make([]string, len(ids))
	for index, id := range ids {
		if strings.ContainsAny(id, `'\`) {
			return fmt.Errorf("invalid profile id %q", id)
		}
		quoted[index] = "'" + id + "'"
	}
	query := fmt.Sprintf(`
		SELECT
			sum(c),
			count(),
			min(c),
			max(c)
		FROM (
			SELECT profile_id, count() AS c
			FROM recap.activity_events FINAL
			WHERE profile_id IN (%s) AND toYear(occurred_at) = %d
			GROUP BY profile_id
		)
		FORMAT TabSeparated`, strings.Join(quoted, ","), year)
	// A large IN list exceeds the default max_query_size (256 KiB).
	target := strings.TrimRight(endpoint, "/") + "/?max_query_size=" + url.QueryEscape("268435456")
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, target, strings.NewReader(query))
	if err != nil {
		return err
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(body)))
	}
	fields := strings.Fields(strings.TrimSpace(string(body)))
	if len(fields) != 4 {
		return fmt.Errorf("unexpected ClickHouse response %q", strings.TrimSpace(string(body)))
	}
	values := make([]int64, 4)
	for index, field := range fields {
		values[index], err = strconv.ParseInt(field, 10, 64)
		if err != nil {
			return fmt.Errorf("parse ClickHouse value %q: %w", field, err)
		}
	}
	result.EventsTotal, result.ProfilesWithEvent, result.MinEvents, result.MaxEvents = values[0], values[1], values[2], values[3]
	return nil
}
