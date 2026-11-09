// Command bench-verify checks and reports bench runs: preflight of the data
// set, waiting for a confirmed backlog or drain, reconciliation of the k6
// manifest with PostgreSQL, metric target checks, burst and soak analysis
// and the final Markdown summary with an exit code.
//
// It runs inside the bench Docker network (PostgreSQL is not published on
// the host).
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

const usage = `usage: bench-verify <command> [flags]

commands:
  preflight    check seeded profiles/events and absence of old work
  wait-queued  wait until exactly N accepted requests are queued
  wait-drain   wait until accepted requests reach ready/failed
  reconcile    join the k6 manifest with PostgreSQL
  metrics      check scrape targets, k6 series and narrative mode in VictoriaMetrics
  burst        analyse the burst scenario time series
  soak         build 5-minute soak windows and first/last comparison
  report       write summary.md and verdict.json and set the exit code
`

func main() {
	log.SetFlags(log.LstdFlags | log.LUTC)
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	command, args := os.Args[1], os.Args[2:]
	var err error
	switch command {
	case "preflight":
		err = runPreflight(args)
	case "wait-queued":
		err = runWaitQueued(args)
	case "wait-drain":
		err = runWaitDrain(args)
	case "reconcile":
		err = runReconcile(args)
	case "metrics":
		err = runMetrics(args)
	case "burst":
		err = runBurst(args)
	case "soak":
		err = runSoak(args)
	case "report":
		err = runReport(args)
	default:
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	if err != nil {
		var failed checkFailed
		if ok := asCheckFailed(err, &failed); ok {
			log.Printf("%s: check failed: %s", command, failed.reason)
			os.Exit(1)
		}
		log.Printf("%s: %v", command, err)
		os.Exit(2)
	}
}

// checkFailed marks a completed check whose result is negative; the output
// file is still written. Other errors mean the check itself could not run.
type checkFailed struct{ reason string }

func (c checkFailed) Error() string { return c.reason }

func asCheckFailed(err error, target *checkFailed) bool {
	value, ok := err.(checkFailed)
	if ok {
		*target = value
	}
	return ok
}

type multiFlag []string

func (m *multiFlag) String() string { return strings.Join(*m, ",") }

func (m *multiFlag) Set(value string) error {
	for _, part := range strings.Split(value, ",") {
		if part = strings.TrimSpace(part); part != "" {
			*m = append(*m, part)
		}
	}
	return nil
}

func env(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func defaultDatabaseURL() string {
	return env("DATABASE_URL", "postgres://avito:avito_password@postgres:5432/avito_recap?sslmode=disable")
}

func openDB(databaseURL string) (*sql.DB, error) {
	db, err := sql.Open("postgres", databaseURL)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(4)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping PostgreSQL: %w", err)
	}
	return db, nil
}

func writeJSON(path string, value interface{}) error {
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	if path == "" || path == "-" {
		fmt.Println(string(encoded))
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, encoded, 0o644)
}

func readJSON(path string, target interface{}) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, target)
}

// loadProfileIDs reads the JSON array written by bench-data --ids-output and
// returns the [offset, offset+count) slice; count<=0 means "to the end".
func loadProfileIDs(path string, offset, count int) ([]string, error) {
	var ids []string
	if err := readJSON(path, &ids); err != nil {
		return nil, fmt.Errorf("read profile IDs: %w", err)
	}
	if offset < 0 || offset > len(ids) {
		return nil, fmt.Errorf("offset %d outside %d profile IDs", offset, len(ids))
	}
	end := len(ids)
	if count > 0 {
		end = offset + count
		if end > len(ids) {
			return nil, fmt.Errorf("range %d+%d exceeds %d profile IDs", offset, count, len(ids))
		}
	}
	return ids[offset:end], nil
}

func parseTime(value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, nil
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05.000Z07:00"} {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("cannot parse time %q (expected RFC3339)", value)
}
