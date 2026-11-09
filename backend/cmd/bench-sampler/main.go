// Command bench-sampler periodically reads queue state from PostgreSQL and
// exports it in the Prometheus text format for VictoriaMetrics.
//
// One sampler runs per bench stand. A failed sample sets
// recap_bench_sampler_up to 0 and drops the data gauges, so dashboards show a
// gap instead of stale values; recap_bench_sampler_last_success_timestamp_seconds
// tells how old the last good sample is.
package main

import (
	"context"
	"database/sql"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"recap-personalization/internal/telemetry"
)

var relations = []string{
	"recap_requests", "outbox_events", "consumed_events", "recaps",
	"recap_cards", "recap_metrics", "recap_achievements", "recap_explanations",
	"recap_rule_facts", "share_cards", "share_facts", "share_achievements",
}

type gauges struct {
	up            *telemetry.GaugeVec
	lastSuccess   *telemetry.GaugeVec
	duration      *telemetry.GaugeVec
	requests      *telemetry.GaugeVec
	created       *telemetry.GaugeVec
	backlog       *telemetry.GaugeVec
	oldestQueued  *telemetry.GaugeVec
	oldestBacklog *telemetry.GaugeVec
	delayed       *telemetry.GaugeVec
	expired       *telemetry.GaugeVec
	attempts      *telemetry.GaugeVec
	retried       *telemetry.GaugeVec
	failed        *telemetry.GaugeVec
	outbox        *telemetry.GaugeVec
	outboxOldest  *telemetry.GaugeVec
	snapshots     *telemetry.GaugeVec
	relationSize  *telemetry.GaugeVec
	databaseSize  *telemetry.GaugeVec
}

func newGauges(registry *telemetry.Registry) gauges {
	return gauges{
		up:            registry.GaugeVec("recap_bench_sampler_up", "1 when the last PostgreSQL sample succeeded."),
		lastSuccess:   registry.GaugeVec("recap_bench_sampler_last_success_timestamp_seconds", "Unix time of the last successful sample."),
		duration:      registry.GaugeVec("recap_bench_sampler_duration_seconds", "Duration of the last sample."),
		requests:      registry.GaugeVec("recap_bench_requests", "recap_requests rows by status.", "status"),
		created:       registry.GaugeVec("recap_bench_requests_created", "Total recap_requests rows (monotonic: rows are never deleted)."),
		backlog:       registry.GaugeVec("recap_bench_backlog", "queued + processing requests."),
		oldestQueued:  registry.GaugeVec("recap_bench_oldest_queued_age_seconds", "now() - min(created_at) over queued requests; 0 when none."),
		oldestBacklog: registry.GaugeVec("recap_bench_oldest_backlog_age_seconds", "now() - min(created_at) over queued and processing requests; 0 when none."),
		delayed:       registry.GaugeVec("recap_bench_delayed_retries", "Queued requests whose available_at is in the future (retry backoff)."),
		expired:       registry.GaugeVec("recap_bench_expired_leases", "Processing requests whose lease already expired."),
		attempts:      registry.GaugeVec("recap_bench_attempts_sum", "sum(attempt_count) over all requests."),
		retried:       registry.GaugeVec("recap_bench_requests_retried", "Requests with more than one attempt."),
		failed:        registry.GaugeVec("recap_bench_failed", "Failed requests by error code.", "error_code"),
		outbox:        registry.GaugeVec("recap_bench_outbox", "outbox_events rows by status.", "status"),
		outboxOldest:  registry.GaugeVec("recap_bench_outbox_oldest_unpublished_age_seconds", "now() - min(created_at) over unpublished outbox events; 0 when none."),
		snapshots:     registry.GaugeVec("recap_bench_snapshots", "Rows in recaps."),
		relationSize:  registry.GaugeVec("recap_bench_relation_size_bytes", "pg_total_relation_size by table.", "relation"),
		databaseSize:  registry.GaugeVec("recap_bench_database_size_bytes", "pg_database_size of the current database."),
	}
}

func (g gauges) resetData() {
	for _, value := range []*telemetry.GaugeVec{
		g.requests, g.created, g.backlog, g.oldestQueued, g.oldestBacklog, g.delayed, g.expired,
		g.attempts, g.retried, g.failed, g.outbox, g.outboxOldest, g.snapshots, g.relationSize, g.databaseSize,
	} {
		value.Reset()
	}
}

func main() {
	databaseURL := flag.String("database-url", env("DATABASE_URL", "postgres://avito:avito_password@postgres:5432/avito_recap?sslmode=disable"), "PostgreSQL URL")
	listen := flag.String("listen", ":9200", "metrics listen address")
	interval := flag.Duration("interval", 5*time.Second, "sampling interval")
	sizeEvery := flag.Int("size-every", 6, "sample table sizes every N intervals")
	flag.Parse()

	db, err := sql.Open("postgres", *databaseURL)
	if err != nil {
		log.Fatal(err)
	}
	db.SetMaxOpenConns(2)
	defer db.Close()

	registry := telemetry.NewRegistry()
	registry.RegisterProcessCollector()
	values := newGauges(registry)
	values.up.Set(0)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	mux := http.NewServeMux()
	mux.Handle("/metrics", registry.Handler())
	server := &http.Server{Addr: *listen, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		_ = server.Shutdown(context.Background())
	}()
	go func() {
		log.Printf("bench sampler listening on %s, interval %s", *listen, *interval)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	ticker := time.NewTicker(*interval)
	defer ticker.Stop()
	for iteration := 0; ; iteration++ {
		started := time.Now()
		sampleCtx, cancel := context.WithTimeout(ctx, *interval)
		err := sample(sampleCtx, db, values, iteration%max(1, *sizeEvery) == 0)
		cancel()
		values.duration.Set(time.Since(started).Seconds())
		if err != nil {
			log.Printf("sample failed: %v", err)
			values.up.Set(0)
			values.resetData()
		} else {
			values.up.Set(1)
			values.lastSuccess.Set(float64(time.Now().UnixNano()) / 1e9)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func sample(ctx context.Context, db *sql.DB, g gauges, withSizes bool) error {
	var created, backlog, delayed, expired, attempts, retried, snapshots int64
	var oldestQueued, oldestBacklog, outboxOldest float64
	err := db.QueryRowContext(ctx, `
		SELECT
			(SELECT count(*) FROM recap_requests),
			(SELECT count(*) FROM recap_requests WHERE status IN ('queued', 'processing')),
			(SELECT count(*) FROM recap_requests WHERE status = 'queued' AND available_at > CURRENT_TIMESTAMP),
			(SELECT count(*) FROM recap_requests WHERE status = 'processing' AND lease_expires_at <= CURRENT_TIMESTAMP),
			(SELECT COALESCE(sum(attempt_count), 0) FROM recap_requests),
			(SELECT count(*) FROM recap_requests WHERE attempt_count > 1),
			(SELECT count(*) FROM recaps),
			(SELECT COALESCE(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP - min(created_at)), 0)::float8
				FROM recap_requests WHERE status = 'queued'),
			(SELECT COALESCE(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP - min(created_at)), 0)::float8
				FROM recap_requests WHERE status IN ('queued', 'processing')),
			(SELECT COALESCE(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP - min(created_at)), 0)::float8
				FROM outbox_events WHERE status <> 'published')
	`).Scan(&created, &backlog, &delayed, &expired, &attempts, &retried, &snapshots, &oldestQueued, &oldestBacklog, &outboxOldest)
	if err != nil {
		return err
	}
	byStatus, err := groupCounts(ctx, db, `SELECT status::text, count(*) FROM recap_requests GROUP BY status`)
	if err != nil {
		return err
	}
	failed, err := groupCounts(ctx, db, `SELECT COALESCE(error_code, 'unknown'), count(*) FROM recap_requests WHERE status = 'failed' GROUP BY 1`)
	if err != nil {
		return err
	}
	outbox, err := groupCounts(ctx, db, `SELECT status::text, count(*) FROM outbox_events GROUP BY status`)
	if err != nil {
		return err
	}

	g.created.Set(float64(created))
	g.backlog.Set(float64(backlog))
	g.delayed.Set(float64(delayed))
	g.expired.Set(float64(expired))
	g.attempts.Set(float64(attempts))
	g.retried.Set(float64(retried))
	g.snapshots.Set(float64(snapshots))
	g.oldestQueued.Set(oldestQueued)
	g.oldestBacklog.Set(oldestBacklog)
	g.outboxOldest.Set(outboxOldest)
	setGroup(g.requests, byStatus, []string{"queued", "processing", "ready", "failed"})
	g.failed.Reset()
	setGroup(g.failed, failed, nil)
	setGroup(g.outbox, outbox, []string{"pending", "publishing", "published"})

	if withSizes {
		rows, err := db.QueryContext(ctx, `
			SELECT relname, pg_total_relation_size(oid)
			FROM pg_class
			WHERE relkind = 'r' AND relname = ANY(string_to_array($1, ','))
		`, strings.Join(relations, ","))
		if err != nil {
			return err
		}
		for rows.Next() {
			var name string
			var size int64
			if err := rows.Scan(&name, &size); err != nil {
				rows.Close()
				return err
			}
			g.relationSize.Set(float64(size), name)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		if err := rows.Close(); err != nil {
			return err
		}
		var databaseSize int64
		if err := db.QueryRowContext(ctx, `SELECT pg_database_size(current_database())`).Scan(&databaseSize); err != nil {
			return err
		}
		g.databaseSize.Set(float64(databaseSize))
	}
	return nil
}

func groupCounts(ctx context.Context, db *sql.DB, query string) (map[string]int64, error) {
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[string]int64)
	for rows.Next() {
		var key string
		var count int64
		if err := rows.Scan(&key, &count); err != nil {
			return nil, err
		}
		result[key] = count
	}
	return result, rows.Err()
}

// setGroup writes every known key (0 when absent) plus any unexpected keys.
func setGroup(gauge *telemetry.GaugeVec, values map[string]int64, known []string) {
	for _, key := range known {
		gauge.Set(float64(values[key]), key)
	}
	for key, value := range values {
		gauge.Set(float64(value), key)
	}
}

func env(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}
