package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/lib/pq"
)

// soakWindow aggregates one fixed window. Values are NaN (null in JSON) when
// the series is missing, which the report shows as "нет данных".
type soakWindow struct {
	Index int       `json:"index"`
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`

	IngressPerSecond  jsonFloat `json:"ingress_per_second"`
	CreatedPerSecond  jsonFloat `json:"created_per_second"`
	ReadyPerSecond    jsonFloat `json:"ready_per_second"`
	DroppedIterations jsonFloat `json:"dropped_iterations"`
	PostFailures      jsonFloat `json:"post_failures"`
	ServerTTRP95      jsonFloat `json:"server_time_to_ready_p95_seconds"`
	ServerTTRCount    int       `json:"server_time_to_ready_samples"`
	BacklogAvg        jsonFloat `json:"backlog_avg"`
	BacklogMax        jsonFloat `json:"backlog_max"`
	OldestAgeMax      jsonFloat `json:"oldest_backlog_age_max_seconds"`
	Retries           jsonFloat `json:"retries_requeued"`
	FailedAttempts    jsonFloat `json:"failed_attempts"`
	FailedRequests    jsonFloat `json:"failed_requests_delta"`

	Restarts      map[string]jsonFloat `json:"process_restarts_by_job"`
	RSSMax        map[string]jsonFloat `json:"rss_max_bytes_by_job"`
	HeapLiveMax   map[string]jsonFloat `json:"heap_after_gc_max_bytes_by_job"`
	GoroutinesMax map[string]jsonFloat `json:"goroutines_max_by_job"`
	PoolInUseMax  map[string]jsonFloat `json:"pg_pool_in_use_max_by_job"`
	PoolWaitDelta map[string]jsonFloat `json:"pg_pool_wait_seconds_increase_by_job"`
}

type trendFlag struct {
	Metric      string    `json:"metric"`
	First       jsonFloat `json:"first_mean"`
	Last        jsonFloat `json:"last_mean"`
	ChangeRatio jsonFloat `json:"change_ratio"`
	Rising      float64   `json:"share_of_rising_steps"`
	Sustained   bool      `json:"sustained_growth"`
}

type tableSizes struct {
	Start map[string]jsonFloat `json:"start"`
	End   map[string]jsonFloat `json:"end"`
}

type soakResult struct {
	Start             time.Time    `json:"start"`
	End               time.Time    `json:"end"`
	Window            string       `json:"window"`
	CompareWindows    int          `json:"compare_windows"`
	Windows           []soakWindow `json:"windows"`
	Trends            []trendFlag  `json:"trends"`
	TableSizes        tableSizes   `json:"table_sizes_bytes"`
	DatabaseSizeStart jsonFloat    `json:"database_size_start_bytes"`
	DatabaseSizeEnd   jsonFloat    `json:"database_size_end_bytes"`
	Note              string       `json:"note"`
}

func runSoak(args []string) error {
	flags := flag.NewFlagSet("soak", flag.ExitOnError)
	vmURL := flags.String("vm-url", env("VM_URL", "http://victoriametrics:8428"), "VictoriaMetrics URL")
	databaseURL := flags.String("database-url", defaultDatabaseURL(), "PostgreSQL URL")
	testID := flags.String("testid", "", "k6 testid tag")
	var manifests multiFlag
	flags.Var(&manifests, "manifest", "k6 manifest log of the measurement phase")
	startValue := flags.String("start", "", "measurement start (RFC3339); default is the first POST in --manifest")
	endValue := flags.String("end", "", "measurement end (RFC3339); default is start + --duration")
	duration := flags.Duration("duration", 0, "measurement duration when --end is omitted")
	window := flags.Duration("window", 5*time.Minute, "window size")
	compare := flags.Duration("compare", 15*time.Minute, "first/last span to compare")
	outputDir := flags.String("output-dir", "", "directory for soak.json, soak-windows.csv and soak-series.json")
	_ = flags.Parse(args)

	start, err := parseTime(*startValue)
	if err != nil {
		return err
	}
	end, err := parseTime(*endValue)
	if err != nil {
		return err
	}
	if start.IsZero() && len(manifests) > 0 {
		if start, err = manifestStart(manifests); err != nil {
			return err
		}
	}
	if end.IsZero() && *duration > 0 && !start.IsZero() {
		end = start.Add(*duration)
	}
	if start.IsZero() || end.IsZero() || !end.After(start) {
		return fmt.Errorf("--start and --end are required and end must be after start")
	}
	entries, _, err := readManifests(manifests, nil)
	if err != nil {
		return err
	}
	ids := acceptedIDs(entries)
	db, err := openDB(*databaseURL)
	if err != nil {
		return err
	}
	defer db.Close()
	client := newVMClient(*vmURL)

	result := soakResult{
		Start: start, End: end, Window: window.String(),
		CompareWindows: int(math.Max(1, math.Round(compare.Seconds()/window.Seconds()))),
		Note:           "Memory growth is reported as an observation only (baseline run, no threshold). A single RSS peak does not prove a leak.",
	}
	for index := 0; ; index++ {
		windowStart := start.Add(time.Duration(index) * *window)
		windowEnd := windowStart.Add(*window)
		if windowEnd.After(end.Add(time.Second)) {
			break
		}
		value, err := soakWindowAt(client, db, ids, *testID, index, windowStart, windowEnd)
		if err != nil {
			return err
		}
		result.Windows = append(result.Windows, value)
	}
	result.Trends = soakTrends(result.Windows, result.CompareWindows)

	sizesStart, err := client.byLabel(`max by (relation) (recap_bench_relation_size_bytes)`, "relation", start)
	if err != nil {
		return err
	}
	sizesEnd, err := client.byLabel(`max by (relation) (recap_bench_relation_size_bytes)`, "relation", end)
	if err != nil {
		return err
	}
	result.TableSizes.Start = toJSONFloats(sizesStart)
	result.TableSizes.End = toJSONFloats(sizesEnd)
	dbStart, _ := client.scalar(`max(recap_bench_database_size_bytes)`, start)
	dbEnd, _ := client.scalar(`max(recap_bench_database_size_bytes)`, end)
	result.DatabaseSizeStart, result.DatabaseSizeEnd = jsonFloat(dbStart), jsonFloat(dbEnd)

	if err := writeJSON(filepath.Join(*outputDir, "soak.json"), result); err != nil {
		return err
	}
	if err := writeSoakCSV(filepath.Join(*outputDir, "soak-windows.csv"), result.Windows); err != nil {
		return err
	}
	return writeSoakSeries(client, filepath.Join(*outputDir, "soak-series.json"), *testID, start, end)
}

func soakWindowAt(client vmClient, db *sql.DB, ids []string, testID string, index int, start, end time.Time) (soakWindow, error) {
	span := fmt.Sprintf("%ds", int(end.Sub(start).Seconds()))
	seconds := end.Sub(start).Seconds()
	value := soakWindow{Index: index, Start: start, End: end}
	scalar := func(query string) (jsonFloat, error) {
		number, err := client.scalar(query, end)
		return jsonFloat(number), err
	}
	perSecond := func(query string) (jsonFloat, error) {
		number, err := client.scalar(query, end)
		return jsonFloat(number / seconds), err
	}
	var err error
	steps := []struct {
		target *jsonFloat
		query  string
		rate   bool
	}{
		{&value.IngressPerSecond, fmt.Sprintf(`sum(increase(k6_posts_started_total{testid=%q}[%s]))`, testID, span), true},
		{&value.CreatedPerSecond, fmt.Sprintf(`max(increase(recap_bench_requests_created[%s]))`, span), true},
		{&value.ReadyPerSecond, fmt.Sprintf(`max(increase(recap_bench_requests{status="ready"}[%s]))`, span), true},
		{&value.DroppedIterations, fmt.Sprintf(`sum(increase(k6_dropped_iterations_total{testid=%q}[%s]))`, testID, span), false},
		{&value.PostFailures, fmt.Sprintf(`sum(increase(k6_post_failures_total{testid=%q}[%s]))`, testID, span), false},
		{&value.BacklogAvg, fmt.Sprintf(`max(avg_over_time(recap_bench_backlog[%s]))`, span), false},
		{&value.BacklogMax, fmt.Sprintf(`max(max_over_time(recap_bench_backlog[%s]))`, span), false},
		{&value.OldestAgeMax, fmt.Sprintf(`max(max_over_time(recap_bench_oldest_backlog_age_seconds[%s]))`, span), false},
		{&value.Retries, fmt.Sprintf(`sum(increase(recap_worker_attempt_results_total{result="requeued"}[%s]))`, span), false},
		{&value.FailedAttempts, fmt.Sprintf(`sum(increase(recap_worker_attempt_results_total{result=~"failed_.*"}[%s]))`, span), false},
		{&value.FailedRequests, fmt.Sprintf(`max(increase(recap_bench_requests{status="failed"}[%s]))`, span), false},
	}
	for _, step := range steps {
		if step.rate {
			*step.target, err = perSecond(step.query)
		} else {
			*step.target, err = scalar(step.query)
		}
		if err != nil {
			return value, err
		}
	}
	byJob := func(query string) (map[string]jsonFloat, error) {
		values, err := client.byLabel(query, "job", end)
		return toJSONFloats(values), err
	}
	jobQueries := []struct {
		target *map[string]jsonFloat
		query  string
	}{
		{&value.Restarts, fmt.Sprintf(`sum by (job) (changes(process_start_time_seconds[%s]))`, span)},
		{&value.RSSMax, fmt.Sprintf(`max by (job) (max_over_time(process_resident_memory_bytes[%s]))`, span)},
		{&value.HeapLiveMax, fmt.Sprintf(`max by (job) (max_over_time(go_gc_heap_live_bytes[%s]))`, span)},
		{&value.GoroutinesMax, fmt.Sprintf(`max by (job) (max_over_time(go_goroutines[%s]))`, span)},
		{&value.PoolInUseMax, fmt.Sprintf(`max by (job) (max_over_time(go_sql_in_use_connections[%s]))`, span)},
		{&value.PoolWaitDelta, fmt.Sprintf(`sum by (job) (increase(go_sql_wait_duration_seconds_total[%s]))`, span)},
	}
	for _, item := range jobQueries {
		*item.target, err = byJob(item.query)
		if err != nil {
			return value, err
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	var p95 sql.NullFloat64
	err = db.QueryRowContext(ctx, `
		SELECT
			percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM finished_at - created_at)),
			count(*)
		FROM recap_requests
		WHERE id = ANY($1::uuid[]) AND status = 'ready' AND created_at >= $2 AND created_at < $3
	`, pq.Array(ids), start, end).Scan(&p95, &value.ServerTTRCount)
	if err != nil {
		return value, fmt.Errorf("window %d server time to ready: %w", index, err)
	}
	value.ServerTTRP95 = jsonFloat(math.NaN())
	if p95.Valid {
		value.ServerTTRP95 = jsonFloat(p95.Float64)
	}
	return value, nil
}

// soakTrends compares the mean of the first and last N windows and marks a
// sustained growth when the last mean is >10% above the first and at least
// 80% of consecutive window steps rise. It is an observation, not a gate.
func soakTrends(windows []soakWindow, compare int) []trendFlag {
	series := map[string][]float64{}
	add := func(name string, value jsonFloat) { series[name] = append(series[name], float64(value)) }
	jobs := map[string]struct{}{}
	for _, window := range windows {
		for job := range window.RSSMax {
			jobs[job] = struct{}{}
		}
	}
	for _, window := range windows {
		add("ingress_per_second", window.IngressPerSecond)
		add("ready_per_second", window.ReadyPerSecond)
		add("server_time_to_ready_p95_seconds", window.ServerTTRP95)
		add("backlog_avg", window.BacklogAvg)
		add("oldest_backlog_age_max_seconds", window.OldestAgeMax)
		for job := range jobs {
			add("rss_max_bytes:"+job, lookup(window.RSSMax, job))
			add("heap_after_gc_max_bytes:"+job, lookup(window.HeapLiveMax, job))
			add("goroutines_max:"+job, lookup(window.GoroutinesMax, job))
			add("pg_pool_wait_seconds_increase:"+job, lookup(window.PoolWaitDelta, job))
		}
	}
	names := make([]string, 0, len(series))
	for name := range series {
		names = append(names, name)
	}
	sort.Strings(names)
	flags := make([]trendFlag, 0, len(names))
	for _, name := range names {
		flags = append(flags, trendOf(name, series[name], compare))
	}
	return flags
}

func lookup(values map[string]jsonFloat, key string) jsonFloat {
	if value, ok := values[key]; ok {
		return value
	}
	return jsonFloat(math.NaN())
}

func trendOf(name string, values []float64, compare int) trendFlag {
	item := trendFlag{Metric: name, First: jsonFloat(math.NaN()), Last: jsonFloat(math.NaN()), ChangeRatio: jsonFloat(math.NaN())}
	if compare <= 0 || len(values) < 2*compare {
		return item
	}
	first := meanOf(values[:compare])
	last := meanOf(values[len(values)-compare:])
	item.First, item.Last = jsonFloat(first), jsonFloat(last)
	if !math.IsNaN(first) && first != 0 {
		item.ChangeRatio = jsonFloat(last/first - 1)
	}
	rising, steps := 0, 0
	for index := 1; index < len(values); index++ {
		if math.IsNaN(values[index]) || math.IsNaN(values[index-1]) {
			continue
		}
		steps++
		if values[index] > values[index-1] {
			rising++
		}
	}
	if steps > 0 {
		item.Rising = float64(rising) / float64(steps)
	}
	item.Sustained = !math.IsNaN(float64(item.ChangeRatio)) && float64(item.ChangeRatio) > 0.10 && item.Rising >= 0.8
	return item
}

func meanOf(values []float64) float64 {
	sum, count := 0.0, 0
	for _, value := range values {
		if math.IsNaN(value) {
			continue
		}
		sum += value
		count++
	}
	if count == 0 {
		return math.NaN()
	}
	return sum / float64(count)
}

func toJSONFloats(values map[string]float64) map[string]jsonFloat {
	result := make(map[string]jsonFloat, len(values))
	for key, value := range values {
		result[key] = jsonFloat(value)
	}
	return result
}

func writeSoakCSV(path string, windows []soakWindow) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	header := []string{
		"index", "start", "end", "ingress_per_s", "created_per_s", "ready_per_s", "dropped", "post_failures",
		"server_ttr_p95_s", "server_ttr_n", "backlog_avg", "backlog_max", "oldest_age_max_s", "retries",
		"failed_attempts", "failed_requests",
	}
	jobs := []string{"api", "worker", "outbox"}
	for _, job := range jobs {
		header = append(header, "rss_max_"+job, "heap_live_max_"+job, "goroutines_max_"+job, "pool_in_use_max_"+job, "pool_wait_s_"+job, "restarts_"+job)
	}
	if err := writer.Write(header); err != nil {
		return err
	}
	format := func(value jsonFloat) string {
		if math.IsNaN(float64(value)) {
			return ""
		}
		return strconv.FormatFloat(float64(value), 'f', 3, 64)
	}
	for _, window := range windows {
		row := []string{
			strconv.Itoa(window.Index), window.Start.Format(time.RFC3339), window.End.Format(time.RFC3339),
			format(window.IngressPerSecond), format(window.CreatedPerSecond), format(window.ReadyPerSecond),
			format(window.DroppedIterations), format(window.PostFailures), format(window.ServerTTRP95),
			strconv.Itoa(window.ServerTTRCount), format(window.BacklogAvg), format(window.BacklogMax),
			format(window.OldestAgeMax), format(window.Retries), format(window.FailedAttempts), format(window.FailedRequests),
		}
		for _, job := range jobs {
			row = append(row,
				format(lookup(window.RSSMax, job)), format(lookup(window.HeapLiveMax, job)),
				format(lookup(window.GoroutinesMax, job)), format(lookup(window.PoolInUseMax, job)),
				format(lookup(window.PoolWaitDelta, job)), format(lookup(window.Restarts, job)),
			)
		}
		if err := writer.Write(row); err != nil {
			return err
		}
	}
	writer.Flush()
	return writer.Error()
}

// writeSoakSeries stores raw 15-second time series for later plotting.
func writeSoakSeries(client vmClient, path, testID string, start, end time.Time) error {
	queries := map[string]string{
		"ingress_per_second":     fmt.Sprintf(`sum(rate(k6_posts_started_total{testid=%q}[1m]))`, testID),
		"ready_per_second":       `max(rate(recap_bench_requests{status="ready"}[1m]))`,
		"backlog":                `max(recap_bench_backlog)`,
		"oldest_backlog_age":     `max(recap_bench_oldest_backlog_age_seconds)`,
		"rss_worker_max":         `max(process_resident_memory_bytes{job="worker"})`,
		"rss_api":                `max(process_resident_memory_bytes{job="api"})`,
		"rss_outbox":             `max(process_resident_memory_bytes{job="outbox"})`,
		"heap_live_worker_max":   `max(go_gc_heap_live_bytes{job="worker"})`,
		"heap_live_api":          `max(go_gc_heap_live_bytes{job="api"})`,
		"goroutines_api":         `max(go_goroutines{job="api"})`,
		"goroutines_worker_max":  `max(go_goroutines{job="worker"})`,
		"pg_pool_wait_rate_api":  `sum(rate(go_sql_wait_duration_seconds_total{job="api"}[1m]))`,
		"pg_pool_in_use_api":     `max(go_sql_in_use_connections{job="api"})`,
		"database_size_bytes":    `max(recap_bench_database_size_bytes)`,
		"post_accept_202_p95_ms": fmt.Sprintf(`max(k6_post_accept_202_p95{testid=%q})`, testID),
	}
	series := map[string][]point{}
	for name, query := range queries {
		points, err := client.rangeQuery(query, start, end, 15*time.Second)
		if err != nil {
			return err
		}
		series[name] = points
	}
	return writeJSON(path, series)
}
