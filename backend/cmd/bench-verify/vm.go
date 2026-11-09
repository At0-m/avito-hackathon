package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

type vmClient struct {
	baseURL string
	client  *http.Client
}

func newVMClient(baseURL string) vmClient {
	return vmClient{baseURL: strings.TrimRight(baseURL, "/"), client: &http.Client{Timeout: 30 * time.Second}}
}

type vmSeries struct {
	Metric map[string]string `json:"metric"`
	Value  [2]interface{}    `json:"value"`
	Values [][2]interface{}  `json:"values"`
}

type vmResponse struct {
	Status string `json:"status"`
	Error  string `json:"error"`
	Data   struct {
		ResultType string     `json:"resultType"`
		Result     []vmSeries `json:"result"`
	} `json:"data"`
}

func (c vmClient) get(path string, params url.Values) (vmResponse, error) {
	var value vmResponse
	response, err := c.client.Get(c.baseURL + path + "?" + params.Encode())
	if err != nil {
		return value, err
	}
	defer response.Body.Close()
	if err := json.NewDecoder(response.Body).Decode(&value); err != nil {
		return value, fmt.Errorf("decode VictoriaMetrics response: %w", err)
	}
	if value.Status != "success" {
		return value, fmt.Errorf("VictoriaMetrics query failed: %s", value.Error)
	}
	return value, nil
}

// instant evaluates a query at the given time (zero = now).
func (c vmClient) instant(query string, at time.Time) ([]vmSeries, error) {
	params := url.Values{"query": {query}}
	if !at.IsZero() {
		params.Set("time", strconv.FormatFloat(float64(at.UnixNano())/1e9, 'f', 3, 64))
	}
	value, err := c.get("/api/v1/query", params)
	return value.Data.Result, err
}

// scalar returns the first sample value or NaN when the result is empty.
func (c vmClient) scalar(query string, at time.Time) (float64, error) {
	result, err := c.instant(query, at)
	if err != nil {
		return math.NaN(), err
	}
	if len(result) == 0 {
		return math.NaN(), nil
	}
	return sampleValue(result[0].Value[1]), nil
}

// byLabel returns label value -> sample value for a vector result.
func (c vmClient) byLabel(query, label string, at time.Time) (map[string]float64, error) {
	result, err := c.instant(query, at)
	if err != nil {
		return nil, err
	}
	values := make(map[string]float64, len(result))
	for _, item := range result {
		values[item.Metric[label]] = sampleValue(item.Value[1])
	}
	return values, nil
}

type point struct {
	At    time.Time `json:"t"`
	Value float64   `json:"v"`
}

// rangeQuery returns the first series of a range query (empty when absent).
func (c vmClient) rangeQuery(query string, start, end time.Time, step time.Duration) ([]point, error) {
	params := url.Values{
		"query": {query},
		"start": {strconv.FormatInt(start.Unix(), 10)},
		"end":   {strconv.FormatInt(end.Unix(), 10)},
		"step":  {strconv.FormatFloat(step.Seconds(), 'f', -1, 64) + "s"},
	}
	value, err := c.get("/api/v1/query_range", params)
	if err != nil {
		return nil, err
	}
	if len(value.Data.Result) == 0 {
		return nil, nil
	}
	points := make([]point, 0, len(value.Data.Result[0].Values))
	for _, item := range value.Data.Result[0].Values {
		timestamp := sampleValue(item[0])
		points = append(points, point{
			At:    time.Unix(0, int64(timestamp*1e9)).UTC(),
			Value: sampleValue(item[1]),
		})
	}
	return points, nil
}

func sampleValue(raw interface{}) float64 {
	switch value := raw.(type) {
	case float64:
		return value
	case string:
		parsed, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return math.NaN()
		}
		return parsed
	default:
		return math.NaN()
	}
}

// jsonFloat keeps NaN (missing series) as null in JSON output.
type jsonFloat float64

func (f jsonFloat) MarshalJSON() ([]byte, error) {
	if math.IsNaN(float64(f)) || math.IsInf(float64(f), 0) {
		return []byte("null"), nil
	}
	return []byte(strconv.FormatFloat(float64(f), 'f', -1, 64)), nil
}

func (f *jsonFloat) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		*f = jsonFloat(math.NaN())
		return nil
	}
	value, err := strconv.ParseFloat(string(data), 64)
	*f = jsonFloat(value)
	return err
}

type metricsResult struct {
	CheckedAt       time.Time            `json:"checked_at"`
	Window          string               `json:"window,omitempty"`
	TargetsUp       map[string]int       `json:"targets_up"`
	TargetsExpected map[string]int       `json:"targets_expected"`
	StaleSeconds    map[string]jsonFloat `json:"min_scrape_age_seconds"`
	StaleSeries     map[string]int       `json:"stale_target_series"`
	K6Series        int                  `json:"k6_series_for_testid"`
	K6Required      bool                 `json:"k6_series_required"`
	ActiveConsumers int                  `json:"active_broker_consumers"`

	NarrativeModes     map[string]int       `json:"narrative_modes_by_instance_count"`
	NarrativeOutcomes  map[string]jsonFloat `json:"narrative_outcomes_increase"`
	ProviderCalls      jsonFloat            `json:"narrative_provider_calls_increase"`
	NarrativeEffective string               `json:"narrative_effective"`

	WorkerAttempts map[string]jsonFloat `json:"worker_attempt_results_increase,omitempty"`
	ConsumerResets map[string]jsonFloat `json:"broker_consumer_restarts_increase,omitempty"`
	Stats          *windowStats         `json:"window_stats,omitempty"`
	Passed         bool                 `json:"passed"`
	Problems       []string             `json:"problems,omitempty"`
}

// windowStats summarises the server-side view of [start, end] from the
// sampler and process metrics (5 s resolution).
type windowStats struct {
	Seconds             float64              `json:"seconds"`
	PostsStartedPerSec  jsonFloat            `json:"k6_posts_started_per_second"`
	CreatedPerSec       jsonFloat            `json:"new_requests_per_second"`
	ReadyPerSec         jsonFloat            `json:"ready_per_second"`
	BacklogMax          jsonFloat            `json:"backlog_max"`
	OldestBacklogAgeMax jsonFloat            `json:"oldest_backlog_age_max_seconds"`
	OutboxOldestAgeMax  jsonFloat            `json:"outbox_oldest_unpublished_age_max_seconds"`
	DroppedIterations   jsonFloat            `json:"k6_dropped_iterations"`
	CreateOutcomes      map[string]jsonFloat `json:"api_create_outcomes_increase"`
	StageP95Seconds     map[string]jsonFloat `json:"worker_stage_p95_seconds"`
	PoolWaitSeconds     map[string]jsonFloat `json:"pg_pool_wait_seconds_increase_by_job"`
	SamplerUpMin        jsonFloat            `json:"sampler_up_min"`
}

func collectWindowStats(client vmClient, testID string, start, end time.Time) (*windowStats, error) {
	if end.IsZero() {
		end = time.Now().UTC()
	}
	span := end.Sub(start).Seconds()
	if span < 10 {
		span = 10
	}
	window := fmt.Sprintf("%ds", int(math.Ceil(span)))
	stats := &windowStats{Seconds: span}
	scalars := []struct {
		target *jsonFloat
		query  string
		perSec bool
	}{
		{&stats.PostsStartedPerSec, fmt.Sprintf(`sum(increase(k6_posts_started_total{testid=%q}[%s]))`, testID, window), true},
		{&stats.CreatedPerSec, fmt.Sprintf(`max(increase(recap_bench_requests_created[%s]))`, window), true},
		{&stats.ReadyPerSec, fmt.Sprintf(`max(increase(recap_bench_requests{status="ready"}[%s]))`, window), true},
		{&stats.BacklogMax, fmt.Sprintf(`max(max_over_time(recap_bench_backlog[%s]))`, window), false},
		{&stats.OldestBacklogAgeMax, fmt.Sprintf(`max(max_over_time(recap_bench_oldest_backlog_age_seconds[%s]))`, window), false},
		{&stats.OutboxOldestAgeMax, fmt.Sprintf(`max(max_over_time(recap_bench_outbox_oldest_unpublished_age_seconds[%s]))`, window), false},
		{&stats.DroppedIterations, fmt.Sprintf(`sum(increase(k6_dropped_iterations_total{testid=%q}[%s]))`, testID, window), false},
		{&stats.SamplerUpMin, fmt.Sprintf(`min(min_over_time(recap_bench_sampler_up[%s]))`, window), false},
	}
	for _, item := range scalars {
		value, err := client.scalar(item.query, end)
		if err != nil {
			return nil, err
		}
		if item.perSec {
			value /= span
		}
		*item.target = jsonFloat(value)
	}
	outcomes, err := client.byLabel(fmt.Sprintf(`sum by (outcome) (increase(recap_create_outcomes_total{job="api"}[%s]))`, window), "outcome", end)
	if err != nil {
		return nil, err
	}
	stats.CreateOutcomes = toJSONFloats(outcomes)
	stages, err := client.byLabel(fmt.Sprintf(
		`histogram_quantile(0.95, sum by (stage, le) (increase(recap_worker_stage_duration_seconds_bucket{job="worker"}[%s])))`, window), "stage", end)
	if err != nil {
		return nil, err
	}
	stats.StageP95Seconds = toJSONFloats(stages)
	waits, err := client.byLabel(fmt.Sprintf(`sum by (job) (increase(go_sql_wait_duration_seconds_total[%s]))`, window), "job", end)
	if err != nil {
		return nil, err
	}
	stats.PoolWaitSeconds = toJSONFloats(waits)
	return stats, nil
}

// metrics checks that every expected scrape target is up and fresh, that k6
// remote-write series for the test id exist, and records the narrative mode
// and outcomes observed during [start, end].
func runMetrics(args []string) error {
	flags := flag.NewFlagSet("metrics", flag.ExitOnError)
	vmURL := flags.String("vm-url", env("VM_URL", "http://victoriametrics:8428"), "VictoriaMetrics URL")
	var expected multiFlag
	flags.Var(&expected, "expect", "job=count of live targets, e.g. api=1,worker=3 (repeatable)")
	testID := flags.String("testid", "", "k6 testid tag that must have remote-write series")
	consumers := flags.Int("expect-consumers", -1, "required number of workers with an active Redpanda consumer (-1 = do not check)")
	startValue := flags.String("start", "", "window start (RFC3339) for increases")
	endValue := flags.String("end", "", "window end (RFC3339) for increases; default now")
	wait := flags.Duration("wait", 0, "retry until the checks pass or this timeout expires")
	maxAge := flags.Duration("max-scrape-age", 20*time.Second, "maximum age of the last scrape per job")
	output := flags.String("output", "", "JSON output path")
	_ = flags.Parse(args)

	start, err := parseTime(*startValue)
	if err != nil {
		return err
	}
	end, err := parseTime(*endValue)
	if err != nil {
		return err
	}
	expectedTargets := map[string]int{}
	for _, item := range expected {
		job, count, ok := strings.Cut(item, "=")
		if !ok {
			return fmt.Errorf("--expect %q must be job=count", item)
		}
		parsed, err := strconv.Atoi(count)
		if err != nil {
			return fmt.Errorf("--expect %q: %w", item, err)
		}
		expectedTargets[job] = parsed
	}

	client := newVMClient(*vmURL)
	deadline := time.Now().Add(*wait)
	var result metricsResult
	for {
		result, err = collectMetrics(client, expectedTargets, *testID, start, end, *maxAge)
		if err == nil && *consumers >= 0 {
			active, queryErr := client.scalar(`sum(recap_broker_consumer_active{job="worker"})`, end)
			if queryErr != nil {
				err = queryErr
			} else {
				if math.IsNaN(active) {
					active = 0
				}
				result.ActiveConsumers = int(active)
				if int(active) != *consumers {
					result.Problems = append(result.Problems, fmt.Sprintf("active Redpanda consumers: %d, expected %d", int(active), *consumers))
					result.Passed = false
				}
			}
		}
		if err == nil && result.Passed {
			break
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Second)
	}
	if err != nil {
		result.Problems = append(result.Problems, err.Error())
		result.Passed = false
	}
	if writeErr := writeJSON(*output, result); writeErr != nil {
		return writeErr
	}
	if !result.Passed {
		return checkFailed{reason: strings.Join(result.Problems, "; ")}
	}
	return nil
}

func collectMetrics(client vmClient, expected map[string]int, testID string, start, end time.Time, maxAge time.Duration) (metricsResult, error) {
	result := metricsResult{
		CheckedAt: time.Now().UTC(), TargetsUp: map[string]int{}, TargetsExpected: expected,
		StaleSeconds: map[string]jsonFloat{}, NarrativeModes: map[string]int{},
		NarrativeOutcomes: map[string]jsonFloat{}, WorkerAttempts: map[string]jsonFloat{},
		ConsumerResets: map[string]jsonFloat{}, K6Required: testID != "",
	}
	// Only targets scraped within maxAge count as live. A recreated container
	// gets a new IP; the series of the old instance stays visible within the
	// query lookback and must neither be counted nor fail the freshness check.
	fresh := fmt.Sprintf(`(time() - timestamp(up)) <= %g and up == 1`, maxAge.Seconds())
	up, err := client.byLabel(`count by (job) (`+fresh+`)`, "job", end)
	if err != nil {
		return result, err
	}
	ages, err := client.byLabel(`min by (job) (time() - timestamp(up))`, "job", end)
	if err != nil {
		return result, err
	}
	stale, err := client.byLabel(fmt.Sprintf(`count by (job) ((time() - timestamp(up)) > %g)`, maxAge.Seconds()), "job", end)
	if err != nil {
		return result, err
	}
	result.StaleSeries = map[string]int{}
	for job, count := range stale {
		result.StaleSeries[job] = int(count)
	}
	jobs := make([]string, 0, len(expected))
	for job := range expected {
		jobs = append(jobs, job)
	}
	sort.Strings(jobs)
	for _, job := range jobs {
		result.TargetsUp[job] = int(up[job])
		age, ok := ages[job]
		if !ok {
			age = math.NaN()
		}
		result.StaleSeconds[job] = jsonFloat(age)
		if int(up[job]) != expected[job] {
			result.Problems = append(result.Problems, fmt.Sprintf("job %s: %d live targets scraped within %s, expected %d", job, int(up[job]), maxAge, expected[job]))
		}
	}
	if testID != "" {
		series, err := client.scalar(fmt.Sprintf(`count(k6_posts_started_total{testid=%q})`, testID), end)
		if err != nil {
			return result, err
		}
		if !math.IsNaN(series) {
			result.K6Series = int(series)
		}
		if result.K6Series == 0 {
			result.Problems = append(result.Problems, "no k6 remote-write series k6_posts_started_total for testid "+testID)
		}
	}

	modes, err := client.instant(`recap_narrative_provider_mode{job=~"worker|api"} == 1`, end)
	if err != nil {
		return result, err
	}
	for _, item := range modes {
		result.NarrativeModes[item.Metric["job"]+":"+item.Metric["mode"]]++
	}
	window := "5m"
	if !start.IsZero() {
		effectiveEnd := end
		if effectiveEnd.IsZero() {
			effectiveEnd = time.Now()
		}
		window = fmt.Sprintf("%ds", int(math.Max(10, math.Ceil(effectiveEnd.Sub(start).Seconds()))))
	}
	result.Window = window
	outcomes, err := client.byLabel(fmt.Sprintf(`sum by (outcome) (increase(recap_narrative_outcomes_total{job="worker"}[%s]))`, window), "outcome", end)
	if err != nil {
		return result, err
	}
	for key, value := range outcomes {
		result.NarrativeOutcomes[key] = jsonFloat(value)
	}
	calls, err := client.scalar(fmt.Sprintf(`sum(increase(recap_narrative_provider_calls_total{job="worker"}[%s]))`, window), end)
	if err != nil {
		return result, err
	}
	result.ProviderCalls = jsonFloat(calls)
	attempts, err := client.byLabel(fmt.Sprintf(`sum by (result) (increase(recap_worker_attempt_results_total[%s]))`, window), "result", end)
	if err != nil {
		return result, err
	}
	for key, value := range attempts {
		result.WorkerAttempts[key] = jsonFloat(value)
	}
	resets, err := client.byLabel(fmt.Sprintf(`sum by (reason) (increase(recap_broker_consumer_restarts_total[%s]))`, window), "reason", end)
	if err != nil {
		return result, err
	}
	for key, value := range resets {
		result.ConsumerResets[key] = jsonFloat(value)
	}
	result.NarrativeEffective = narrativeVerdict(result.NarrativeModes, outcomes, calls)
	if !start.IsZero() {
		stats, err := collectWindowStats(client, testID, start, end)
		if err != nil {
			return result, err
		}
		result.Stats = stats
	}
	result.Passed = len(result.Problems) == 0
	return result, nil
}

// narrativeVerdict separates the configured template (provider disabled)
// from a template produced as fallback after an enabled provider failed.
func narrativeVerdict(modes map[string]int, outcomes map[string]float64, calls float64) string {
	positive := func(value float64) bool { return !math.IsNaN(value) && value > 0.5 }
	workerMistral := modes["worker:mistral"] > 0
	workerDisabled := modes["worker:disabled"] > 0
	fallback := outcomes["fallback_provider_error"] + outcomes["fallback_validation_error"]
	switch {
	case workerMistral && workerDisabled:
		return "mixed: some workers have the provider enabled"
	case positive(fallback):
		return fmt.Sprintf("template fallback after provider error/validation (%.0f), provider enabled", fallback)
	case workerMistral && positive(outcomes["provider_success"]):
		return "mistral"
	case workerDisabled && !positive(calls):
		return "template (provider disabled by configuration, no provider calls)"
	case workerDisabled && positive(calls):
		return "inconsistent: provider disabled but provider calls observed"
	default:
		return "unknown (no narrative mode series)"
	}
}
