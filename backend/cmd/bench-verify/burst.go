package main

import (
	"flag"
	"fmt"
	"math"
	"time"
)

// queueSample is one aligned observation of backlog and oldest backlog age.
type queueSample struct {
	At      time.Time `json:"t"`
	Backlog float64   `json:"backlog"`
	Age     float64   `json:"oldest_age_seconds"`
}

type rangeStats struct {
	Samples int       `json:"samples"`
	Min     jsonFloat `json:"min"`
	Max     jsonFloat `json:"max"`
	Mean    jsonFloat `json:"mean"`
	// Slope of a least-squares line in units per second.
	Slope jsonFloat `json:"slope_per_second"`
}

type recovery struct {
	Recovered       bool       `json:"recovered"`
	At              *time.Time `json:"recovered_at,omitempty"`
	Seconds         jsonFloat  `json:"seconds_after_spike_end"`
	HoldSeconds     float64    `json:"required_hold_seconds"`
	BacklogLimit    float64    `json:"backlog_limit"`
	AgeLimitSeconds float64    `json:"age_limit_seconds"`
	Note            string     `json:"note"`
}

type burstResult struct {
	TestStart  time.Time `json:"test_start"`
	SpikeStart time.Time `json:"spike_start"`
	SpikeEnd   time.Time `json:"spike_end"`
	LoadEnd    time.Time `json:"load_end"`
	Step       string    `json:"step"`

	BaselineWindow    [2]time.Time  `json:"baseline_window"`
	BaselineBacklog   rangeStats    `json:"baseline_backlog"`
	BaselineAge       rangeStats    `json:"baseline_oldest_age_seconds"`
	BaselineReadyRate rangeStats    `json:"baseline_ready_per_second"`
	BaselineIngress   rangeStats    `json:"baseline_ingress_per_second"`
	BaselineGrowing   bool          `json:"baseline_backlog_growing"`
	BaselineMaxSlope  float64       `json:"baseline_max_slope_per_second"`
	PeakBacklog       jsonFloat     `json:"peak_backlog"`
	PeakAge           jsonFloat     `json:"peak_oldest_age_seconds"`
	SpikeIngress      rangeStats    `json:"spike_ingress_per_second"`
	Recovery          recovery      `json:"recovery"`
	MissingSeries     []string      `json:"missing_series,omitempty"`
	Series            []queueSample `json:"series"`
	ReadyRateSeries   []point       `json:"ready_rate_series"`
	IngressSeries     []point       `json:"ingress_series"`
}

func runBurst(args []string) error {
	flags := flag.NewFlagSet("burst", flag.ExitOnError)
	vmURL := flags.String("vm-url", env("VM_URL", "http://victoriametrics:8428"), "VictoriaMetrics URL")
	testID := flags.String("testid", "", "k6 testid tag")
	startValue := flags.String("start", "", "k6 test start (RFC3339); default is the first POST in --manifest")
	var manifests multiFlag
	flags.Var(&manifests, "manifest", "k6 manifest log used to locate the test start")
	pre := flags.Duration("pre", 120*time.Second, "duration before the spike")
	spike := flags.Duration("spike", 10*time.Second, "spike duration")
	post := flags.Duration("post", 180*time.Second, "duration after the spike at the base rate")
	skip := flags.Duration("baseline-skip", 30*time.Second, "ignore the start of the pre window when computing the baseline")
	hold := flags.Duration("hold", 30*time.Second, "time values must stay within the baseline range")
	step := flags.Duration("step", 5*time.Second, "sampling step (sampler interval)")
	backlogTolerance := flags.Float64("backlog-tolerance", 0, "allowed absolute excess over the baseline backlog maximum")
	ageTolerance := flags.Float64("age-tolerance", 0, "allowed absolute excess (s) over the baseline age maximum")
	maxSlope := flags.Float64("max-baseline-slope", 0.1, "baseline backlog slope (jobs/s) treated as sustained growth")
	output := flags.String("output", "", "JSON output path")
	_ = flags.Parse(args)

	start, err := parseTime(*startValue)
	if err != nil {
		return err
	}
	if start.IsZero() && len(manifests) > 0 {
		start, err = manifestStart(manifests)
		if err != nil {
			return err
		}
	}
	if start.IsZero() {
		return fmt.Errorf("--start or --manifest is required")
	}
	client := newVMClient(*vmURL)
	spikeStart := start.Add(*pre)
	spikeEnd := spikeStart.Add(*spike)
	loadEnd := spikeEnd.Add(*post)
	result := burstResult{
		TestStart: start, SpikeStart: spikeStart, SpikeEnd: spikeEnd, LoadEnd: loadEnd, Step: step.String(),
		BaselineMaxSlope: *maxSlope,
	}
	from := start.Add(-*step)
	to := loadEnd.Add(*step)

	backlog, err := client.rangeQuery(`max(recap_bench_backlog)`, from, to, *step)
	if err != nil {
		return err
	}
	age, err := client.rangeQuery(`max(recap_bench_oldest_backlog_age_seconds)`, from, to, *step)
	if err != nil {
		return err
	}
	ready, err := client.rangeQuery(`sum(rate(recap_bench_requests{status="ready"}[30s]))`, from, to, *step)
	if err != nil {
		return err
	}
	ingress, err := client.rangeQuery(fmt.Sprintf(`sum(rate(k6_posts_started_total{testid=%q}[10s]))`, *testID), from, to, *step)
	if err != nil {
		return err
	}
	for name, series := range map[string][]point{"backlog": backlog, "oldest_age": age, "ready_rate": ready, "ingress": ingress} {
		if len(series) == 0 {
			result.MissingSeries = append(result.MissingSeries, name)
		}
	}
	result.Series = alignQueue(backlog, age)
	result.ReadyRateSeries = ready
	result.IngressSeries = ingress

	baselineFrom := start.Add(*skip)
	result.BaselineWindow = [2]time.Time{baselineFrom, spikeStart}
	result.BaselineBacklog = statsOf(backlog, baselineFrom, spikeStart)
	result.BaselineAge = statsOf(age, baselineFrom, spikeStart)
	result.BaselineReadyRate = statsOf(ready, baselineFrom, spikeStart)
	result.BaselineIngress = statsOf(ingress, baselineFrom, spikeStart)
	result.SpikeIngress = statsOf(ingress, spikeStart.Add(*step), spikeEnd.Add(*step))
	result.BaselineGrowing = !math.IsNaN(float64(result.BaselineBacklog.Slope)) && float64(result.BaselineBacklog.Slope) > *maxSlope
	result.PeakBacklog = statsOf(backlog, spikeStart, loadEnd).Max
	result.PeakAge = statsOf(age, spikeStart, loadEnd).Max

	limitBacklog := float64(result.BaselineBacklog.Max) + *backlogTolerance
	limitAge := float64(result.BaselineAge.Max) + *ageTolerance
	result.Recovery = findRecovery(result.Series, spikeEnd, loadEnd, *hold, limitBacklog, limitAge)
	return writeJSON(*output, result)
}

// findRecovery returns the earliest time t >= from at which backlog and age
// are within the limits and stay there for hold, with t+hold <= until (the
// base load still running). Missing baseline data means no verdict.
func findRecovery(samples []queueSample, from, until time.Time, hold time.Duration, limitBacklog, limitAge float64) recovery {
	value := recovery{
		HoldSeconds: hold.Seconds(), BacklogLimit: limitBacklog, AgeLimitSeconds: limitAge,
		Seconds: jsonFloat(math.NaN()),
	}
	if math.IsNaN(limitBacklog) || math.IsNaN(limitAge) {
		value.Note = "baseline range is not available"
		return value
	}
	within := func(sample queueSample) bool {
		return !math.IsNaN(sample.Backlog) && !math.IsNaN(sample.Age) &&
			sample.Backlog <= limitBacklog && sample.Age <= limitAge
	}
	for index, candidate := range samples {
		if candidate.At.Before(from) || !within(candidate) {
			continue
		}
		end := candidate.At.Add(hold)
		if end.After(until) {
			break
		}
		// The hold interval must be fully observed by the series.
		if samples[len(samples)-1].At.Before(end) {
			break
		}
		holds := true
		for _, next := range samples[index:] {
			if next.At.After(end) {
				break
			}
			if !within(next) {
				holds = false
				break
			}
		}
		if holds {
			at := candidate.At
			value.Recovered = true
			value.At = &at
			value.Seconds = jsonFloat(candidate.At.Sub(from).Seconds())
			value.Note = "values returned to the pre-spike range and stayed there for the hold period"
			return value
		}
	}
	value.Note = "no recovery to the pre-spike range within the observation window"
	return value
}

func alignQueue(backlog, age []point) []queueSample {
	ages := make(map[int64]float64, len(age))
	for _, item := range age {
		ages[item.At.Unix()] = item.Value
	}
	result := make([]queueSample, 0, len(backlog))
	for _, item := range backlog {
		value, ok := ages[item.At.Unix()]
		if !ok {
			value = math.NaN()
		}
		result = append(result, queueSample{At: item.At, Backlog: item.Value, Age: value})
	}
	return result
}

func statsOf(series []point, from, to time.Time) rangeStats {
	value := rangeStats{Min: jsonFloat(math.NaN()), Max: jsonFloat(math.NaN()), Mean: jsonFloat(math.NaN()), Slope: jsonFloat(math.NaN())}
	var xs, ys []float64
	for _, item := range series {
		if item.At.Before(from) || item.At.After(to) || math.IsNaN(item.Value) {
			continue
		}
		xs = append(xs, item.At.Sub(from).Seconds())
		ys = append(ys, item.Value)
	}
	value.Samples = len(ys)
	if len(ys) == 0 {
		return value
	}
	minimum, maximum, sum := ys[0], ys[0], 0.0
	for _, y := range ys {
		minimum = math.Min(minimum, y)
		maximum = math.Max(maximum, y)
		sum += y
	}
	value.Min, value.Max, value.Mean = jsonFloat(minimum), jsonFloat(maximum), jsonFloat(sum/float64(len(ys)))
	value.Slope = jsonFloat(slope(xs, ys))
	return value
}

func slope(xs, ys []float64) float64 {
	if len(xs) < 2 {
		return math.NaN()
	}
	var sx, sy, sxx, sxy float64
	n := float64(len(xs))
	for index := range xs {
		sx += xs[index]
		sy += ys[index]
		sxx += xs[index] * xs[index]
		sxy += xs[index] * ys[index]
	}
	denominator := n*sxx - sx*sx
	if denominator == 0 {
		return math.NaN()
	}
	return (n*sxy - sx*sy) / denominator
}
