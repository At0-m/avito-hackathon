package benchmark

import (
	"sort"
	"time"
)

type LatencySummary struct {
	Count int           `json:"count"`
	Min   time.Duration `json:"min"`
	P50   time.Duration `json:"p50"`
	P95   time.Duration `json:"p95"`
	P99   time.Duration `json:"p99"`
	Max   time.Duration `json:"max"`
	Mean  time.Duration `json:"mean"`
}

func Summarize(values []time.Duration) LatencySummary {
	if len(values) == 0 {
		return LatencySummary{}
	}
	ordered := append([]time.Duration(nil), values...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	var total time.Duration
	for _, value := range ordered {
		total += value
	}
	return LatencySummary{
		Count: len(ordered),
		Min:   ordered[0],
		P50:   percentile(ordered, 0.50),
		P95:   percentile(ordered, 0.95),
		P99:   percentile(ordered, 0.99),
		Max:   ordered[len(ordered)-1],
		Mean:  total / time.Duration(len(ordered)),
	}
}

func percentile(ordered []time.Duration, quantile float64) time.Duration {
	if len(ordered) == 0 {
		return 0
	}
	if quantile <= 0 {
		return ordered[0]
	}
	if quantile >= 1 {
		return ordered[len(ordered)-1]
	}
	index := int(float64(len(ordered)-1)*quantile + 0.5)
	if index < 0 {
		index = 0
	}
	if index >= len(ordered) {
		index = len(ordered) - 1
	}
	return ordered[index]
}
