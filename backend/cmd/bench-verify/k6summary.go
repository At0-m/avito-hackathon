package main

import (
	"encoding/json"
	"fmt"
	"os"
)

// k6Summary is the subset of the k6 handleSummary data used by the report.
type k6Summary struct {
	Path    string              `json:"-"`
	Metrics map[string]k6Metric `json:"metrics"`
	State   struct {
		TestRunDurationMS float64 `json:"testRunDurationMs"`
	} `json:"state"`
}

type k6Metric struct {
	Type       string                 `json:"type"`
	Contains   string                 `json:"contains"`
	Values     map[string]float64     `json:"values"`
	Thresholds map[string]k6Threshold `json:"thresholds"`
}

type k6Threshold struct {
	OK bool `json:"ok"`
}

func readK6Summary(path string) (k6Summary, error) {
	var value k6Summary
	data, err := os.ReadFile(path)
	if err != nil {
		return value, fmt.Errorf("read k6 summary: %w", err)
	}
	if err := json.Unmarshal(data, &value); err != nil {
		return value, fmt.Errorf("decode k6 summary %s: %w", path, err)
	}
	value.Path = path
	return value, nil
}

// value returns a metric statistic or 0 when the metric was never emitted
// (for example dropped_iterations in a run without drops).
func (s k6Summary) value(metric, field string) float64 {
	item, ok := s.Metrics[metric]
	if !ok {
		return 0
	}
	return item.Values[field]
}

func (s k6Summary) has(metric string) bool {
	_, ok := s.Metrics[metric]
	return ok
}

// failedThresholds lists threshold expressions that k6 evaluated as failed.
func (s k6Summary) failedThresholds() []string {
	var failed []string
	for name, metric := range s.Metrics {
		for expression, threshold := range metric.Thresholds {
			if !threshold.OK {
				failed = append(failed, name+": "+expression)
			}
		}
	}
	return failed
}

func (s k6Summary) durationSeconds() float64 { return s.State.TestRunDurationMS / 1000 }
