package benchmark

import (
	"testing"
	"time"
)

func TestSummarizePercentiles(t *testing.T) {
	values := make([]time.Duration, 100)
	for index := range values {
		values[index] = time.Duration(index+1) * time.Millisecond
	}
	result := Summarize(values)
	if result.Count != 100 || result.Min != time.Millisecond || result.Max != 100*time.Millisecond {
		t.Fatalf("unexpected range: %+v", result)
	}
	if result.P50 != 51*time.Millisecond || result.P95 != 95*time.Millisecond || result.P99 != 99*time.Millisecond {
		t.Fatalf("unexpected percentiles: %+v", result)
	}
}
