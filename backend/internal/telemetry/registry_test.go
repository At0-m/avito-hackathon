package telemetry

import (
	"bufio"
	"bytes"
	"strings"
	"testing"
)

func render(registry *Registry) string {
	var buffer bytes.Buffer
	writer := bufio.NewWriter(&buffer)
	registry.WriteText(writer)
	_ = writer.Flush()
	return buffer.String()
}

func TestCounterAndGaugeExposition(t *testing.T) {
	registry := NewRegistry()
	counter := registry.CounterVec("test_requests_total", "Requests.", "route", "status_class")
	gauge := registry.GaugeVec("test_backlog", "Backlog.")
	counter.Inc("/api/v1/recaps", "2xx")
	counter.Add(2, "/api/v1/recaps", "2xx")
	counter.Add(-5, "/api/v1/recaps", "2xx")
	gauge.Set(7)

	text := render(registry)
	for _, expected := range []string{
		"# TYPE test_requests_total counter",
		`test_requests_total{route="/api/v1/recaps",status_class="2xx"} 3`,
		"# TYPE test_backlog gauge",
		"test_backlog 7",
	} {
		if !strings.Contains(text, expected) {
			t.Fatalf("expected %q in exposition:\n%s", expected, text)
		}
	}
}

func TestHistogramIsCumulative(t *testing.T) {
	registry := NewRegistry()
	histogram := registry.HistogramVec("test_duration_seconds", "Duration.", []float64{0.1, 1}, "stage")
	histogram.Observe(0.0625, "generate")
	histogram.Observe(0.5, "generate")
	histogram.Observe(4, "generate")

	text := render(registry)
	for _, expected := range []string{
		`test_duration_seconds_bucket{stage="generate",le="0.1"} 1`,
		`test_duration_seconds_bucket{stage="generate",le="1"} 2`,
		`test_duration_seconds_bucket{stage="generate",le="+Inf"} 3`,
		`test_duration_seconds_count{stage="generate"} 3`,
		`test_duration_seconds_sum{stage="generate"} 4.5625`,
	} {
		if !strings.Contains(text, expected) {
			t.Fatalf("expected %q in exposition:\n%s", expected, text)
		}
	}
	if histogram.Count("generate") != 3 {
		t.Fatalf("unexpected count %d", histogram.Count("generate"))
	}
}

func TestLabelValuesAreEscaped(t *testing.T) {
	registry := NewRegistry()
	counter := registry.CounterVec("test_escape_total", "Escape.", "code")
	counter.Inc("a\"b\\c\nd")
	text := render(registry)
	if !strings.Contains(text, `test_escape_total{code="a\"b\\c\nd"} 1`) {
		t.Fatalf("label value is not escaped:\n%s", text)
	}
}

func TestGaugeResetDropsStaleSeries(t *testing.T) {
	registry := NewRegistry()
	gauge := registry.GaugeVec("test_rows", "Rows.", "status")
	gauge.Set(3, "queued")
	gauge.Reset()
	if strings.Contains(render(registry), `status="queued"`) {
		t.Fatal("reset gauge still exports stale series")
	}
}

func TestDuplicateRegistrationPanics(t *testing.T) {
	registry := NewRegistry()
	registry.CounterVec("test_dup_total", "Dup.")
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on duplicate metric")
		}
	}()
	registry.CounterVec("test_dup_total", "Dup.")
}

func TestStatusClass(t *testing.T) {
	cases := map[int]string{200: "2xx", 202: "2xx", 404: "4xx", 503: "5xx", 0: "1xx"}
	for status, expected := range cases {
		if got := StatusClass(status); got != expected {
			t.Fatalf("StatusClass(%d) = %s, expected %s", status, got, expected)
		}
	}
}
