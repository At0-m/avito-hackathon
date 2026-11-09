// Package telemetry exposes process metrics in the Prometheus text format
// (version 0.0.4) without third-party dependencies.
//
// Label values must come from finite sets (route templates, stage names,
// outcome codes). Never pass profile IDs, request IDs, raw URLs or error
// texts as label values.
package telemetry

import (
	"bufio"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

type collector interface {
	write(w *bufio.Writer)
}

// Registry is an ordered set of metric collectors.
type Registry struct {
	mu         sync.Mutex
	collectors []collector
	names      map[string]struct{}
}

// Default holds application metrics shared by API, worker and outbox.
var Default = NewRegistry()

func NewRegistry() *Registry {
	return &Registry{names: make(map[string]struct{})}
}

func (r *Registry) register(name string, value collector) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.names[name]; exists {
		panic("telemetry: duplicate metric " + name)
	}
	r.names[name] = struct{}{}
	r.collectors = append(r.collectors, value)
}

// Register adds a custom collector function that writes complete metric
// families (including HELP/TYPE lines).
func (r *Registry) Register(name string, write func(w *bufio.Writer)) {
	r.register(name, collectorFunc(write))
}

type collectorFunc func(w *bufio.Writer)

func (f collectorFunc) write(w *bufio.Writer) { f(w) }

// WriteText renders all registered metrics.
func (r *Registry) WriteText(w *bufio.Writer) {
	r.mu.Lock()
	collectors := append([]collector(nil), r.collectors...)
	r.mu.Unlock()
	for _, value := range collectors {
		value.write(w)
	}
}

// Handler serves the registry in the Prometheus text format.
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		buffered := bufio.NewWriterSize(w, 32<<10)
		r.WriteText(buffered)
		_ = buffered.Flush()
	})
}

type series struct {
	labelValues []string
	bits        atomic.Uint64
}

func (s *series) add(delta float64) {
	for {
		previous := s.bits.Load()
		next := math.Float64bits(math.Float64frombits(previous) + delta)
		if s.bits.CompareAndSwap(previous, next) {
			return
		}
	}
}

func (s *series) set(value float64) { s.bits.Store(math.Float64bits(value)) }

func (s *series) get() float64 { return math.Float64frombits(s.bits.Load()) }

type vector struct {
	name   string
	help   string
	kind   string
	labels []string
	mu     sync.RWMutex
	values map[string]*series
}

func newVector(name, help, kind string, labels []string) *vector {
	return &vector{name: name, help: help, kind: kind, labels: labels, values: make(map[string]*series)}
}

func (v *vector) series(labelValues []string) *series {
	if len(labelValues) != len(v.labels) {
		panic(fmt.Sprintf("telemetry: %s expects %d label values, got %d", v.name, len(v.labels), len(labelValues)))
	}
	key := strings.Join(labelValues, "\xff")
	v.mu.RLock()
	value, ok := v.values[key]
	v.mu.RUnlock()
	if ok {
		return value
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if value, ok = v.values[key]; ok {
		return value
	}
	value = &series{labelValues: append([]string(nil), labelValues...)}
	v.values[key] = value
	return value
}

func (v *vector) reset() {
	v.mu.Lock()
	v.values = make(map[string]*series)
	v.mu.Unlock()
}

func (v *vector) sorted() []*series {
	v.mu.RLock()
	result := make([]*series, 0, len(v.values))
	for _, value := range v.values {
		result = append(result, value)
	}
	v.mu.RUnlock()
	sort.Slice(result, func(i, j int) bool {
		return strings.Join(result[i].labelValues, "\xff") < strings.Join(result[j].labelValues, "\xff")
	})
	return result
}

func (v *vector) write(w *bufio.Writer) {
	writeHeader(w, v.name, v.help, v.kind)
	for _, value := range v.sorted() {
		writeSample(w, v.name, v.labels, value.labelValues, nil, value.get())
	}
}

// CounterVec is a monotonically increasing counter with labels.
type CounterVec struct{ v *vector }

func (r *Registry) CounterVec(name, help string, labels ...string) *CounterVec {
	value := &CounterVec{v: newVector(name, help, "counter", labels)}
	r.register(name, value.v)
	return value
}

func (c *CounterVec) Inc(labelValues ...string) { c.Add(1, labelValues...) }

func (c *CounterVec) Add(delta float64, labelValues ...string) {
	if delta < 0 || math.IsNaN(delta) {
		return
	}
	c.v.series(labelValues).add(delta)
}

// Value returns the current value; it is intended for tests.
func (c *CounterVec) Value(labelValues ...string) float64 {
	return c.v.series(labelValues).get()
}

// GaugeVec is a settable value with labels.
type GaugeVec struct{ v *vector }

func (r *Registry) GaugeVec(name, help string, labels ...string) *GaugeVec {
	value := &GaugeVec{v: newVector(name, help, "gauge", labels)}
	r.register(name, value.v)
	return value
}

func (g *GaugeVec) Set(value float64, labelValues ...string) { g.v.series(labelValues).set(value) }

func (g *GaugeVec) Add(delta float64, labelValues ...string) { g.v.series(labelValues).add(delta) }

func (g *GaugeVec) Value(labelValues ...string) float64 { return g.v.series(labelValues).get() }

// Reset removes every series so stale values are not exported.
func (g *GaugeVec) Reset() { g.v.reset() }

// HistogramVec is a cumulative histogram with fixed buckets.
type HistogramVec struct {
	name    string
	help    string
	labels  []string
	buckets []float64
	mu      sync.RWMutex
	values  map[string]*histogramSeries
}

type histogramSeries struct {
	labelValues []string
	mu          sync.Mutex
	counts      []uint64
	count       uint64
	sum         float64
}

func (r *Registry) HistogramVec(name, help string, buckets []float64, labels ...string) *HistogramVec {
	ordered := append([]float64(nil), buckets...)
	sort.Float64s(ordered)
	value := &HistogramVec{
		name: name, help: help, labels: labels, buckets: ordered,
		values: make(map[string]*histogramSeries),
	}
	r.register(name, value)
	return value
}

func (h *HistogramVec) Observe(value float64, labelValues ...string) {
	if math.IsNaN(value) {
		return
	}
	if len(labelValues) != len(h.labels) {
		panic(fmt.Sprintf("telemetry: %s expects %d label values, got %d", h.name, len(h.labels), len(labelValues)))
	}
	key := strings.Join(labelValues, "\xff")
	h.mu.RLock()
	item, ok := h.values[key]
	h.mu.RUnlock()
	if !ok {
		h.mu.Lock()
		if item, ok = h.values[key]; !ok {
			item = &histogramSeries{
				labelValues: append([]string(nil), labelValues...),
				counts:      make([]uint64, len(h.buckets)),
			}
			h.values[key] = item
		}
		h.mu.Unlock()
	}
	item.mu.Lock()
	for index, bound := range h.buckets {
		if value <= bound {
			item.counts[index]++
		}
	}
	item.count++
	item.sum += value
	item.mu.Unlock()
}

// Count returns the number of observations; it is intended for tests.
func (h *HistogramVec) Count(labelValues ...string) uint64 {
	key := strings.Join(labelValues, "\xff")
	h.mu.RLock()
	item, ok := h.values[key]
	h.mu.RUnlock()
	if !ok {
		return 0
	}
	item.mu.Lock()
	defer item.mu.Unlock()
	return item.count
}

func (h *HistogramVec) write(w *bufio.Writer) {
	writeHeader(w, h.name, h.help, "histogram")
	h.mu.RLock()
	items := make([]*histogramSeries, 0, len(h.values))
	for _, item := range h.values {
		items = append(items, item)
	}
	h.mu.RUnlock()
	sort.Slice(items, func(i, j int) bool {
		return strings.Join(items[i].labelValues, "\xff") < strings.Join(items[j].labelValues, "\xff")
	})
	for _, item := range items {
		item.mu.Lock()
		counts := append([]uint64(nil), item.counts...)
		count := item.count
		sum := item.sum
		item.mu.Unlock()
		for index, bound := range h.buckets {
			writeSample(w, h.name+"_bucket", h.labels, item.labelValues, []string{"le", formatFloat(bound)}, float64(counts[index]))
		}
		writeSample(w, h.name+"_bucket", h.labels, item.labelValues, []string{"le", "+Inf"}, float64(count))
		writeSample(w, h.name+"_sum", h.labels, item.labelValues, nil, sum)
		writeSample(w, h.name+"_count", h.labels, item.labelValues, nil, float64(count))
	}
}

func writeHeader(w *bufio.Writer, name, help, kind string) {
	w.WriteString("# HELP ")
	w.WriteString(name)
	w.WriteByte(' ')
	w.WriteString(strings.NewReplacer("\\", `\\`, "\n", `\n`).Replace(help))
	w.WriteString("\n# TYPE ")
	w.WriteString(name)
	w.WriteByte(' ')
	w.WriteString(kind)
	w.WriteByte('\n')
}

// WriteGauge writes a single unlabelled gauge family; used by collectors.
func WriteGauge(w *bufio.Writer, name, help string, value float64) {
	writeHeader(w, name, help, "gauge")
	writeSample(w, name, nil, nil, nil, value)
}

// WriteCounter writes a single unlabelled counter family; used by collectors.
func WriteCounter(w *bufio.Writer, name, help string, value float64) {
	writeHeader(w, name, help, "counter")
	writeSample(w, name, nil, nil, nil, value)
}

func writeSample(w *bufio.Writer, name string, labels, labelValues, extra []string, value float64) {
	w.WriteString(name)
	if len(labels) > 0 || len(extra) > 0 {
		w.WriteByte('{')
		first := true
		for index, label := range labels {
			if !first {
				w.WriteByte(',')
			}
			first = false
			w.WriteString(label)
			w.WriteString(`="`)
			w.WriteString(escapeLabel(labelValues[index]))
			w.WriteByte('"')
		}
		for index := 0; index+1 < len(extra); index += 2 {
			if !first {
				w.WriteByte(',')
			}
			first = false
			w.WriteString(extra[index])
			w.WriteString(`="`)
			w.WriteString(escapeLabel(extra[index+1]))
			w.WriteByte('"')
		}
		w.WriteByte('}')
	}
	w.WriteByte(' ')
	w.WriteString(formatFloat(value))
	w.WriteByte('\n')
}

func escapeLabel(value string) string {
	return strings.NewReplacer("\\", `\\`, "\n", `\n`, `"`, `\"`).Replace(value)
}

func formatFloat(value float64) string {
	switch {
	case math.IsInf(value, 1):
		return "+Inf"
	case math.IsInf(value, -1):
		return "-Inf"
	case math.IsNaN(value):
		return "NaN"
	}
	return strconv.FormatFloat(value, 'g', -1, 64)
}
