package telemetry

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"log"
	"net/http"
	"os"
	"runtime"
	"runtime/metrics"
	"strconv"
	"strings"
	"time"
)

var processStart = time.Now()

// RegisterProcessCollector exports Go runtime and process resource metrics.
func (r *Registry) RegisterProcessCollector() {
	r.Register("process_runtime", func(w *bufio.Writer) {
		WriteGauge(w, "go_goroutines", "Number of goroutines that currently exist.", float64(runtime.NumGoroutine()))

		samples := []metrics.Sample{
			{Name: "/gc/heap/live:bytes"},
			{Name: "/memory/classes/heap/objects:bytes"},
			{Name: "/memory/classes/total:bytes"},
			{Name: "/gc/cycles/total:gc-cycles"},
		}
		metrics.Read(samples)
		names := []struct {
			metric string
			help   string
			kind   string
		}{
			{"go_gc_heap_live_bytes", "Heap memory occupied by live objects that were marked by the previous GC (heap after GC).", "gauge"},
			{"go_memory_heap_objects_bytes", "Memory occupied by live and not yet swept heap objects.", "gauge"},
			{"go_memory_total_bytes", "All memory mapped by the Go runtime.", "gauge"},
			{"go_gc_cycles_total", "Completed GC cycles.", "counter"},
		}
		for index, sample := range samples {
			if sample.Value.Kind() != metrics.KindUint64 {
				continue
			}
			value := float64(sample.Value.Uint64())
			if names[index].kind == "counter" {
				WriteCounter(w, names[index].metric, names[index].help, value)
			} else {
				WriteGauge(w, names[index].metric, names[index].help, value)
			}
		}

		if rss, ok := residentMemoryBytes(); ok {
			WriteGauge(w, "process_resident_memory_bytes", "Resident set size of the process.", rss)
		}
		if cpu, ok := processCPUSeconds(); ok {
			WriteCounter(w, "process_cpu_seconds_total", "User and system CPU time spent by the process.", cpu)
		}
		WriteGauge(w, "process_start_time_seconds", "Process start time since the Unix epoch.", float64(processStart.UnixNano())/1e9)
	})
}

// RegisterDBStats exports database/sql pool statistics for one pool.
func (r *Registry) RegisterDBStats(pool string, db *sql.DB) {
	if db == nil {
		return
	}
	r.Register("go_sql_"+pool, func(w *bufio.Writer) {
		stats := db.Stats()
		labels := []string{"pool"}
		values := []string{pool}
		family := func(name, help, kind string, value float64) {
			writeHeader(w, name, help, kind)
			writeSample(w, name, labels, values, nil, value)
		}
		family("go_sql_max_open_connections", "Maximum number of open connections.", "gauge", float64(stats.MaxOpenConnections))
		family("go_sql_open_connections", "Established connections, in use and idle.", "gauge", float64(stats.OpenConnections))
		family("go_sql_in_use_connections", "Connections currently in use.", "gauge", float64(stats.InUse))
		family("go_sql_idle_connections", "Idle connections.", "gauge", float64(stats.Idle))
		family("go_sql_wait_count_total", "Total number of connections waited for.", "counter", float64(stats.WaitCount))
		family("go_sql_wait_duration_seconds_total", "Total time blocked waiting for a new connection.", "counter", stats.WaitDuration.Seconds())
	})
}

func residentMemoryBytes() (float64, bool) {
	data, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0, false
	}
	fields := strings.Fields(string(data))
	if len(fields) < 2 {
		return 0, false
	}
	pages, err := strconv.ParseFloat(fields[1], 64)
	if err != nil {
		return 0, false
	}
	return pages * float64(os.Getpagesize()), true
}

func processCPUSeconds() (float64, bool) {
	data, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		return 0, false
	}
	// The command name may contain spaces; fields start after the last ')'.
	text := string(data)
	closing := strings.LastIndexByte(text, ')')
	if closing < 0 {
		return 0, false
	}
	fields := strings.Fields(text[closing+1:])
	// utime and stime are fields 14 and 15 of /proc/<pid>/stat; after the
	// command name they are at indexes 11 and 12.
	if len(fields) < 13 {
		return 0, false
	}
	utime, errU := strconv.ParseFloat(fields[11], 64)
	stime, errS := strconv.ParseFloat(fields[12], 64)
	if errU != nil || errS != nil {
		return 0, false
	}
	// USER_HZ is 100 on Linux builds used by the Docker images.
	return (utime + stime) / 100, true
}

// StartServer serves Default on addr at /metrics. Empty addr disables the
// endpoint and returns nil. The server stops when ctx is cancelled.
func StartServer(ctx context.Context, addr string) *http.Server {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return nil
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", Default.Handler())
	server := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		log.Printf("metrics endpoint listening on %s/metrics", addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("metrics endpoint stopped: %v", err)
		}
	}()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	return server
}
