package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"recap-personalization/internal/benchmark"
)

type result struct {
	Index         int           `json:"index"`
	ProfileID     string        `json:"profile_id"`
	RequestID     string        `json:"request_id,omitempty"`
	AcceptedAt    time.Time     `json:"accepted_at,omitempty"`
	FinishedAt    time.Time     `json:"finished_at,omitempty"`
	AcceptLatency time.Duration `json:"accept_latency"`
	ReadyLatency  time.Duration `json:"ready_latency,omitempty"`
	HTTPStatus    int           `json:"http_status"`
	FinalStatus   string        `json:"final_status"`
	Error         string        `json:"error,omitempty"`
}

type report struct {
	Scenario         string                   `json:"scenario"`
	CommandSource    string                   `json:"command_source"`
	StartedAt        time.Time                `json:"started_at"`
	FinishedAt       time.Time                `json:"finished_at"`
	BaseURL          string                   `json:"base_url"`
	Requests         int                      `json:"requests"`
	Concurrency      int                      `json:"concurrency"`
	RatePerSecond    float64                  `json:"rate_per_second"`
	Successes        int                      `json:"successes"`
	Failures         int                      `json:"failures"`
	LostRequests     int                      `json:"lost_requests"`
	DuplicateIDs     int                      `json:"duplicate_request_ids"`
	PeakInFlight     int64                    `json:"peak_in_flight"`
	PeakQueueDepth   int64                    `json:"peak_queue_depth,omitempty"`
	PeakOutboxDepth  int64                    `json:"peak_outbox_depth,omitempty"`
	FinalQueued      int64                    `json:"final_queued,omitempty"`
	FinalProcessing  int64                    `json:"final_processing,omitempty"`
	FinalOutboxDepth int64                    `json:"final_outbox_depth,omitempty"`
	DuplicateDBRows  int64                    `json:"duplicate_snapshot_groups,omitempty"`
	DrainTime        time.Duration            `json:"drain_time"`
	ThroughputPerSec float64                  `json:"throughput_per_sec"`
	Acceptance       benchmark.LatencySummary `json:"acceptance"`
	Ready            benchmark.LatencySummary `json:"ready"`
	Results          []result                 `json:"results"`
}

type queueMetrics struct {
	PeakQueueDepth   int64
	PeakOutboxDepth  int64
	FinalQueued      int64
	FinalProcessing  int64
	FinalOutboxDepth int64
	DuplicateDBRows  int64
}

func main() {
	baseURL := flag.String("base-url", "http://localhost:8080/api/v1", "API base URL")
	scenario := flag.String("scenario", "burst", "scenario label: burst, steady, ramp or custom")
	commandSource := flag.String("command-source", env("WORKER_COMMAND_SOURCE", "broker"), "worker command source used by the running stack")
	requests := flag.Int("requests", 100, "number of recap requests")
	concurrency := flag.Int("concurrency", 20, "maximum concurrent requests")
	year := flag.Int("year", 2026, "recap year")
	seed := flag.Int64("seed", 42, "benchmark profile seed")
	timeout := flag.Duration("timeout", 2*time.Minute, "deadline per recap")
	rate := flag.Float64("rate", 0, "request starts per second; 0 means an immediate burst")
	databaseURL := flag.String("database-url", env("DATABASE_URL", "postgres://avito:avito_password@localhost:5432/avito_recap?sslmode=disable"), "optional PostgreSQL URL for queue-depth sampling")
	output := flag.String("output", "", "JSON report path; a Markdown summary is written next to it")
	flag.Parse()

	if *requests <= 0 || *concurrency <= 0 {
		log.Fatal("requests and concurrency must be positive")
	}
	if *rate < 0 {
		log.Fatal("rate must not be negative")
	}

	started := time.Now().UTC()
	client := &http.Client{Timeout: 20 * time.Second}
	results := make([]result, *requests)
	stopSampler, metricsResult := startQueueSampler(*databaseURL)
	semaphore := make(chan struct{}, *concurrency)
	var group sync.WaitGroup
	var inFlight atomic.Int64
	var peakInFlight atomic.Int64
	interval := time.Duration(0)
	if *rate > 0 {
		interval = time.Duration(float64(time.Second) / *rate)
	}

	for index := 0; index < *requests; index++ {
		if interval > 0 {
			target := started.Add(time.Duration(index) * interval)
			if delay := time.Until(target); delay > 0 {
				time.Sleep(delay)
			}
		}
		semaphore <- struct{}{}
		current := inFlight.Add(1)
		updatePeak(&peakInFlight, current)
		group.Add(1)
		go func(index int) {
			defer group.Done()
			defer func() {
				inFlight.Add(-1)
				<-semaphore
			}()
			ctx, cancel := context.WithTimeout(context.Background(), *timeout)
			defer cancel()
			profileID := benchmarkProfileID(*seed, index).String()
			results[index] = runOne(ctx, client, *baseURL, profileID, *year, index)
		}(index)
	}
	group.Wait()
	finished := time.Now().UTC()
	close(stopSampler)
	metrics := <-metricsResult

	value := buildReport(*scenario, *commandSource, started, finished, *baseURL, *concurrency, *rate, peakInFlight.Load(), metrics, results)
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		log.Fatal(err)
	}
	if *output == "" {
		fmt.Println(string(encoded))
		return
	}
	if err := os.MkdirAll(filepath.Dir(*output), 0o755); err != nil {
		log.Fatal(err)
	}
	if err := os.WriteFile(*output, encoded, 0o644); err != nil {
		log.Fatal(err)
	}
	markdownPath := strings.TrimSuffix(*output, filepath.Ext(*output)) + ".md"
	if err := os.WriteFile(markdownPath, []byte(renderMarkdown(value)), 0o644); err != nil {
		log.Fatal(err)
	}
	log.Printf("benchmark reports written to %s and %s", *output, markdownPath)
}

func runOne(ctx context.Context, client *http.Client, baseURL, profileID string, year, index int) result {
	value := result{Index: index, ProfileID: profileID}
	payload, err := json.Marshal(map[string]interface{}{"profile_id": profileID, "year": year})
	if err != nil {
		value.Error = err.Error()
		return value
	}
	started := time.Now()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(baseURL, "/")+"/recaps", bytes.NewReader(payload))
	if err != nil {
		value.Error = err.Error()
		return value
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", fmt.Sprintf("bench:%s:%d", profileID, year))
	response, err := client.Do(request)
	value.AcceptLatency = time.Since(started)
	value.AcceptedAt = time.Now().UTC()
	if err != nil {
		value.Error = err.Error()
		value.FinalStatus = "transport_error"
		value.FinishedAt = time.Now().UTC()
		return value
	}
	body, readErr := io.ReadAll(io.LimitReader(response.Body, 4<<20))
	_ = response.Body.Close()
	value.HTTPStatus = response.StatusCode
	if readErr != nil {
		value.Error = readErr.Error()
		value.FinalStatus = "transport_error"
		value.FinishedAt = time.Now().UTC()
		return value
	}
	if response.StatusCode != http.StatusAccepted && response.StatusCode != http.StatusOK {
		value.Error = fmt.Sprintf("POST HTTP %d: %s", response.StatusCode, body)
		value.FinalStatus = "rejected"
		value.FinishedAt = time.Now().UTC()
		return value
	}
	var envelope map[string]interface{}
	if err := json.Unmarshal(body, &envelope); err != nil {
		value.Error = err.Error()
		value.FinalStatus = "invalid_response"
		value.FinishedAt = time.Now().UTC()
		return value
	}
	id, _ := envelope["id"].(string)
	value.RequestID = id
	if _, ready := envelope["schema_version"]; ready {
		value.FinalStatus = "ready"
		value.ReadyLatency = value.AcceptLatency
		value.FinishedAt = time.Now().UTC()
		return value
	}
	if id == "" {
		value.Error = "response has no request id"
		value.FinalStatus = "invalid_response"
		value.FinishedAt = time.Now().UTC()
		return value
	}

	pollDelay := 200 * time.Millisecond
	for {
		if !sleepContext(ctx, pollDelay) {
			value.FinalStatus = "timeout"
			value.Error = ctx.Err().Error()
			value.FinishedAt = time.Now().UTC()
			return value
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(baseURL, "/")+"/recaps/"+id, nil)
		if err != nil {
			value.Error = err.Error()
			value.FinalStatus = "transport_error"
			value.FinishedAt = time.Now().UTC()
			return value
		}
		response, err := client.Do(request)
		if err != nil {
			value.Error = err.Error()
			value.FinalStatus = "transport_error"
			value.FinishedAt = time.Now().UTC()
			return value
		}
		body, readErr := io.ReadAll(io.LimitReader(response.Body, 4<<20))
		_ = response.Body.Close()
		if readErr != nil {
			value.Error = readErr.Error()
			value.FinalStatus = "transport_error"
			value.FinishedAt = time.Now().UTC()
			return value
		}
		if response.StatusCode != http.StatusOK {
			value.Error = fmt.Sprintf("GET HTTP %d: %s", response.StatusCode, body)
			value.FinalStatus = "rejected"
			value.FinishedAt = time.Now().UTC()
			return value
		}
		var state map[string]interface{}
		if err := json.Unmarshal(body, &state); err != nil {
			value.Error = err.Error()
			value.FinalStatus = "invalid_response"
			value.FinishedAt = time.Now().UTC()
			return value
		}
		if _, ready := state["schema_version"]; ready {
			value.FinalStatus = "ready"
			value.ReadyLatency = time.Since(started)
			value.FinishedAt = time.Now().UTC()
			return value
		}
		if state["status"] == "failed" {
			value.FinalStatus = "failed"
			value.ReadyLatency = time.Since(started)
			value.Error = string(body)
			value.FinishedAt = time.Now().UTC()
			return value
		}
		if wait, ok := state["poll_after_ms"].(float64); ok && wait >= 100 && wait <= 5000 {
			pollDelay = time.Duration(wait) * time.Millisecond
		}
	}
}

func buildReport(
	scenario, commandSource string,
	started, finished time.Time,
	baseURL string,
	concurrency int,
	rate float64,
	peakInFlight int64,
	metrics queueMetrics,
	results []result,
) report {
	acceptance := make([]time.Duration, 0, len(results))
	ready := make([]time.Duration, 0, len(results))
	ids := make(map[string]int)
	successes := 0
	lost := 0
	lastAccepted := started
	for _, value := range results {
		if value.AcceptLatency > 0 {
			acceptance = append(acceptance, value.AcceptLatency)
		}
		if value.ReadyLatency > 0 {
			ready = append(ready, value.ReadyLatency)
		}
		if value.AcceptedAt.After(lastAccepted) {
			lastAccepted = value.AcceptedAt
		}
		if value.FinalStatus == "ready" {
			successes++
		} else if value.FinalStatus == "timeout" || value.RequestID == "" {
			lost++
		}
		if value.RequestID != "" {
			ids[value.RequestID]++
		}
	}
	duplicates := 0
	for _, count := range ids {
		if count > 1 {
			duplicates += count - 1
		}
	}
	duration := finished.Sub(started).Seconds()
	throughput := 0.0
	if duration > 0 {
		throughput = float64(successes) / duration
	}
	drainTime := finished.Sub(lastAccepted)
	if drainTime < 0 {
		drainTime = 0
	}
	return report{
		Scenario: scenario, CommandSource: commandSource, StartedAt: started, FinishedAt: finished, BaseURL: baseURL,
		Requests: len(results), Concurrency: concurrency, RatePerSecond: rate,
		Successes: successes, Failures: len(results) - successes,
		LostRequests: lost, DuplicateIDs: duplicates, PeakInFlight: peakInFlight,
		PeakQueueDepth: metrics.PeakQueueDepth, PeakOutboxDepth: metrics.PeakOutboxDepth,
		FinalQueued: metrics.FinalQueued, FinalProcessing: metrics.FinalProcessing,
		FinalOutboxDepth: metrics.FinalOutboxDepth, DuplicateDBRows: metrics.DuplicateDBRows,
		DrainTime: drainTime, ThroughputPerSec: throughput,
		Acceptance: benchmark.Summarize(acceptance), Ready: benchmark.Summarize(ready), Results: results,
	}
}

func renderMarkdown(value report) string {
	return fmt.Sprintf(`# Нагрузочный тест асинхронной генерации

- Сценарий: %s
- Источник команд worker: %s
- Запросов: %d
- Конкурентность: %d
- Заданная скорость: %.2f запросов/с (0 означает burst)
- Успешно: %d
- Ошибок: %d
- Потеряно запросов: %d
- Повторяющихся request ID: %d
- Максимум запросов в работе: %d
- Максимальная глубина очереди PostgreSQL: %d
- Максимальная глубина outbox: %d
- Осталось queued / processing: %d / %d
- Осталось в outbox: %d
- Групп повторяющихся snapshots: %d
- Время обработки очереди: %s
- Пропускная способность: %.2f готовых recaps/с

| Метрика | p50 | p95 | p99 | максимум | среднее |
|---|---:|---:|---:|---:|---:|
| Принятие POST | %s | %s | %s | %s | %s |
| Время до ready | %s | %s | %s | %s | %s |
`,
		value.Scenario, value.CommandSource, value.Requests, value.Concurrency, value.RatePerSecond,
		value.Successes, value.Failures, value.LostRequests, value.DuplicateIDs,
		value.PeakInFlight, value.PeakQueueDepth, value.PeakOutboxDepth,
		value.FinalQueued, value.FinalProcessing, value.FinalOutboxDepth, value.DuplicateDBRows,
		value.DrainTime, value.ThroughputPerSec,
		value.Acceptance.P50, value.Acceptance.P95, value.Acceptance.P99, value.Acceptance.Max, value.Acceptance.Mean,
		value.Ready.P50, value.Ready.P95, value.Ready.P99, value.Ready.Max, value.Ready.Mean,
	)
}

func startQueueSampler(databaseURL string) (chan struct{}, <-chan queueMetrics) {
	stop := make(chan struct{})
	result := make(chan queueMetrics, 1)
	go func() {
		defer close(result)
		if strings.TrimSpace(databaseURL) == "" {
			<-stop
			result <- queueMetrics{}
			return
		}
		db, err := sql.Open("postgres", databaseURL)
		if err != nil {
			log.Printf("queue sampler disabled: %v", err)
			<-stop
			result <- queueMetrics{}
			return
		}
		defer db.Close()
		pingCtx, cancelPing := context.WithTimeout(context.Background(), 2*time.Second)
		err = db.PingContext(pingCtx)
		cancelPing()
		if err != nil {
			log.Printf("queue sampler disabled: %v", err)
			<-stop
			result <- queueMetrics{}
			return
		}

		metrics := queueMetrics{}
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			queued, processing, outboxDepth, duplicates, err := sampleQueue(db)
			if err == nil {
				if depth := queued + processing; depth > metrics.PeakQueueDepth {
					metrics.PeakQueueDepth = depth
				}
				if outboxDepth > metrics.PeakOutboxDepth {
					metrics.PeakOutboxDepth = outboxDepth
				}
				metrics.FinalQueued = queued
				metrics.FinalProcessing = processing
				metrics.FinalOutboxDepth = outboxDepth
				metrics.DuplicateDBRows = duplicates
			}
			select {
			case <-stop:
				queued, processing, outboxDepth, duplicates, finalErr := sampleQueue(db)
				if finalErr == nil {
					metrics.FinalQueued = queued
					metrics.FinalProcessing = processing
					metrics.FinalOutboxDepth = outboxDepth
					metrics.DuplicateDBRows = duplicates
				}
				result <- metrics
				return
			case <-ticker.C:
			}
		}
	}()
	return stop, result
}

func sampleQueue(db *sql.DB) (queued, processing, outboxDepth, duplicateSnapshotGroups int64, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = db.QueryRowContext(ctx, `
		SELECT
			(SELECT count(*) FROM recap_requests WHERE status = 'queued'),
			(SELECT count(*) FROM recap_requests WHERE status = 'processing'),
			(SELECT count(*) FROM outbox_events WHERE status <> 'published'),
			(SELECT count(*) FROM (
				SELECT profile_id, year FROM recaps GROUP BY profile_id, year HAVING count(*) > 1
			) duplicates)
	`).Scan(&queued, &processing, &outboxDepth, &duplicateSnapshotGroups)
	return
}

func env(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func benchmarkProfileID(seed int64, index int) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(fmt.Sprintf("avito-recap-bench:%d:%d", seed, index)))
}

func updatePeak(peak *atomic.Int64, current int64) {
	for {
		previous := peak.Load()
		if current <= previous || peak.CompareAndSwap(previous, current) {
			return
		}
	}
}

func sleepContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
