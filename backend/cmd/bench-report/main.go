package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type latency struct {
	P95 time.Duration `json:"p95"`
}

type inputReport struct {
	Scenario        string        `json:"scenario"`
	CommandSource   string        `json:"command_source"`
	Requests        int           `json:"requests"`
	Failures        int           `json:"failures"`
	LostRequests    int           `json:"lost_requests"`
	DuplicateIDs    int           `json:"duplicate_request_ids"`
	DuplicateDBRows int64         `json:"duplicate_snapshot_groups"`
	DrainTime       time.Duration `json:"drain_time"`
	Acceptance      latency       `json:"acceptance"`
}

type evaluation struct {
	Scenario             string
	CommandSource        string
	AcceptanceOK         bool
	ErrorRateOK          bool
	LostOK               bool
	DuplicateIDsOK       bool
	DuplicateSnapshotsOK bool
	DrainOK              bool
	AcceptanceP95        time.Duration
	ErrorRate            float64
	DrainTime            time.Duration
}

func main() {
	output := flag.String("output", "", "optional Markdown output path")
	strict := flag.Bool("strict", false, "exit non-zero when a decision gate fails")
	acceptanceGate := flag.Duration("acceptance-p95", 150*time.Millisecond, "POST acceptance p95 gate")
	errorRateGate := flag.Float64("error-rate", 0.01, "maximum failure ratio")
	drainGate := flag.Duration("drain-time", 60*time.Second, "maximum backlog drain time")
	flag.Parse()

	if flag.NArg() == 0 {
		log.Fatal("at least one benchmark JSON report is required")
	}
	values := make([]evaluation, 0, flag.NArg())
	allPassed := true
	for _, filename := range flag.Args() {
		report, err := readReport(filename)
		if err != nil {
			log.Fatal(err)
		}
		value := evaluate(report, *acceptanceGate, *errorRateGate, *drainGate)
		values = append(values, value)
		if !passed(value) {
			allPassed = false
		}
	}
	markdown := render(values, *acceptanceGate, *errorRateGate, *drainGate)
	if *output == "" {
		fmt.Print(markdown)
	} else {
		if err := os.MkdirAll(filepath.Dir(*output), 0o755); err != nil {
			log.Fatal(err)
		}
		if err := os.WriteFile(*output, []byte(markdown), 0o644); err != nil {
			log.Fatal(err)
		}
	}
	if *strict && !allPassed {
		os.Exit(1)
	}
}

func readReport(filename string) (inputReport, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return inputReport{}, fmt.Errorf("read %s: %w", filename, err)
	}
	var value inputReport
	if err := json.Unmarshal(data, &value); err != nil {
		return inputReport{}, fmt.Errorf("decode %s: %w", filename, err)
	}
	if value.Scenario == "" {
		value.Scenario = strings.TrimSuffix(filepath.Base(filename), filepath.Ext(filename))
	}
	return value, nil
}

func evaluate(value inputReport, acceptanceGate time.Duration, errorRateGate float64, drainGate time.Duration) evaluation {
	errorRate := 0.0
	if value.Requests > 0 {
		errorRate = float64(value.Failures) / float64(value.Requests)
	}
	return evaluation{
		Scenario: value.Scenario, CommandSource: value.CommandSource, AcceptanceOK: value.Acceptance.P95 <= acceptanceGate,
		ErrorRateOK: errorRate < errorRateGate, LostOK: value.LostRequests == 0,
		DuplicateIDsOK: value.DuplicateIDs == 0, DuplicateSnapshotsOK: value.DuplicateDBRows == 0,
		DrainOK:       value.DrainTime <= drainGate,
		AcceptanceP95: value.Acceptance.P95, ErrorRate: errorRate, DrainTime: value.DrainTime,
	}
}

func passed(value evaluation) bool {
	return value.AcceptanceOK && value.ErrorRateOK && value.LostOK && value.DuplicateIDsOK && value.DuplicateSnapshotsOK && value.DrainOK
}

func render(values []evaluation, acceptanceGate time.Duration, errorRateGate float64, drainGate time.Duration) string {
	var builder strings.Builder
	fmt.Fprintf(&builder, "# Итоговый отчёт benchmark\n\n")
	fmt.Fprintf(&builder, "Критерии: p95 принятия <= %s, доля ошибок < %.2f%%, нет потерянных запросов и повторяющихся ID/snapshots, время обработки очереди <= %s.\n\n", acceptanceGate, errorRateGate*100, drainGate)
	builder.WriteString("| Сценарий | Источник | p95 принятия | Ошибки | Потери | Повторы ID | Повторы snapshots | Обработка очереди | Решение |\n")
	builder.WriteString("|---|---|---:|---:|:---:|:---:|:---:|---:|:---:|\n")
	for _, value := range values {
		decision := "ПРОЙДЕНО"
		if !passed(value) {
			decision = "ПРОВЕРИТЬ"
		}
		fmt.Fprintf(
			&builder,
			"| %s | %s | %s | %.2f%% | %s | %s | %s | %s | %s |\n",
			value.Scenario,
			value.CommandSource,
			value.AcceptanceP95,
			value.ErrorRate*100,
			mark(value.LostOK),
			mark(value.DuplicateIDsOK),
			mark(value.DuplicateSnapshotsOK),
			value.DrainTime,
			decision,
		)
	}
	return builder.String()
}

func mark(ok bool) string {
	if ok {
		return "0"
	}
	return "ОШИБКА"
}
