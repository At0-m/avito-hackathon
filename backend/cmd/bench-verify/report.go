package main

import (
	"bufio"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"recap-personalization/internal/benchmark"
)

// Gate categories. The performance verdict and the health of the measurement
// itself are reported separately (spec §6.6).
const (
	categoryMeasurement = "измеритель"
	categoryLoad        = "нагрузка"
	categoryCorrectness = "корректность"
	categoryPerformance = "производительность"
	categoryConfig      = "конфигурация"
	categoryRecovery    = "восстановление"
)

type gate struct {
	Category  string `json:"category"`
	Name      string `json:"name"`
	Value     string `json:"value"`
	Threshold string `json:"threshold"`
	Passed    bool   `json:"passed"`
}

type reportData struct {
	Scenario string            `json:"scenario"`
	RunID    string            `json:"run_id"`
	Env      map[string]string `json:"run_env"`
	Phases   []phaseMark       `json:"phases"`
	Gates    []gate            `json:"gates"`
	Verdicts map[string]string `json:"verdicts"`
	Missing  []string          `json:"missing_files,omitempty"`
	Passed   bool              `json:"passed"`
	sections []string
}

type phaseMark struct {
	Name string    `json:"name"`
	At   time.Time `json:"at"`
}

type reportInputs struct {
	dir      string
	env      map[string]string
	data     *reportData
	gateOpts gateOptions
}

type gateOptions struct {
	p95          time.Duration
	errorRate    float64
	drain        time.Duration
	ingressRatio float64
}

func runReport(args []string) error {
	flags := flag.NewFlagSet("report", flag.ExitOnError)
	dir := flags.String("run-dir", "", "result directory of the run")
	p95 := flags.Duration("p95", 150*time.Millisecond, "cold POST acceptance p95 gate")
	errorRate := flags.Float64("error-rate", 0.01, "maximum POST error ratio")
	drain := flags.Duration("drain-gate", 60*time.Second, "backlog drain gate")
	ingress := flags.Float64("min-ingress-ratio", 0.98, "minimum actual/target POST rate")
	_ = flags.Parse(args)
	if *dir == "" {
		return fmt.Errorf("--run-dir is required")
	}
	env, err := readEnvFile(filepath.Join(*dir, "run.env"))
	if err != nil {
		return err
	}
	data := &reportData{
		Scenario: env["SCENARIO"], RunID: env["RUN_ID"], Env: env, Verdicts: map[string]string{},
	}
	data.Phases, _ = readPhases(filepath.Join(*dir, "phases.log"))
	inputs := reportInputs{dir: *dir, env: env, data: data, gateOpts: gateOptions{
		p95: *p95, errorRate: *errorRate, drain: *drain, ingressRatio: *ingress,
	}}

	inputs.commonGates()
	switch data.Scenario {
	case "smoke":
		inputs.smoke()
	case "steady":
		inputs.steady()
	case "hotkey":
		inputs.hotkey()
	case "backlog":
		inputs.backlog()
	case "burst":
		inputs.burst()
	case "soak":
		inputs.soak()
	default:
		return fmt.Errorf("unknown scenario %q in run.env", data.Scenario)
	}

	data.Passed = len(data.Missing) == 0
	categoryPassed := map[string]bool{}
	for _, item := range data.Gates {
		if _, ok := categoryPassed[item.Category]; !ok {
			categoryPassed[item.Category] = true
		}
		if !item.Passed {
			data.Passed = false
			categoryPassed[item.Category] = false
		}
	}
	for category, passed := range categoryPassed {
		if passed {
			data.Verdicts[category] = "пройдено"
		} else {
			data.Verdicts[category] = "НЕ ПРОЙДЕНО"
		}
	}
	if len(data.Missing) > 0 {
		data.Verdicts[categoryMeasurement] = "НЕ ПРОЙДЕНО (нет обязательных данных)"
	}
	if err := writeJSON(filepath.Join(*dir, "verdict.json"), data); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(*dir, "summary.md"), []byte(renderReport(data)), 0o644); err != nil {
		return err
	}
	if !data.Passed {
		return checkFailed{reason: "one or more gates failed, see " + filepath.Join(*dir, "summary.md")}
	}
	return nil
}

// ---- inputs ----------------------------------------------------------------

func readEnvFile(path string) (map[string]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("read run.env: %w", err)
	}
	defer file.Close()
	values := map[string]string{}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if ok {
			values[strings.TrimSpace(key)] = strings.Trim(strings.TrimSpace(value), `"'`)
		}
	}
	return values, scanner.Err()
}

func readPhases(path string) ([]phaseMark, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var marks []phaseMark
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) != 2 {
			continue
		}
		at, err := parseTime(fields[1])
		if err != nil {
			continue
		}
		marks = append(marks, phaseMark{Name: fields[0], At: at})
	}
	return marks, scanner.Err()
}

func (r reportInputs) envFloat(key string, fallback float64) float64 {
	if value, err := strconv.ParseFloat(r.env[key], 64); err == nil {
		return value
	}
	return fallback
}

func (r reportInputs) path(name string) string { return filepath.Join(r.dir, name) }

func (r reportInputs) missing(name string) {
	r.data.Missing = append(r.data.Missing, name)
}

func (r reportInputs) reconcile(label string) (ReconcileResult, bool) {
	var value ReconcileResult
	name := "reconcile-" + label + ".json"
	if err := readJSON(r.path(name), &value); err != nil {
		r.missing(name)
		return value, false
	}
	return value, true
}

func (r reportInputs) summary(label string) (k6Summary, bool) {
	name := "k6-" + label + "-summary.json"
	value, err := readK6Summary(r.path(name))
	if err != nil {
		r.missing(name)
		return value, false
	}
	return value, true
}

func (r reportInputs) drain(label string) (drainResult, bool) {
	var value drainResult
	name := "drain-" + label + ".json"
	if err := readJSON(r.path(name), &value); err != nil {
		r.missing(name)
		return value, false
	}
	return value, true
}

func (r reportInputs) metrics(name string) (metricsResult, bool) {
	var value metricsResult
	if err := readJSON(r.path(name), &value); err != nil {
		r.missing(name)
		return value, false
	}
	return value, true
}

func (r reportInputs) add(category, name, value, threshold string, passed bool) {
	r.data.Gates = append(r.data.Gates, gate{Category: category, Name: name, Value: value, Threshold: threshold, Passed: passed})
}

func (r reportInputs) section(title string, lines ...string) {
	r.data.sections = append(r.data.sections, "### "+title+"\n\n"+strings.Join(lines, "\n")+"\n")
}

// ---- gates -------------------------------------------------------------------

func (r reportInputs) commonGates() {
	var preflight preflightResult
	if err := readJSON(r.path("preflight.json"), &preflight); err != nil {
		r.missing("preflight.json")
	} else {
		r.add(categoryMeasurement, "preflight данных", strings.Join(preflight.Problems, "; "), "без замечаний", preflight.Passed)
	}
	if members, ok := r.env["GROUP_MEMBERS_START"]; ok {
		// Stale HTTP Proxy consumers hold partitions until they expire; a run
		// started with them measures the stale member, not the stand.
		r.add(categoryMeasurement, "члены consumer group перед нагрузкой", members, r.env["WORKERS"], members == r.env["WORKERS"])
	}
	if start, ok := r.metrics("metrics-start.json"); ok {
		r.add(categoryMeasurement, "scrape targets до нагрузки", describeTargets(start), describeExpected(start), start.Passed)
	}
	if end, ok := r.metrics("metrics-end.json"); ok {
		r.add(categoryMeasurement, "scrape targets и ряды k6 после нагрузки", describeTargets(end)+fmt.Sprintf(", k6 series=%d", end.K6Series), "все targets живы, k6 series > 0", end.Passed)
		expect := r.env["NARRATIVE_EXPECT"]
		value := end.NarrativeEffective + fmt.Sprintf("; provider calls=%s; outcomes=%s", formatFloat(end.ProviderCalls), describeFloats(end.NarrativeOutcomes))
		if expect == "disabled" {
			r.add(categoryConfig, "режим narrative", value, "template при отключённом provider, 0 вызовов provider",
				strings.HasPrefix(end.NarrativeEffective, "template (provider disabled") && !(float64(end.ProviderCalls) > 0))
		} else {
			r.section("Режим narrative", "- "+value)
		}
		if end.Stats != nil {
			r.windowSection("Серверная картина окна нагрузки (VictoriaMetrics, шаг 5 с)", *end.Stats)
		}
	}
}

func (r reportInputs) windowSection(title string, stats windowStats) {
	r.section(title,
		fmt.Sprintf("- Окно: %.0f с", stats.Seconds),
		fmt.Sprintf("- Начатые POST/с (k6): %s; новые работы/с (PG): %s; ready/с (PG): %s",
			formatFloat(stats.PostsStartedPerSec), formatFloat(stats.CreatedPerSec), formatFloat(stats.ReadyPerSec)),
		fmt.Sprintf("- Максимум backlog: %s; возраст старейшей работы (max): %s с; старейшая неопубликованная запись outbox (max): %s с",
			formatFloat(stats.BacklogMax), formatFloat(stats.OldestBacklogAgeMax), formatFloat(stats.OutboxOldestAgeMax)),
		fmt.Sprintf("- Решения API по POST: %s", describeFloats(stats.CreateOutcomes)),
		fmt.Sprintf("- p95 этапов worker, с: %s", describeFloats(stats.StageP95Seconds)),
		fmt.Sprintf("- Прирост ожидания PG pool, с: %s", describeFloats(stats.PoolWaitSeconds)),
		fmt.Sprintf("- Минимум sampler_up в окне: %s (0 — были пропуски выборки PG)", formatFloat(stats.SamplerUpMin)),
	)
}

// loadGates checks that the generator achieved the planned load.
func (r reportInputs) loadGates(summary k6Summary, posts int, durationSeconds, target float64, label string) {
	dropped := summary.value("dropped_iterations", "count")
	exhausted := summary.value("profiles_exhausted", "count")
	r.add(categoryLoad, label+": dropped_iterations", formatFloat(jsonFloat(dropped)), "0", dropped == 0)
	r.add(categoryLoad, label+": исчерпание профилей", formatFloat(jsonFloat(exhausted)), "0", exhausted == 0)
	if target > 0 && durationSeconds > 0 {
		actual := float64(posts) / durationSeconds
		r.add(categoryLoad, label+": фактические POST/с", fmt.Sprintf("%.2f из %.2f", actual, target),
			fmt.Sprintf(">= %.0f%% заданного", r.gateOpts.ingressRatio*100), actual >= target*r.gateOpts.ingressRatio)
	}
}

// correctnessGates applies the happy-path invariants to a reconciliation.
func (r reportInputs) correctnessGates(value ReconcileResult, label string, expected int) {
	r.add(categoryCorrectness, label+": manifest полный", fmt.Sprintf("строк=%d, k6 posts_started=%d, ошибок разбора=%d", value.PostsSent, value.K6PostsStarted, value.Manifest.ParseErrors),
		"совпадает с k6", value.ManifestComplete)
	if expected > 0 {
		r.add(categoryCorrectness, label+": подтверждённые работы", strconv.Itoa(value.AcceptedIDs), strconv.Itoa(expected), value.AcceptedIDs == expected)
		r.add(categoryCorrectness, label+": новые 202", strconv.Itoa(value.New202), strconv.Itoa(expected), value.New202 == expected)
	}
	r.add(categoryCorrectness, label+": ready со snapshot", fmt.Sprintf("%d из %d", value.ReadyWithSnap, value.AcceptedIDs), "все подтверждённые", value.ReadyWithSnap == value.AcceptedIDs && value.AcceptedIDs > 0)
	r.add(categoryCorrectness, label+": пропавшие подтверждённые ID", strconv.Itoa(value.ConfirmedMissing), "0", value.ConfirmedMissing == 0)
	r.add(categoryCorrectness, label+": неожиданные failed", fmt.Sprintf("%d (%s)", value.UnexpectedFailed, describeCounts(value.Failed)), "0", value.UnexpectedFailed == 0)
	r.add(categoryCorrectness, label+": незавершённые на deadline", describeCounts(value.Pending), "0", sumCounts(value.Pending) == 0)
	r.add(categoryCorrectness, label+": ready без валидного snapshot", strconv.Itoa(value.ReadyBrokenLink), "0", value.ReadyBrokenLink == 0)
	r.add(categoryCorrectness, label+": дубли request/snapshot", fmt.Sprintf("%d/%d", value.DuplicateRequestGroups, value.DuplicateSnapshotGroups), "0/0",
		value.DuplicateRequestGroups == 0 && value.DuplicateSnapshotGroups == 0)
	r.add(categoryCorrectness, label+": несовпадение профиля/года/ID", fmt.Sprintf("identity=%d, contract=%d", value.IdentityMismatch, value.ContractError), "0", value.IdentityMismatch == 0 && value.ContractError == 0)
	r.add(categoryCorrectness, label+": неожиданные 200 у холодной когорты", strconv.Itoa(value.Ready200), "0", value.Ready200 == 0)
}

func (r reportInputs) errorRateGate(summary k6Summary, label string) {
	started := summary.value("posts_started", "count")
	failures := summary.value("post_failures", "count")
	rate := 0.0
	if started > 0 {
		rate = failures / started
	}
	r.add(categoryPerformance, label+": доля ошибок POST", fmt.Sprintf("%.3f%% (%0.f из %0.f)", rate*100, failures, started),
		fmt.Sprintf("< %.2f%%", r.gateOpts.errorRate*100), started > 0 && rate < r.gateOpts.errorRate)
}

func (r reportInputs) p95Gate(summary k6Summary, metric, label string) {
	p95 := summary.value(metric, "p(95)")
	limit := float64(r.gateOpts.p95.Milliseconds())
	r.add(categoryPerformance, label+": p95 принятия cold POST (202)", fmt.Sprintf("%.1f мс (n=%0.f)", p95, summary.value(metric, "count")),
		fmt.Sprintf("<= %.0f мс", limit), summary.has(metric) && p95 <= limit)
}

func (r reportInputs) reconcileSection(title string, value ReconcileResult) {
	lines := []string{
		fmt.Sprintf("- POST отправлено: %d (по сценариям: %s); пропуски генератора (исчерпание): %d", value.PostsSent, describeCounts(value.PostsByScenario), value.GeneratorExhausted),
		fmt.Sprintf("- Исходы POST: новый 202=%d, повторный 202=%d, готовый 200=%d, failed 202=%d, 4xx=%d, transport/5xx=%d, нарушение контракта=%d",
			value.New202, value.Repeat202, value.Ready200, value.Failed202, value.Rejected4xx, value.Ambiguous, value.ContractError),
		fmt.Sprintf("- HTTP статусы: %s", describeCounts(value.HTTPStatuses)),
		fmt.Sprintf("- Неоднозначные исходы после сверки по (profile_id, year): %s", describeCounts(value.AmbiguousResolution)),
		fmt.Sprintf("- Подтверждено работ: %d; ready со snapshot: %d; failed: %s; незавершённые: %s; пропали: %d; с повторными попытками: %d",
			value.AcceptedIDs, value.ReadyWithSnap, describeCounts(value.Failed), describeCounts(value.Pending), value.ConfirmedMissing, value.Retried),
		fmt.Sprintf("- Источник narrative в snapshots: %s", describeCounts(value.NarrativeSources)),
	}
	phases := make([]string, 0, len(value.ServerTimeToReady))
	for phase := range value.ServerTimeToReady {
		phases = append(phases, phase)
	}
	sort.Strings(phases)
	if len(phases) > 0 {
		lines = append(lines, "", "| Server time to ready (finished_at − created_at) | n | p50 | p95 | p99 | max |", "|---|---:|---:|---:|---:|---:|")
		for _, phase := range phases {
			lines = append(lines, latencyRow(phase, value.ServerTimeToReady[phase]))
		}
		lines = append(lines, "", "| Очередь до первого claim (started_at − created_at) | n | p50 | p95 | p99 | max |", "|---|---:|---:|---:|---:|---:|")
		for _, phase := range phases {
			lines = append(lines, latencyRow(phase, value.QueueWait[phase]))
		}
	}
	if value.User.Observed > 0 {
		lines = append(lines, "",
			fmt.Sprintf("- Пользовательская когорта (polling c poll_after_ms): наблюдалось %d, ready=%d, failed=%d, timeout=%d, ошибки контракта=%d, ошибки POST=%d, GET-запросов=%d",
				value.User.Observed, value.User.Ready, value.User.Failed, value.User.Timeout, value.User.ContractErrors, value.User.PostErrors, value.User.Polls),
			"", "| User time to ready | n | p50 | p95 | p99 | max |", "|---|---:|---:|---:|---:|---:|",
			latencyRow("polling", value.User.TimeToReady))
	}
	r.section(title, lines...)
}

func (r reportInputs) k6Section(title string, summary k6Summary) {
	row := func(metric string) string {
		if !summary.has(metric) {
			return fmt.Sprintf("| %s | нет данных | | | |", metric)
		}
		return fmt.Sprintf("| %s | %.0f | %.1f | %.1f | %.1f |", metric,
			summary.value(metric, "count"), summary.value(metric, "med"), summary.value(metric, "p(95)"), summary.value(metric, "max"))
	}
	lines := []string{
		fmt.Sprintf("- Длительность прогона k6: %.1f с", summary.durationSeconds()),
		fmt.Sprintf("- posts_started=%.0f, 202=%.0f, 200=%.0f, failures=%.0f, dropped_iterations=%.0f, profiles_exhausted=%.0f",
			summary.value("posts_started", "count"), summary.value("posts_accepted_202", "count"),
			summary.value("posts_ready_200", "count"), summary.value("post_failures", "count"),
			summary.value("dropped_iterations", "count"), summary.value("profiles_exhausted", "count")),
		"",
		"| Метрика k6, мс | n | p50 | p95 | max |",
		"|---|---:|---:|---:|---:|",
		row("post_accept_202"),
		row("post_accept_200"),
		row("http_req_duration{name:POST /recaps}"),
		row("user_time_to_ready"),
	}
	if failed := summary.failedThresholds(); len(failed) > 0 {
		sort.Strings(failed)
		lines = append(lines, "", "- Нарушенные пороги k6: "+strings.Join(failed, "; "))
	}
	r.section(title, lines...)
}

func (r reportInputs) smoke() {
	expected := int(r.envFloat("SMOKE_COUNT", 100))
	if summary, ok := r.summary("smoke"); ok {
		r.loadGates(summary, int(summary.value("posts_started", "count")), r.envFloat("SMOKE_COUNT", 100)/r.envFloat("SMOKE_RATE", 5), r.envFloat("SMOKE_RATE", 5), "smoke")
		r.k6Section("k6 · smoke", summary)
	}
	if drain, ok := r.drain("smoke"); ok {
		r.add(categoryCorrectness, "smoke: drain завершён", drainValue(drain), "до deadline "+drain.Deadline, drain.Completed)
	}
	if value, ok := r.reconcile("smoke"); ok {
		r.correctnessGates(value, "smoke", expected)
		r.add(categoryCorrectness, "smoke: пользовательская когорта дошла до ready", fmt.Sprintf("%d из %d", value.User.Ready, value.User.Observed), "все", value.User.Observed > 0 && value.User.Ready == value.User.Observed)
		r.reconcileSection("Сверка с PostgreSQL · smoke", value)
	}
}

func (r reportInputs) steady() {
	rate := r.envFloat("STEADY_RATE", 20)
	duration := r.envFloat("STEADY_DURATION_S", 300)
	if summary, ok := r.summary("warmup"); ok {
		r.loadGates(summary, int(summary.value("posts_started", "count")), r.envFloat("STEADY_WARMUP_S", 60), rate, "прогрев")
	}
	if value, ok := r.reconcile("warmup"); ok {
		r.correctnessGates(value, "прогрев", int(rate*r.envFloat("STEADY_WARMUP_S", 60)))
	}
	value, reconciled := r.reconcile("measure")
	if summary, ok := r.summary("measure"); ok {
		r.loadGates(summary, value.PostsByScenario["measure"], duration, rate, "измерение")
		r.p95Gate(summary, "post_accept_202", "измерение")
		r.errorRateGate(summary, "измерение")
		r.k6Section("k6 · измерение", summary)
	}
	if drain, ok := r.drain("measure"); ok {
		r.add(categoryCorrectness, "измерение: drain завершён", drainValue(drain), "до deadline "+drain.Deadline, drain.Completed)
	}
	if reconciled {
		users := value.User.Observed
		r.correctnessGates(value, "измерение", int(rate*duration)+users)
		if r.envFloat("STEADY_USER_EVERY_S", 5) > 0 {
			r.add(categoryCorrectness, "измерение: пользовательская когорта", fmt.Sprintf("ready=%d из %d, timeout=%d", value.User.Ready, users, value.User.Timeout),
				"все ready", users > 0 && value.User.Ready == users)
		}
		r.reconcileSection("Сверка с PostgreSQL · измерение", value)
	}
}

func (r reportInputs) hotkey() {
	vus := int(r.envFloat("HOTKEY_VUS", 100))
	wave1, ok1 := r.reconcile("hotkey-wave1")
	if ok1 {
		r.add(categoryCorrectness, "волна 1: все ответы 202", fmt.Sprintf("202=%d из %d (%s)", wave1.Kinds[KindAccepted], wave1.PostsSent, describeCounts(wave1.Kinds)),
			strconv.Itoa(vus), wave1.Kinds[KindAccepted] == vus && wave1.PostsSent == vus)
		r.add(categoryCorrectness, "волна 1: один request ID", strconv.Itoa(wave1.AcceptedIDs), "1", wave1.AcceptedIDs == 1)
		r.add(categoryCorrectness, "волна 1: новый/повторный 202", fmt.Sprintf("%d/%d", wave1.New202, wave1.Repeat202), fmt.Sprintf("1/%d", vus-1),
			wave1.New202 == 1 && wave1.Repeat202 == vus-1)
		r.add(categoryCorrectness, "волна 1: одна работа и одна команда в outbox", fmt.Sprintf("requests=%d, commands=%d", wave1.RequestsForProfiles, wave1.OutboxCommands), "1 и 1",
			wave1.RequestsForProfiles == 1 && wave1.OutboxCommands == 1)
		r.reconcileSection("Сверка · волна 1 (workers остановлены)", wave1)
	}
	if drain, ok := r.drain("hotkey"); ok {
		r.add(categoryCorrectness, "работа дошла до ready после возврата workers", fmt.Sprintf("%.1f с, ready=%d", drain.DrainSeconds, drain.Final.Ready), "ready=1", drain.Completed && drain.Final.Ready == 1)
	}
	wave2, ok2 := r.reconcile("hotkey-wave2")
	if ok2 {
		r.add(categoryCorrectness, "волна 2: все ответы 200 со snapshot", fmt.Sprintf("200=%d из %d (%s)", wave2.Ready200, wave2.PostsSent, describeCounts(wave2.Kinds)),
			strconv.Itoa(vus), wave2.Ready200 == vus && wave2.PostsSent == vus && wave2.SnapshotResponseBad == 0)
		sameSnapshot := ok1 && wave2.SnapshotIDs == 1 && len(wave2.SnapshotIDSample) == 1 && len(wave1.AcceptedIDSample) == 1 &&
			wave2.SnapshotIDSample[0] == wave1.AcceptedIDSample[0]
		r.add(categoryCorrectness, "волна 2: тот же snapshot", fmt.Sprintf("snapshot ids=%v, request id=%v", wave2.SnapshotIDSample, wave1.AcceptedIDSample), "совпадают", sameSnapshot)
		r.add(categoryCorrectness, "после волны 2: работ и snapshots", fmt.Sprintf("requests=%d, snapshots=%d", wave2.RequestsForProfiles, wave2.SnapshotsForProfiles), "1 и 1",
			wave2.RequestsForProfiles == 1 && wave2.SnapshotsForProfiles == 1 && wave2.DuplicateSnapshotGroups == 0)
		r.reconcileSection("Сверка · волна 2 (warm hit)", wave2)
	}
	if summary, ok := r.summary("hotkey-wave1"); ok {
		r.k6Section("k6 · волна 1", summary)
	}
	if summary, ok := r.summary("hotkey-wave2"); ok {
		r.k6Section("k6 · волна 2", summary)
	}
}

func (r reportInputs) backlog() {
	expected := int(r.envFloat("BACKLOG_COUNT", 1000))
	r.add(categoryConfig, "режим и число workers", fmt.Sprintf("source=%s, workers=%s", r.env["COMMAND_SOURCE"], r.env["WORKERS"]), "broker, 3",
		r.env["COMMAND_SOURCE"] == "broker" && r.env["WORKERS"] == "3")
	if fill, ok := r.reconcile("backlog-fill"); ok {
		r.add(categoryCorrectness, "приём: уникальные подтверждённые работы", strconv.Itoa(fill.AcceptedIDs), strconv.Itoa(expected), fill.AcceptedIDs == expected && fill.New202 == expected)
		r.add(categoryCorrectness, "приём: ошибки POST", fmt.Sprintf("4xx=%d, transport/5xx=%d, контракт=%d", fill.Rejected4xx, fill.Ambiguous, fill.ContractError), "0",
			fill.Rejected4xx+fill.Ambiguous+fill.ContractError == 0)
	}
	var queued queuedResult
	if err := readJSON(r.path("queued.json"), &queued); err != nil {
		r.missing("queued.json")
	} else {
		r.add(categoryCorrectness, "в PostgreSQL ровно N queued до возврата workers",
			fmt.Sprintf("queued=%d, processing=%d, system backlog=%d", queued.Queued, queued.Processing, queued.SystemBacklog), fmt.Sprintf("%d, 0, %d", expected, expected), queued.Confirmed)
	}
	if drain, ok := r.drain("backlog"); ok {
		r.add(categoryCorrectness, "drain завершён до deadline", fmt.Sprintf("completed=%v", drain.Completed), "true", drain.Completed)
		r.add(categoryPerformance, "drain 1000 от команды возврата workers", fmt.Sprintf("%.2f с (±%.2f с интервал опроса)", drain.DrainSeconds, drain.ResolutionSeconds),
			"<= "+r.gateOpts.drain.String(), drain.Completed && drain.DrainSeconds <= r.gateOpts.drain.Seconds())
		r.section("Drain backlog",
			fmt.Sprintf("- t0 (команда возврата workers): %s", drain.Since.Format(time.RFC3339Nano)),
			fmt.Sprintf("- Первая готовность: %.2f с; все готовы: %.2f с; outbox опустошён: %.2f с", drain.FirstReadySeconds, drain.DrainSeconds, drain.OutboxEmptySeconds),
			fmt.Sprintf("- Средняя скорость drain: %s ready/с", rateOf(drain.Final.Ready, drain.DrainSeconds)),
			fmt.Sprintf("- Погрешность интервала наблюдения: %.2f с", drain.ResolutionSeconds),
		)
	}
	if value, ok := r.reconcile("backlog"); ok {
		r.correctnessGates(value, "backlog", expected)
		r.reconcileSection("Сверка с PostgreSQL · backlog", value)
	}
}

func (r reportInputs) burst() {
	base := r.envFloat("BURST_BASE_RATE", 10)
	peak := r.envFloat("BURST_PEAK_RATE", 100)
	pre := r.envFloat("BURST_PRE_S", 120)
	spike := r.envFloat("BURST_SPIKE_S", 10)
	post := r.envFloat("BURST_POST_S", 180)
	if summary, ok := r.summary("burst"); ok {
		dropped := summary.value("dropped_iterations", "count")
		r.add(categoryLoad, "dropped_iterations (base/spike)", fmt.Sprintf("%.0f / %.0f", summary.value("dropped_iterations{scenario:base}", "count"), summary.value("dropped_iterations{scenario:spike}", "count")),
			"0", dropped == 0)
		exhausted := summary.value("profiles_exhausted", "count")
		r.add(categoryLoad, "исчерпание профилей", formatFloat(jsonFloat(exhausted)), "0", exhausted == 0)
		preActual := summary.value("posts_started{phase:pre}", "count") / pre
		spikeActual := summary.value("posts_started{phase:spike}", "count") / spike
		postActual := summary.value("posts_started{phase:post}", "count") / post
		ratio := r.gateOpts.ingressRatio
		r.add(categoryLoad, "ingress до всплеска", fmt.Sprintf("%.2f из %.2f POST/с", preActual, base), fmt.Sprintf(">= %.0f%%", ratio*100), preActual >= base*ratio)
		r.add(categoryLoad, "ingress в пике", fmt.Sprintf("%.2f из %.2f POST/с", spikeActual, peak), fmt.Sprintf(">= %.0f%%", ratio*100), spikeActual >= peak*ratio)
		r.add(categoryLoad, "ingress после всплеска", fmt.Sprintf("%.2f из %.2f POST/с", postActual, base), fmt.Sprintf(">= %.0f%%", ratio*100), postActual >= base*ratio)
		r.errorRateGate(summary, "весь прогон")
		spikeStarted := summary.value("posts_started{phase:spike}", "count")
		spikeFailures := summary.value("post_failures{phase:spike}", "count")
		spikeRate := 0.0
		if spikeStarted > 0 {
			spikeRate = spikeFailures / spikeStarted
		}
		r.add(categoryPerformance, "пик: доля ошибок POST", fmt.Sprintf("%.3f%% (%.0f из %.0f)", spikeRate*100, spikeFailures, spikeStarted),
			fmt.Sprintf("< %.2f%%", r.gateOpts.errorRate*100), spikeStarted > 0 && spikeRate < r.gateOpts.errorRate)
		r.section("Приём POST по фазам (k6)",
			"| Фаза | POST | p50, мс | p95, мс | max, мс | ошибки |", "|---|---:|---:|---:|---:|---:|",
			phaseRow(summary, "pre"), phaseRow(summary, "spike"), phaseRow(summary, "post"))
		r.k6Section("k6 · burst", summary)
	}
	var analysis burstResult
	if err := readJSON(r.path("burst.json"), &analysis); err != nil {
		r.missing("burst.json")
	} else {
		r.add(categoryMeasurement, "ряды backlog/возраста/ready/ingress", strings.Join(analysis.MissingSeries, ", "), "все есть", len(analysis.MissingSeries) == 0)
		r.add(categoryLoad, "базовая нагрузка без устойчивого роста очереди",
			fmt.Sprintf("наклон backlog %s задач/с", formatFloat(analysis.BaselineBacklog.Slope)), fmt.Sprintf("<= %.2f", analysis.BaselineMaxSlope), !analysis.BaselineGrowing)
		recoveryValue := "не восстановилось за окно наблюдения"
		if analysis.Recovery.Recovered {
			recoveryValue = fmt.Sprintf("%s с после конца пика", formatFloat(analysis.Recovery.Seconds))
		}
		r.add(categoryRecovery, "возврат backlog и возраста в диапазон до всплеска с удержанием",
			recoveryValue, fmt.Sprintf("backlog <= %.0f, возраст <= %.1f с, удержание %.0f с при продолжающейся базовой нагрузке",
				analysis.Recovery.BacklogLimit, analysis.Recovery.AgeLimitSeconds, analysis.Recovery.HoldSeconds), analysis.Recovery.Recovered)
		r.section("Очередь до, во время и после всплеска (VictoriaMetrics, шаг "+analysis.Step+")",
			fmt.Sprintf("- Базовое окно: %s — %s", analysis.BaselineWindow[0].Format(time.RFC3339), analysis.BaselineWindow[1].Format(time.RFC3339)),
			fmt.Sprintf("- До всплеска: backlog %s…%s (среднее %s), возраст старейшей работы max %s с, ready/с среднее %s, ingress среднее %s",
				formatFloat(analysis.BaselineBacklog.Min), formatFloat(analysis.BaselineBacklog.Max), formatFloat(analysis.BaselineBacklog.Mean),
				formatFloat(analysis.BaselineAge.Max), formatFloat(analysis.BaselineReadyRate.Mean), formatFloat(analysis.BaselineIngress.Mean)),
			fmt.Sprintf("- Пик: backlog %s, возраст старейшей работы %s с; ingress в пике (VM) max %s POST/с",
				formatFloat(analysis.PeakBacklog), formatFloat(analysis.PeakAge), formatFloat(analysis.SpikeIngress.Max)),
			fmt.Sprintf("- Recovery: %s. %s", recoveryValue, analysis.Recovery.Note),
		)
	}
	if value, ok := r.reconcile("burst"); ok {
		r.correctnessGates(value, "burst", int(base*(pre+spike+post)+math.Max(0, peak-base)*spike))
		if spikeTTR, ok := value.ServerTimeToReady["spike"]; ok {
			r.section("Server time to ready для работ, поступивших в пик",
				fmt.Sprintf("- n=%d, p50=%s, p95=%s, p99=%s, max=%s (включая завершившиеся после пика)",
					spikeTTR.Count, spikeTTR.P50, spikeTTR.P95, spikeTTR.P99, spikeTTR.Max))
		}
		r.reconcileSection("Сверка с PostgreSQL · burst", value)
	}
	if drain, ok := r.drain("burst"); ok {
		r.add(categoryCorrectness, "drain после остановки подачи", drainValue(drain), "до deadline "+drain.Deadline, drain.Completed)
		r.section("Drain после остановки нагрузки", fmt.Sprintf("- От конца подачи до готовности всех работ: %.1f с (первая готовность в окне: %.1f с)", drain.DrainSeconds, drain.FirstReadySeconds))
	}
	r.verdictLine("burst",
		"Стенд выдержал заданный burst: нагрузка достигнута, базовая очередь стабильна, ошибки в бюджете, очередь вернулась в исходный диапазон.",
		"По этому прогону нельзя утверждать, что стенд выдержал заданный burst — см. непройденные пункты.")
}

func (r reportInputs) soak() {
	rate := r.envFloat("SOAK_RATE", 5)
	duration := r.envFloat("SOAK_DURATION_S", 3600)
	if summary, ok := r.summary("soak-warmup"); ok {
		r.loadGates(summary, int(summary.value("posts_started", "count")), r.envFloat("SOAK_WARMUP_S", 120), rate, "прогрев")
	}
	value, reconciled := r.reconcile("soak")
	if summary, ok := r.summary("soak-measure"); ok {
		r.loadGates(summary, value.PostsByScenario["measure"], duration, rate, "измерение")
		r.errorRateGate(summary, "измерение")
		r.k6Section("k6 · soak", summary)
	}
	if drain, ok := r.drain("soak"); ok {
		r.add(categoryCorrectness, "drain после остановки подачи", drainValue(drain), "до deadline "+drain.Deadline, drain.Completed)
	}
	if reconciled {
		r.correctnessGates(value, "soak", int(rate*duration)+value.User.Observed)
		r.reconcileSection("Сверка с PostgreSQL · soak", value)
	}
	var analysis soakResult
	if err := readJSON(r.path("soak.json"), &analysis); err != nil {
		r.missing("soak.json")
		return
	}
	lines := []string{
		"| # | начало | POST/с | ready/с | p95 server-ready, с | backlog avg/max | возраст max, с | retries | failed | рестарты | RSS max worker, МиБ | heap max worker, МиБ | gor. api | pool wait api, с |",
		"|---:|---|---:|---:|---:|---:|---:|---:|---:|---|---:|---:|---:|---:|",
	}
	for _, window := range analysis.Windows {
		lines = append(lines, fmt.Sprintf("| %d | %s | %s | %s | %s | %s/%s | %s | %s | %s | %s | %s | %s | %s | %s |",
			window.Index, window.Start.Format("15:04:05"), formatFloat(window.IngressPerSecond), formatFloat(window.ReadyPerSecond),
			formatFloat(window.ServerTTRP95), formatFloat(window.BacklogAvg), formatFloat(window.BacklogMax), formatFloat(window.OldestAgeMax),
			formatFloat(window.Retries), formatFloat(window.FailedRequests), describeFloats(window.Restarts),
			mebibytes(lookup(window.RSSMax, "worker")), mebibytes(lookup(window.HeapLiveMax, "worker")),
			formatFloat(lookup(window.GoroutinesMax, "api")), formatFloat(lookup(window.PoolWaitDelta, "api"))))
	}
	r.section("Окна soak по "+analysis.Window+" (полные ряды: soak-windows.csv, soak-series.json)", lines...)
	trendLines := []string{
		fmt.Sprintf("Сравниваются первые и последние %d окна. «Устойчивый рост» = последнее среднее выше первого более чем на 10%% и ≥80%% шагов растут. Это наблюдение baseline, не порог.", analysis.CompareWindows),
		"",
		"| Показатель | первые | последние | изменение | доля растущих шагов | устойчивый рост |",
		"|---|---:|---:|---:|---:|:---:|",
	}
	for _, trend := range analysis.Trends {
		mark := ""
		if trend.Sustained {
			mark = "да"
		}
		trendLines = append(trendLines, fmt.Sprintf("| %s | %s | %s | %s | %.2f | %s |", trend.Metric, formatFloat(trend.First), formatFloat(trend.Last),
			percent(trend.ChangeRatio), trend.Rising, mark))
	}
	r.section("Первые и последние 15 минут", trendLines...)
	sizes := []string{"| Таблица | начало, МиБ | конец, МиБ |", "|---|---:|---:|"}
	names := make([]string, 0, len(analysis.TableSizes.End))
	for name := range analysis.TableSizes.End {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		sizes = append(sizes, fmt.Sprintf("| %s | %s | %s |", name, mebibytes(lookup(analysis.TableSizes.Start, name)), mebibytes(analysis.TableSizes.End[name])))
	}
	sizes = append(sizes, fmt.Sprintf("| база целиком | %s | %s |", mebibytes(analysis.DatabaseSizeStart), mebibytes(analysis.DatabaseSizeEnd)),
		"", "Рост таблиц ожидаем: стенд создаёт новые snapshots весь час. Он не означает утечку в процессах.")
	r.section("Размер таблиц и диска PostgreSQL", sizes...)
}

func (r reportInputs) verdictLine(category, passed, failed string) {
	ok := len(r.data.Missing) == 0
	for _, item := range r.data.Gates {
		if !item.Passed {
			ok = false
		}
	}
	if ok {
		r.data.Verdicts[category] = passed
	} else {
		r.data.Verdicts[category] = failed
	}
}

// ---- rendering ----------------------------------------------------------------

func renderReport(data *reportData) string {
	var builder strings.Builder
	fmt.Fprintf(&builder, "# Bench %s · %s\n\n", data.Scenario, data.RunID)
	status := "ПРОЙДЕН"
	if !data.Passed {
		status = "НЕ ПРОЙДЕН"
	}
	fmt.Fprintf(&builder, "**Итог: %s.** Вердикт по производительности и исправность измерителя указаны отдельно.\n\n", status)
	keys := make([]string, 0, len(data.Verdicts))
	for key := range data.Verdicts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		fmt.Fprintf(&builder, "- %s: %s\n", key, data.Verdicts[key])
	}
	if len(data.Missing) > 0 {
		fmt.Fprintf(&builder, "- Отсутствуют обязательные файлы: %s\n", strings.Join(data.Missing, ", "))
	}
	builder.WriteString("\n## Параметры запуска\n\n")
	for _, key := range []string{"RUN_ID", "SCENARIO", "TESTID", "SEED", "PROFILES", "EVENTS_PER_PROFILE", "YEAR", "COMMAND_SOURCE", "WORKERS", "NARRATIVE_PROVIDER", "WORKER_RETRY_BASE", "WORKER_RETRY_MAX", "WORKER_MAX_ATTEMPTS", "WORKER_LEASE_DURATION", "WORKER_HEARTBEAT_INTERVAL", "GIT_SHA"} {
		if value, ok := data.Env[key]; ok {
			fmt.Fprintf(&builder, "- %s: `%s`\n", key, value)
		}
	}
	extra := make([]string, 0)
	for key := range data.Env {
		if strings.HasPrefix(key, strings.ToUpper(data.Scenario)+"_") {
			extra = append(extra, key)
		}
	}
	sort.Strings(extra)
	for _, key := range extra {
		fmt.Fprintf(&builder, "- %s: `%s`\n", key, data.Env[key])
	}
	if len(data.Phases) > 0 {
		builder.WriteString("\n## Границы фаз (UTC)\n\n| Отметка | Время |\n|---|---|\n")
		for _, mark := range data.Phases {
			fmt.Fprintf(&builder, "| %s | %s |\n", mark.Name, mark.At.Format(time.RFC3339Nano))
		}
	}
	builder.WriteString("\n## Проверки\n\n| Категория | Проверка | Значение | Цель | Статус |\n|---|---|---|---|:---:|\n")
	for _, item := range data.Gates {
		mark := "ok"
		if !item.Passed {
			mark = "**FAIL**"
		}
		fmt.Fprintf(&builder, "| %s | %s | %s | %s | %s |\n", item.Category, item.Name, escapeCell(item.Value), escapeCell(item.Threshold), mark)
	}
	builder.WriteString("\n## Детали\n\n")
	for _, section := range data.sections {
		builder.WriteString(section)
		builder.WriteString("\n")
	}
	builder.WriteString("## Файлы прогона\n\n")
	builder.WriteString("- `environment.json` — хост, версии, эффективные значения конфигурации из /metrics, env контейнеров без секретов\n")
	builder.WriteString("- `k6-*-summary.json` — итог k6 по фазам; `manifest-*.log` — manifest попыток (JSON-логи k6)\n")
	builder.WriteString("- `reconcile-*.json` — сверка manifest с PostgreSQL; `drain-*.json` — ожидание завершения\n")
	builder.WriteString("- `metrics-*.json` — targets, ряды k6, режим narrative, серверные показатели окна\n")
	builder.WriteString("- `phases.log` — UTC-границы фаз; `docker-stats.jsonl` — CPU/память контейнеров; `logs/` — логи сервисов\n")
	return builder.String()
}

// drainValue describes a drain wait; an unfinished drain has no duration.
func drainValue(drain drainResult) string {
	if !drain.Completed {
		return fmt.Sprintf("не завершён за %s: ready=%d, failed=%d, незавершённых=%d, найдено в PG %d из %d",
			drain.Deadline, drain.Final.Ready, drain.Final.Failed, drain.Final.Pending, drain.Final.FoundInP, drain.Expected)
	}
	return fmt.Sprintf("%.1f с от конца подачи (первая готовность %.1f с)", drain.DrainSeconds, drain.FirstReadySeconds)
}

func escapeCell(value string) string {
	return strings.ReplaceAll(strings.ReplaceAll(value, "|", "\\|"), "\n", " ")
}

func latencyRow(name string, value benchmark.LatencySummary) string {
	return fmt.Sprintf("| %s | %d | %s | %s | %s | %s |", name, value.Count, value.P50.Round(time.Millisecond), value.P95.Round(time.Millisecond), value.P99.Round(time.Millisecond), value.Max.Round(time.Millisecond))
}

func phaseRow(summary k6Summary, phase string) string {
	metric := "post_accept_202{phase:" + phase + "}"
	return fmt.Sprintf("| %s | %.0f | %.1f | %.1f | %.1f | %.0f |", phase,
		summary.value("posts_started{phase:"+phase+"}", "count"), summary.value(metric, "med"), summary.value(metric, "p(95)"),
		summary.value(metric, "max"), summary.value("post_failures{phase:"+phase+"}", "count"))
}

func describeTargets(value metricsResult) string {
	jobs := make([]string, 0, len(value.TargetsUp))
	for job := range value.TargetsUp {
		jobs = append(jobs, job)
	}
	sort.Strings(jobs)
	parts := make([]string, 0, len(jobs))
	for _, job := range jobs {
		parts = append(parts, fmt.Sprintf("%s=%d (возраст %s с)", job, value.TargetsUp[job], formatFloat(value.StaleSeconds[job])))
	}
	if len(value.Problems) > 0 {
		parts = append(parts, "замечания: "+strings.Join(value.Problems, "; "))
	}
	return strings.Join(parts, ", ")
}

func describeExpected(value metricsResult) string {
	jobs := make([]string, 0, len(value.TargetsExpected))
	for job := range value.TargetsExpected {
		jobs = append(jobs, job)
	}
	sort.Strings(jobs)
	parts := make([]string, 0, len(jobs))
	for _, job := range jobs {
		parts = append(parts, fmt.Sprintf("%s=%d", job, value.TargetsExpected[job]))
	}
	return strings.Join(parts, ", ")
}

func describeFloats(values map[string]jsonFloat) string {
	if len(values) == 0 {
		return "нет данных"
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+formatFloat(values[key]))
	}
	return strings.Join(parts, ", ")
}

func formatFloat(value jsonFloat) string {
	number := float64(value)
	if math.IsNaN(number) || math.IsInf(number, 0) {
		return "нет данных"
	}
	if number == math.Trunc(number) && math.Abs(number) < 1e12 {
		return strconv.FormatFloat(number, 'f', 0, 64)
	}
	return strconv.FormatFloat(number, 'f', 3, 64)
}

func mebibytes(value jsonFloat) string {
	if math.IsNaN(float64(value)) {
		return "нет данных"
	}
	return strconv.FormatFloat(float64(value)/(1<<20), 'f', 1, 64)
}

func percent(value jsonFloat) string {
	if math.IsNaN(float64(value)) {
		return "нет данных"
	}
	return strconv.FormatFloat(float64(value)*100, 'f', 1, 64) + "%"
}

func rateOf(count int, seconds float64) string {
	if seconds <= 0 {
		return "нет данных"
	}
	return strconv.FormatFloat(float64(count)/seconds, 'f', 2, 64)
}

func sumCounts(values map[string]int) int {
	total := 0
	for _, value := range values {
		total += value
	}
	return total
}
