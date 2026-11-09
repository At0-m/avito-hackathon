package telemetry

// Bucket layouts are fixed so that histograms from every replica can be
// summed with sum(rate(..._bucket)) before histogram_quantile.
var (
	httpBuckets  = []float64{0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.15, 0.25, 0.5, 1, 2.5, 5, 10}
	stageBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60}
	waitBuckets  = []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600}
)

// Finite label values used by the application. Keep this list in sync with
// bench/grafana dashboards.
const (
	CreateOutcomeSnapshotHit = "snapshot_hit"
	CreateOutcomeCreated     = "created"
	CreateOutcomeRestarted   = "restarted_failed"
	CreateOutcomeExisting    = "existing_request"
	CreateOutcomeError       = "error"

	ClaimSourceDatabase = "database"
	ClaimSourceBroker   = "broker"

	AttemptReady           = "ready"
	AttemptRequeued        = "requeued"
	AttemptFailedTerminal  = "failed_terminal"
	AttemptFailedExhausted = "failed_exhausted"
	AttemptLeaseLost       = "lease_lost"
	AttemptStateError      = "state_error"

	StageActivityRead = "activity_read"
	StageGenerate     = "generate"
	StagePGFinalize   = "pg_finalize"
	StageAttempt      = "attempt"

	OutboxRecordPublished = "published"
	OutboxRecordRequeued  = "requeued"
	OutboxRecordError     = "error"

	NarrativeProviderDisabled    = "provider_disabled"
	NarrativeProviderSuccess     = "provider_success"
	NarrativeFallbackProviderErr = "fallback_provider_error"
	NarrativeFallbackValidation  = "fallback_validation_error"
)

var (
	HTTPRequests = Default.CounterVec(
		"recap_http_requests_total",
		"HTTP requests by route template, method and status class.",
		"route", "method", "status_class",
	)
	HTTPDuration = Default.HistogramVec(
		"recap_http_request_duration_seconds",
		"Server-side HTTP handling time by route template and method.",
		httpBuckets, "route", "method",
	)
	CreateOutcomes = Default.CounterVec(
		"recap_create_outcomes_total",
		"POST /recaps decision: snapshot_hit, created, restarted_failed, existing_request or error.",
		"outcome",
	)

	WorkerClaims = Default.CounterVec(
		"recap_worker_claims_total",
		"Successful recap request claims by command source.",
		"source",
	)
	WorkerQueueWait = Default.HistogramVec(
		"recap_worker_queue_wait_seconds",
		"Time from available_at to a successful claim (includes retry backoff and lease expiry waits).",
		waitBuckets, "source",
	)
	WorkerAttemptResults = Default.CounterVec(
		"recap_worker_attempt_results_total",
		"Outcome of each processing attempt.",
		"result",
	)
	WorkerFailures = Default.CounterVec(
		"recap_worker_failures_total",
		"Failed attempts by stable error code (requeued or terminal).",
		"code",
	)
	WorkerRetryDelay = Default.HistogramVec(
		"recap_worker_retry_delay_seconds",
		"Backoff assigned to requeued attempts.",
		waitBuckets,
	)
	WorkerStageDuration = Default.HistogramVec(
		"recap_worker_stage_duration_seconds",
		"Duration of processing stages: activity_read (ClickHouse), generate (including narrative), pg_finalize, attempt (whole attempt).",
		stageBuckets, "stage",
	)
	WorkerBusy = Default.GaugeVec(
		"recap_worker_busy",
		"1 while the worker processes an attempt.",
	)
	WorkerExpiredLeaseFailed = Default.CounterVec(
		"recap_worker_expired_lease_failed_total",
		"Requests marked failed by the maintenance sweep after lease expiry on the last attempt.",
	)
	EventPublishes = Default.CounterVec(
		"recap_event_publish_total",
		"Direct broker publications by workers: lifecycle and dlq events.",
		"kind", "result",
	)
	BrokerConsumerActive = Default.GaugeVec(
		"recap_broker_consumer_active",
		"1 while a subscribed Redpanda command consumer exists in this worker.",
	)
	BrokerConsumerRestarts = Default.CounterVec(
		"recap_broker_consumer_restarts_total",
		"Redpanda HTTP consumer recreations by reason: deferred, error, create_failed.",
		"reason",
	)

	NarrativeOutcomes = Default.CounterVec(
		"recap_narrative_outcomes_total",
		"Narrative result: provider_disabled (template by configuration), provider_success, fallback_provider_error, fallback_validation_error.",
		"outcome",
	)
	NarrativeProviderCalls = Default.CounterVec(
		"recap_narrative_provider_calls_total",
		"Calls made to the external narrative provider.",
	)
	NarrativeProviderMode = Default.GaugeVec(
		"recap_narrative_provider_mode",
		"1 for the effective narrative provider mode of this process.",
		"mode",
	)

	OutboxClaimed = Default.CounterVec(
		"recap_outbox_claimed_total",
		"Outbox events claimed for publication.",
	)
	OutboxPublishes = Default.CounterVec(
		"recap_outbox_publish_total",
		"Outbox publication attempts by result: published, requeued, mark_failed.",
		"result",
	)
	OutboxPublishDuration = Default.HistogramVec(
		"recap_outbox_publish_duration_seconds",
		"Duration of one broker publish call from the outbox publisher.",
		httpBuckets,
	)
	OutboxRecordDuration = Default.HistogramVec(
		"recap_outbox_record_duration_seconds",
		"PostgreSQL UPDATE recording the publication result of one outbox event: published (MarkOutboxEventPublished), requeued (RequeueOutboxEvent) or error.",
		httpBuckets, "result",
	)
	OutboxRetryDelay = Default.HistogramVec(
		"recap_outbox_retry_delay_seconds",
		"Backoff assigned to outbox events after a failed publish.",
		waitBuckets,
	)

	ConfigValue = Default.GaugeVec(
		"recap_config_value",
		"Effective runtime configuration after defaults and normalization (durations in seconds).",
		"param",
	)
)

// SetConfigSeconds records an effective duration setting.
func SetConfigSeconds(param string, seconds float64) {
	ConfigValue.Set(seconds, param)
}

// StatusClass maps an HTTP status code to 1xx..5xx.
func StatusClass(status int) string {
	switch {
	case status >= 500:
		return "5xx"
	case status >= 400:
		return "4xx"
	case status >= 300:
		return "3xx"
	case status >= 200:
		return "2xx"
	default:
		return "1xx"
	}
}

func init() {
	Default.RegisterProcessCollector()
}
