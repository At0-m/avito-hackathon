// k6 load profile for the Avito Recap bench stand.
//
// One capacity iteration sends exactly one POST /recaps and never polls, so a
// slow worker does not hold VUs of the capacity scenario and does not reduce
// the arrival rate (open model). A separate small "user" scenario polls until
// ready honouring poll_after_ms to measure user time to ready.
//
// Every POST writes one MANIFEST line to the k6 log (run k6 with
// --log-format=json --log-output=file=...). bench-verify joins the manifest
// with PostgreSQL after the run. IDs and profile IDs never become metric tags.
//
// Selected with PHASE: smoke, steady-warmup, steady-measure, hotkey,
// backlog-fill, burst, soak-warmup, soak-measure.

import http from 'k6/http';
import exec from 'k6/execution';
import { sleep } from 'k6';
import { SharedArray } from 'k6/data';
import { Counter, Trend, Gauge } from 'k6/metrics';

function num(name, fallback) {
  const raw = __ENV[name];
  if (raw === undefined || raw === '') return fallback;
  const value = Number(raw);
  if (!Number.isFinite(value)) throw new Error(`${name} must be a number, got ${raw}`);
  return value;
}

const PHASE = __ENV.PHASE || 'smoke';
const LABEL = __ENV.LABEL || PHASE;
const RESULTS_DIR = __ENV.RESULTS_DIR || '/results';
const BASE_URL = (__ENV.BASE_URL || 'http://backend:8080/api/v1').replace(/\/$/, '');
const YEAR = num('YEAR', 2026);
const IDS_FILE = __ENV.IDS_FILE || `${RESULTS_DIR}/profiles.json`;
const POST_TIMEOUT_S = num('POST_TIMEOUT_S', 10);
const USER_TIMEOUT_S = num('USER_TIMEOUT_S', 180);
const P95_MS = num('POST_P95_MS', 150);
const ERROR_RATE = num('POST_ERROR_RATE', 0.01);

const profiles = new SharedArray('profiles', () => JSON.parse(open(IDS_FILE)));
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

// Client-side metrics (exported via remote write as k6_<name>_*).
const postsStarted = new Counter('posts_started');
const postFailures = new Counter('post_failures');
const postsAccepted202 = new Counter('posts_accepted_202');
const postsReady200 = new Counter('posts_ready_200');
const postsFailed202 = new Counter('posts_failed_202');
const postsContractError = new Counter('posts_contract_error');
const postsRejected = new Counter('posts_rejected');
const postsTransport = new Counter('posts_transport_error');
const profilesExhausted = new Counter('profiles_exhausted');
const accept202 = new Trend('post_accept_202', true);
const accept200 = new Trend('post_accept_200', true);
const targetRate = new Gauge('target_post_rate');
const userReady = new Counter('user_ready');
const userFailed = new Counter('user_failed');
const userTimeout = new Counter('user_timeout');
const userContractError = new Counter('user_contract_error');
const userGetErrors = new Counter('user_get_errors');
const userTimeToReady = new Trend('user_time_to_ready', true);
const userPolls = new Counter('user_polls');

// ---- workload description -------------------------------------------------

function rateSpec(rate) {
  // constant-arrival-rate needs an integer rate; scale the time unit for
  // fractional rates (for example 2.5/s -> 25 per 10s, evenly spaced).
  for (const scale of [1, 10, 100, 1000]) {
    const scaled = Math.round(rate * scale * 1e6) / 1e6;
    if (Number.isInteger(scaled) && scaled > 0) return { rate: scaled, timeUnit: `${scale}s` };
  }
  throw new Error(`unsupported rate ${rate}`);
}

function vusFor(rate, iterationSeconds) {
  // BENCH_* names avoid clashing with k6's own K6_* option variables.
  const pre = num('BENCH_PREALLOCATED_VUS', Math.max(5, Math.ceil(rate * 2)));
  const max = Math.max(pre, num('BENCH_MAX_VUS', Math.ceil(rate * iterationSeconds) + 10));
  return { preAllocatedVUs: pre, maxVUs: max };
}

function arrival(rate, durationS, startS, execName) {
  return Object.assign(
    {
      executor: 'constant-arrival-rate',
      duration: `${durationS}s`,
      startTime: `${startS}s`,
      exec: execName || 'capacity',
      gracefulStop: `${Math.ceil(POST_TIMEOUT_S) + 5}s`,
    },
    rateSpec(rate),
    vusFor(rate, POST_TIMEOUT_S),
  );
}

// cohorts[scenario] = { offset, count, rate, phases? }
const cohorts = {};
const scenarios = {};
const thresholds = {};
let userCohort = null;
let pollEveryNth = 0;

function addCapacity(name, rate, durationS, startS, offset) {
  const count = Math.round(rate * durationS);
  scenarios[name] = arrival(rate, durationS, startS);
  cohorts[name] = { offset, count, rate };
  return offset + count;
}

function addUserCohort(name, everyS, durationS, startS, offset) {
  const count = Math.max(1, Math.floor(durationS / everyS));
  scenarios[name] = Object.assign(
    {
      executor: 'constant-arrival-rate',
      rate: 1,
      timeUnit: `${everyS}s`,
      duration: `${durationS}s`,
      startTime: `${startS}s`,
      exec: 'user',
      gracefulStop: `${USER_TIMEOUT_S + 5}s`,
    },
    vusFor(1 / everyS, USER_TIMEOUT_S),
  );
  userCohort = { name, offset, count };
  cohorts[name] = { offset, count, rate: 1 / everyS };
  return offset + count;
}

switch (PHASE) {
  case 'smoke': {
    // 100 new profiles at 5 POST/s; every Nth iteration also polls to ready.
    const rate = num('RATE', 5);
    const count = num('COUNT', 100);
    addCapacity('smoke', rate, count / rate, 0, num('OFFSET', 0));
    pollEveryNth = num('POLL_EVERY_NTH', 10);
    scenarios.smoke.gracefulStop = `${USER_TIMEOUT_S + 5}s`;
    Object.assign(scenarios.smoke, vusFor(rate, USER_TIMEOUT_S));
    break;
  }
  case 'steady-warmup':
  case 'soak-warmup': {
    addCapacity('warmup', num('RATE', 20), num('DURATION_S', 60), 0, num('OFFSET', 0));
    break;
  }
  case 'steady-measure':
  case 'soak-measure': {
    const rate = num('RATE', 20);
    const duration = num('DURATION_S', 300);
    const next = addCapacity('measure', rate, duration, 0, num('OFFSET', 0));
    const everyS = num('USER_EVERY_S', 5);
    if (everyS > 0) addUserCohort('user', everyS, duration, 0, num('USER_OFFSET', next));
    thresholds['post_accept_202'] = [`p(95)<${P95_MS}`];
    thresholds['post_failures'] = [`count<=${Math.floor(rate * duration * ERROR_RATE)}`];
    thresholds['dropped_iterations'] = ['count==0'];
    break;
  }
  case 'hotkey': {
    // 100 concurrent POSTs for one profile/year.
    const vus = num('VUS', 100);
    scenarios.hotkey = {
      executor: 'per-vu-iterations',
      vus,
      iterations: 1,
      maxDuration: '60s',
      exec: 'capacity',
    };
    cohorts.hotkey = { offset: num('OFFSET', 0), count: vus, rate: 0, fixed: true };
    break;
  }
  case 'backlog-fill': {
    const count = num('COUNT', 1000);
    scenarios.fill = {
      executor: 'shared-iterations',
      vus: num('VUS', 50),
      iterations: count,
      maxDuration: `${num('MAX_DURATION_S', 120)}s`,
      exec: 'capacity',
    };
    cohorts.fill = { offset: num('OFFSET', 0), count, rate: 0 };
    break;
  }
  case 'burst': {
    // Base rate for pre+spike+post, plus an extra scenario during the spike so
    // that the total is the peak rate. Jumps are instantaneous.
    const base = num('BASE_RATE', 10);
    const peak = num('PEAK_RATE', 100);
    const pre = num('PRE_S', 120);
    const spike = num('SPIKE_S', 10);
    const post = num('POST_S', 180);
    let next = addCapacity('base', base, pre + spike + post, 0, num('OFFSET', 0));
    if (peak > base) next = addCapacity('spike', peak - base, spike, pre, next);
    for (const name of ['base', 'spike']) {
      if (cohorts[name]) cohorts[name].phases = { pre, spike };
    }
    for (const phase of ['pre', 'spike', 'post']) {
      // Permissive thresholds only make k6 keep per-phase sub-metrics.
      thresholds[`post_accept_202{phase:${phase}}`] = ['p(95)<1000000'];
      thresholds[`posts_started{phase:${phase}}`] = ['count>=0'];
      thresholds[`post_failures{phase:${phase}}`] = ['count>=0'];
    }
    thresholds['dropped_iterations{scenario:base}'] = ['count>=0'];
    thresholds['dropped_iterations{scenario:spike}'] = ['count>=0'];
    break;
  }
  default:
    throw new Error(`unknown PHASE ${PHASE}`);
}

// Permissive thresholds only make k6 keep these sub-metrics in the summary.
thresholds['http_req_duration{name:POST /recaps}'] = thresholds['http_req_duration{name:POST /recaps}'] || ['max>=0'];
thresholds['dropped_iterations'] = thresholds['dropped_iterations'] || ['count>=0'];

export const options = {
  scenarios,
  thresholds,
  discardResponseBodies: false,
  // Drop the url tag so GET /recaps/<uuid> does not create a series per id.
  systemTags: ['status', 'method', 'name', 'group', 'check', 'error', 'error_code', 'scenario', 'expected_response'],
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)', 'count'],
};

// ---- request logic ----------------------------------------------------------

function phaseOf(cohort) {
  if (!cohort || !cohort.phases) return PHASE;
  const elapsed = exec.instance.currentTestRunDuration / 1000;
  if (elapsed < cohort.phases.pre) return 'pre';
  if (elapsed < cohort.phases.pre + cohort.phases.spike) return 'spike';
  return 'post';
}

function manifest(record) {
  console.log('MANIFEST ' + JSON.stringify(record));
}

function classify(res, profileId) {
  if (res.status === 0) return { kind: 'transport', reason: String(res.error_code || '') };
  if (res.status !== 200 && res.status !== 202) return { kind: 'rejected', reason: String(res.status) };
  let body;
  try {
    body = res.json();
  } catch (e) {
    return { kind: 'contract_error', reason: 'invalid_json' };
  }
  if (!body || typeof body !== 'object' || typeof body.id !== 'string' || !UUID.test(body.id)) {
    return { kind: 'contract_error', reason: 'invalid_id' };
  }
  if (body.profile_id !== profileId || body.year !== YEAR) {
    return { kind: 'contract_error', reason: 'identity_mismatch', id: body.id };
  }
  if (res.status === 200) {
    if (!body.schema_version) return { kind: 'contract_error', reason: 'missing_schema_version', id: body.id };
    return { kind: 'snapshot', id: body.id };
  }
  if (body.status === 'failed') return { kind: 'failed_202', id: body.id, reason: (body.error && body.error.code) || '' };
  if (!['queued', 'processing', 'ready'].includes(body.status)) {
    return { kind: 'contract_error', reason: 'unexpected_status', id: body.id };
  }
  if (typeof body.poll_after_ms !== 'number' || !body.links || typeof body.links.self !== 'string') {
    return { kind: 'contract_error', reason: 'missing_polling_fields', id: body.id };
  }
  return { kind: 'accepted', id: body.id, pollAfter: body.poll_after_ms, self: body.links.self };
}

function postRecap(profileId, index, scenario, phase, iteration, isUser) {
  const tags = { phase };
  postsStarted.add(1, tags);
  const started = Date.now();
  const res = http.post(`${BASE_URL}/recaps`, JSON.stringify({ profile_id: profileId, year: YEAR }), {
    headers: { 'Content-Type': 'application/json' },
    tags: { name: 'POST /recaps', phase },
    timeout: `${POST_TIMEOUT_S}s`,
    responseCallback: http.expectedStatuses(200, 202),
  });
  const result = classify(res, profileId);
  const duration = Date.now() - started; // includes body read and validation
  switch (result.kind) {
    case 'accepted':
      postsAccepted202.add(1, tags);
      accept202.add(duration, tags);
      break;
    case 'snapshot':
      postsReady200.add(1, tags);
      accept200.add(duration, tags);
      break;
    case 'failed_202':
      postsFailed202.add(1, tags);
      break;
    case 'contract_error':
      postsContractError.add(1, tags);
      postFailures.add(1, tags);
      break;
    case 'rejected':
      postsRejected.add(1, tags);
      postFailures.add(1, tags);
      break;
    default:
      postsTransport.add(1, tags);
      postFailures.add(1, tags);
  }
  result.record = {
    s: scenario,
    ph: phase,
    it: iteration,
    pi: index,
    p: profileId,
    y: YEAR,
    t: started,
    d: duration,
    c: res.status,
    id: result.id || '',
    k: result.kind,
    r: result.reason || '',
    u: Boolean(isUser),
  };
  return result;
}

function pickProfile(cohort, iteration) {
  if (cohort.fixed) return cohort.offset;
  if (iteration >= cohort.count) return -1;
  const index = cohort.offset + iteration;
  return index < profiles.length ? index : -1;
}

function exhausted(scenario, phase, iteration) {
  profilesExhausted.add(1, { phase });
  manifest({ s: scenario, ph: phase, it: iteration, k: 'exhausted' });
}

export function capacity() {
  const scenario = exec.scenario.name;
  const cohort = cohorts[scenario];
  const iteration = exec.scenario.iterationInTest;
  const phase = phaseOf(cohort);
  if (cohort.rate > 0) targetRate.add(cohort.rate, { phase });
  const index = pickProfile(cohort, iteration);
  if (index < 0) {
    exhausted(scenario, phase, iteration);
    return;
  }
  const result = postRecap(profiles[index], index, scenario, phase, iteration, false);
  if (pollEveryNth > 0 && iteration % pollEveryNth === 0) {
    pollUntilReady(result, profiles[index]);
  }
  manifest(result.record);
}

export function user() {
  const scenario = exec.scenario.name;
  const cohort = cohorts[scenario];
  const iteration = exec.scenario.iterationInTest;
  const index = pickProfile(cohort, iteration);
  if (index < 0) {
    exhausted(scenario, PHASE, iteration);
    return;
  }
  const result = postRecap(profiles[index], index, scenario, PHASE, iteration, true);
  pollUntilReady(result, profiles[index]);
  manifest(result.record);
}

function pollUntilReady(result, profileId) {
  const record = result.record;
  record.u = true;
  const started = record.t;
  const finish = (outcome) => {
    record.o = outcome;
    record.tr = Date.now() - started;
  };
  if (result.kind === 'snapshot') {
    userReady.add(1);
    userTimeToReady.add(Date.now() - started);
    finish('ready');
    return;
  }
  if (result.kind !== 'accepted') {
    finish(result.kind === 'failed_202' ? 'failed' : 'post_error');
    if (result.kind === 'failed_202') userFailed.add(1);
    return;
  }
  let wait = result.pollAfter;
  let polls = 0;
  while (Date.now() - started < USER_TIMEOUT_S * 1000) {
    sleep(Math.min(Math.max(wait, 100), 5000) / 1000);
    polls++;
    userPolls.add(1);
    const res = http.get(`${BASE_URL}/recaps/${result.id}`, {
      tags: { name: 'GET /recaps/:id' },
      timeout: `${POST_TIMEOUT_S}s`,
      responseCallback: http.expectedStatuses(200),
    });
    if (res.status !== 200) {
      userGetErrors.add(1);
      continue;
    }
    let body;
    try {
      body = res.json();
    } catch (e) {
      userContractError.add(1);
      record.n = polls;
      finish('contract_error');
      return;
    }
    if (body && body.schema_version) {
      record.n = polls;
      if (body.id !== result.id || body.profile_id !== profileId || body.year !== YEAR) {
        userContractError.add(1);
        finish('contract_error');
        return;
      }
      userReady.add(1);
      userTimeToReady.add(Date.now() - started);
      finish('ready');
      return;
    }
    if (body && body.status === 'failed') {
      record.n = polls;
      userFailed.add(1);
      finish('failed');
      return;
    }
    if (body && typeof body.poll_after_ms === 'number') wait = body.poll_after_ms;
  }
  record.n = polls;
  userTimeout.add(1);
  finish('timeout');
}

// ---- summary ----------------------------------------------------------------

function metricLine(data, name, field) {
  const metric = data.metrics[name];
  if (!metric || metric.values[field] === undefined) return 'n/a';
  const value = metric.values[field];
  return typeof value === 'number' ? value.toFixed(2) : String(value);
}

export function handleSummary(data) {
  const out = {};
  out[`${RESULTS_DIR}/k6-${LABEL}-summary.json`] = JSON.stringify(data, null, 2);
  const lines = [
    `k6 phase ${LABEL}`,
    `  posts_started        ${metricLine(data, 'posts_started', 'count')}`,
    `  posts_accepted_202   ${metricLine(data, 'posts_accepted_202', 'count')}`,
    `  posts_ready_200      ${metricLine(data, 'posts_ready_200', 'count')}`,
    `  post_failures        ${metricLine(data, 'post_failures', 'count')}`,
    `  dropped_iterations   ${metricLine(data, 'dropped_iterations', 'count')}`,
    `  profiles_exhausted   ${metricLine(data, 'profiles_exhausted', 'count')}`,
    `  post_accept_202 p95  ${metricLine(data, 'post_accept_202', 'p(95)')} ms`,
    `  user_time_to_ready p95 ${metricLine(data, 'user_time_to_ready', 'p(95)')} ms`,
    '',
  ];
  out.stdout = lines.join('\n');
  return out;
}
