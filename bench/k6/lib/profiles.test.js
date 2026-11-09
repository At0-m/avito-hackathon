// Regression tests for profile selection at the quota boundary.
// Run: node --test bench/k6/lib/profiles.test.js
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { selectProfile, SELECT_EXHAUSTED, SELECT_OVER_QUOTA, SELECT_PROFILE } from './profiles.js';

test('smoke: the last planned iteration uses the last seeded profile', () => {
  const cohort = { offset: 0, count: 100 };
  assert.deepEqual(selectProfile(cohort, 99, 100), { kind: SELECT_PROFILE, index: 99 });
});

test('smoke: an extra arrival after 100 planned POSTs completes the quota, not exhaustion', () => {
  // constant-arrival-rate 5/s x 20s may start iterationInTest=100.
  const cohort = { offset: 0, count: 100 };
  assert.equal(selectProfile(cohort, 100, 100).kind, SELECT_OVER_QUOTA);
  assert.equal(selectProfile(cohort, 101, 100).kind, SELECT_OVER_QUOTA);
});

test('a planned POST without a seeded profile is exhaustion', () => {
  const cohort = { offset: 0, count: 100 };
  assert.deepEqual(selectProfile(cohort, 99, 99), { kind: SELECT_EXHAUSTED, index: 99 });
});

test('offset cohorts: boundary of the measurement and user ranges', () => {
  // steady defaults: warmup 1200, measure 6000 at offset 1200, users 60 at 7200.
  const measure = { offset: 1200, count: 6000 };
  assert.deepEqual(selectProfile(measure, 5999, 7260), { kind: SELECT_PROFILE, index: 7199 });
  assert.equal(selectProfile(measure, 6000, 7260).kind, SELECT_OVER_QUOTA);
  const users = { offset: 7200, count: 60 };
  assert.deepEqual(selectProfile(users, 59, 7260), { kind: SELECT_PROFILE, index: 7259 });
  assert.equal(selectProfile(users, 60, 7260).kind, SELECT_OVER_QUOTA);
  assert.deepEqual(selectProfile(users, 59, 7259), { kind: SELECT_EXHAUSTED, index: 7259 });
});

test('hot key: fixed profile, quota is not applied', () => {
  const cohort = { offset: 0, count: 100, fixed: true };
  assert.deepEqual(selectProfile(cohort, 150, 1), { kind: SELECT_PROFILE, index: 0 });
  assert.equal(selectProfile({ offset: 1, count: 100, fixed: true }, 0, 1).kind, SELECT_EXHAUSTED);
});
