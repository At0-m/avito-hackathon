// Profile selection for k6 iterations. Pure module without k6 imports so the
// boundary rules are testable with `node --test bench/k6/lib/profiles.test.js`.
//
// A cohort plans exactly `count` POSTs on profiles [offset, offset + count).
// k6 executors may start more iterations than planned: constant-arrival-rate
// can schedule one extra arrival at the end of the window (101 starts for
// 5/s x 20s). Such an iteration completes the quota and must not be confused
// with a planned POST that has no seeded profile.

export const SELECT_PROFILE = 'profile';
// The planned quota is complete; no POST is sent. Not an error.
export const SELECT_OVER_QUOTA = 'over_quota';
// A planned POST (iteration < count) has no seeded profile. Load not reached.
export const SELECT_EXHAUSTED = 'exhausted';

/**
 * @param {{offset: number, count: number, fixed?: boolean}} cohort
 * @param {number} iteration exec.scenario.iterationInTest (0-based)
 * @param {number} available number of seeded profile IDs
 * @returns {{kind: string, index: number}}
 */
export function selectProfile(cohort, iteration, available) {
  if (cohort.fixed) {
    // Hot key: every iteration targets the same profile.
    return cohort.offset < available
      ? { kind: SELECT_PROFILE, index: cohort.offset }
      : { kind: SELECT_EXHAUSTED, index: cohort.offset };
  }
  if (iteration >= cohort.count) {
    return { kind: SELECT_OVER_QUOTA, index: -1 };
  }
  const index = cohort.offset + iteration;
  if (index >= available) {
    return { kind: SELECT_EXHAUSTED, index };
  }
  return { kind: SELECT_PROFILE, index };
}
