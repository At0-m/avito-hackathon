import assert from 'node:assert/strict';
import test from 'node:test';
import { GenerationFailedError, pollRecapUntilReady } from './polling.ts';
import type { RecapDTO, RecapRequestStatusDTO, RecapResponseDTO } from './dto.ts';

const requestId = '11111111-1111-1111-1111-111111111111';
const profileId = '22222222-2222-2222-2222-222222222222';

function status(
  state: RecapRequestStatusDTO['status'],
  progress: number,
): RecapRequestStatusDTO {
  return {
    id: requestId,
    profile_id: profileId,
    year: 2026,
    status: state,
    stage: state === 'processing' ? 'computing_features' : state,
    progress_percent: progress,
    attempt: state === 'queued' ? 0 : 1,
    max_attempts: 3,
    poll_after_ms: 500,
    links: { self: `/api/v1/recaps/${requestId}` },
  };
}

function readyRecap(): RecapDTO {
  return {
    schema_version: '2.0',
    id: requestId,
    profile_id: profileId,
    year: 2026,
    profile: { id: profileId, name: 'Test' },
    generation: {
      algorithm_version: 'rules-v1',
      feature_schema_version: 'features-v1',
      activity_hash: `sha256:${'a'.repeat(64)}`,
      generated_at: '2026-08-10T00:00:00Z',
      narrative: { source: 'template', prompt_version: 'template-v1' },
    },
    theme: { code: 'city', main_district: { code: 'goods', title: 'Товары' } },
    cards: [],
    capabilities: {
      share_available: false,
      explanation_available: false,
      feedback_available: false,
    },
  };
}

void test('pollRecapUntilReady follows lifecycle statuses and returns ready recap', async () => {
  const responses: RecapResponseDTO[] = [status('processing', 35), readyRecap()];
  const observed: number[] = [];
  const delays: number[] = [];

  const result = await pollRecapUntilReady(status('queued', 0), {
    fetchStatus: () => Promise.resolve(responses.shift() ?? readyRecap()),
    wait: (milliseconds) => {
      delays.push(milliseconds);
      return Promise.resolve();
    },
    onProgress: (value) => observed.push(value.progress_percent),
  });

  assert.equal(result.id, requestId);
  assert.deepEqual(observed, [0, 35]);
  assert.deepEqual(delays, [500, 500]);
});

void test('pollRecapUntilReady surfaces worker failure and retryability', async () => {
  const failed = status('failed', 35);
  failed.error = {
    code: 'dependency_unavailable',
    message: 'Generation dependency is temporarily unavailable',
    retryable: true,
  };

  await assert.rejects(
    pollRecapUntilReady(failed, {
      fetchStatus: () => Promise.resolve(readyRecap()),
      wait: () => Promise.resolve(),
    }),
    (error: unknown) => {
      assert.ok(error instanceof GenerationFailedError);
      assert.equal(error.code, 'dependency_unavailable');
      assert.equal(error.retryable, true);
      return true;
    },
  );
});

void test('pollRecapUntilReady stops immediately when aborted', async () => {
  const controller = new AbortController();
  controller.abort();

  await assert.rejects(
    pollRecapUntilReady(status('queued', 0), {
      fetchStatus: () => Promise.resolve(readyRecap()),
      signal: controller.signal,
      minimumDelayMS: 0,
    }),
    (error: unknown) => error instanceof DOMException && error.name === 'AbortError',
  );
});

void test('pollRecapUntilReady does not loop forever on inconsistent ready status', async () => {
  await assert.rejects(
    pollRecapUntilReady(status('ready', 100), {
      fetchStatus: () => Promise.resolve(readyRecap()),
      wait: () => Promise.resolve(),
    }),
    (error: unknown) => {
      assert.ok(error instanceof GenerationFailedError);
      assert.equal(error.code, 'internal_error');
      assert.equal(error.retryable, true);
      return true;
    },
  );
});
