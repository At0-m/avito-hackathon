import assert from 'node:assert/strict';
import test from 'node:test';
import { GenerationFailedError } from './polling.ts';
import { streamRecapUntilReady, StreamUnavailableError, type EventSourceLike } from './streaming.ts';
import type { RecapDTO, RecapRequestStatusDTO } from './dto.ts';

class FakeEventSource implements EventSourceLike {
  readonly listeners = new Map<string, (event: MessageEvent<string>) => void>();
  onerror: ((event: Event) => void) | null = null;
  closed = false;

  addEventListener(type: string, listener: (event: MessageEvent<string>) => void): void {
    this.listeners.set(type, listener);
  }
  close(): void {
    this.closed = true;
  }
  emit(type: string, value: unknown): void {
    this.listeners.get(type)?.({ data: JSON.stringify(value) } as MessageEvent<string>);
  }
}

const request: RecapRequestStatusDTO = {
  id: '11111111-1111-1111-1111-111111111111',
  profile_id: '22222222-2222-2222-2222-222222222222',
  year: 2026,
  status: 'queued',
  stage: 'queued',
  progress_percent: 0,
  attempt: 0,
  max_attempts: 3,
  poll_after_ms: 500,
  links: {
    self: '/api/v1/recaps/11111111-1111-1111-1111-111111111111',
    stream: '/api/v1/recaps/11111111-1111-1111-1111-111111111111/stream',
  },
};

function recap(): RecapDTO {
  return {
    schema_version: '2.0',
    id: request.id,
    profile_id: request.profile_id,
    year: 2026,
    profile: { id: request.profile_id, name: 'Test' },
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

void test('streamRecapUntilReady reports progress and resolves ready recap', async () => {
  const source = new FakeEventSource();
  const observed: number[] = [];
  const promise = streamRecapUntilReady(request, {
    eventSourceFactory: () => source,
    onProgress: (status) => observed.push(status.progress_percent),
  });
  source.emit('status', { ...request, status: 'processing', progress_percent: 35 });
  source.emit('ready', recap());

  const value = await promise;
  assert.equal(value.id, request.id);
  assert.deepEqual(observed, [35]);
  assert.equal(source.closed, true);
});

void test('streamRecapUntilReady rejects when transport fails', async () => {
  const source = new FakeEventSource();
  const promise = streamRecapUntilReady(request, { eventSourceFactory: () => source });
  source.onerror?.(new Event('error'));
  await assert.rejects(promise, StreamUnavailableError);
});


void test('streamRecapUntilReady preserves terminal backend failure details', async () => {
  const source = new FakeEventSource();
  const promise = streamRecapUntilReady(request, { eventSourceFactory: () => source });
  source.emit('failed', {
    ...request,
    status: 'failed',
    stage: 'failed',
    error: {
      code: 'dependency_unavailable',
      message: 'Try again later',
      retryable: true,
    },
  });

  await assert.rejects(promise, (cause: unknown) => {
    assert.ok(cause instanceof GenerationFailedError);
    assert.equal(cause.code, 'dependency_unavailable');
    assert.equal(cause.retryable, true);
    return true;
  });
  assert.equal(source.closed, true);
});

void test('streamRecapUntilReady rejects when response has no stream link', async () => {
  const withoutStream: RecapRequestStatusDTO = {
    ...request,
    links: { self: request.links.self },
  };
  await assert.rejects(streamRecapUntilReady(withoutStream), StreamUnavailableError);
});
