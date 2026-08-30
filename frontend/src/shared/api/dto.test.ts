import assert from 'node:assert/strict';
import test from 'node:test';
import { isRecapDTO, type RecapRequestStatusDTO, type RecapDTO } from './dto.ts';

void test('isRecapDTO distinguishes async status from ready snapshot', () => {
  const status: RecapRequestStatusDTO = {
    id: '11111111-1111-1111-1111-111111111111',
    profile_id: '22222222-2222-2222-2222-222222222222',
    year: 2026,
    status: 'processing',
    stage: 'computing_features',
    progress_percent: 35,
    attempt: 1,
    max_attempts: 3,
    poll_after_ms: 500,
    links: { self: '/api/v1/recaps/11111111-1111-1111-1111-111111111111' },
  };

  const recap = {
    schema_version: '2.0',
    id: status.id,
    profile_id: status.profile_id,
    year: 2026,
    profile: { id: status.profile_id, name: 'Test' },
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
  } satisfies RecapDTO;

  assert.equal(isRecapDTO(status), false);
  assert.equal(isRecapDTO(recap), true);
});
