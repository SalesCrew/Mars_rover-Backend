import assert from 'node:assert/strict';
import test from 'node:test';
import distribution from '../src/utils/fragebogenDistribution.ts';

const { scoreFragebogenDistributionAnswers } = distribution;

test('uses the export ratio for Perfect Store distribution answers', () => {
  const score = scoreFragebogenDistributionAnswers([
    { question_id: 'distribution-1', answer_boolean: true },
    { question_id: 'distribution-2', answer_boolean: false },
    { question_id: 'distribution-3', answer_boolean: true },
    { question_id: 'quality-1', answer_boolean: true },
  ], new Set(['distribution-1', 'distribution-2', 'distribution-3']));

  assert.deepEqual(score, { yes: 2, total: 3, percentage: 67 });
});

test('does not show zero percent when no distribution answer exists', () => {
  const score = scoreFragebogenDistributionAnswers([
    { question_id: 'distribution-1', answer_boolean: null },
    { question_id: 'quality-1', answer_boolean: false },
  ], new Set(['distribution-1']));

  assert.deepEqual(score, { yes: 0, total: 0, percentage: null });
});
