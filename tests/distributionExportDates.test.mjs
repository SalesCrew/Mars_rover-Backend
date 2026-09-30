import assert from 'node:assert/strict';
import test from 'node:test';
import dates from '../src/utils/distributionExportDates.ts';

const { normalizeDistributionExportDateRange: range, isInDistributionExportDateRange: includes, distributionReportingQuarter: quarter } = dates;

test('date range is optional, inclusive and validates calendar dates and ordering', () => {
  assert.deepEqual(range(undefined, null), { startDate: null, endDate: null });
  assert.deepEqual(range('', '2026-07-10'), { startDate: null, endDate: '2026-07-10' });
  assert.throws(() => range('2026-02-29', null));
  assert.throws(() => range('2026-07-11', '2026-07-10'));
  assert.throws(() => range('2026-7-1', null));
  assert.throws(() => range(42, null));
  const selected = range('2026-07-01', '2026-07-10');
  assert.equal(includes('2026-06-30T21:59:59.999Z', selected), false);
  assert.equal(includes('2026-06-30T22:00:00Z', selected), true);
  assert.equal(includes('2026-07-10T21:59:59.999Z', selected), true);
  assert.equal(includes('2026-07-10T22:00:00Z', selected), false);
  assert.equal(includes('invalid', selected), false);
});

test('Vienna day boundaries follow winter time and the DST transition', () => {
  const winter = range('2026-01-01', '2026-01-01');
  assert.equal(includes('2025-12-31T23:00:00Z', winter), true);
  assert.equal(includes('2026-01-01T23:00:00Z', winter), false);
  const dst = range('2026-03-29', '2026-03-29');
  assert.equal(includes('2026-03-28T23:00:00Z', dst), true);
  assert.equal(includes('2026-03-29T21:59:59Z', dst), true);
  assert.equal(includes('2026-03-29T22:00:00Z', dst), false);
});

test('extended Q2 answers in July do not enter the Q3 reporting quarter', () => {
  assert.deepEqual(quarter(new Date('2026-07-08T12:00:00Z'), {
    name: 'Perfect Store PET', start_date: '2026-05-01', end_date: '2026-07-10'
  }), { quarterKey: '2026-Q2', quarterLabel: 'Q2 2026' });
  assert.deepEqual(quarter(new Date('2026-08-06T12:00:00Z'), {
    name: 'Perfect store SPT Q3', start_date: '2000-01-01', end_date: '2099-12-31'
  }), { quarterKey: '2026-Q3', quarterLabel: 'Q3 2026' });
  assert.equal(quarter(new Date('2026-07-08T12:00:00Z'), {
    name: 'Other questionnaire', start_date: '2026-05-01', end_date: '2026-07-10'
  }).quarterKey, '2026-Q3');
});
