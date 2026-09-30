import assert from 'node:assert/strict';
import { after, before, test } from 'node:test';
import express from 'express';

process.env.SUPABASE_URL = 'http://127.0.0.1:1';
process.env.SUPABASE_SERVICE_KEY = 'sb_secret_isolated_export_test';
process.env.PERFECTSTORE_EXPORT_BACKEND_URL = 'http://export.test.invalid';
const originalFetch = globalThis.fetch;
const calls = [];
let exported;
const tables = {
  fb_fragebogen: [
    { id: 'q2', name: 'Perfect Store PET', start_date: '2026-05-01', end_date: '2026-07-10' },
    { id: 'q3', name: 'Perfect store SPT Q3', start_date: '2000-01-01', end_date: '2099-12-31' }
  ],
  fb_questions: [{ id: 'item', question_text: 'Produkt verfügbar?', type: 'yesno', distributionsziel: true }],
  fb_fragebogen_modules: [],
  fb_responses: [
    { id: 'start', fragebogen_id: 'q2', market_id: 'market', gebietsleiter_id: 'gl', status: 'completed', completed_at: '2026-06-30T22:00:00Z' },
    { id: 'end', fragebogen_id: 'q2', market_id: 'market', gebietsleiter_id: 'gl', status: 'completed', completed_at: '2026-07-10T21:59:59.999Z' },
    { id: 'outside', fragebogen_id: 'q2', market_id: 'market', gebietsleiter_id: 'gl', status: 'completed', completed_at: '2026-07-10T22:00:00Z' },
    { id: 'q3-response', fragebogen_id: 'q3', market_id: 'market', gebietsleiter_id: 'gl', status: 'completed', completed_at: '2026-09-02T12:00:00Z' }
  ],
  fb_response_answers: ['start', 'end', 'outside', 'q3-response'].map((id, i) => ({ response_id: id, question_id: 'item', answer_boolean: i !== 1 })),
  markets: [{ id: 'market', name: 'Testmarkt', chain: 'Wau Miau', internal_id: '123' }],
  users: [{ id: 'gl', first_name: 'Test', last_name: 'GL' }]
};
globalThis.fetch = async (input, init) => {
  const url = new URL(String(input));
  if (url.host === 'export.test.invalid') {
    exported = JSON.parse(init.body);
    return new Response(new Uint8Array([80, 75, 3, 4]));
  }
  if (url.host !== '127.0.0.1:1' || (init?.method || 'GET') !== 'GET') {
    throw new Error(`Test blocks network/write: ${url.host}`);
  }
  calls.push(url.pathname);
  const table = url.pathname.split('/').pop();
  assert.ok(table in tables, `Unexpected table ${table}`);
  let rows = tables[table];
  for (const [column, criterion] of url.searchParams) {
    if (criterion.startsWith('in.(')) {
      const ids = criterion.slice(4, -1).split(',').map(id => id.replaceAll('"', ''));
      rows = rows.filter(row => ids.includes(String(row[column])));
    }
    if (criterion.startsWith('eq.')) rows = rows.filter(row => String(row[column]) === criterion.slice(3));
  }
  return new Response(JSON.stringify(rows), { headers: { 'Content-Type': 'application/json' } });
};
const { default: router } = await import('../src/routes/fragebogen.ts');
const app = express();
app.use(express.json());
app.use((req, _res, next) => { req.user = { id: 'test', role: req.headers['test-role'] || 'admin' }; next(); });
app.use(router);
let server;
let base;
before(async () => {
  server = app.listen(0, '127.0.0.1');
  await new Promise(resolve => server.once('listening', resolve));
  base = `http://127.0.0.1:${server.address().port}`;
});
after(async () => { globalThis.fetch = originalFetch; await new Promise(resolve => server.close(resolve)); });
const request = (extra = {}, role = 'admin') => originalFetch(`${base}/fragebogen/distribution-export.xlsx`, {
  method: 'POST', headers: { 'Content-Type': 'application/json', 'test-role': role },
  body: JSON.stringify({ fragebogen_ids: ['q2', 'q3'], question_ids: ['item'], ...extra })
});

test('export filters original Vienna dates inclusively and keeps July Q2 answers in Q2', async () => {
  const response = await request({ start_date: '2026-07-01', end_date: '2026-07-10' });
  assert.equal(response.status, 200);
  assert.deepEqual(exported.rows.map(row => row.responseId), ['start', 'end']);
  assert.ok(exported.rows.every(row => row.quarterKey === '2026-Q2'));
  assert.ok(exported.rows.every(row => row.monthKey === '2026-07'));
  assert.ok(exported.rows.every(row => row.chain === 'Wau,Miau'));
  assert.equal(exported.rows[0].originalDateKey, '2026-07-01');
  assert.deepEqual(exported.dateRange, { startDate: '2026-07-01', endDate: '2026-07-10' });
});

test('Q3 sentinel questionnaire dates cannot put its answers in year 2000', async () => {
  assert.equal((await request()).status, 200);
  assert.equal(exported.rows.find(row => row.responseId === 'q3-response').quarterKey, '2026-Q3');
});

test('quarter compression is applied after filtering the real dates', async () => {
  assert.equal((await request({ start_date: '2026-07-01', end_date: '2026-07-10', quarter_compression: { enabled: true, year: 2026, quarter: 1 } })).status, 200);
  assert.equal(exported.rows.length, 2);
  assert.ok(exported.rows.every(row => row.quarterKey === '2026-Q1'));
  assert.equal(exported.rows[1].originalDateKey, '2026-07-10');
});

test('bad dates and GL access fail before database queries or workbook generation', async () => {
  const beforeCount = calls.length;
  for (const extra of [
    { start_date: '2026-02-29' },
    { start_date: '2026-07-11', end_date: '2026-07-10' }
  ]) assert.equal((await request(extra)).status, 400);
  assert.equal((await request({}, 'gl')).status, 403);
  assert.equal(calls.length, beforeCount);
});
