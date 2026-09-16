import assert from 'node:assert/strict';
import { after, before, beforeEach, test } from 'node:test';
import express from 'express';

process.env.SUPABASE_URL = 'http://127.0.0.1:1';
process.env.SUPABASE_SERVICE_KEY = 'sb_secret_isolated_test_not_a_real_key';
const { createMarketAdminCommentsRouter } = await import('../src/routes/marketAdminComments.ts');

const calls = [];
let marketExists = true;
let savedComment = '';
let databaseError = false;
const fakeClient = {
  from(table) {
    calls.push({ table });
    return {
      select() {
        return {
          order(column, options) {
            calls.push({ table, column, options });
            return {
              async range() {
                if (databaseError) return { data: null, error: { message: 'private details' } };
                return { data: savedComment ? [{ market_id: 'market-1', comment: savedComment }] : [], error: null };
              },
            };
          },
          eq(column, id) {
            calls.push({ table, column, id });
            return {
              async maybeSingle() {
                if (databaseError) return { data: null, error: { message: 'private details' } };
                if (table === 'markets') return { data: marketExists ? { id } : null, error: null };
                return { data: savedComment ? { comment: savedComment, updated_at: '2026-09-16T12:00:00Z' } : null, error: null };
              },
            };
          },
        };
      },
      upsert(payload, options) {
        calls.push({ table, payload, options });
        return {
          select() {
            return {
              async single() {
                if (databaseError) return { data: null, error: { message: 'private details' } };
                savedComment = payload.comment;
                return { data: { comment: savedComment, updated_at: payload.updated_at }, error: null };
              },
            };
          },
        };
      },
    };
  },
};

const app = express();
app.use(express.json());
app.use((req, _res, next) => {
  if (req.headers['test-role']) req.user = { role: req.headers['test-role'], id: 'admin-test' };
  next();
});
app.use(createMarketAdminCommentsRouter(() => fakeClient));

let server;
let base;
const request = (method, role, body, id = 'market-1') => fetch(`${base}/${id}/admin-comment`, {
  method,
  headers: { ...(role ? { 'test-role': role } : {}), 'Content-Type': 'application/json' },
  body: body === undefined ? undefined : JSON.stringify(body),
});

before(async () => {
  server = app.listen(0, '127.0.0.1');
  await new Promise((resolve) => server.once('listening', resolve));
  base = `http://127.0.0.1:${server.address().port}`;
});
after(async () => { await new Promise((resolve) => server.close(resolve)); });
beforeEach(() => { calls.length = 0; marketExists = true; savedComment = ''; databaseError = false; });

test('admin comment is inaccessible to unauthenticated users and GLs', async () => {
  for (const method of ['GET', 'PUT']) {
    for (const role of [null, 'gl']) {
      const response = await request(method, role, method === 'PUT' ? { comment: 'secret' } : undefined);
      assert.equal(response.status, role ? 403 : 401);
    }
  }
  for (const role of [null, 'gl']) {
    const response = await fetch(`${base}/admin-comments`, { headers: role ? { 'test-role': role } : {} });
    assert.equal(response.status, role ? 403 : 401);
  }
  assert.equal(calls.length, 0);
});

test('admin can read and edit a market comment', async () => {
  const empty = await request('GET', 'admin');
  assert.equal(empty.status, 200);
  assert.equal(empty.headers.get('cache-control'), 'no-store');
  assert.equal((await empty.json()).comment, '');

  const update = await request('PUT', 'admin', { comment: '  Umbau ab Montag  ' });
  assert.equal(update.status, 200);
  assert.equal((await update.json()).comment, 'Umbau ab Montag');
  assert.equal(calls.find((call) => call.payload)?.table, 'market_admin_comments');
  assert.equal(calls.find((call) => call.payload)?.options.onConflict, 'market_id');

  const read = await request('GET', 'admin');
  assert.equal((await read.json()).comment, 'Umbau ab Montag');

  const list = await fetch(`${base}/admin-comments`, { headers: { 'test-role': 'admin' } });
  assert.equal(list.status, 200);
  assert.deepEqual(await list.json(), [{ market_id: 'market-1', comment: 'Umbau ab Montag' }]);
});

test('invalid data and missing markets do not write comments', async () => {
  assert.equal((await request('PUT', 'admin', { comment: 42 })).status, 400);
  assert.equal((await request('PUT', 'admin', { comment: 'a'.repeat(5001) })).status, 400);
  assert.equal(calls.length, 0);
  marketExists = false;
  assert.equal((await request('PUT', 'admin', { comment: 'x' })).status, 404);
  assert.ok(!calls.some((call) => call.payload));
});

test('database errors do not expose private details', async () => {
  databaseError = true;
  const response = await request('GET', 'admin');
  assert.equal(response.status, 500);
  assert.ok(!(await response.text()).includes('private details'));
});
