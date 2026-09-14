import assert from 'node:assert/strict';
import { before, after, beforeEach, test } from 'node:test';
import express from 'express';

process.env.SUPABASE_URL = 'http://127.0.0.1:1';
process.env.SUPABASE_SERVICE_KEY = 'sb_secret_isolated_test_not_a_real_key';
const { createWellePriceCorrectionRouter } = await import('../src/routes/wellePriceCorrection.ts');
const calls = [];
let result;
const app = express();
app.use(express.json());
app.use((req, _res, next) => {
  if (req.headers['test-role']) req.user = { role: req.headers['test-role'], id: '00000000-0000-4000-8000-000000000003' };
  next();
});
app.use(createWellePriceCorrectionRouter(() => ({ rpc: async (name,args) => {
  calls.push({name,args});
  return result;
} })));
let server, base;
const id = '00000000-0000-4000-8000-000000000001';
const token = 'a'.repeat(64);
const request = (method = 'GET', role = 'admin', body, wave = id) => fetch(`${base}/${wave}/submission-prices`, {
  method,
  headers: { ...(role ? {'test-role':role}:{}), 'Content-Type':'application/json' },
  body: body === undefined ? undefined : JSON.stringify(body),
});
before(async () => {
  server = app.listen(0,'127.0.0.1');
  await new Promise(resolve => server.once('listening',resolve));
  base = `http://127.0.0.1:${server.address().port}`;
});
after(async () => { await new Promise(resolve => server.close(resolve)); });
beforeEach(() => { calls.length = 0; result = {data:{welleId:id,count:5},error:null}; });

test('both endpoints deny unauthenticated users and GLs before any database call', async () => {
  for (const method of ['GET','POST']) for (const role of [null,'gl']) {
    const response = await request(method,role,method === 'POST' ? {token} : undefined);
    assert.equal(response.status,role ? 403:401);
  }
  assert.equal(calls.length,0);
});
test('GET is preview only, uncached and wave scoped', async () => {
  const response = await request();
  assert.equal(response.status,200);
  assert.equal(response.headers.get('cache-control'),'no-store');
  assert.deepEqual(calls,[{name:'correct_welle_submission_prices',args:{p_welle_id:id,p_apply:false,p_expected_token:null,p_actor_id:null}}]);
});
test('POST derives actor from authentication and ignores caller prices, item IDs and foreign wave IDs', async () => {
  const response = await request('POST','admin',{token,welle_id:'other-wave',actor_id:'forged',prices:[{item_id:'foreign',price:1}]});
  assert.equal(response.status,200);
  assert.deepEqual(calls[0].args,{p_welle_id:id,p_apply:true,p_expected_token:token,p_actor_id:'00000000-0000-4000-8000-000000000003'});
});
test('invalid inputs never touch the database', async () => {
  assert.equal((await request('GET','admin',undefined,'invalid')).status,400);
  assert.equal((await request('POST','admin',{})).status,400);
  assert.equal((await request('POST','admin',{token:'invalid'})).status,400);
  assert.equal(calls.length,0);
});
test('stale preview returns specific conflict; unknown wave returns not found', async () => {
  result = {data:{code:'WELLE_PRICE_PREVIEW_STALE'}};
  const response = await request('POST','admin',{token});
  assert.equal(response.status,409);
  assert.equal((await response.json()).code,'MR-WELLE-PRICE-STALE-001');
  result = {data:{code:'WELLE_PRICE_NOT_FOUND'}};
  assert.equal((await request()).status,404);
});
test('RPC failures report unconfirmed apply without leaking internals or retrying', async () => {
  result = {data:null,error:{message:'private database internals'}};
  const response = await request('POST','admin',{token});
  assert.equal(response.status,503);
  const body = await response.json();
  assert.equal(body.code,'MR-WELLE-PRICE-APPLY-001');
  assert.ok(!JSON.stringify(body).includes('private'));
  assert.equal(calls.length,1);
});
