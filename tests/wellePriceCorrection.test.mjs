import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { before, beforeEach, after, test } from 'node:test';
import { PGlite } from '@electric-sql/pglite';

// In-memory Postgres only. No Supabase connection or production credentials are used.
const db = new PGlite();
const id = n => `00000000-0000-4000-8000-${String(n).padStart(12, '0')}`;
const waveA = id(1), waveB = id(2), actor = id(3);
const migration = await readFile(new URL('../supabase/migrations/20260914100721_welle_submission_price_correction.sql', import.meta.url), 'utf8');
const scalar = async (sql, args = []) => (await db.query(sql, args)).rows[0];
const compare = async (wave = waveA, apply = false, token = null, actorId = actor) =>
  (await scalar('select public.correct_welle_submission_prices($1, $2, $3, $4) as result', [wave, apply, token, actorId])).result;
const bookings = async () => (await db.query('select * from wellen_submissions order by id')).rows;

before(async () => {
  await db.exec(`
    create role anon; create role authenticated; create role service_role bypassrls;
    create table wellen(id uuid primary key, name text, is_deleted boolean default false);
    create table wellen_displays(id uuid primary key, welle_id uuid references wellen(id), name text, item_value numeric);
    create table wellen_kartonware(like wellen_displays including all);
    create table wellen_einzelprodukte(like wellen_displays including all);
    create table wellen_paletten(id uuid primary key, welle_id uuid references wellen(id), name text);
    create table wellen_schuetten(like wellen_paletten including all);
    create table wellen_paletten_products(id uuid primary key, palette_id uuid references wellen_paletten(id), name text, value_per_ve numeric);
    create table wellen_schuetten_products(id uuid primary key, schuette_id uuid references wellen_schuetten(id), name text, value_per_ve numeric);
    create table wellen_submissions(id uuid primary key, welle_id uuid references wellen(id), item_type text,
      item_id uuid, quantity integer, value_per_unit numeric, market_id text default 'market',
      gebietsleiter_id uuid default '${actor}', created_at timestamptz default '2026-08-01 10:00Z', photo_url text default 'untouched');
    grant select, update on all tables in schema public to service_role;
  `);
  await db.exec(migration);
});

beforeEach(async () => {
  await db.exec(`
    truncate wellen_submissions, wellen_displays, wellen_kartonware, wellen_einzelprodukte,
      wellen_paletten_products, wellen_schuetten_products, wellen_paletten, wellen_schuetten,
      wellen_submission_price_corrections, wellen cascade;
    insert into wellen values ('${waveA}', 'Welle A', false), ('${waveB}', 'Welle B', false);
    insert into wellen_displays values ('${id(10)}','${waveA}','Same name',12), ('${id(20)}','${waveB}','Same name',99);
    insert into wellen_kartonware values ('${id(11)}','${waveA}','Kartonware',12);
    insert into wellen_einzelprodukte values ('${id(12)}','${waveA}','Einzelprodukt',12);
    insert into wellen_paletten values ('${id(30)}','${waveA}','Palette A'), ('${id(31)}','${waveB}','Palette B');
    insert into wellen_schuetten values ('${id(40)}','${waveA}','Schuette A'), ('${id(41)}','${waveB}','Schuette B');
    insert into wellen_paletten_products values ('${id(13)}','${id(30)}','Same product',12), ('${id(23)}','${id(31)}','Same product',99);
    insert into wellen_schuetten_products values ('${id(14)}','${id(40)}','Same product',12), ('${id(24)}','${id(41)}','Same product',99);
  `);
  for (const [index, type] of ['display', 'kartonware', 'einzelprodukt', 'palette', 'schuette'].entries()) {
    await db.query('insert into wellen_submissions(id,welle_id,item_type,item_id,quantity,value_per_unit) values ($1,$2,$3,$4,2,8)', [id(100 + index), waveA, type, id(10 + index)]);
  }
  // Another wave even referencing A's product must never be updated.
  await db.query(`insert into wellen_submissions(id,welle_id,item_type,item_id,quantity,value_per_unit) values
    ($1,$2,'display',$3,3,7), ($4,$2,'display',$5,3,7), ($6,$2,'schuette',$7,3,7)`,
  [id(200),waveB,id(10),id(201),id(20),id(202),id(24)]);
});

after(async () => { await db.close(); });

test('detects prices already changed before editing; preview is strictly read only', async () => {
  const original = await bookings();
  const preview = await compare();
  assert.equal(preview.count, 5);
  assert.equal(Number(preview.oldTotal), 80);
  assert.equal(Number(preview.newTotal), 120);
  assert.equal(preview.groups.length, 5);
  assert.deepEqual(await bookings(), original);
  assert.equal((await scalar('select count(*)::int as count from wellen_submission_price_corrections')).count, 0);
});

test('applies all five item types to this wave only, preserving every other field and auditing prices', async () => {
  const original = await bookings();
  const preview = await compare();
  const applied = await compare(waveA, true, preview.token);
  assert.equal(applied.updatedCount, 5);
  for (const row of await bookings()) {
    const old = original.find(candidate => candidate.id === row.id);
    const { value_per_unit, ...rest } = row;
    const { value_per_unit: oldPrice, ...oldRest } = old;
    assert.deepEqual(rest, oldRest);
    assert.equal(Number(value_per_unit), row.welle_id === waveA ? 12 : Number(oldPrice));
  }
  const audit = await scalar('select * from wellen_submission_price_corrections');
  assert.equal(audit.welle_id, waveA);
  assert.equal(audit.actor_id, actor);
  assert.equal(audit.changes.length, 5);
  assert.ok(audit.changes.every(row => row.oldPrice === 8 && row.newPrice === 12));
  assert.equal((await compare()).count, 0);
  assert.equal((await compare(waveA, true, preview.token)).code, 'WELLE_PRICE_PREVIEW_STALE');
});

test('refuses cross-wave confirmation tokens without changes', async () => {
  const original = await bookings();
  const preview = await compare();
  assert.equal((await compare(waveB, true, preview.token)).code, 'WELLE_PRICE_PREVIEW_STALE');
  assert.deepEqual(await bookings(), original);
});

test('skips foreign product IDs, wrong types, deleted/unmatched products and absent target prices', async () => {
  for (const [offset,type,product] of [[0,'display',id(20)],[1,'schuette',id(24)],[2,'display',id(14)],[3,'palette',id(999)]]) {
    await db.query('insert into wellen_submissions(id,welle_id,item_type,item_id,quantity,value_per_unit) values ($1,$2,$3,$4,1,8)', [id(300+offset),waveA,type,product]);
  }
  await db.exec(`update wellen_kartonware set item_value=null where id='${id(11)}'`);
  const preview = await compare();
  assert.equal(preview.skippedCount, 5);
  assert.equal(preview.count, 4);
  await compare(waveA,true,preview.token);
  const rows = (await bookings()).filter(row => Number(row.id.slice(-12)) >= 300 || row.item_id === id(11));
  assert.ok(rows.every(row => Number(row.value_per_unit) === 8));
});

test('preserves duplicates as separate bookings and handles zero/new and null/old prices', async () => {
  await db.exec(`update wellen_displays set item_value=0 where id='${id(10)}';
    update wellen_submissions set value_per_unit=null where id='${id(101)}';
    insert into wellen_submissions(id,welle_id,item_type,item_id,quantity,value_per_unit)
      values ('${id(150)}','${waveA}','display','${id(10)}',2,8)`);
  const preview = await compare();
  assert.equal(preview.count,6);
  assert.equal(preview.missingOldPriceCount,1);
  await compare(waveA,true,preview.token);
  assert.equal((await bookings()).length,9);
  assert.equal(Number((await scalar('select value_per_unit from wellen_submissions where id=$1',[id(150)])).value_per_unit),0);
});

for (const [name, sql] of [
  ['changed price', `update wellen_displays set item_value=15 where id='${id(10)}'`],
  ['changed quantity', `update wellen_submissions set quantity=7 where id='${id(100)}'`],
  ['new booking', `insert into wellen_submissions(id,welle_id,item_type,item_id,quantity,value_per_unit) values ('${id(160)}','${waveA}','display','${id(10)}',1,8)`],
  ['removed product', `delete from wellen_displays where id='${id(10)}'`],
]) test(`stale preview after ${name} cannot partially update any bookings`, async () => {
  const preview = await compare();
  await db.exec(sql);
  const original = await bookings();
  assert.equal((await compare(waveA,true,preview.token)).code,'WELLE_PRICE_PREVIEW_STALE');
  assert.deepEqual(await bookings(),original);
});

test('another wave changing does not invalidate this wave preview', async () => {
  const preview = await compare();
  await db.exec(`update wellen_displays set item_value=21 where welle_id='${waveB}'`);
  assert.equal((await compare(waveA,true,preview.token)).updatedCount,5);
});

test('over 1000 bookings are corrected without API pagination truncation', async () => {
  await db.exec(`insert into wellen_submissions(id,welle_id,item_type,item_id,quantity,value_per_unit)
    select md5(i::text)::uuid,'${waveA}','display','${id(10)}',1,8 from generate_series(1,1100) i`);
  const preview = await compare();
  assert.equal(preview.count,1105);
  assert.equal((await compare(waveA,true,preview.token)).updatedCount,1105);
});

test('transaction rolls back every price if audit insertion fails', async () => {
  await db.exec(`create function reject_price_audit() returns trigger language plpgsql as $$begin raise exception 'test audit failure'; end$$;
    create trigger reject_audit before insert on wellen_submission_price_corrections for each row execute function reject_price_audit()`);
  try {
    const original = await bookings();
    const preview = await compare();
    await assert.rejects(compare(waveA,true,preview.token), /test audit failure/);
    assert.deepEqual(await bookings(),original);
  } finally {
    await db.exec('drop trigger reject_audit on wellen_submission_price_corrections; drop function reject_price_audit()');
  }
});

test('requires confirmation, a valid wave and actor; rejects deleted waves', async () => {
  const original = await bookings();
  assert.equal((await compare(waveA,true,null)).code,'WELLE_PRICE_CONFIRMATION_REQUIRED');
  assert.equal((await compare(waveA,true,'token',null)).code,'WELLE_PRICE_CONFIRMATION_REQUIRED');
  assert.equal((await compare(id(999))).code,'WELLE_PRICE_NOT_FOUND');
  await db.exec(`update wellen set is_deleted=true where id='${waveA}'`);
  assert.equal((await compare()).code,'WELLE_PRICE_NOT_FOUND');
  assert.deepEqual(await bookings(),original);
});

test('an unexpected skipped update rolls back the whole correction', async () => {
  await db.exec(`create function skip_price_update() returns trigger language plpgsql as $$begin
    if old.id='${id(100)}'::uuid then return null; end if; return new; end$$;
    create trigger skip_update before update on wellen_submissions for each row execute function skip_price_update()`);
  try {
    const original = await bookings();
    const preview = await compare();
    await assert.rejects(compare(waveA,true,preview.token), /WELLE_PRICE_UPDATE_COUNT_MISMATCH/);
    assert.deepEqual(await bookings(),original);
    assert.equal((await scalar('select count(*)::int as count from wellen_submission_price_corrections')).count,0);
  } finally {
    await db.exec('drop trigger skip_update on wellen_submissions; drop function skip_price_update()');
  }
});

test('RPC and audit are denied to anon/authenticated; service_role is authorized', async () => {
  for (const role of ['anon','authenticated']) {
    await db.exec(`set role ${role}`);
    try {
      await assert.rejects(compare(), /permission denied/);
      await assert.rejects(db.query('select * from wellen_submission_price_corrections'), /permission denied/);
    } finally { await db.exec('reset role'); }
  }
  await db.exec('set role service_role');
  try {
    const preview = await compare();
    assert.equal((await compare(waveA,true,preview.token)).updatedCount,5);
  } finally { await db.exec('reset role'); }
});
