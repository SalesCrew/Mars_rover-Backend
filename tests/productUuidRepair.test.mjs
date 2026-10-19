import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { before, beforeEach, after, test } from 'node:test';
import { PGlite } from '@electric-sql/pglite';
const sql=await readFile(new URL('../sql/repair_cesar_product_ids_20261019.sql',import.meta.url),'utf8');
const mapping=[...sql.matchAll(/\('([^']+)', '([^']+)', '(\d+)'\)/g)].map(([,old_id,new_id,article])=>({old_id,new_id,article}));
assert.equal(mapping.length,7);
const db=new PGlite();
const tables=['products','nara_incentive_items','vorverkauf_items','vorverkauf_submission_products','wellen_submissions','wellen_gl_progress','products_update'];
const snapshot=async()=>Object.fromEntries(await Promise.all(tables.map(async t=>[t,(await db.query(`select * from ${t} order by id`)).rows])));
const run=async()=>{try{return await db.exec(sql);}catch(error){await db.exec('rollback');throw error;}};
before(async()=>{
  await db.exec(`
    create table products(id text primary key,name text,artikel_nr text,price numeric,content text,
      is_active bool default true,is_deleted bool default false,palette_products jsonb,
      created_at timestamptz default '2026-08-26T14:20:26.276961Z',updated_at timestamptz default now());
    create function update_products_updated_at() returns trigger language plpgsql as $$begin new.updated_at=now();return new;end$$;
    create trigger products_updated_at_trigger before update on products for each row execute function update_products_updated_at();
    create table nara_incentive_items(id int primary key,product_id text references products(id),quantity int);
    create table vorverkauf_items(id int primary key,product_id text,quantity int);
    create table vorverkauf_submission_products(like vorverkauf_items including all);
    create table products_update(id text primary key,palette_products jsonb);
    create table wellen_submissions(id int primary key,item_id uuid,quantity int,value_per_unit numeric);
    create table wellen_gl_progress(like wellen_submissions including all);
  `);
});
beforeEach(async()=>{
  await db.exec(`truncate ${tables.join(',')} cascade`);
  for(const p of mapping)await db.query('insert into products(id,name,artikel_nr,price,content) values($1,$2,$3,6.83,\'3\')',[p.old_id,`Cesar ${p.article}`,p.article]);
  await db.exec(`insert into products(id,name,artikel_nr,is_deleted,price) values('historical-text-id','Archived','old',true,1.23);
    insert into nara_incentive_items values(1,'historical-text-id',5);
    insert into vorverkauf_items values(1,'historical-text-id',7);
    insert into vorverkauf_submission_products values(1,'historical-text-id',9);
    insert into wellen_submissions values(1,'00000000-0000-4000-8000-000000000001',11,2.34);
    insert into wellen_gl_progress values(1,'00000000-0000-4000-8000-000000000001',13,3.45);
  `);
});
after(async()=>{await db.close();});

test('seven UUID replacements preserve every product field and historical row; rerun is a no-op',async()=>{
  const before=await snapshot();
  await run();
  const after=await snapshot();
  for(const t of tables.filter(t=>t!=='products'))assert.deepEqual(after[t],before[t],t);
  assert.equal(after.products.length,before.products.length);
  for(const p of before.products){
    const m=mapping.find(m=>m.old_id===p.id);
    const saved=after.products.find(a=>a.id===(m?.new_id||p.id));
    if(m){const {id,updated_at,...rest}=saved;const {id:oldId,updated_at:oldUpdated,...oldRest}=p;assert.deepEqual(rest,oldRest);}
    else assert.deepEqual(saved,p);
  }
  await run();
  assert.deepEqual(await snapshot(),after);
});

test('new booking references abort the entire repair without changing any row',async()=>{
  await db.query('insert into vorverkauf_items values(2,$1,99)',[mapping[0].old_id]);
  const before=await snapshot();
  await assert.rejects(run(),/referenced/);
  assert.deepEqual(await snapshot(),before);
});

test('missing original row or target UUID collision aborts atomically',async()=>{
  await db.query('update products set id=$1 where id=$2',['different-id',mapping[0].old_id]);
  const before=await snapshot();
  await assert.rejects(run(),/Expected exactly/);
  assert.deepEqual(await snapshot(),before);
});

test('embedded palette references also abort without touching history',async()=>{
  await db.query('update products set palette_products=$1 where id=\'historical-text-id\'',[JSON.stringify([{id:mapping[0].old_id}])]);
  const before=await snapshot();
  await assert.rejects(run(),/referenced/);
  assert.deepEqual(await snapshot(),before);
});
