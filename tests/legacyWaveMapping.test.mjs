import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {before,after,beforeEach,test} from 'node:test';
import {PGlite} from '@electric-sql/pglite';
import {validatePlan,previewSQL,applySQL} from '../scripts/legacy-wave-mapping.mjs';
const db=new PGlite(),id=n=>'20000000-0000-4000-8000-'+String(n).padStart(12,'0');
const wave=id(1),other=id(2),actor=id(3),display=id(4),carton=id(5),otherItem=id(6);
const plan=()=>({version:1,year:2026,actorId:actor,source:{filename:"synthetic'; $mapping_apply$ source.xlsx",sheet:'Synthetic',sha256:'a'.repeat(64)},mappings:[
 {wave_id:wave,item_id:display,item_type:'display',expected_wave_name:'Billa Plus Synthetic KW10-14',expected_item_name:'Synthetic ambiguous snack display',article:'900101',source_row:2,source_period:'KW10-14',expected_booking_count:2},
 {wave_id:wave,item_id:carton,item_type:'kartonware',expected_wave_name:'Billa Plus Synthetic KW10-14',expected_item_name:'Synthetic carton',article:'900102',source_row:3,source_period:'KW10-14',expected_booking_count:1}
]});
const scalar=async(sql,args=[])=>(await db.query(sql,args)).rows[0];
const preview=async(p=plan())=>(await scalar(previewSQL(p))).preview;
const apply=async(p=plan(),token)=>{const results=await db.exec(applySQL(p,token));return results.find(r=>r.rows?.[0]?.result)?.rows[0].result;};
const rejects=async(fn,pattern)=>{await assert.rejects(fn,pattern);await db.exec('rollback');};
const snapshot=async()=>JSON.stringify((await db.query('select to_jsonb(s) as row from wellen_submissions s order by id')).rows);
before(async()=>{
 await db.exec(`create role anon;create role authenticated;create role service_role bypassrls;
 create table users(id uuid primary key,role text);
 create table wellen(id uuid primary key,name text,is_deleted bool default false);
 create table wellen_displays(id uuid primary key,welle_id uuid references wellen(id),name text,item_value numeric,target_number int,description text,ve int);
 create table wellen_kartonware(like wellen_displays including all);
 create table wellen_einzelprodukte(like wellen_displays including all);alter table wellen_einzelprodukte add column artikel_nr text;
 create table wellen_submissions(id uuid primary key default gen_random_uuid(),welle_id uuid references wellen(id),item_type text,item_id uuid,quantity int,value_per_unit numeric,created_at timestamptz,market_id text default 'synthetic-market');
 create table products(id text primary key,name text,artikel_nr text);`);
 for(const file of ['20261112132449_wave_quarter_catalog.sql','20261112152854_wave_catalog_validity_and_exact_legacy_matches.sql'])await db.exec(await readFile(new URL('../supabase/migrations/'+file,import.meta.url),'utf8'));
});
after(()=>db.close());
beforeEach(async()=>{
 await db.exec('truncate wellen_submissions,wellen_displays,wellen_kartonware,wellen_einzelprodukte,wellen,quarter_price_catalog,quarter_price_imports,wave_quarter_price_audit,users cascade');
 await db.query("insert into users values($1,'admin')",[actor]);
 await db.query("insert into wellen(id,name) values($1,'Billa Plus Synthetic KW10-14'),($2,'Synthetic unrelated banner')",[wave,other]);
 await db.query("insert into wellen_displays(id,welle_id,name,item_value,target_number,description) values($1,$2,'Synthetic ambiguous snack display',null,50,'Synthetic date/label stays'),($3,$4,'Synthetic ambiguous snack display',999,20,'Outside scope')",[display,wave,otherItem,other]);
 await db.query("insert into wellen_kartonware(id,welle_id,name,item_value,target_number) values($1,$2,'Synthetic carton',5,20)",[carton,wave]);
 await db.query("insert into wellen_submissions(welle_id,item_type,item_id,quantity,created_at) values($1,'display',$2,2,'2026-03-31T21:59:59Z'),($1,'display',$2,3,'2026-03-31T22:00:00Z'),($1,'kartonware',$3,4,'2026-02-12T12:00Z'),($4,'display',$5,7,'2026-02-12T12:00Z')",[wave,display,carton,other,otherItem]);
 for(const q of [1,2])await db.query('select import_quarter_prices($1,$2,$3,$4,$5,$6)',[2026,q,'synthetic.xlsx','b'.repeat(64),JSON.stringify([
  {department:'pets',item_type:'display',artikel_nr:'900101',name:'Synthetic explicit pack',units_per_ve:1,price_per_order:q===1?100:110,source_cell:'Synthetic!A2:C2'},
  {department:'pets',item_type:'display',artikel_nr:'900199',name:'Synthetic explicit pack',units_per_ve:1,price_per_order:999,source_cell:'Synthetic!A3:C3'},
  {department:'pets',item_type:'kartonware',artikel_nr:'900102',name:'Synthetic carton canonical',units_per_ve:1,price_per_order:25,source_cell:'Synthetic!A4:C4'}
 ]),actor]);
});
test('preview is read-only; exact explicit mappings freeze Vienna quarter prices without altering quantities, dates, labels or other customers',async()=>{
 const before=await snapshot(),p=await preview();
 assert.deepEqual(p.errors,[]);assert.equal(p.bookingChanges,3);assert.equal(p.definitionChanges,2);assert.equal(await snapshot(),before);
 const outside=await scalar('select to_jsonb(s) as row from wellen_submissions s where welle_id=$1',[other]);
 const result=await apply(plan(),p.token);assert.equal(result.bookingsUpdated,3);assert.equal(result.definitionsUpdated,2);assert.equal(result.nonPriceDataVerified,true);assert.ok(result.auditId);
 const booked=(await db.query('select * from wellen_submissions where welle_id=$1 order by created_at',[wave])).rows;
 assert.equal(booked.reduce((s,b)=>s+Number(b.order_quantity)*Number(b.order_unit_price),0),630);
 assert.deepEqual(booked.map(b=>b.quantity),[4,2,3]);assert.deepEqual(booked.map(b=>b.price_quarter),[1,1,2]);
 const afterItem=await scalar('select * from wellen_displays where id=$1',[display]);assert.equal(afterItem.name,'Synthetic ambiguous snack display');assert.equal(afterItem.description,'Synthetic date/label stays');assert.equal(afterItem.target_number,50);assert.equal(afterItem.item_value,null);
 assert.equal(afterItem.artikel_nr,'900101');assert.equal(afterItem.catalog_key,'pets:display:900101');
 assert.deepEqual(await scalar('select to_jsonb(s) as row from wellen_submissions s where welle_id=$1',[other]),outside);
 const audit=(await scalar('select changes from wave_quarter_price_audit where id=$1',[result.auditId])).changes;
 assert.equal(audit.length,3);assert.ok(audit.every(a=>a.mappingSource.sha256==='a'.repeat(64)&&a.before&&a.waveItemBefore&&a.catalogEvidence));
 const again=await preview();assert.equal(again.bookingChanges,0);assert.equal(again.definitionChanges,0);assert.deepEqual(again.errors,[]);
 const repeat=await apply(plan(),again.token);assert.equal(repeat.bookingsUpdated,0);assert.equal((await scalar('select count(*)::int as n from wave_quarter_price_audit')).n,1);
});
test('changed booking, new booking and changed source label invalidate the exact reviewed snapshot',async()=>{
 for(const mutation of [
  ()=>db.query('update wellen_submissions set quantity=8 where item_id=$1',[display]),
  ()=>db.query("insert into wellen_submissions(welle_id,item_type,item_id,quantity,created_at) values($1,'display',$2,1,'2026-03-12')",[wave,display]),
  ()=>db.query("update wellen_displays set name='Changed synthetic label' where id=$1",[display])
 ]){
  const p=await preview();await mutation();await rejects(()=>apply(plan(),p.token),/snapshot changed/);
  assert.equal((await scalar('select count(*)::int as n from wave_quarter_price_audit')).n,0);
 }
});
test('missing quarter price, conflicting article and wrong frozen identity reject without partial changes',async()=>{
 await db.query("update wellen_displays set artikel_nr='900199' where id=$1",[display]);
 let p=await preview();assert.ok(p.errors.some(e=>/Conflicting/.test(e.reason)));
 const before=await snapshot();await rejects(()=>apply(plan(),p.token),/evidence is incomplete/);assert.equal(await snapshot(),before);
 await db.query('update wellen_displays set artikel_nr=null where id=$1',[display]);
 await db.query("update wellen_submissions set created_at='2026-07-15' where item_id=$1",[display]);
 p=await preview();assert.ok(p.errors.some(e=>/Missing exact/.test(e.reason)));await rejects(()=>apply(plan(),p.token),/evidence is incomplete/);
 assert.equal((await scalar('select count(*)::int as n from wellen_submissions where price_source_id is not null')).n,0);
});
test('an audit failure rolls back every booking price and article definition update',async()=>{
 const before=await snapshot(),p=await preview();
 await db.exec("create function synthetic_audit_failure() returns trigger language plpgsql as $$ begin raise exception 'Synthetic audit failure';end;$$;create trigger synthetic_audit_failure before insert on wave_quarter_price_audit for each row execute function synthetic_audit_failure()");
 try{await rejects(()=>apply(plan(),p.token),/Synthetic audit failure/);assert.equal(await snapshot(),before);assert.equal((await scalar('select catalog_key from wellen_displays where id=$1',[display])).catalog_key,null);}
 finally{await db.exec('drop trigger synthetic_audit_failure on wave_quarter_price_audit;drop function synthetic_audit_failure()');}
});
test('a previously frozen different article cannot be overwritten by the reviewed mapping',async()=>{
 const source=await scalar("select id from quarter_price_catalog where quarter=1 and artikel_nr='900199'");
 await db.query("update wellen_submissions set price_source_id=$1,value_per_unit=999,order_unit='Display' where id=(select id from wellen_submissions where item_id=$2 order by created_at limit 1)",[source.id,display]);
 const before=await snapshot(),p=await preview();assert.ok(p.errors.some(e=>/Conflicting frozen/.test(e.reason)));
 await rejects(()=>apply(plan(),p.token),/evidence is incomplete/);assert.equal(await snapshot(),before);
});
test('bulk application handles more than a REST page and leaves no target rows unpriced',async()=>{
 await db.query("insert into wellen_submissions(welle_id,item_type,item_id,quantity,created_at) select $1,'display',$2,2,'2026-02-12T12:00Z' from generate_series(1,1401)",[wave,display]);
 const bulk=plan();bulk.mappings[0].expected_booking_count=1403;const p=await preview(bulk);assert.deepEqual(p.errors,[]);assert.equal(p.bookingChanges,1404);
 const result=await apply(bulk,p.token);assert.equal(result.bookingsUpdated,1404);assert.equal((await preview(bulk)).bookingChanges,0);
 assert.equal((await scalar('select count(*)::int as n from wellen_submissions where welle_id=$1 and price_source_id is null',[wave])).n,0);
});
test('non-admin actor and invalid/duplicate/out-of-scope plans cannot apply',async()=>{
 const p=await preview();await db.query("update users set role='gl' where id=$1",[actor]);await rejects(()=>apply(plan(),p.token),/admin actor/);
 for(const bad of [
  {...plan(),mappings:[...plan().mappings,plan().mappings[0]]},
  {...plan(),mappings:[{...plan().mappings[0],expected_wave_name:'Other banner'}]},
  {...plan(),mappings:[{...plan().mappings[0],article:"900101';drop table users;--"}]}
 ])assert.throws(()=>validatePlan(bad),/Invalid/);
 assert.throws(()=>applySQL(plan(),'not-reviewed'),/preview token/);
});

