import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {before,after,beforeEach,test} from 'node:test';
import {PGlite} from '@electric-sql/pglite';
import {correctionPreviewSQL,correctionApplySQL} from '../scripts/correct-confirmed-ve-orders.mjs';
const db=new PGlite(),id=n=>'30000000-0000-4000-8000-'+String(n).padStart(12,'0');
const wave=id(1),item=id(2),master=id(3),actor=id(4),unknown=id(5),one=id(6);
const scalar=async(sql,args=[])=>(await db.query(sql,args)).rows[0];
before(async()=>{
 await db.exec(`create role anon;create role authenticated;create role service_role bypassrls;
 create table wellen(id uuid primary key,name text,is_deleted bool default false);
 create table users(id uuid primary key,role text);
 create table wellen_displays(id uuid primary key,welle_id uuid,name text,item_value numeric,ve int,ve_size int,vpe int);
 create table wellen_kartonware(like wellen_displays including all);create table wellen_einzelprodukte(like wellen_displays including all);alter table wellen_einzelprodukte add column artikel_nr text;
 create table products(id text primary key,name text,artikel_nr text,content text,pallet_size int,price numeric);
 create table wellen_submissions(id uuid primary key default gen_random_uuid(),welle_id uuid references wellen(id),item_type text,item_id uuid,quantity int,value_per_unit numeric,created_at timestamptz default now(),market_id text default 'synthetic-market',submission_batch_id uuid);`);
 for(const f of ['20261112132449_wave_quarter_catalog.sql','20261112152854_wave_catalog_validity_and_exact_legacy_matches.sql','20261113091431_wave_single_products_always_ve.sql'])await db.exec(await readFile(new URL('../supabase/migrations/'+f,import.meta.url),'utf8'));
});
after(()=>db.close());
beforeEach(async()=>{
 await db.exec('rollback;truncate wellen_submissions,wellen_displays,wellen_kartonware,wellen_einzelprodukte,wellen,products,quarter_price_catalog,quarter_price_imports,wave_quarter_price_audit,users cascade');
 await db.query("insert into users values($1,'admin')",[actor]);
 await db.query("insert into wellen(id,name) values($1,'Synthetic legacy unit wave')",[wave]);
 await db.query("insert into wellen_einzelprodukte(id,welle_id,name,item_value,ve,artikel_nr) values($1,$2,'Synthetic six-pack',3,6,'900001'),($3,$2,'Synthetic missing VE',5,null,'900099'),($4,$2,'Synthetic one-pack',4.5,1,'900002')",[item,wave,unknown,one]);
 await db.query("insert into products values($1,'Synthetic master','900003','VE: 4',null,2.5)",[master]);
});
test('new legacy orders store whole VEs and server price; cached clients cannot save one piece of a six-pack',async()=>{
 await assert.rejects(db.query("insert into wellen_submissions(welle_id,item_type,item_id,quantity) values($1,'einzelprodukt',$2,1)",[wave,item]),/ganzen VEs/);
 await db.query("insert into wellen_submissions(welle_id,item_type,item_id,quantity,value_per_unit) values($1,'einzelprodukt',$2,12,0.01)",[wave,item]);
 const s=await scalar('select * from wellen_submissions');assert.equal(s.quantity,12);assert.equal(Number(s.value_per_unit),3);assert.equal(Number(s.order_quantity),2);assert.equal(Number(s.order_unit_price),18);assert.equal(s.ve_size,6);assert.equal(s.order_unit,'VE');assert.equal(s.price_source_id,null);
 await db.query('update wellen_submissions set quantity=18');assert.equal(Number((await scalar('select order_quantity from wellen_submissions')).order_quantity),3);
 await assert.rejects(db.query('update wellen_submissions set quantity=19'),/ganze VEs/);
 await assert.rejects(db.query('update wellen_submissions set ve_size=3'),/VE darf/);
});
test('direct master products, one-unit VEs and exact unambiguous name lookup work; pallet capacity is never a VE',async()=>{
 await db.query("insert into wellen_submissions(welle_id,item_type,item_id,quantity) values($1,'einzelprodukt',$2,8),($1,'einzelprodukt',$3,1)",[wave,master,one]);
 let rows=(await db.query('select * from wellen_submissions order by quantity')).rows;assert.equal(rows[0].ve_size,1);assert.equal(rows[1].ve_size,4);assert.equal(Number(rows[1].order_unit_price),10);
 await db.query("update products set content=null,pallet_size=4");assert.equal((await scalar('select wave_single_order_details($1,$2) as r',[wave,master])).r.units,null);
 await db.query("update products set content='4'");
 await db.query("update wellen_einzelprodukte set ve=null,artikel_nr=null,name='Synthetic master' where id=$1",[item]);
 assert.equal((await scalar('select wave_single_order_details($1,$2) as r',[wave,item])).r.units,4);
 await db.query("insert into products values($1,'Synthetic master','900007','8',null,2.5)",[id(7)]);
 assert.equal((await scalar('select wave_single_order_details($1,$2) as r',[wave,item])).r.units,null);
});
test('missing or ambiguous VE refuses the entire mixed batch before any record survives',async()=>{
 await assert.rejects(db.query("insert into wellen_submissions(welle_id,item_type,item_id,quantity) values($1,'einzelprodukt',$2,6),($1,'einzelprodukt',$3,1)",[wave,item,unknown]),/eindeutige VE/);
 assert.equal((await scalar('select count(*)::int as n from wellen_submissions')).n,0);
});
test('existing historical quantities survive migration and confirmed one-VE correction updates order metadata consistently',async()=>{
 // Disable only the in-memory fixture insert trigger to model pre-migration records.
 await db.exec('alter table wellen_submissions disable trigger snapshot_wave_quarter_price');
 await db.query("insert into wellen_submissions(welle_id,item_type,item_id,quantity,value_per_unit,created_at) values($1,'einzelprodukt',$2,1,3,'2026-02-10T12:00Z')",[wave,item]);
 await db.exec('alter table wellen_submissions enable trigger snapshot_wave_quarter_price');
 const before=await scalar('select to_jsonb(s) as r from wellen_submissions s');
 await db.exec(await readFile(new URL('../supabase/migrations/20261113091431_wave_single_products_always_ve.sql',import.meta.url),'utf8'));
 assert.deepEqual(await scalar('select to_jsonb(s) as r from wellen_submissions s'),before);
 await db.query("update wellen_submissions set quantity=6,ve_size=6,order_unit='VE'");
 const s=await scalar('select * from wellen_submissions');assert.equal(Number(s.order_quantity),1);assert.equal(Number(s.order_unit_price),18);assert.equal(s.quantity,6);assert.equal(new Date(s.created_at).getTime(),new Date(before.r.created_at).getTime());assert.equal(s.price_source_id,null);
});
test('linked catalog prices remain authoritative and fractional historical piece snapshots stay readable',async()=>{
 const period=await scalar("select extract(year from now())::int as y,extract(quarter from now())::int as q");
 await db.query('select import_quarter_prices($1,$2,$3,$4,$5,$6)',[period.y,period.q,'synthetic.xlsx','e'.repeat(64),JSON.stringify([{department:'pets',item_type:'einzelprodukt',artikel_nr:'900001',name:'Synthetic six-pack',units_per_ve:6,price_per_order:24,source_cell:'Synthetic!A1:C1'}]),actor]);
 await db.query("update wellen_einzelprodukte set catalog_key='pets:einzelprodukt:900001' where id=$1",[item]);
 await db.query("insert into wellen_submissions(welle_id,item_type,item_id,quantity,value_per_unit) values($1,'einzelprodukt',$2,6,0.01)",[wave,item]);
 let s=await scalar('select * from wellen_submissions');assert.equal(Number(s.value_per_unit),4);assert.equal(Number(s.order_unit_price),24);assert.ok(s.price_source_id);
 await db.query("update wellen_submissions set quantity=1,order_unit='Stück'");s=await scalar('select * from wellen_submissions');assert.equal(Number(s.order_unit_price),4);
 await db.query("update wellen_submissions set quantity=6,order_unit='VE'");assert.equal(Number((await scalar('select order_quantity from wellen_submissions')).order_quantity),1);
});
test('VE parser rejects invalid/fractional sizes and helper reads are service-only',async()=>{
 for(const [v,want] of [['12',12],['VE: 6 Stück',6],['6,0',6],['0',null],['1.5',null],['abc',null],['100001',null]])assert.equal((await scalar('select parse_wave_ve($1) as n',[v])).n,want);
 for(const role of ['anon','authenticated'])assert.equal((await scalar("select has_function_privilege($1,'wave_single_order_details(uuid,uuid)','EXECUTE') as allowed",[role])).allowed,false);
});

async function syntheticCorrection(){
 const batch=id(8),booking=id(20),single=id(21);
 await db.exec('alter table wellen_submissions disable trigger snapshot_wave_quarter_price');
 await db.query("insert into wellen_submissions(id,welle_id,item_type,item_id,quantity,value_per_unit,submission_batch_id) values($1,$2,'einzelprodukt',$3,1,3,$4),($5,$2,'einzelprodukt',$6,1,4.5,$4)",[booking,wave,item,batch,single,one]);
 await db.exec('alter table wellen_submissions enable trigger snapshot_wave_quarter_price');
 return {actorId:actor,waveId:wave,batchIds:[batch],approval:{text:"Synthetic approver confirms one VE, not one piece; $confirmed_ve$ ' quoted",at:'2026-11-13T09:55:49+01:00'},entries:[{id:booking,ve:6,article:'900001',unitPrice:'3',sourceId:null},{id:single,ve:1,article:'900002',unitPrice:'4.5',sourceId:null}]};
}
test('confirmed VE correction is read-only in preview, narrowly audited, repeatable and preserves dates/markets',async()=>{
 const p=await syntheticCorrection(),before=(await db.query('select * from wellen_submissions order by id')).rows;
 const preview=(await scalar(correctionPreviewSQL(p))).preview;assert.equal(preview.count,2);assert.deepEqual(preview.issues,[]);
 assert.deepEqual((await db.query('select * from wellen_submissions order by id')).rows,before);
 const results=await db.exec(correctionApplySQL(p,preview.token)),result=results.find(r=>r.rows?.[0]?.result).rows[0].result;assert.equal(result.updated,2);assert.ok(result.auditId);
 const after=(await db.query('select * from wellen_submissions order by id')).rows;assert.deepEqual(after.map(s=>s.quantity),[6,1]);assert.ok(after.every(s=>Number(s.order_quantity)===1&&s.order_unit==='VE'));
 for(const s of after){const old=before.find(r=>r.id===s.id);assert.equal(s.created_at.getTime(),old.created_at.getTime());assert.equal(s.market_id,old.market_id);}
 const audit=(await scalar('select changes from wave_quarter_price_audit')).changes;assert.ok(audit.every(e=>e.approval.text===p.approval.text&&e.before&&e.after));
 const again=(await scalar(correctionPreviewSQL(p))).preview;assert.equal(again.count,0);await db.exec(correctionApplySQL(p,again.token));assert.equal((await scalar('select count(*)::int as n from wave_quarter_price_audit')).n,1);
});
test('confirmed correction rejects stale state and an audit failure rolls all quantity changes back',async()=>{
 const p=await syntheticCorrection(),preview=(await scalar(correctionPreviewSQL(p))).preview;
 await db.query("update wellen_submissions set market_id='synthetic-new' where id=$1",[p.entries[0].id]);
 await assert.rejects(db.exec(correctionApplySQL(p,preview.token)),/correction changed/);await db.exec('rollback');
 const current=(await scalar(correctionPreviewSQL(p))).preview,before=(await db.query('select * from wellen_submissions order by id')).rows;
 await db.exec("create function synthetic_ve_audit_fail() returns trigger language plpgsql as $$ begin raise exception 'Synthetic VE audit failure';end;$$;create trigger synthetic_ve_audit_fail before insert on wave_quarter_price_audit for each row execute function synthetic_ve_audit_fail()");
 try{await assert.rejects(db.exec(correctionApplySQL(p,current.token)),/Synthetic VE audit failure/);await db.exec('rollback');assert.deepEqual((await db.query('select * from wellen_submissions order by id')).rows,before);}
 finally{await db.exec('drop trigger synthetic_ve_audit_fail on wave_quarter_price_audit;drop function synthetic_ve_audit_fail()');}
});

for(const alreadyFrozen of [false,true])test('confirmed correction '+(alreadyFrozen?'preserves an existing':'adds a proven')+' quarterly source while converting one piece to one VE',async()=>{
 const p=await syntheticCorrection(),period=await scalar("select extract(year from now())::int as y,extract(quarter from now())::int as q");
 await db.query('select import_quarter_prices($1,$2,$3,$4,$5,$6)',[period.y,period.q,'synthetic-confirmed.xlsx','f'.repeat(64),JSON.stringify([{department:'pets',item_type:'einzelprodukt',artikel_nr:'900001',name:'Synthetic six-pack',units_per_ve:6,price_per_order:18,source_cell:'Synthetic!A1:C1'}]),actor]);
 const source=await scalar("select id from quarter_price_catalog where artikel_nr='900001'");p.entries[0].sourceId=source.id;
 if(alreadyFrozen)await db.query("update wellen_submissions set price_source_id=$1,order_unit='Stück' where id=$2",[source.id,p.entries[0].id]);
 const preview=(await scalar(correctionPreviewSQL(p))).preview;assert.deepEqual(preview.issues,[]);
 await db.exec(correctionApplySQL(p,preview.token));const s=await scalar('select * from wellen_submissions where id=$1',[p.entries[0].id]);
 assert.equal(s.quantity,6);assert.equal(Number(s.order_quantity),1);assert.equal(Number(s.order_unit_price),18);assert.equal(s.price_source_id,source.id);
});

test('unconfirmed booking prices cannot silently change during VE correction',async()=>{
 const p=await syntheticCorrection();await db.query('update wellen_submissions set value_per_unit=3.1 where id=$1',[p.entries[0].id]);
 const preview=(await scalar(correctionPreviewSQL(p))).preview;assert.ok(preview.issues.some(i=>i.reason==='Existing booking price changed'));
 await assert.rejects(db.exec(correctionApplySQL(p,preview.token)),/evidence is incomplete/);await db.exec('rollback');
 assert.equal((await scalar('select quantity from wellen_submissions where id=$1',[p.entries[0].id])).quantity,1);
});




