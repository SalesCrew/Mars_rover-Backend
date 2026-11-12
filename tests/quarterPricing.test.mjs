import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { before, beforeEach, after, test } from 'node:test';
import { PGlite } from '@electric-sql/pglite';
import pricing from '../src/utils/quarterPricing.ts';
const { viennaQuarter, submissionPrice } = pricing;

// Synthetic products and an in-memory Postgres only. No production env or data.
const db = new PGlite();
const id = n => `10000000-0000-4000-8000-${String(n).padStart(12,'0')}`;
const wave=id(1),actor=id(2),display=id(3),single=id(4),carton=id(5);
const scalar = async (sql, args=[]) => (await db.query(sql,args)).rows[0];
let period;
const row=(type='display',price=120,ve=1)=>({department:'food',item_type:type,artikel_nr:`SYNTHETIC-${type}`,name:`Synthetic ${type}`,units_per_ve:ve,price_per_order:price,source_cell:'Synthetic!A5:K5'});
const load=async(rows,year=period.year,quarter=period.quarter)=> (await scalar('select import_quarter_prices($1,$2,$3,$4,$5,$6) as r',[year,quarter,'synthetic.xlsx','a'.repeat(64),JSON.stringify(rows),actor])).r;
const reconcile=async(apply=false,token=null)=>(await scalar('select reconcile_wave_quarter_prices($1,$2,$3) as r',[apply,token,actor])).r;

before(async()=>{
 await db.exec(`create role anon;create role authenticated;create role service_role bypassrls;
 create table wellen(id uuid primary key,name text,is_deleted bool default false);
 create table wellen_displays(id uuid primary key,welle_id uuid references wellen(id),name text,item_value numeric,ve int);
 create table wellen_kartonware(like wellen_displays including all);
 create table wellen_einzelprodukte(like wellen_displays including all);alter table wellen_einzelprodukte add column artikel_nr text;
 create table wellen_submissions(id uuid primary key default gen_random_uuid(),welle_id uuid references wellen(id),item_type text,item_id uuid,quantity integer,value_per_unit numeric,created_at timestamptz default now(),market_id text default 'synthetic-market');
 create table products(id text primary key,name text,artikel_nr text);`);
 await db.exec(await readFile(new URL('../supabase/migrations/20261112132449_wave_quarter_catalog.sql',import.meta.url),'utf8'));
 await db.exec(await readFile(new URL('../supabase/migrations/20261112152854_wave_catalog_validity_and_exact_legacy_matches.sql',import.meta.url),'utf8'));
 period=await scalar(`select extract(year from now() at time zone 'Europe/Vienna')::int as year,extract(quarter from now() at time zone 'Europe/Vienna')::int as quarter`);
});
after(()=>db.close());
beforeEach(async()=>{
 await db.exec('truncate wellen_submissions,wellen_displays,wellen_kartonware,wellen_einzelprodukte,wellen,products,quarter_price_catalog,quarter_price_imports,wave_quarter_price_audit cascade');
 await db.query("insert into wellen(id,name) values($1,'Synthetic percentage wave')",[wave]);
 for(const [table,key,name] of [['wellen_displays',display,'Display'],['wellen_kartonware',carton,'Carton'],['wellen_einzelprodukte',single,'Single']]) await db.query(`insert into ${table}(id,welle_id,name,item_value) values($1,$2,$3,999)`,[key,wave,name]);
});
const link=async()=>{
 await load([row(),row('kartonware',24,1),row('einzelprodukt',18,6)]);
 for(const [table,type] of [['wellen_displays','display'],['wellen_kartonware','kartonware'],['wellen_einzelprodukte','einzelprodukt']]) await db.query(`update ${table} set catalog_key=$1`,[`food:${type}:SYNTHETIC-${type}`]);
 await db.query('update wellen set catalog_pricing_required=true');
};

test('Vienna quarter boundaries include year rollover and prices preserve explicit zero',()=>{
 assert.deepEqual(viennaQuarter(new Date('2026-03-31T21:59:59Z')),{year:2026,quarter:1});
 assert.deepEqual(viennaQuarter(new Date('2026-03-31T22:00:00Z')),{year:2026,quarter:2});
 assert.deepEqual(viennaQuarter(new Date('2026-12-31T23:00:00Z')),{year:2027,quarter:1});
 assert.equal(submissionPrice({value_per_unit:0},100),0);
 assert.equal(submissionPrice({value_per_unit:12},100),12);
});
test('catalog import is idempotent and conflicting prices cannot silently overwrite evidence',async()=>{
 assert.equal((await load([row()])).inserted,1);
 assert.equal((await load([row()])).unchanged,1);
 await assert.rejects(load([row('display',121)]),/anderer Preis/);
 assert.equal(Number((await scalar('select price_per_order from quarter_price_catalog')).price_per_order),120);
 await assert.rejects(load([row(),row()]),/Doppelte/);
});
test('new display/carton and whole-VE single orders get server-side immutable prices',async()=>{
 await link();
 for(const [type,key,quantity] of [['display',display,2],['kartonware',carton,3],['einzelprodukt',single,12]]) await db.query('insert into wellen_submissions(welle_id,item_type,item_id,quantity,value_per_unit) values($1,$2,$3,$4,0.01)',[wave,type,key,quantity]);
 const rows=(await db.query('select * from wellen_submissions order by item_type')).rows;
 assert.equal(Number(rows[0].value_per_unit),120);
 const individual=rows.find(r=>r.item_type==='einzelprodukt');
 assert.equal(individual.quantity,12);assert.equal(Number(individual.order_quantity),2);assert.equal(Number(individual.order_unit_price),18);assert.equal(Number(individual.value_per_unit),3);
 assert.equal(rows.reduce((sum,r)=>sum+Number(r.order_quantity)*Number(r.order_unit_price),0),348);
 await db.query('update wellen_displays set item_value=9999');
 assert.equal(Number((await scalar("select value_per_unit from wellen_submissions where item_type='display'")).value_per_unit),120);
 await assert.rejects(db.query("update wellen_submissions set value_per_unit=9999 where item_type='display'"),/Quartalspreis/);
});
test('one invalid partial VE rolls back the entire batch; missing quarters and wrong-wave IDs are refused',async()=>{
 await link();
 await assert.rejects(db.query("insert into wellen_submissions(welle_id,item_type,item_id,quantity) values($1,'display',$2,2),($1,'einzelprodukt',$3,7)",[wave,display,single]),/ganzen VEs/);
 assert.equal((await scalar('select count(*)::int as n from wellen_submissions')).n,0);
 await db.query("update wellen_displays set catalog_key='food:display:MISSING'");
 await assert.rejects(db.query("insert into wellen_submissions(welle_id,item_type,item_id,quantity) values($1,'display',$2,1)",[wave,display]),/Quartal/);
 await assert.rejects(db.query("insert into wellen_submissions(welle_id,item_type,item_id,quantity) values($1,'display',$2,1)",[wave,id(999)]),/verknüpft/);
});
test('historical preview is read-only, quarter-specific, audited, stale-safe and idempotent',async()=>{
 await load([row('display',100)],2026,1);await load([row('display',120)],2026,2);
 await db.query("insert into wellen_submissions(welle_id,item_type,item_id,quantity,value_per_unit,created_at) values($1,'display',$2,2,null,'2026-03-31 21:59Z'),($1,'display',$2,3,99,'2026-03-31 22:00Z'),($1,'kartonware',$3,1,null,'2026-01-15 12:00Z')",[wave,display,carton]);
 await db.query("update wellen_displays set catalog_key='food:display:SYNTHETIC-display'");
 const before=(await db.query('select * from wellen_submissions order by id')).rows;
 const preview=await reconcile();assert.equal(preview.count,2);assert.equal(preview.unresolved.length,1);
 assert.deepEqual((await db.query('select * from wellen_submissions order by id')).rows,before);
 assert.equal((await reconcile(true,preview.token)).updated,2);
 const after=(await db.query('select * from wellen_submissions order by id')).rows;
 for(const r of after){const old=before.find(x=>x.id===r.id);assert.equal(r.quantity,old.quantity);assert.equal(String(r.created_at),String(old.created_at));assert.equal(r.market_id,old.market_id);}
 const values=after.filter(r=>r.item_type==='display').map(r=>Number(r.value_per_unit)).sort();assert.deepEqual(values,[100,120]);
 assert.equal((await scalar('select count(*)::int as n from wave_quarter_price_audit')).n,1);
 assert.equal((await reconcile()).count,0);
 await assert.rejects(reconcile(true,preview.token),/veraltet/);
});
test('quantity edits retain frozen prices and update the VE quantity consistently',async()=>{
 await link();await db.query("insert into wellen_submissions(welle_id,item_type,item_id,quantity) values($1,'einzelprodukt',$2,12)",[wave,single]);
 await db.query('update wellen_submissions set quantity=18');
 assert.equal(Number((await scalar('select order_quantity from wellen_submissions')).order_quantity),3);
 await assert.rejects(db.query('update wellen_submissions set quantity=13'),/ganze VEs/);
});
test('legacy article matching requires both SKU and exact normalized name, with unresolved evidence retained',async()=>{
 await load([row('einzelprodukt',18,6)],2026,1);
 await db.query('insert into products(id,name,artikel_nr) values($1,$2,$3)',[id(20),'  SYNTHETIC™  einzelprodukt  ','SYNTHETIC-einzelprodukt']);
 await db.query('insert into products(id,name,artikel_nr) values($1,$2,$3)',[id(21),'Synthetic display misclassified as individual','SYNTHETIC-einzelprodukt']);
 await db.query("insert into wellen_submissions(welle_id,item_type,item_id,quantity,created_at) values($1,'einzelprodukt',$2,7,'2026-01-15'),($1,'einzelprodukt',$3,6,'2026-01-15')",[wave,id(20),id(21)]);
 const preview=await reconcile();assert.equal(preview.count,1);assert.equal(preview.unresolved.length,1);assert.equal(preview.unresolved[0].article,'SYNTHETIC-einzelprodukt');
 assert.equal(preview.changes[0].orderUnit,'Stück');assert.equal(preview.changes[0].orderQuantity,7);assert.equal(Number(preview.changes[0].newPrice),3);
 assert.equal((await reconcile(true,preview.token)).updated,1);
});
test('snapshot article identity survives a later wave definition change',async()=>{
 await link();await db.query("insert into wellen_submissions(welle_id,item_type,item_id,quantity) values($1,'display',$2,2)",[wave,display]);
 await load([{...row('display',500),artikel_nr:'SYNTHETIC-OTHER'}]);
 await db.query("update wellen_displays set catalog_key='food:display:SYNTHETIC-OTHER'");
 assert.equal((await reconcile()).count,0);assert.equal((await reconcile()).unresolved.length,0);
 assert.equal(Number((await scalar('select value_per_unit from wellen_submissions')).value_per_unit),120);
});
test('an audit failure rolls back every historical price change',async()=>{
 await load([row()],2026,1);
 await db.query("insert into wellen_submissions(welle_id,item_type,item_id,quantity,created_at) values($1,'display',$2,2,'2026-01-15')",[wave,display]);
 await db.query("update wellen_displays set catalog_key='food:display:SYNTHETIC-display'");
 const preview=await reconcile();
 await db.exec("create function fail_synthetic_audit() returns trigger language plpgsql as $$ begin raise exception 'Synthetic audit unavailable';end;$$;create trigger fail_synthetic_audit before insert on wave_quarter_price_audit for each row execute function fail_synthetic_audit();");
 try { await assert.rejects(reconcile(true,preview.token),/Synthetic audit unavailable/);assert.equal((await scalar('select value_per_unit from wellen_submissions')).value_per_unit,null); }
 finally {await db.exec('drop trigger fail_synthetic_audit on wave_quarter_price_audit;drop function fail_synthetic_audit();');}
});
test('catalog and historical writes are service-only and imports reject absent provenance',async()=>{
 for(const role of ['anon','authenticated']) {
   assert.equal((await scalar('select has_table_privilege($1,$2,$3) as allowed',[role,'quarter_price_catalog','SELECT'])).allowed,false);
   assert.equal((await scalar('select has_function_privilege($1,$2,$3) as allowed',[role,'import_quarter_prices(integer,integer,text,text,jsonb,uuid)','EXECUTE'])).allowed,false);
   assert.equal((await scalar('select has_function_privilege($1,$2,$3) as allowed',[role,'reconcile_wave_quarter_prices(boolean,text,uuid)','EXECUTE'])).allowed,false);
 }
 assert.equal((await scalar("select has_table_privilege('service_role','quarter_price_catalog','INSERT') as allowed")).allowed,true);
 await assert.rejects(db.query('select import_quarter_prices(null,1,null,null,null,$1)',[actor]),/Ungültige/);
 assert.equal((await scalar('select count(*)::int as n from quarter_price_imports')).n,0);
});
test('historical preview and apply process more than a PostgREST page without dropping bookings',async()=>{
 await load([row(),...Array.from({length:999},(_,i)=>({...row(),artikel_nr:'DECOY-'+i,name:'Synthetic decoy '+i}))],2026,1);
 await db.query("insert into wellen_submissions(welle_id,item_type,item_id,quantity,created_at) select $1,'display',$2,1,'2026-01-15' from generate_series(1,1201)",[wave,display]);
 await db.query("update wellen_displays set name='Synthetic display'");
 const preview=await reconcile();assert.equal(preview.count,1201);assert.equal((await reconcile(true,preview.token)).updated,1201);assert.equal((await reconcile()).count,0);
});
test('a supplier date outside the quarter boundary requires recorded business approval',async()=>{
 const entry={...row(),source_valid_from:'2025-10-01'};
 await assert.rejects(load([entry],2026,1),/Freigabe/);assert.equal((await scalar('select count(*)::int as n from quarter_price_imports')).n,0);
 const note='Synthetic approver confirms this source for Q1 2026';await load([{...entry,source_note:note}],2026,1);
 assert.equal((await scalar('select source_note from quarter_price_imports')).source_note,note);assert.equal((await scalar('select source_note from quarter_price_catalog')).source_note,note);
 await assert.rejects(load([{...row(),source_valid_from:'2026-02-01'}],2026,1),/Freigabe/);
});
test('only a unique full name/type/quarter can recover an article number that was never saved',async()=>{
 await load([row('display',100)],2026,1);await load([row('display',120)],2026,2);
 await db.query("update wellen_displays set name='SYNTHETIC™   display'");
 await db.query("insert into wellen_submissions(welle_id,item_type,item_id,quantity,created_at) values($1,'display',$2,2,'2026-01-15'),($1,'display',$2,3,'2026-04-15')",[wave,display]);
 const p=await reconcile();assert.equal(p.count,2);assert.deepEqual(p.changes.map(c=>Number(c.newPrice)).sort(),[100,120]);assert.ok(p.changes.every(c=>c.matchMethod.includes('vollständiger Name')));assert.equal((await reconcile(true,p.token)).updated,2);
});
test('duplicate names and conflicting recorded article numbers cannot use the name-only fallback',async()=>{
 await load([row('display',100),{...row('display',500),artikel_nr:'DIFFERENT-SKU'}],2026,1);
 await db.query("update wellen_displays set name='Synthetic display'");
 await db.query("insert into wellen_submissions(welle_id,item_type,item_id,quantity,created_at) values($1,'display',$2,2,'2026-01-15')",[wave,display]);
 assert.equal((await reconcile()).count,0);assert.equal((await reconcile()).unresolved.length,1);
 await db.query("update wellen_displays set artikel_nr='WRONG-SKU'");assert.equal((await reconcile()).count,0);
 assert.equal((await scalar('select price_source_id from wellen_submissions')).price_source_id,null);
});
