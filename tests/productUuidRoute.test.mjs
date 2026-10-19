import assert from 'node:assert/strict';
import { before, beforeEach, after, test } from 'node:test';
import { createProductUuidHarness, testId, waveId, glId } from './helpers/productUuidHarness.mjs';
let h;
const product = {id:'cesar-excel-row-5',name:'Cesar Klassik Terrine',department:'pets',productType:'standard',weight:'1200g',content:'3',price:6.83,artikelNr:'334822'};
const request = (path,body,role='admin',method='POST') => fetch(`${h.origin}/api/${path}`,{
  method,headers:{'Content-Type':'application/json','test-role':role},body:JSON.stringify(body)
});
const submit = (items,batch=testId(100)) => request(`wellen/${waveId}/progress/batch`,{gebietsleiter_id:glId,market_id:'test-market',items,submission_batch_id:batch},'gl');
before(async()=>{h=await createProductUuidHarness();});
beforeEach(async()=>{
  await h.db.exec("truncate products,wellen_submissions,markets,market_visits; insert into markets(id) values ('test-market')");
  h.writes.length=0;
});
after(async()=>{await h.close();});

test('Excel/manual IDs are replaced on creation; returned UUID saves as Zusatzumsatz; retry is idempotent',async()=>{
  const response=await request('products',[product,{...product,id:'standard-pets-123-0',name:'Manuell'},{...product,id:testId(50),name:'Caller UUID'}]);
  assert.equal(response.status,201);
  const products=await response.json();
  assert.equal(new Set(products.map(p=>p.id)).size,3);
  for(const p of products) assert.match(p.id,/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i);
  assert.ok(!products.some(p=>p.id===testId(50)));
  const items=products.map(p=>({item_type:'einzelprodukt',item_id:p.id,current_number:6,value_per_unit:p.price}));
  assert.equal((await submit(items)).status,200);
  assert.equal((await (await submit(items)).json()).already_processed,true);
  const saved=(await h.db.query('select * from wellen_submissions')).rows;
  assert.equal(saved.length,3);
  assert.ok(saved.every(s=>s.quantity===6 && Number(s.value_per_unit)===6.83));
  assert.equal((await h.db.query('select current_visits from markets')).rows[0].current_visits,1);
  assert.equal((await h.db.query('select * from market_visits')).rows.length,1);
});

test('invalid ID in a mixed batch names the article and writes neither products nor visits nor submissions',async()=>{
  await h.db.query('insert into products(id,name,artikel_nr,price) values ($1,$2,$3,$4)',[product.id,product.name,product.artikelNr,product.price]);
  const response=await submit([{item_type:'einzelprodukt',item_id:testId(9),current_number:1},{item_type:'einzelprodukt',item_id:product.id,current_number:1}]);
  assert.equal(response.status,422);
  const error=await response.json();
  assert.equal(error.code,'MR-WELLE-PRODUCT-ID-001');
  assert.match(error.error,/Cesar Klassik Terrine.*334822/);
  assert.match(error.error,/nichts gespeichert/);
  assert.equal(h.writes.length,0);
  assert.equal((await h.db.query('select * from wellen_submissions')).rows.length,0);
  assert.equal((await h.db.query('select current_visits from markets')).rows[0].current_visits,0);
});

test('missing/null item IDs are controlled validation failures',async()=>{
  for(const item of [null,{}, {item_id:null}]) assert.equal((await submit([item])).status,422);
  assert.equal(h.writes.length,0);
});

test('editing a historical text-ID product retains its ID and ignores caller replacement IDs',async()=>{
  await h.db.query('insert into products(id,name,price) values ($1,$2,$3)',[product.id,product.name,product.price]);
  const response=await request(`products/${product.id}`,{name:'Updated',id:testId(70)},'admin','PUT');
  assert.equal(response.status,200);
  assert.equal((await response.json()).id,product.id);
  assert.equal((await h.db.query('select count(*)::int as n from products')).rows[0].n,1);
});

test('product creation still denies GLs and unauthenticated requests',async()=>{
  for(const role of ['gl','none']) assert.ok([401,403].includes((await request('products',[product],role)).status));
  assert.equal(h.writes.length,0);
});
