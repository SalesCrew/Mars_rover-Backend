import express from 'express';
import cors from 'cors';
import { PGlite } from '@electric-sql/pglite';

export const testId = n => `00000000-0000-4000-8000-${String(n).padStart(12, '0')}`;
export const waveId = testId(1), glId = testId(2);
const listen = async app => {
  const server = app.listen(0, '127.0.0.1');
  await new Promise(resolve => server.once('listening', resolve));
  return { server, origin: `http://127.0.0.1:${server.address().port}` };
};
const identifier = name => {
  if (!/^[a-z_]+$/.test(name)) throw new Error('Unexpected test SQL identifier');
  return `"${name}"`;
};

// Real application routers and supabase-js talk only to this loopback PostgREST
// adapter, backed by in-memory PostgreSQL. Never import the production server.
export async function createProductUuidHarness() {
  const db = new PGlite();
  await db.exec(`
    create table products(id varchar(255) primary key, name text not null, department text,
      product_type text, weight text, content text, pallet_size int, price numeric(10,2),
      sku text, artikel_nr text, palette_products jsonb, is_active bool default true,
      is_deleted bool default false, created_at timestamptz default now(), updated_at timestamptz default now());
    create table wellen_submissions(id uuid primary key default gen_random_uuid(),
      welle_id uuid, gebietsleiter_id uuid, market_id text, item_type text, item_id uuid,
      quantity int, value_per_unit numeric(10,2), submission_batch_id uuid,
      created_at timestamptz default now(), unique(submission_batch_id,item_type,item_id));
    create table markets(id text primary key, current_visits int default 0, last_visit_date text);
    create table market_visits(market_id text, gebietsleiter_id uuid, visit_date text,
      source text, unique(market_id,visit_date));
  `);
  const rest = express();
  rest.use(express.json());
  const writes = [];
  rest.all('/rest/v1/:table', async (req, res) => {
    const table = req.params.table;
    if (!['products','wellen_submissions','markets','market_visits'].includes(table)) return res.status(500).json({message:`Unexpected test table: ${table}`});
    const args = [];
    const where = [];
    for (const [column, value] of Object.entries(req.query)) {
      if (['select','order','limit','columns','on_conflict'].includes(column)) continue;
      const filter = String(value);
      if (filter.startsWith('eq.')) {
        args.push(filter.slice(3)); where.push(`${identifier(column)}=$${args.length}`);
      } else if (filter.startsWith('in.(')) {
        const values = filter.slice(4,-1).split(',').map(s => s.replace(/^"|"$/g,''));
        const placeholders = values.map(value => {args.push(value);return `$${args.length}`;});
        where.push(`${identifier(column)} in (${placeholders.join(',')})`);
      } else throw new Error(`Unexpected fixture filter: ${filter}`);
    }
    const clause = where.length ? ` where ${where.join(' and ')}` : '';
    try {
      let result;
      if (req.method === 'GET') {
        result = await db.query(`select * from ${identifier(table)}${clause}${req.query.order ? ' order by name' : ''}${req.query.limit ? ` limit ${Number(req.query.limit)}` : ''}`,args);
      } else if (req.method === 'POST') {
        const rows = Array.isArray(req.body) ? req.body : [req.body];
        const columns = Object.keys(rows[0]);
        const parameters = [];
        const values = rows.map(row => `(${columns.map(column => {parameters.push(column === 'palette_products' && row[column] != null ? JSON.stringify(row[column]) : row[column]);return `$${parameters.length}`;}).join(',')})`).join(',');
        result = await db.query(`insert into ${identifier(table)} (${columns.map(identifier).join(',')}) values ${values}${req.headers.prefer?.includes('resolution=ignore-duplicates') ? ' on conflict do nothing' : ''} returning *`,parameters);
        writes.push({table,method:req.method,rows:result.rows});
      } else if (req.method === 'PATCH') {
        const setters = Object.entries(req.body).map(([column,value]) => {args.push(value);return `${identifier(column)}=$${args.length}`;});
        result = await db.query(`update ${identifier(table)} set ${setters.join(',')}${clause} returning *`,args);
        writes.push({table,method:req.method,rows:result.rows});
      } else return res.status(405).end();
      const single = req.headers.accept?.includes('application/vnd.pgrst.object');
      res.status(req.method === 'POST' ? 201 : 200).json(single ? result.rows[0] ?? null : result.rows);
    } catch (error) {
      res.status(400).json({code:error.code,message:error.message,details:null,hint:null});
    }
  });
  const restListener = await listen(rest);
  process.env.SUPABASE_URL = restListener.origin;
  process.env.SUPABASE_SERVICE_KEY = 'sb_secret_isolated_product_uuid_test';
  const [{default:products},{default:wellen}] = await Promise.all([
    import('../../src/routes/products.ts'), import('../../src/routes/wellen.ts')
  ]);
  const app = express();
  app.use(cors());
  app.use(express.json());
  app.use((req,_res,next) => {
    const role = req.headers['test-role'] || 'gl';
    if (role !== 'none') req.user = {id:glId,role,gebietsleiter_id:glId};
    next();
  });
  app.get('/api/wellen', (_req,res) => res.json([{
    id:waveId,name:'Zusatz-Umsatz UUID Test',status:'active',types:['einzelprodukt'],
    startDate:'2026-01-01',endDate:'2026-12-31',goalType:'value',goalValue:1000,
    displays:[],kartonwareItems:[],einzelproduktItems:[],paletteItems:[],schutteItems:[],
    assignedMarketIds:['test-market'],assignedMarketCount:1,fotoEnabled:false,fotoOnly:false,
    noLimitWelle:false,fotoTags:[]
  }]));
  app.get('/api/markets', (_req,res) => res.json([{
    id:'test-market',name:'UUID Testmarkt',chain:'BILLA',address:'Teststraße 1',city:'Wien',
    postal_code:'1010',gebietsleiter_id:glId,is_active:true,current_visits:0,last_visit_date:null
  }]));
  app.get('/api/wellen/:id/submitted-markets/:gl', (_req,res) => res.json({marketIds:[],allMarketIds:[],visitedMarketIds:[],allVisitedMarketIds:[]}));
  app.get('/api/wellen/market/:market/pending-photos', (_req,res) => res.json([]));
  app.post('/api/markets/:market/visit', (_req,res) => res.json({success:true}));
  app.use('/api/products', products);
  app.use('/api/wellen', wellen);
  const api = await listen(app);
  return {db,writes,origin:api.origin,close:async () => {
    await Promise.all([new Promise(r=>api.server.close(r)),new Promise(r=>restListener.server.close(r))]);
    await db.close();
  }};
}
