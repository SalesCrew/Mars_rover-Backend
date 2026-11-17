import express from 'express';
import cors from 'cors';
import { PGlite } from '@electric-sql/pglite';
import { readFile } from 'node:fs/promises';

export const id = (n) =>
  `c0000000-0000-4000-8000-${String(n).padStart(12, '0')}`;
export const adminId = id(1),
  customerId = id(2),
  glId = id(3),
  testGlId = id(4),
  waveId = id(10),
  photoWaveId = id(11),
  photoId = id(30);
export const password = 'Preview-Only-2026!';
export const defaultPermissions = {
  dashboard: { view: true, export: false },
  vorbesteller: { view: true, export: true },
  fotos: { view: true, export: false },
};
const identifier = (value) => {
  if (!/^[a-z_][a-z0-9_]*$/.test(value)) throw Error('Invalid identifier');
  return '"' + value + '"';
};
const listen = async (app, port = 0) => {
  const server = app.listen(port, '127.0.0.1');
  await new Promise((resolve, reject) => {
    server.once('listening', resolve);
    server.once('error', reject);
  });
  return { server, origin: `http://127.0.0.1:${server.address().port}` };
};

// A disposable Postgres and an Auth/PostgREST loopback adapter. Uses the actual
// application auth, customer router and SQL migration, with generated records only.
export async function createCustomerAccessHarness({
  port = 0,
  rowCap = 1000,
  actualAdminPages = false,
} = {}) {
  if (
    process.env.SUPABASE_URL &&
    !/^http:\/\/127\.0\.0\.1(?::|\/)/.test(process.env.SUPABASE_URL)
  )
    throw Error('Refusing non-isolated Supabase configuration');
  const db = new PGlite();
  const errors = [],
    calls = [],
    tokens = new Map(),
    accounts = new Map();
  const businessWrites = [];
  let seq = 500;
  await db.exec(`CREATE ROLE anon;CREATE ROLE authenticated;CREATE ROLE service_role BYPASSRLS;
    CREATE SCHEMA auth; CREATE TABLE auth.users(id uuid PRIMARY KEY);
    CREATE FUNCTION auth.uid() RETURNS uuid LANGUAGE sql STABLE AS $$ SELECT nullif(current_setting('request.jwt.claim.sub',true),'')::uuid $$;
    GRANT USAGE ON SCHEMA auth TO authenticated;GRANT EXECUTE ON FUNCTION auth.uid() TO authenticated;
    CREATE TABLE users(id uuid PRIMARY KEY REFERENCES auth.users ON DELETE CASCADE,role varchar(20) CHECK(role IN ('admin','gl')),first_name text,last_name text,gebietsleiter_id text,created_at timestamptz DEFAULT now(),updated_at timestamptz DEFAULT now());
    CREATE TABLE wellen(id uuid PRIMARY KEY,name text,start_date date,end_date date,goal_type text,goal_value numeric,goal_percentage numeric,foto_only bool DEFAULT false,foto_enabled bool DEFAULT false,is_deleted bool DEFAULT false);
    CREATE TABLE wellen_submissions(id uuid PRIMARY KEY,welle_id uuid,gebietsleiter_id uuid,market_id text,item_type text,item_id uuid,quantity int,value_per_unit numeric,artikel_nr text,order_quantity numeric,order_unit text,order_unit_price numeric,ve_size int,created_at timestamptz DEFAULT now());
    CREATE TABLE wellen_photos(id uuid PRIMARY KEY,welle_id uuid,gebietsleiter_id uuid,market_id text,photo_url text,tags text[],comment text,created_at timestamptz DEFAULT now());
    CREATE TABLE gebietsleiter(id uuid PRIMARY KEY,name text,email text,is_active bool DEFAULT true,is_test bool DEFAULT false,password_hash text DEFAULT 'must-never-be-returned');
    CREATE TABLE markets(id text PRIMARY KEY,name text,chain text,address text,postal_code text,city text,is_active bool DEFAULT true);
    CREATE TABLE products(id uuid PRIMARY KEY,name text,artikel_nr text,department text,product_type text,content text,price numeric,is_deleted bool DEFAULT false);
    CREATE TABLE vorverkauf_wellen(id uuid PRIMARY KEY,name text,start_date date,end_date date,status text);
    CREATE TABLE vorverkauf_entries(id uuid PRIMARY KEY,market_id text,reason text,status text,created_at timestamptz DEFAULT now());
    CREATE TABLE nara_incentive_submissions(id uuid PRIMARY KEY,market_id text,gebietsleiter_id uuid,created_at timestamptz DEFAULT now());
    CREATE TABLE fb_fragebogen(id uuid PRIMARY KEY,name text,start_date date,end_date date,status text,is_deleted bool DEFAULT false,archived bool DEFAULT false);
    CREATE TABLE fb_zeiterfassung_submissions(id uuid PRIMARY KEY,market_id text,gebietsleiter_id uuid,besuchszeit_diff text,fahrzeit_diff text,distanz_km numeric,created_at timestamptz DEFAULT now());
    CREATE SCHEMA storage;CREATE TABLE storage.objects(id uuid PRIMARY KEY,name text);
    DO $$ DECLARE t record; BEGIN FOR t IN SELECT schemaname,tablename FROM pg_tables WHERE schemaname IN ('public','storage') LOOP
      EXECUTE format('ALTER TABLE %I.%I ENABLE ROW LEVEL SECURITY',t.schemaname,t.tablename);
      EXECUTE format('GRANT SELECT,INSERT,UPDATE,DELETE ON %I.%I TO authenticated',t.schemaname,t.tablename);
      EXECUTE format('CREATE POLICY existing_authenticated_access ON %I.%I FOR ALL TO authenticated USING(true) WITH CHECK(true)',t.schemaname,t.tablename);
    END LOOP; END $$;
    GRANT USAGE ON SCHEMA storage TO authenticated;
    -- Managed storage metadata already has RLS and no permissive policy.
    CREATE TABLE storage.buckets(id text PRIMARY KEY);
    ALTER TABLE storage.buckets ENABLE ROW LEVEL SECURITY;
    GRANT SELECT ON storage.buckets TO authenticated;
    INSERT INTO storage.buckets VALUES('synthetic-private-bucket');
  `);
  await db.exec(
    await readFile(
      new URL(
        '../../supabase/migrations/20261117162534_customer_access.sql',
        import.meta.url
      ),
      'utf8'
    )
  );
  const seedAuth = async (uid, email, role, first, last) => {
    await db.query('insert into auth.users values($1)', [uid]);
    if (role !== 'customer')
      await db.query(
        'insert into users(id,role,first_name,last_name,gebietsleiter_id) values($1,$2,$3,$4,$5)',
        [uid, role, first, last, role === 'gl' ? glId : null]
      );
    accounts.set(uid, {
      id: uid,
      email,
      password,
      role: 'authenticated',
      aud: 'authenticated',
      app_metadata: {},
      user_metadata: {},
      created_at: new Date().toISOString(),
    });
  };
  await seedAuth(adminId, 'admin@example.test', 'admin', 'Alex', 'Admin');
  await seedAuth(glId, 'gl@example.test', 'gl', 'Robin', 'Gebietsleiter');
  await seedAuth(
    customerId,
    'bettina@example.test',
    'customer',
    'Bettina',
    'Kundin'
  );
  await db.query('select save_customer_access($1,$2,$3,$4,$5,$6,$7,$8)', [
    adminId,
    customerId,
    'bettina@example.test',
    'Bettina',
    'Kundin',
    true,
    JSON.stringify(defaultPermissions),
    null,
  ]);
  await db.query(
    'insert into gebietsleiter(id,name,email,is_test) values($1,$2,$3,false),($4,$5,$6,true)',
    [
      glId,
      'Robin Muster',
      'gl@example.test',
      testGlId,
      'Testperson',
      'test@example.test',
    ]
  );
  for (const [uid, name, start, end, photo, deleted] of [
    [
      waveId,
      'Zusatz-Umsatz · Herbst 2026',
      '2026-10-01',
      '2026-12-31',
      false,
      false,
    ],
    [
      photoWaveId,
      'Fotowelle · Marktauftritt',
      '2026-11-01',
      '2026-11-30',
      true,
      false,
    ],
    [
      id(12),
      'Historisch – unsichtbar',
      '2026-01-01',
      '2026-03-31',
      false,
      false,
    ],
    [
      id(13),
      'Zukünftig – unsichtbar',
      '2027-01-01',
      '2027-03-31',
      false,
      false,
    ],
    [id(14), 'Gelöscht – unsichtbar', '2026-10-01', '2026-12-31', false, true],
  ]) {
    await db.query(
      'insert into wellen(id,name,start_date,end_date,goal_type,goal_value,foto_only,foto_enabled,is_deleted) values($1,$2,$3,$4,$5,$6,$7,$8,$9)',
      [uid, name, start, end, 'value', 10000, photo, true, deleted]
    );
  }
  for (const [uid, wave, gl, quantity, price, article] of [
    [id(20), waveId, glId, 12, 2.5, 'TEST-100'],
    [id(21), waveId, glId, 2, 100, 'TEST-200'],
    [id(22), waveId, glId, 6, null, 'TEST-300'],
    [id(23), waveId, testGlId, 1000, 1000, 'TEST-IGNORE'],
  ])
    await db.query(
      'insert into wellen_submissions(id,welle_id,gebietsleiter_id,market_id,item_type,item_id,quantity,value_per_unit,artikel_nr,order_quantity,order_unit,order_unit_price,ve_size) values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)',
      [
        uid,
        wave,
        gl,
        'DEMO-1',
        'einzelprodukt',
        id(99),
        quantity,
        price,
        article,
        quantity / 6,
        'VE',
        price === null ? null : price * 6,
        6,
      ]
    );
  await db.query(
    "insert into wellen_photos(id,welle_id,gebietsleiter_id,market_id,photo_url,tags,comment) values($1,$2,$3,'DEMO-1','demo/market.png',ARRAY['Zweitplatzierung'],'Beispielfoto für die lokale Vorschau')",
    [photoId, photoWaveId, glId]
  );
  await db.query(
    "insert into markets(id,name,chain,address,postal_code,city) values('DEMO-1','Musterfiliale','BILLA','Beispielstraße 1','1010','Wien')"
  );
  await db.query(
    "insert into products(id,name,artikel_nr,department,product_type,content,price) values($1,'Beispielprodukt','TEST-100','food','standard','6',2.5)",
    [id(99)]
  );
  if (actualAdminPages) { const {seedAdminPages} = await import('./adminPageFixture.mjs'); await seedAdminPages(db, id); }
  const tokenFor = (uid) => {
    const token =
      Buffer.from(JSON.stringify({ alg: 'HS256', typ: 'JWT' })).toString(
        'base64url'
      ) +
      '.' +
      Buffer.from(
        JSON.stringify({
          sub: uid,
          role: 'authenticated',
          exp: Math.floor(Date.now() / 1000) + 3600,
        })
      ).toString('base64url') +
      '.isolated-' +
      seq++;
    tokens.set(token, uid);
    return token;
  };
  const authUser = (uid) => {
    const { password: _, ...user } = accounts.get(uid);
    return user;
  };
  const rest = express();
  rest.use(express.json({ limit: '2mb' }));
  if (actualAdminPages) rest.use((req,res,next)=>{if(req.path.startsWith('/rest/v1/') && !['GET','HEAD'].includes(req.method) && !req.path.startsWith('/rest/v1/rpc/save_customer_access')) { businessWrites.push({method:req.method,path:req.path}); return res.status(400).json({message:'Fixture forbids business writes'}); } next();});
  rest.post('/auth/v1/token', (req, res) => {
    const account = [...accounts.values()].find(
      (a) => a.email === req.body.email && a.password === req.body.password
    );
    const uid = req.body.refresh_token?.startsWith('refresh-')
      ? req.body.refresh_token.slice(8)
      : account?.id;
    if (!uid || !accounts.has(uid))
      return res.status(400).json({
        error: 'invalid_grant',
        error_description: 'Invalid credentials',
      });
    res.json({
      access_token: tokenFor(uid),
      refresh_token: 'refresh-' + uid,
      expires_in: 3600,
      token_type: 'bearer',
      user: authUser(uid),
    });
  });
  rest.get('/auth/v1/user', (req, res) => {
    const uid = tokens.get(req.headers.authorization?.replace(/^Bearer /, ''));
    if (!uid) return res.status(401).json({ msg: 'Invalid token' });
    res.json(authUser(uid));
  });
  rest.post('/auth/v1/logout', (req, res) => {
    tokens.delete(req.headers.authorization?.replace(/^Bearer /, ''));
    res.status(204).end();
  });
  rest.post('/auth/v1/admin/users', async (req, res) => {
    if ([...accounts.values()].some((a) => a.email === req.body.email))
      return res.status(422).json({ msg: 'Email exists' });
    const uid = id(seq++);
    await db.query('insert into auth.users values($1)', [uid]);
    accounts.set(uid, {
      id: uid,
      email: req.body.email,
      password: req.body.password,
      role: 'authenticated',
      aud: 'authenticated',
      app_metadata: {},
      user_metadata: {},
    });
    res.json(authUser(uid));
  });
  rest.delete('/auth/v1/admin/users/:id', async (req, res) => {
    await db.query('delete from auth.users where id=$1', [req.params.id]);
    accounts.delete(req.params.id);
    res.json({});
  });
  const png = Buffer.from(
    'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jRZkAAAAASUVORK5CYII=',
    'base64'
  );
  rest.get(
    [
      '/storage/v1/object/authenticated/wellen-photos/*',
      '/storage/v1/object/wellen-photos/*',
    ],
    (_req, res) => res.type('png').send(png)
  );
  rest.post('/storage/v1/object/sign/:bucket/*', (req,res) => res.json({signedURL:'/object/authenticated/'+req.params.bucket+'/'+req.params[0]+'?fixture=1'}));
  rest.post('/rest/v1/rpc/save_customer_access', async (req, res) => {
    try {
      const keys = Object.keys(req.body),
        args = keys.map((k) =>
          typeof req.body[k] === 'object' && req.body[k] !== null
            ? JSON.stringify(req.body[k])
            : req.body[k]
        );
      const r = await db.query(
        `select save_customer_access(${keys.map((k, i) => identifier(k) + '=> $' + (i + 1)).join(',')}) result`,
        args
      );
      res.json(r.rows[0].result);
    } catch (e) {
      res.status(400).json({ message: e.message, code: e.code });
    }
  });
  if (actualAdminPages) { const { adminPageRest } = await import('./adminPageFixture.mjs'); rest.get('/rest/v1/:table', await adminPageRest(db,{errors,calls,rowCap})); }
  rest.get('/rest/v1/:table', async (req, res) => {
    calls.push({ table: req.params.table, select: req.query.select });
    try {
      const args = [],
        where = [];
      for (const [key, raw] of Object.entries(req.query)) {
        if (['select', 'order', 'limit', 'offset'].includes(key)) continue;
        const value = String(raw);
        if (value.startsWith('in.(')) {
          const list = value
            .slice(4, -1)
            .split(',')
            .filter(Boolean)
            .map((s) => s.replace(/^"|"$/g, ''));
          where.push(
            list.length
              ? `${identifier(key)} in (${list
                  .map((v) => {
                    args.push(v);
                    return '$' + args.length;
                  })
                  .join(',')})`
              : 'false'
          );
        } else {
          const op = value.startsWith('eq.')
            ? '='
            : value.startsWith('lte.')
              ? '<='
              : value.startsWith('gte.')
                ? '>='
                : null;
          if (!op) throw Error('Unexpected filter ' + value);
          args.push(value.slice(value.indexOf('.') + 1));
          where.push(identifier(key) + op + '$' + args.length);
        }
      }
      const selection = String(req.query.select || '*')
        .split(',')
        .map(identifier)
        .join(',');
      const order = req.query.order
        ? ' ORDER BY ' +
          String(req.query.order)
            .split(',')
            .map((o) => identifier(o.split('.')[0]))
            .join(',')
        : '';
      const rows = (
        await db.query(
          `SELECT ${selection} FROM ${identifier(req.params.table)}${where.length ? ' WHERE ' + where.join(' AND ') : ''}${order} LIMIT ${Math.min(Number(req.query.limit || 1000), rowCap)} OFFSET ${Number(req.query.offset || 0)}`,
          args
        )
      ).rows.map((row) =>
        Object.fromEntries(
          Object.entries(row).map(([key, v]) => [
            key,
            v instanceof Date
              ? ['start_date', 'end_date'].includes(key)
                ? v.toISOString().slice(0, 10)
                : v.toISOString()
              : v,
          ])
        )
      );
      res.json(
        req.headers.accept?.includes('application/vnd.pgrst.object')
          ? (rows[0] ?? null)
          : rows
      );
    } catch (e) {
      errors.push(e.message);
      res.status(400).json({ message: e.message, code: e.code });
    }
  });
  const dataApi = await listen(rest);
  process.env.SUPABASE_URL = dataApi.origin;
  process.env.SUPABASE_SERVICE_KEY = 'sb_secret_customer_isolated_fixture';
  const originalFetch = globalThis.fetch;
  globalThis.fetch = (input, init) => {
    const url = new URL(
      typeof input === 'string' ? input : input.url || String(input)
    );
    if (!['127.0.0.1', 'localhost'].includes(url.hostname))
      throw Error('External network blocked: ' + url.origin);
    return originalFetch(input, init);
  };
  const [
    { default: authRouter },
    { createCustomerRouter },
    { authenticateToken, blockCustomerBusinessRoutes },
  ] = await Promise.all([
    import('../../src/routes/auth.ts'),
    import('../../src/routes/customer.ts'),
    import('../../src/middleware/auth.ts'),
  ]);
  const app = express();
  app.use(cors({ origin: true, exposedHeaders: ['Content-Disposition'] }));
  app.use(express.json());
  app.use((_req, res, next) => {
    res.setHeader('Cache-Control', 'no-store');
    next();
  });
  app.use('/api/auth', authRouter);
  app.use('/api', authenticateToken);
  app.use(
    '/api/customer',
    createCustomerRouter(
      undefined,
      undefined,
      () => new Date('2026-11-17T11:00:00Z')
    )
  );
  app.use('/api', blockCustomerBusinessRoutes);
  if (actualAdminPages) {
    const {requireAdminRead} = await import('../../src/middleware/auth.ts');
    for (const [prefix,file,guard] of [['markets','marketAdminComments',false],['markets','markets',false],['gebietsleiter','gebietsleiter',false],['products','products',false],['wellen','wellen',false],['vorverkauf','vorverkauf',false],['nara-incentive','naraIncentive',false],['fragebogen','fragebogen',false],['activities','activities',true],['export','export',true],['action-history','actionHistory',true]]) {
      const {default:router}=await import('../../src/routes/'+file+'.ts');
      if(guard)app.use('/api/'+prefix,requireAdminRead,router);else app.use('/api/'+prefix,router);
    }
  }
  app.get('/api/wellen/dashboard/waves', (_req, res) => res.json([]));
  app.get('/api/markets', (_req, res) => res.json([]));
  app.get('/api/gebietsleiter', (_req, res) => res.json([]));
  app.get('/api/products', (_req, res) => res.json([]));
  app.get('/api/products/*', (_req, res) => res.json([]));
  app.get('/api/*', (_req, res) => res.json([]));
  app.all('/api/*', (_req, res) =>
    res.status(403).json({ error: 'Preview blocks unrelated mutations' })
  );
  const api = await listen(app, port);
  return {
    db,
    errors,
    calls,
    accounts,
    tokens,
    businessWrites,
    tokenFor,
    base: api.origin + '/api',
    dataOrigin: dataApi.origin,
    async close() {
      globalThis.fetch = originalFetch;
      await Promise.all([
        new Promise((r) => api.server.close(r)),
        new Promise((r) => dataApi.server.close(r)),
      ]);
      await db.close();
    },
  };
}
