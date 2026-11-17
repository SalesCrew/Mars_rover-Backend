import assert from 'node:assert/strict';
import { before, after, test } from 'node:test';
import ExcelJS from 'exceljs';
import {
  createCustomerAccessHarness,
  id,
  adminId,
  customerId,
  glId,
  waveId,
  photoId,
  defaultPermissions,
  password,
} from './helpers/customerAccessHarness.mjs';
let h, admin, customer;
const input = {
  email: 'bettina@example.test',
  firstName: 'Bettina',
  lastName: 'Kundin',
  isActive: true,
  permissions: defaultPermissions,
};
const req = (path, token = customer, method = 'GET', body) =>
  fetch(h.base + path, {
    method,
    headers: {
      ...(token ? { Authorization: 'Bearer ' + token } : {}),
      'Content-Type': 'application/json',
    },
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
  });
const version = async () =>
  Number(
    (
      await h.db.query('select version from customer_access where user_id=$1', [
        customerId,
      ])
    ).rows[0].version
  );
const update = async (extra = {}) =>
  req('/customer/accounts/' + customerId, admin, 'PUT', {
    ...input,
    version: await version(),
    ...extra,
  });
before(async () => {
  h = await createCustomerAccessHarness({ rowCap: 100 });
  admin = h.tokenFor(adminId);
  customer = h.tokenFor(customerId);
});
after(async () => {
  await h?.close();
});

test('unauthenticated and forged tokens cannot query any report, photo, export or account', async () => {
  for (const token of [null, 'forged-token'])
    for (const path of [
      '/customer/me',
      '/customer/accounts',
      '/customer/pages/dashboard',
      '/customer/pages/vorbesteller/export',
      '/customer/photos/' + photoId,
      '/wellen',
    ]) {
      const before = h.calls.length;
      const r = await req(path, token);
      assert.ok([401, 403].includes(r.status));
      assert.equal(h.calls.length, before);
    }
});
test('login and refresh load the customer role and permissions from the database', async () => {
  const r = await req('/auth/login', null, 'POST', {
    username: input.email,
    password,
  });
  assert.equal(r.status, 200);
  const data = await r.json();
  assert.equal(data.user.role, 'customer');
  assert.deepEqual(data.user.permissions, defaultPermissions);
  const refreshed = await req('/auth/refresh', null, 'POST', {
    refreshToken: data.refreshToken,
  });
  assert.equal(refreshed.status, 200);
  assert.equal((await refreshed.json()).user.role, 'customer');
  const me = await req('/auth/me');
  assert.equal((await me.json()).user.role, 'customer');
});
test('default-deny blocks legacy admin/GL routes and every customer mutation', async () => {
  for (const path of [
    '/markets',
    '/products',
    '/wellen',
    '/wellen/' + waveId,
    '/export/custom',
    '/gebietsleiter',
    '/customer/accounts',
  ])
    for (const method of ['GET', 'POST', 'PUT', 'DELETE'])
      assert.equal(
        (await req(path, customer, method, method === 'GET' ? undefined : {}))
          .status,
        403,
        path + ' ' + method
      );
  for (const method of ['POST', 'PUT', 'DELETE'])
    assert.equal(
      (await req('/customer/pages/dashboard', customer, method, {})).status,
      403
    );
  assert.equal(
    (await req('/auth/register', customer, 'POST', { role: 'admin' })).status,
    403
  );
  assert.equal((await req('/customer/accounts', h.tokenFor(glId))).status, 403);
});
test('active wave dashboard uses frozen prices, excludes test GLs, exposes missing prices and no private fields', async () => {
  const r = await req('/customer/pages/dashboard');
  assert.equal(r.status, 200);
  assert.equal(r.headers.get('cache-control'), 'no-store');
  const report = await r.json();
  assert.equal(report.rows.length, 2);
  assert.deepEqual(report.rows.map((w) => w.name).sort(), [
    'Fotowelle · Marktauftritt',
    'Zusatz-Umsatz · Herbst 2026',
  ]);
  const wave = report.rows.find((w) => w.id === waveId);
  assert.equal(wave.revenue, 230);
  assert.equal(wave.quantity, 20);
  assert.equal(wave.unpriced, 1);
  assert.equal(
    report.rows.reduce((n, w) => n + w.photos, 0),
    1
  );
  assert.ok(!JSON.stringify(report).includes('photo_url'));
  assert.ok(!JSON.stringify(report).includes('must-never'));
});
test('permissions restrict data, drilldowns and export separately, even with caller-supplied roles', async () => {
  assert.equal((await req('/customer/pages/markets')).status, 403);
  assert.equal((await req('/customer/pages/dashboard/export')).status, 403);
  assert.equal((await req('/customer/pages/fotos/export')).status, 403);
  assert.equal((await req('/customer/pages/constructor')).status, 404);
  assert.equal(
    (await req('/customer/pages/dashboard/waves/' + waveId)).status,
    404
  );
  assert.equal(
    (await req('/customer/pages/vorbesteller/waves/' + id(12))).status,
    404
  );
  assert.equal(
    (await req('/customer/pages/vorbesteller/waves/' + waveId)).status,
    200
  );
  const r = await fetch(h.base + '/customer/pages/markets', {
    headers: {
      Authorization: 'Bearer ' + customer,
      'X-Role': 'admin',
      'test-role': 'admin',
    },
  });
  assert.equal(r.status, 403);
});
test('real Excel output uses the same scope and preserves unknown prices as blank', async () => {
  const r = await req('/customer/pages/vorbesteller/export?waveId=' + waveId);
  assert.equal(r.status, 200);
  const workbook = new ExcelJS.Workbook();
  await workbook.xlsx.load(Buffer.from(await r.arrayBuffer()));
  const sheet = workbook.getWorksheet('Daten');
  assert.equal(sheet.rowCount, 4);
  assert.equal(sheet.getRow(2).getCell(8).value, 30);
  assert.equal(sheet.getRow(3).getCell(8).value, 200);
  assert.equal(sheet.getRow(4).getCell(8).value, null);
  assert.equal(
    (await req('/customer/pages/fotos/export?waveId=' + waveId)).status,
    403
  );
});
test('protected photos require current photo rights and never return a storage URL', async () => {
  const r = await req('/customer/pages/fotos');
  const report = await r.json();
  assert.equal(report.rows.length, 1);
  assert.ok(!JSON.stringify(report).includes('photo_url'));
  const image = await req('/customer/photos/' + photoId);
  assert.equal(image.status, 200);
  assert.equal(image.headers.get('content-type'), 'image/png');
  assert.ok((await image.arrayBuffer()).byteLength > 0);
  assert.equal((await req('/customer/photos/' + id(400))).status, 404);
  await h.db.query(
    "update wellen_photos set photo_url='https://untrusted.example.test/storage/v1/object/public/wellen-photos/fake.png' where id=$1",
    [photoId]
  );
  assert.equal((await req('/customer/photos/' + photoId)).status, 404);
  await h.db.query(
    "update wellen_photos set photo_url='demo/market.png' where id=$1",
    [photoId]
  );
});
test('revocations and deactivation take effect with an already issued token', async () => {
  assert.equal(
    (
      await update({
        permissions: { dashboard: { view: true, export: false } },
      })
    ).status,
    200
  );
  assert.equal((await req('/customer/pages/vorbesteller')).status, 403);
  assert.equal((await req('/customer/photos/' + photoId)).status, 403);
  assert.equal((await update({ isActive: false })).status, 200);
  assert.equal((await req('/customer/pages/dashboard')).status, 403);
  assert.equal(
    (
      await req('/auth/login', null, 'POST', {
        username: input.email,
        password,
      })
    ).status,
    403
  );
  assert.equal(
    (
      await req('/auth/refresh', null, 'POST', {
        refreshToken: 'refresh-' + customerId,
      })
    ).status,
    403
  );
  assert.equal((await update()).status, 200);
  assert.equal((await req('/customer/pages/dashboard')).status, 200);
});
test('management validates permissions, immutable identity and optimistic version; audit has no passwords', async () => {
  const v = await version();
  const r = await update();
  assert.equal(r.status, 200);
  assert.equal(
    (
      await req('/customer/accounts/' + customerId, admin, 'PUT', {
        ...input,
        version: v,
      })
    ).status,
    409
  );
  for (const permissions of [
    { admin: { view: true, export: true } },
    { dashboard: { view: false, export: true } },
    { dashboard: { view: 'true', export: false } },
    { dashboard: { view: true, export: false, write: true } },
  ])
    assert.equal((await update({ permissions })).status, 400);
  assert.equal((await update({ role: 'admin' })).status, 400);
  assert.equal((await update({ email: 'different@example.test' })).status, 400);
  assert.equal(
    (
      await req('/customer/accounts/' + adminId, admin, 'PUT', {
        ...input,
        version: 1,
      })
    ).status,
    409
  );
  const audits = (await h.db.query('select * from customer_access_audit')).rows;
  assert.ok(audits.length >= 4);
  assert.ok(!JSON.stringify(audits).includes(password));
});
test('admin creates a customer with a one-time password, customer can sign in, duplicate creates do not leave profiles', async () => {
  const body = {
    ...input,
    email: 'new-customer@example.test',
    permissions: {},
  };
  const r = await req('/customer/accounts', admin, 'POST', body);
  assert.equal(r.status, 201);
  const data = await r.json();
  assert.ok(data.oneTimePassword.length >= 24);
  assert.equal(data.account.email, body.email);
  const login = await req('/auth/login', null, 'POST', {
    username: body.email,
    password: data.oneTimePassword,
  });
  assert.equal(login.status, 200);
  const user = await login.json();
  assert.equal(user.user.role, 'customer');
  assert.equal(
    (await req('/customer/pages/dashboard', user.accessToken)).status,
    403
  );
  assert.equal(
    (await req('/customer/accounts', admin, 'POST', body)).status,
    409
  );
  const listed = await (await req('/customer/accounts', admin)).json();
  assert.equal(listed.accounts.length, 2);
  assert.ok(!JSON.stringify(listed).includes(data.oneTimePassword));
});
test('failed profile/access transactions are rolled back and the new Auth account is removed', async () => {
  const count = h.accounts.size;
  await h.db.exec(
    "create function fail_customer_audit() returns trigger language plpgsql as $$ begin raise exception 'synthetic audit failure'; end $$;create trigger fail_customer_audit before insert on customer_access_audit for each row execute function fail_customer_audit()"
  );
  try {
    const r = await req('/customer/accounts', admin, 'POST', {
      ...input,
      email: 'rollback@example.test',
    });
    assert.equal(r.status, 503);
    assert.equal(h.accounts.size, count);
    assert.equal(
      (
        await h.db.query(
          "select count(*)::int n from customer_access where email='rollback@example.test'"
        )
      ).rows[0].n,
      0
    );
    assert.equal(
      (
        await h.db.query(
          'select count(*)::int n from auth.users a left join users u on u.id=a.id where u.id is null'
        )
      ).rows[0].n,
      0
    );
  } finally {
    await h.db.exec(
      'drop trigger fail_customer_audit on customer_access_audit;drop function fail_customer_audit()'
    );
  }
});
test('user-editable Auth metadata cannot grant the admin role or page rights', async () => {
  h.accounts.get(customerId).user_metadata = {
    role: 'admin',
    permissions: { markets: { view: true, export: true } },
  };
  const me = await (await req('/auth/me')).json();
  assert.equal(me.user.role, 'customer');
  assert.equal((await req('/customer/pages/markets')).status, 403);
});
test('every selectable page is a working whitelisted report; exports cannot include secret fields', async () => {
  const pages = [
    'dashboard',
    'vorbesteller',
    'fotos',
    'markets',
    'gebietsleiter',
    'vorverkauf',
    'produktersatz',
    'nara-incentive',
    'fragebogen',
    'zeiterfassung',
    'produkte',
  ];
  const permissions = Object.fromEntries(
    pages.map((p) => [p, { view: true, export: true }])
  );
  assert.equal((await update({ permissions })).status, 200);
  for (const page of pages) {
    const r = await req('/customer/pages/' + page);
    assert.equal(r.status, 200, page);
    const report = await r.json();
    assert.ok(report.columns.length);
    assert.ok(!JSON.stringify(report).includes('must-never-be-returned'));
    assert.equal(
      (await req('/customer/pages/' + page + '/export')).status,
      200,
      page
    );
  }
  assert.equal((await update()).status, 200);
});
test('photo ZIP export is separately authorized and contains only permitted active photos', async () => {
  assert.equal((await req('/customer/pages/fotos/photos.zip')).status, 403);
  assert.equal(
    (
      await update({
        permissions: {
          ...defaultPermissions,
          fotos: { view: true, export: true },
        },
      })
    ).status,
    200
  );
  const response = await req('/customer/pages/fotos/photos.zip');
  assert.equal(response.status, 200);
  assert.equal(response.headers.get('content-type'), 'application/zip');
  const bytes = Buffer.from(await response.arrayBuffer());
  assert.equal(bytes.subarray(0, 2).toString(), 'PK');
  assert.ok(bytes.includes(Buffer.from('Fotoliste.json')));
  assert.ok(bytes.includes(Buffer.from(photoId + '.png')));
  const filtered = await req(
    '/customer/pages/vorbesteller/export?q=NO-SUCH-WAVE'
  );
  const book = new ExcelJS.Workbook();
  await book.xlsx.load(Buffer.from(await filtered.arrayBuffer()));
  assert.equal(book.getWorksheet('Daten').rowCount, 1);
  assert.equal((await update()).status, 200);
});
test('full pagination handles both a 100-row server cap and more than 1000 bookings without changing revenue', async () => {
  await h.db.query(
    'insert into wellen_submissions(id,welle_id,gebietsleiter_id,quantity,value_per_unit) select gen_random_uuid(),$1,$2,1,0.1 from generate_series(1,1051)',
    [waveId, glId]
  );
  const report = await (await req('/customer/pages/dashboard')).json();
  assert.equal(report.rows.find((w) => w.id === waveId).revenue, 335.1);
  await h.db.query('delete from wellen_submissions where market_id is null');
});
test('database RLS denies direct customer access to every public and storage table, including existing permissive policies', async () => {
  const tables = (
    await h.db.query(
      "select schemaname,tablename from pg_tables where schemaname in ('public','storage')"
    )
  ).rows;
  for (const { schemaname, tablename } of tables) {
    await h.db.exec('begin;set local role authenticated;');
    await h.db.query("select set_config('request.jwt.claim.sub',$1,true)", [
      customerId,
    ]);
    try {
      const r = await h.db.query(
        `select * from "${schemaname}"."${tablename}"`
      );
      assert.equal(r.rows.length, 0, tablename);
    } catch (e) {
      assert.match(e.message, /permission denied/, tablename);
    } finally {
      await h.db.exec('rollback');
    }
  }
  for (const uid of [adminId, glId]) {
    await h.db.exec('begin;set local role authenticated;');
    await h.db.query("select set_config('request.jwt.claim.sub',$1,true)", [
      uid,
    ]);
    assert.ok((await h.db.query('select * from wellen')).rows.length > 0);
    await h.db.exec('rollback');
  }
  await h.db.exec('begin;set local role authenticated;');
  await h.db.query("select set_config('request.jwt.claim.sub',$1,true)", [
    id(499),
  ]);
  assert.equal((await h.db.query('select * from wellen')).rows.length, 0);
  await h.db.exec('rollback');
});
test('direct customer writes and management RPC execution fail at the database boundary', async () => {
  for (const sql of [
    "insert into markets(id,name) values('attack','attack')",
    "select save_customer_access(null,null,null,null,null,true,'{}',null)",
  ]) {
    await h.db.exec('begin;set local role authenticated;');
    await h.db.query("select set_config('request.jwt.claim.sub',$1,true)", [
      customerId,
    ]);
    try {
      await assert.rejects(
        h.db.query(sql),
        /row-level security|permission denied/
      );
    } finally {
      await h.db.exec('rollback');
    }
  }
  await h.db.exec('begin;set local role anon;');
  try {
    await assert.rejects(
      h.db.query('select * from wellen'),
      /permission denied/
    );
  } finally {
    await h.db.exec('rollback');
  }
  assert.deepEqual(h.errors, []);
});
