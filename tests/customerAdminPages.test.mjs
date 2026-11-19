import assert from 'node:assert/strict';
import { before, after, test } from 'node:test';
import {
  createCustomerAccessHarness,
  customerId,
  adminId,
  glId,
  waveId,
  photoId,
  id,
} from './helpers/customerAccessHarness.mjs';
let h, customer, admin;
const pages = [
  'dashboard',
  'markets',
  'gebietsleiter',
  'vorbesteller',
  'vorverkauf',
  'produktersatz',
  'nara-incentive',
  'fragebogen',
  'zeiterfassung',
  'fotos',
  'produkte',
];
const rights = (exports = false) =>
  Object.fromEntries(
    pages.map((page) => [page, { view: true, export: exports }])
  );
const set = async (value) =>
  h.db.query('UPDATE customer_access SET permissions=$1 WHERE user_id=$2', [
    JSON.stringify(value),
    customerId,
  ]);
const req = (path, token = customer, method = 'GET', body) =>
  fetch(h.base + path, {
    method,
    headers: {
      ...(token ? { Authorization: 'Bearer ' + token } : {}),
      'Content-Type': 'application/json',
    },
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
  });
before(async () => {
  h = await createCustomerAccessHarness({ actualAdminPages: true });
  customer = h.tokenFor(customerId);
  admin = h.tokenFor(adminId);
});
after(async () => {
  await h?.close();
});
test('existing admin read routes return the same raw rows to an authorized customer without elevating the stored role', async () => {
  await set(rights());
  for (const path of [
    '/markets',
    '/gebietsleiter',
    '/products',
    '/wellen',
    '/wellen/dashboard/waves',
    '/wellen/dashboard/chain-averages',
    '/activities',
    '/vorverkauf',
    '/nara-incentive',
    '/wellen/photos/facets',
    '/wellen/photos',
  ]) {
    const a = await req(path, admin),
      c = await req(path);
    assert.equal(a.status, 200, path + ' admin');
    assert.equal(c.status, 200, path + ' customer');
    assert.deepEqual(await c.json(), await a.json(), path);
  }
  assert.equal(
    (await h.db.query('select role from users where id=$1', [customerId]))
      .rows[0].role,
    'customer'
  );
  assert.deepEqual(h.errors, []);
});
test('every page requires its matching server view right; unknown paths and all writes fail closed', async () => {
  const endpoints = {
    dashboard: '/activities',
    markets: '/markets',
    gebietsleiter: '/gebietsleiter/' + glId,
    vorbesteller: '/wellen',
    vorverkauf: '/vorverkauf',
    produktersatz: '/vorverkauf',
    'nara-incentive': '/nara-incentive',
    fragebogen: '/fragebogen/questions',
    zeiterfassung: '/fragebogen/zeiterfassung/admin',
    fotos: '/wellen/photos',
    produkte: '/products',
  };
  for (const [page, path] of Object.entries(endpoints)) {
    await set({});
    assert.equal((await req(path)).status, 403, path + ' denied');
    await set({ [page]: { view: true, export: false } });
    assert.equal((await req(path)).status, 200, path + ' allowed');
    assert.equal((await req(path, null)).status, 401, path + ' anonymous');
  }
  await set(rights(true));
  for (const [method, path] of [
    ['POST', '/wellen'],
    ['PUT', '/wellen/' + waveId],
    ['DELETE', '/wellen/' + waveId],
    ['POST', '/wellen/' + waveId + '/progress'],
    ['PUT', '/wellen/submissions/' + id(20)],
    ['DELETE', '/wellen/photos/' + photoId],
    ['POST', '/wellen/catalog/import'],
    ['POST', '/wellen/' + waveId + '/submission-prices'],
    ['POST', '/markets/backfill-gl-ids'],
    ['PUT', '/markets/DEMO-1'],
    ['DELETE', '/products/' + id(99)],
    ['PATCH', '/products/' + id(99) + '/archive'],
    ['POST', '/fragebogen/modules'],
    ['PUT', '/fragebogen/questions/' + id(60)],
    ['POST', '/fragebogen/day-tracking/start'],
    ['PATCH', '/fragebogen/zeiterfassung/' + id(61)],
    ['DELETE', '/nara-incentive/' + id(42)],
    ['POST', '/action-history'],
    ['POST', '/activities/vorbestellung/' + id(20)],
    ['POST', '/auth/register'],
    ['GET', '/chat'],
    ['GET', '/maps'],
    ['GET', '/wellen/catalog/reconciliation'],
    ['GET', '/new-unreviewed-route'],
  ])
    assert.equal(
      (await req(path, customer, method, method === 'GET' ? undefined : {}))
        .status,
      403,
      method + ' ' + path
    );
  assert.deepEqual(h.businessWrites, []);
});
test('read-only Fragebogen access computes current status and never invokes the legacy status updates', async () => {
  await set({ fragebogen: { view: true, export: false } });
  await h.db.query(
    "insert into fb_fragebogen(id,name,start_date,end_date,status,archived) values($1,'Abgelaufener Beispielsurvey','2020-01-01','2020-03-31','active',false)",
    [id(70)]
  );
  const list = await req('/fragebogen/fragebogen?archived=false');
  assert.equal(list.status, 200);
  assert.equal((await list.json())[0].status, 'inactive');
  const one = await req('/fragebogen/fragebogen/' + id(70));
  assert.equal(one.status, 200);
  assert.equal((await one.json()).status, 'inactive');
  assert.equal(
    (await h.db.query('select status from fb_fragebogen where id=$1', [id(70)]))
      .rows[0].status,
    'active'
  );
  assert.deepEqual(h.businessWrites, []);
});
test('existing export endpoints check export permission separately and custom exports validate every dataset', async () => {
  await set(rights(false));
  for (const path of [
    '/wellen/photos/export.zip',
    '/wellen/export/progress',
    '/wellen/export/submissions',
    '/fragebogen/photos/export.zip',
    '/fragebogen/fragebogen/' + id(70) + '/export.xlsx',
  ])
    assert.equal((await req(path)).status, 403, path);
  assert.equal(
    (
      await req('/export/custom', customer, 'POST', {
        datasets: ['wellen_submissions'],
        columns: { wellen_submissions: ['quantity'] },
      })
    ).status,
    403
  );
  await set({ dashboard: { view: true, export: true } });
  const x = await req('/export/custom', customer, 'POST', {
    datasets: ['wellen_submissions'],
    columns: { wellen_submissions: ['quantity', 'artikel_nr'] },
  });
  assert.equal(x.status, 200);
  assert.match(x.headers.get('content-type'), /spreadsheet/);
  assert.ok((await x.arrayBuffer()).byteLength > 1000);
  assert.equal(
    (
      await req('/export/custom', customer, 'POST', {
        datasets: ['wellen_submissions', 'markets'],
        columns: {},
      })
    ).status,
    403
  );
  assert.equal(
    (
      await req('/export/custom', customer, 'POST', {
        datasets: ['unreviewed_dataset'],
        columns: {},
      })
    ).status,
    403
  );
  const stats = await req('/export/dataset-stats');
  assert.equal(stats.status, 200);
  assert.deepEqual(Object.keys(await stats.json()).sort(), [
    'vorverkauf_entries',
    'wellen_submissions',
  ]);
  assert.deepEqual(h.businessWrites, []);
});

test('wave summaries identify unlimited revenue waves even with a percentage goal type', async () => {
  await set({ dashboard: { view: true, export: false } });
  await h.db.query(
    'UPDATE wellen SET goal_type=$1,no_limit_welle=true,goal_percentage=0 WHERE id=$2',
    ['percentage', waveId]
  );
  const response = await req('/wellen/dashboard/waves');
  assert.equal(response.status, 200);
  const wave = (await response.json()).find((wave) => wave.id === waveId);
  assert.equal(wave.goalType, 'percentage');
  assert.equal(wave.noLimitWelle, true);
  assert.deepEqual(h.businessWrites, []);
});
