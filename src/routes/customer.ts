import { Router, Response } from 'express';
import { randomBytes } from 'crypto';
import ExcelJS from 'exceljs';
import { createFreshClient, supabase } from '../config/supabase';
import { AuthRequest, requireAdmin } from '../middleware/auth';
import {
  CUSTOMER_PAGES,
  isCustomerPage,
  parseCustomerPermissions,
  type CustomerPage,
} from '../utils/customerAccess';

type Client = ReturnType<typeof createFreshClient>;
type Row = Record<string, any>;
type Column = {
  key: string;
  label: string;
  format?: 'money' | 'date' | 'number';
};
export type CustomerReport = {
  title: string;
  columns: Column[];
  rows: Row[];
  generatedAt: string;
};
const col = (
  key: string,
  label: string,
  format?: Column['format']
): Column => ({ key, label, ...(format ? { format } : {}) });
const UUID =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

// Paginate explicitly: Supabase's default row cap must never silently understate revenue.
async function allRows(
  client: Client,
  table: string,
  columns: string,
  configure = (q: any) => q
): Promise<Row[]> {
  const rows: Row[] = [];
  for (let offset = 0; offset < 100000; ) {
    const { data, error } = await configure(
      client
        .from(table)
        .select(columns)
        .order(table === 'customer_access' ? 'user_id' : 'id')
    ).range(offset, offset + 499);
    if (error) throw error;
    if (!data || data.length === 0) return rows;
    rows.push(...data);
    offset += data.length;
  }
  throw new Error('Report too large; refusing incomplete totals');
}
export const viennaToday = (now = new Date()) =>
  new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Europe/Vienna',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(now);
const waveColumns = [
  col('name', 'Welle'),
  col('start_date', 'Von', 'date'),
  col('end_date', 'Bis', 'date'),
  col('revenue', 'Bewerteter Umsatz', 'money'),
  col('unpriced', 'Positionen ohne Preis', 'number'),
  col('quantity', 'Stück', 'number'),
  col('orders', 'Positionen', 'number'),
  col('photos', 'Fotos', 'number'),
];
const definitions: Partial<
  Record<
    CustomerPage,
    { table: string; columns: Column[]; filter?: (q: any) => any }
  >
> = {
  markets: {
    table: 'markets',
    columns: [
      col('name', 'Markt'),
      col('chain', 'Handelskette'),
      col('address', 'Adresse'),
      col('postal_code', 'PLZ'),
      col('city', 'Ort'),
    ],
    filter: (q) => q.eq('is_active', true),
  },
  gebietsleiter: {
    table: 'gebietsleiter',
    columns: [col('name', 'Gebietsleiter'), col('email', 'E-Mail')],
    filter: (q) => q.eq('is_active', true).eq('is_test', false),
  },
  vorverkauf: {
    table: 'vorverkauf_wellen',
    columns: [
      col('name', 'Welle'),
      col('start_date', 'Von', 'date'),
      col('end_date', 'Bis', 'date'),
      col('status', 'Status'),
    ],
  },
  produktersatz: {
    table: 'vorverkauf_entries',
    columns: [
      col('market_id', 'Markt-ID'),
      col('reason', 'Grund'),
      col('status', 'Status'),
      col('created_at', 'Erfasst', 'date'),
    ],
  },
  'nara-incentive': {
    table: 'nara_incentive_submissions',
    columns: [
      col('market_id', 'Markt-ID'),
      col('gebietsleiter_id', 'Gebietsleiter-ID'),
      col('created_at', 'Erfasst', 'date'),
    ],
  },
  fragebogen: {
    table: 'fb_fragebogen',
    columns: [
      col('name', 'Fragebogen'),
      col('start_date', 'Von', 'date'),
      col('end_date', 'Bis', 'date'),
      col('status', 'Status'),
    ],
    filter: (q) => q.eq('is_deleted', false).eq('archived', false),
  },
  zeiterfassung: {
    table: 'fb_zeiterfassung_submissions',
    columns: [
      col('market_id', 'Markt-ID'),
      col('gebietsleiter_id', 'Gebietsleiter-ID'),
      col('besuchszeit_diff', 'Besuchszeit'),
      col('fahrzeit_diff', 'Fahrzeit'),
      col('distanz_km', 'Kilometer', 'number'),
      col('created_at', 'Erfasst', 'date'),
    ],
  },
  produkte: {
    table: 'products',
    columns: [
      col('artikel_nr', 'Artikelnummer'),
      col('name', 'Produkt'),
      col('department', 'Bereich'),
      col('product_type', 'Typ'),
      col('content', 'VE-Inhalt'),
      col('price', 'Preis', 'money'),
    ],
    filter: (q) => q.eq('is_deleted', false),
  },
};

async function waveData(client: Client, now: Date, includeSubmissions = true) {
  const today = viennaToday(now);
  const waves = await allRows(
    client,
    'wellen',
    'id,name,start_date,end_date,goal_type,goal_percentage,goal_value,foto_only,foto_enabled',
    (q) =>
      q.eq('is_deleted', false).lte('start_date', today).gte('end_date', today)
  );
  if (!waves.length)
    return { waves, submissions: [] as Row[], photos: [] as Row[] };
  const ids = waves.map((w) => w.id);
  const [submissions, photos, testGls] = await Promise.all([
    includeSubmissions
      ? allRows(
          client,
          'wellen_submissions',
          'id,welle_id,gebietsleiter_id,market_id,item_type,item_id,quantity,value_per_unit,artikel_nr,order_quantity,order_unit,order_unit_price,ve_size,created_at',
          (q) => q.in('welle_id', ids)
        )
      : Promise.resolve([]),
    allRows(
      client,
      'wellen_photos',
      'id,welle_id,gebietsleiter_id,market_id,tags,comment,created_at',
      (q) => q.in('welle_id', ids)
    ),
    allRows(client, 'gebietsleiter', 'id', (q) => q.eq('is_test', true)),
  ]);
  const excluded = new Set(testGls.map((g) => g.id));
  return {
    waves,
    submissions: submissions.filter((s) => !excluded.has(s.gebietsleiter_id)),
    photos: photos.filter((p) => !excluded.has(p.gebietsleiter_id)),
  };
}

export async function buildCustomerReport(
  client: Client,
  page: CustomerPage,
  now = new Date(),
  waveId?: string
): Promise<CustomerReport> {
  const title = CUSTOMER_PAGES.find((p) => p.key === page)!.label;
  if (definitions[page]) {
    const d = definitions[page]!;
    return {
      title,
      columns: d.columns,
      rows: await allRows(
        client,
        d.table,
        ['id', ...d.columns.map((c) => c.key)].join(','),
        d.filter
      ),
      generatedAt: now.toISOString(),
    };
  }
  const { waves, submissions, photos } = await waveData(
    client,
    now,
    page !== 'fotos'
  );
  if (page === 'fotos') {
    const names = new Map(
      waves
        .filter((w) => w.foto_enabled || w.foto_only)
        .map((w) => [w.id, w.name])
    );
    return {
      title,
      columns: [
        col('wave', 'Welle'),
        col('market_id', 'Markt-ID'),
        col('tags', 'Tags'),
        col('comment', 'Kommentar'),
        col('created_at', 'Aufgenommen', 'date'),
      ],
      rows: photos
        .filter((p) => names.has(p.welle_id))
        .map((p) => ({ ...p, wave: names.get(p.welle_id) })),
      generatedAt: now.toISOString(),
    };
  }
  if (waveId) {
    if (!waves.some((w) => w.id === waveId && !w.foto_only))
      throw Object.assign(new Error('Welle nicht freigegeben.'), {
        status: 404,
      });
    return {
      title: waves.find((w) => w.id === waveId)!.name,
      columns: [
        col('artikel_nr', 'Artikelnummer'),
        col('market_id', 'Markt-ID'),
        col('item_type', 'Typ'),
        col('order_quantity', 'Bestellmenge', 'number'),
        col('order_unit', 'Einheit'),
        col('quantity', 'Stück', 'number'),
        col('value_per_unit', 'Preis pro Stück', 'money'),
        col('total', 'Umsatz', 'money'),
        col('created_at', 'Erfasst', 'date'),
      ],
      rows: submissions
        .filter((s) => s.welle_id === waveId)
        .map((s) => ({
          ...s,
          total:
            s.value_per_unit == null
              ? null
              : Math.round(
                  Number(s.quantity) * Number(s.value_per_unit) * 100
                ) / 100,
        })),
      generatedAt: now.toISOString(),
    };
  }
  const rows = waves
    .filter((w) => page === 'dashboard' || !w.foto_only)
    .map((w) => {
      const entries = submissions.filter((s) => s.welle_id === w.id);
      // Calculate with frozen booking prices; unknown prices stay visibly incomplete.
      const revenueCents = entries.reduce(
        (sum, s) =>
          sum +
          (s.value_per_unit == null
            ? 0
            : Math.round(Number(s.quantity) * Number(s.value_per_unit) * 100)),
        0
      );
      return {
        ...w,
        revenue: revenueCents / 100,
        unpriced: entries.filter((s) => s.value_per_unit == null).length,
        quantity: entries.reduce((n, s) => n + Number(s.quantity), 0),
        orders: entries.length,
        photos: photos.filter((p) => p.welle_id === w.id).length,
      };
    });
  return { title, columns: waveColumns, rows, generatedAt: now.toISOString() };
}

async function downloadCustomerPhoto(client: Client, value: string) {
  let path = String(value || '');
  if (/^https?:\/\//i.test(path)) {
    const url = new URL(path);
    if (url.origin !== new URL(process.env.SUPABASE_URL!).origin)
      throw new Error('Foreign image');
    const match = url.pathname.match(
      /^\/storage\/v1\/object\/(?:public|sign|authenticated)\/wellen-photos\/(.+)$/
    );
    if (!match) throw new Error('Wrong bucket');
    path = decodeURIComponent(match[1]);
  }
  if (
    !path ||
    path.split('/').some((s) => !s || s === '.' || s === '..') ||
    path.includes('\\')
  )
    throw new Error('Invalid path');
  const downloaded = await client.storage.from('wellen-photos').download(path);
  if (downloaded.error || !downloaded.data) throw downloaded.error;
  if (
    !['image/jpeg', 'image/png', 'image/webp'].includes(downloaded.data.type) ||
    downloaded.data.size > 8 * 1024 * 1024
  )
    throw new Error('Invalid image');
  return {
    type: downloaded.data.type,
    bytes: Buffer.from(await downloaded.data.arrayBuffer()),
  };
}
function filteredReport(report: CustomerReport, query: unknown) {
  if (query === undefined || query === '') return report;
  if (typeof query !== 'string' || query.length > 200)
    throw Object.assign(new Error('Ungültiger Suchbegriff.'), { status: 400 });
  const needle = query.toLocaleLowerCase('de-AT');
  return {
    ...report,
    rows: report.rows.filter((row) =>
      Object.values(row).some((value) =>
        String(value ?? '')
          .toLocaleLowerCase('de-AT')
          .includes(needle)
      )
    ),
  };
}

const requirePage =
  (action: 'view' | 'export') =>
  (req: AuthRequest, res: Response, next: () => void) => {
    const page = req.params.page;
    if (!req.user)
      return res.status(401).json({ error: 'Anmeldung erforderlich.' });
    if (!isCustomerPage(page))
      return res.status(404).json({ error: 'Seite nicht gefunden.' });
    if (
      req.user.role !== 'customer' ||
      req.user.permissions?.[page]?.view !== true ||
      (action === 'export' && req.user.permissions[page]?.export !== true)
    )
      return res
        .status(403)
        .json({ error: 'Diese Funktion ist nicht freigegeben.' });
    next();
  };
const serializedAccount = (access: Row, profile: Row) => ({
  id: access.user_id,
  email: access.email,
  firstName: profile.first_name ?? access.firstName,
  lastName: profile.last_name ?? access.lastName,
  isActive: access.is_active,
  permissions: parseCustomerPermissions(access.permissions),
  version: access.version,
  createdAt: access.created_at,
});
function validateAccount(body: Row) {
  if (
    !body ||
    Object.keys(body).some(
      (k) =>
        ![
          'email',
          'firstName',
          'lastName',
          'isActive',
          'permissions',
          'version',
        ].includes(k)
    )
  )
    throw new Error('Ungültige Eingabe.');
  for (const key of ['firstName', 'lastName'])
    if (
      typeof body[key] !== 'string' ||
      !body[key].trim() ||
      body[key].length > 100
    )
      throw new Error('Bitte Vor- und Nachnamen eingeben.');
  if (
    typeof body.email !== 'string' ||
    body.email.length > 254 ||
    !/^\S+@\S+\.\S+$/.test(body.email)
  )
    throw new Error('Bitte eine gültige E-Mail eingeben.');
  if (typeof body.isActive !== 'boolean')
    throw new Error('Ungültiger Kontostatus.');
  return parseCustomerPermissions(body.permissions);
}

export function createCustomerRouter(
  getClient: () => Client = createFreshClient,
  auth = supabase.auth,
  clock = () => new Date()
) {
  const router = Router();
  router.use((_req, res, next) => {
    res.setHeader('Cache-Control', 'no-store');
    next();
  });
  router.get('/accounts', requireAdmin, async (_req, res) => {
    try {
      const client = getClient();
      const access = await allRows(
        client,
        'customer_access',
        'user_id,email,is_active,permissions,version,created_at',
        (q) => q.order('user_id')
      );
      if (!access.length)
        return res.json({ accounts: [], pages: CUSTOMER_PAGES });
      const { data: profiles, error } = await client
        .from('users')
        .select('id,first_name,last_name')
        .in(
          'id',
          access.map((a) => a.user_id)
        );
      if (error) throw error;
      res.json({
        accounts: access.map((a) =>
          serializedAccount(
            a,
            (profiles || []).find((p) => p.id === a.user_id) || {}
          )
        ),
        pages: CUSTOMER_PAGES,
      });
    } catch {
      res
        .status(503)
        .json({ error: 'Kundenzugänge konnten nicht geladen werden.' });
    }
  });
  async function save(req: AuthRequest, res: Response, create: boolean) {
    let permissions;
    try {
      permissions = validateAccount(req.body);
    } catch (e) {
      return res.status(400).json({ error: (e as Error).message });
    }
    if (
      !create &&
      (!UUID.test(req.params.id) ||
        !Number.isInteger(req.body.version) ||
        req.body.version < 1)
    )
      return res.status(400).json({ error: 'Ungültiger Zugang.' });
    let createdId: string | undefined;
    let password: string | undefined;
    try {
      const email = req.body.email.trim().toLowerCase();
      let id = req.params.id;
      if (create) {
        password = randomBytes(24).toString('base64url');
        const { data, error } = await auth.admin.createUser({
          email,
          password,
          email_confirm: true,
        });
        if (error || !data.user)
          return res.status(409).json({
            error:
              'Zugang konnte nicht angelegt werden. E-Mail möglicherweise bereits vergeben.',
          });
        id = data.user.id;
        createdId = id;
      }
      const { data, error } = await getClient().rpc('save_customer_access', {
        p_actor_id: req.user!.id,
        p_user_id: id,
        p_email: email,
        p_first_name: req.body.firstName.trim(),
        p_last_name: req.body.lastName.trim(),
        p_is_active: req.body.isActive,
        p_permissions: permissions,
        p_version: create ? null : req.body.version,
      });
      if (error) throw error;
      res.status(create ? 201 : 200).json({
        account: serializedAccount(data, {}),
        ...(password ? { oneTimePassword: password } : {}),
      });
    } catch (e) {
      if (createdId) {
        const { error } = await auth.admin.deleteUser(createdId);
        if (error)
          return res.status(503).json({
            error:
              'Anlage nicht bestätigt; Bereinigung fehlgeschlagen. Bitte Administration kontaktieren.',
          });
      }
      const conflict = String((e as any)?.message).includes(
        'CUSTOMER_CONFLICT'
      );
      const invalid = String((e as any)?.message).includes(
        'CUSTOMER_INVALID_INPUT'
      );
      res.status(conflict ? 409 : invalid ? 400 : 503).json({
        error: conflict
          ? 'Zugang wurde inzwischen geändert. Bitte neu laden.'
          : 'Zugang konnte nicht gespeichert werden.',
      });
    }
  }
  router.post('/accounts', requireAdmin, (req, res) => save(req, res, true));
  router.put('/accounts/:id', requireAdmin, (req, res) =>
    save(req, res, false)
  );
  router.get('/me', (req: AuthRequest, res) => {
    if (req.user?.role !== 'customer')
      return res.status(403).json({ error: 'Kundenzugang erforderlich.' });
    res.json({
      permissions: req.user.permissions || {},
      pages: CUSTOMER_PAGES,
    });
  });
  router.get('/pages/:page', requirePage('view'), async (req, res) => {
    try {
      const report = await buildCustomerReport(
        getClient(),
        req.params.page as CustomerPage,
        clock()
      );
      res.json(report);
    } catch {
      res
        .status(503)
        .json({ error: 'Daten konnten nicht vollständig geladen werden.' });
    }
  });
  router.get(
    '/pages/:page/waves/:id',
    requirePage('view'),
    async (req, res) => {
      if (req.params.page !== 'vorbesteller' || !UUID.test(req.params.id))
        return res.status(404).json({ error: 'Nicht gefunden.' });
      try {
        res.json(
          await buildCustomerReport(
            getClient(),
            'vorbesteller',
            clock(),
            req.params.id
          )
        );
      } catch (e) {
        res
          .status((e as any).status === 404 ? 404 : 503)
          .json({ error: 'Welle konnte nicht geladen werden.' });
      }
    }
  );
  router.get('/pages/:page/export', requirePage('export'), async (req, res) => {
    try {
      if (
        req.query.waveId &&
        (req.params.page !== 'vorbesteller' ||
          typeof req.query.waveId !== 'string' ||
          !UUID.test(req.query.waveId))
      )
        return res.status(400).json({ error: 'Ungültige Welle.' });
      const report = filteredReport(
        await buildCustomerReport(
          getClient(),
          req.params.page as CustomerPage,
          clock(),
          req.query.waveId as string | undefined
        ),
        req.query.q
      );
      const book = new ExcelJS.Workbook();
      const sheet = book.addWorksheet('Daten');
      sheet.columns = report.columns.map((c) => ({
        header: c.label,
        key: c.key,
        width: c.format === 'date' ? 22 : 28,
      }));
      for (const row of report.rows)
        sheet.addRow(
          Object.fromEntries(
            report.columns.map((c) => [
              c.key,
              Array.isArray(row[c.key])
                ? row[c.key].join(', ')
                : (row[c.key] ?? null),
            ])
          )
        );
      sheet.getRow(1).font = { bold: true };
      sheet.views = [{ state: 'frozen', ySplit: 1 }];
      for (const c of report.columns)
        if (c.format === 'money')
          sheet.getColumn(c.key).numFmt = '#,##0.00 "€"';
      res.setHeader(
        'Content-Type',
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
      );
      res.setHeader(
        'Content-Disposition',
        `attachment; filename="rover-${req.params.page}.xlsx"`
      );
      await book.xlsx.write(res);
      res.end();
    } catch (e) {
      if (!res.headersSent)
        res
          .status(
            [400, 404].includes((e as any).status) ? (e as any).status : 503
          )
          .json({ error: 'Export konnte nicht vollständig erstellt werden.' });
    }
  });
  router.get(
    '/pages/:page/photos.zip',
    requirePage('export'),
    async (req, res) => {
      if (req.params.page !== 'fotos') return res.status(404).end();
      try {
        const client = getClient();
        const report = filteredReport(
          await buildCustomerReport(client, 'fotos', clock()),
          req.query.q
        );
        if (report.rows.length > 100)
          return res.status(422).json({
            error:
              'Bitte Auswahl einschränken: maximal 100 Fotos pro Download.',
          });
        if (!report.rows.length)
          return res
            .status(404)
            .json({ error: 'Keine Fotos für diese Auswahl.' });
        const module = await (
          new Function('s', 'return import(s)') as (s: string) => Promise<any>
        )('archiver');
        const zip = new module.ZipArchive({ zlib: { level: 0 } });
        const chunks: Buffer[] = [];
        let total = 0;
        const done = new Promise<Buffer>((resolve, reject) => {
          zip.on('data', (chunk: Buffer) => chunks.push(chunk));
          zip.once('error', reject);
          zip.once('end', () => resolve(Buffer.concat(chunks)));
        });
        void done.catch(() => {});
        // Bound memory and finish the entire ZIP before sending any response bytes.
        try {
          for (const row of report.rows) {
            const { data, error } = await client
              .from('wellen_photos')
              .select('photo_url')
              .eq('id', row.id)
              .single();
            if (error || !data) throw error;
            const image = await downloadCustomerPhoto(client, data.photo_url);
            total += image.bytes.length;
            if (total > 50 * 1024 * 1024)
              throw Object.assign(
                new Error(
                  'Bitte Auswahl einschränken: maximal 50 MB pro Download.'
                ),
                { status: 422 }
              );
            zip.append(image.bytes, {
              name: `${row.id}.${image.type === 'image/jpeg' ? 'jpg' : image.type.slice(6)}`,
            });
          }
          zip.append(JSON.stringify(report, null, 2), {
            name: 'Fotoliste.json',
          });
          await zip.finalize();
          const bytes = await done;
          res.setHeader('Content-Type', 'application/zip');
          res.setHeader(
            'Content-Disposition',
            'attachment; filename="rover-fotos.zip"'
          );
          res.send(bytes);
        } catch (e) {
          zip.abort();
          void done.catch(() => {});
          throw e;
        }
      } catch (e) {
        res
          .status(
            [400, 422].includes((e as any).status) ? (e as any).status : 503
          )
          .json({
            error:
              (e as any).status === 422
                ? (e as Error).message
                : 'Fotos konnten nicht vollständig exportiert werden.',
          });
      }
    }
  );
  router.get('/photos/:id', async (req: AuthRequest, res) => {
    if (
      req.user?.role !== 'customer' ||
      req.user.permissions?.fotos?.view !== true
    )
      return res.status(403).json({ error: 'Fotowellen nicht freigegeben.' });
    if (!UUID.test(req.params.id)) return res.status(404).end();
    try {
      const client = getClient();
      const { data, error } = await client
        .from('wellen_photos')
        .select('photo_url,welle_id,gebietsleiter_id')
        .eq('id', req.params.id)
        .maybeSingle();
      if (error || !data) return res.status(404).end();
      const today = viennaToday(clock());
      const [wave, gl] = await Promise.all([
        client
          .from('wellen')
          .select('foto_enabled,foto_only')
          .eq('id', data.welle_id)
          .eq('is_deleted', false)
          .lte('start_date', today)
          .gte('end_date', today)
          .maybeSingle(),
        client
          .from('gebietsleiter')
          .select('is_test')
          .eq('id', data.gebietsleiter_id)
          .maybeSingle(),
      ]);
      if (
        wave.error ||
        gl.error ||
        !wave.data ||
        (!wave.data.foto_enabled && !wave.data.foto_only) ||
        gl.data?.is_test === true
      )
        return res.status(404).end();
      const image = await downloadCustomerPhoto(client, data.photo_url);
      res.setHeader('Content-Type', image.type);
      res.setHeader('X-Content-Type-Options', 'nosniff');
      res.send(image.bytes);
    } catch {
      res.status(404).end();
    }
  });
  router.use((_req, res) =>
    res.status(403).json({ error: 'Diese Funktion ist nicht freigegeben.' })
  );
  return router;
}
export default createCustomerRouter();
