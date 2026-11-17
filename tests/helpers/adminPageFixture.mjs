// Synthetic schema/records for exercising the real admin routers, never a prod copy.
export async function seedAdminPages(db, id) {
  const add = async (table, columns) => {
    await db.exec(
      `CREATE TABLE IF NOT EXISTS ${table}(id uuid PRIMARY KEY DEFAULT gen_random_uuid());`
    );
    for (const col of columns.split(';').filter(Boolean))
      await db.exec(`ALTER TABLE ${table} ADD COLUMN IF NOT EXISTS ${col};`);
  };
  await add(
    'wellen',
    "description text;status text DEFAULT 'active';types text[] DEFAULT ARRAY['einzelprodukt'];image_url text;foto_header text;foto_description text;no_limit_welle bool DEFAULT false;catalog_pricing_required bool DEFAULT false;created_at timestamptz DEFAULT now();updated_at timestamptz DEFAULT now()"
  );
  for (const [table, order] of [
    ['wellen_displays', 'display_order'],
    ['wellen_kartonware', 'kartonware_order'],
    ['wellen_einzelprodukte', 'einzelprodukt_order'],
  ])
    await add(
      table,
      `welle_id uuid;product_id uuid;name text;target_number int DEFAULT 10;item_value numeric;${order} int DEFAULT 0;description text;image_url text;picture_url text;ve int;ve_size int;vpe int;catalog_key text;catalog_ve_size int;catalog_order_price numeric;artikel_nr text`
    );
  for (const [table, parent, order] of [
    ['wellen_paletten', 'palette_id', 'palette_order'],
    ['wellen_schuetten', 'schuette_id', 'schuette_order'],
  ]) {
    await add(
      table,
      `welle_id uuid;name text;description text;image_url text;picture_url text;size text;${order} int DEFAULT 0`
    );
    await add(
      table + '_products',
      `${parent} uuid;name text;value_per_ve numeric;ve int;ean text;product_order int DEFAULT 0`
    );
  }
  await add('wellen_markets', 'welle_id uuid;market_id text');
  await add(
    'wellen_kw_days',
    'welle_id uuid;kw text;days text[];kw_order int DEFAULT 0'
  );
  await add(
    'wellen_photo_tags',
    'welle_id uuid;tag_name text;tag_type text;tag_order int;created_at timestamptz DEFAULT now()'
  );
  await add(
    'wellen_progress',
    'welle_id uuid;gebietsleiter_id uuid;market_id text;item_type text;item_id uuid;current_number int;created_at timestamptz DEFAULT now()'
  );
  await add(
    'wellen_submissions',
    'photo_url text;delivery_photo_url text;price_year int;price_quarter int;price_source_id uuid;parent_palette_id uuid;price_status text;order_unit_price numeric;notes text'
  );
  await add(
    'gebietsleiter',
    'address text;postal_code text;city text;phone text;profile_picture_url text;created_at timestamptz DEFAULT now();updated_at timestamptz DEFAULT now()'
  );
  await add(
    'markets',
    'internal_id text;phone text;email text;gebietsleiter_id uuid;gebietsleiter_name text;gebietsleiter_email text;channel text;banner text;branch text;maingroup text;subgroup text;frequency int DEFAULT 12;current_visits int DEFAULT 1;visit_day text;visit_duration int;last_visit_date date;customer_type text;is_completed bool DEFAULT false;latitude numeric;longitude numeric;market_tel text;market_email text;mars_fil text;created_at timestamptz DEFAULT now()'
  );
  await add(
    'market_admin_comments',
    'market_id text;comment text;updated_by uuid;updated_at timestamptz DEFAULT now()'
  );
  await add(
    'action_history',
    'timestamp timestamptz DEFAULT now();action_type text;market_id text;market_chain text;market_address text;market_city text;market_postal_code text;target_gl text;previous_gl text;performed_by text;notes text'
  );
  await add(
    'products',
    "weight text;pallet_size int;sku text;palette_products jsonb DEFAULT '[]';is_active bool DEFAULT true"
  );
  await add('vorverkauf_entries', 'gebietsleiter_id uuid;notes text');
  await add(
    'vorverkauf_items',
    'vorverkauf_entry_id uuid;product_id uuid;quantity int;item_type text'
  );
  await add(
    'vorverkauf_submissions',
    'gebietsleiter_id uuid;market_id text;wave_id uuid;notes text;created_at timestamptz DEFAULT now()'
  );
  await add(
    'vorverkauf_submission_products',
    'submission_id uuid;product_id uuid;quantity int;reason text'
  );
  await add(
    'nara_incentive_items',
    'submission_id uuid;product_id uuid;quantity int'
  );
  await add(
    'fb_questions',
    'type text;question_text text;instruction text;is_template bool DEFAULT false;options jsonb;likert_scale jsonb;matrix_config jsonb;numeric_constraints jsonb;slider_config jsonb;images jsonb;distributionsziel bool;qualitaetsziel bool;archived bool DEFAULT false;is_deleted bool DEFAULT false;deleted_at timestamptz;created_by uuid;created_at timestamptz DEFAULT now();updated_at timestamptz DEFAULT now();module_usage_count int DEFAULT 0;answer_count int DEFAULT 0'
  );
  await add(
    'fb_modules',
    'name text;description text;archived bool DEFAULT false;is_deleted bool DEFAULT false;deleted_at timestamptz;created_by uuid;created_at timestamptz DEFAULT now();updated_at timestamptz DEFAULT now();question_count int DEFAULT 0;rules_count int DEFAULT 0;fragebogen_usage_count int DEFAULT 0'
  );
  await add(
    'fb_module_questions',
    'module_id uuid;question_id uuid;local_id text;display_order int;order_index int;required bool'
  );
  await add(
    'fb_module_rules',
    'module_id uuid;trigger_local_id text;trigger_answer jsonb;operator text;trigger_answer_max numeric;action text;target_local_ids text[];created_at timestamptz;updated_at timestamptz'
  );
  await add(
    'fb_fragebogen',
    'description text;deleted_at timestamptz;created_by uuid;created_at timestamptz DEFAULT now();updated_at timestamptz DEFAULT now();module_count int DEFAULT 0;market_count int DEFAULT 0;response_count int DEFAULT 0;completed_response_count int DEFAULT 0'
  );
  await add(
    'fb_fragebogen_modules',
    'fragebogen_id uuid;module_id uuid;display_order int;order_index int'
  );
  await add('fb_fragebogen_markets', 'fragebogen_id uuid;market_id text');
  await add(
    'fb_responses',
    'fragebogen_id uuid;gebietsleiter_id uuid;market_id text;zeiterfassung_submission_id uuid;status text;started_at timestamptz;completed_at timestamptz'
  );
  await add(
    'fb_response_answers',
    'response_id uuid;question_id uuid;question_type text;answer_file_url text;answer_json jsonb;answered_at timestamptz'
  );
  await add(
    'fb_answers',
    'response_id uuid;question_id uuid;answer_text text;answer_value jsonb;photo_url text'
  );
  await add(
    'fb_zeiterfassung_submissions',
    'market_start_time text;market_end_time text;besuchszeit_von text;besuchszeit_bis text;fahrzeit_von text;fahrzeit_bis text;calculated_fahrzeit text;visit_order int;kommentar text'
  );
  await add(
    'fb_zusatz_zeiterfassung',
    'gebietsleiter_id uuid;market_id text;entry_date date;reason text;reason_label text;zeit_von text;zeit_bis text;zeit_diff text;kommentar text;schulung_ort text;is_work_time_deduction bool;created_at timestamptz;updated_at timestamptz'
  );
  await add(
    'fb_day_tracking',
    'gebietsleiter_id uuid;tracking_date date;day_start_time text;day_end_time text;skipped_first_fahrzeit bool;km_stand_start int;km_stand_start_deferred bool;km_stand_end int;total_fahrzeit text;total_besuchszeit text;total_unterbrechung text;total_arbeitszeit text;markets_visited int;status text;created_at timestamptz;updated_at timestamptz'
  );
  await db.exec(
    `CREATE VIEW fb_questions_usage AS SELECT * FROM fb_questions;CREATE VIEW fb_questions_with_usage AS SELECT * FROM fb_questions;CREATE VIEW fb_modules_overview AS SELECT * FROM fb_modules;CREATE VIEW fb_fragebogen_overview AS SELECT * FROM fb_fragebogen;`
  );
  await db.query(
    "UPDATE markets SET gebietsleiter_id=$1,gebietsleiter_name='Robin Muster',mars_fil='DEMO-101',internal_id='DEMO-1',chain='Billa+'",
    [id(3)]
  );
  await db.query(
    "INSERT INTO wellen_markets(welle_id,market_id) SELECT id,'DEMO-1' FROM wellen"
  );
  await db.query(
    "INSERT INTO wellen_einzelprodukte(id,welle_id,name,target_number,item_value,artikel_nr,ve) VALUES($1,$2,'Beispielprodukt',10,2.5,'TEST-100',6)",
    [id(99), id(10)]
  );
  await db.query(
    "UPDATE wellen SET status=CASE WHEN start_date>current_date THEN 'upcoming' WHEN end_date<current_date THEN 'past' ELSE 'active' END"
  );
  await db.query(
    "INSERT INTO wellen_kw_days(welle_id,kw,days,kw_order) VALUES($1,'KW47',ARRAY['MO','DI'],0)",
    [id(10)]
  );
  await db.query(
    "INSERT INTO vorverkauf_entries(id,market_id,gebietsleiter_id,reason,status,notes) VALUES($1,'DEMO-1',$2,'OOS','completed','Synthetischer Vorverkauf'),($3,'DEMO-1',$2,'Produkttausch','completed','Synthetischer Produktersatz')",
    [id(40), id(3), id(41)]
  );
  await db.query(
    "INSERT INTO vorverkauf_items(vorverkauf_entry_id,product_id,quantity,item_type) VALUES($1,$2,3,'replace'),($3,$2,2,'take_out')",
    [id(40), id(99), id(41)]
  );
  await db.query(
    "INSERT INTO nara_incentive_submissions(id,market_id,gebietsleiter_id) VALUES($1,'DEMO-1',$2)",
    [id(42), id(3)]
  );
  await db.query(
    'INSERT INTO nara_incentive_items(submission_id,product_id,quantity) VALUES($1,$2,2)',
    [id(42), id(99)]
  );
}
const parts = (text) => {
  let depth = 0,
    start = 0;
  const out = [];
  for (let i = 0; i < text.length; i++) {
    if (text[i] === '(') depth++;
    if (text[i] === ')') depth--;
    if (text[i] === ',' && !depth) {
      out.push(text.slice(start, i).trim());
      start = i + 1;
    }
  }
  out.push(text.slice(start).trim());
  return out.filter(Boolean);
};
export async function adminPageRest(db, { errors, calls, rowCap }) {
  const numeric = new Set(
    (
      await db.query(
        "select table_name,column_name from information_schema.columns where table_schema='public' and data_type='numeric'"
      )
    ).rows.map((row) => row.table_name + '.' + row.column_name)
  );
  const identifier = (value) => {
    if (!/^[a-z_][a-z0-9_]*$/.test(value))
      throw Error('Invalid fixture identifier ' + value);
    return '"' + value + '"';
  };
  const tableRows = async (table) =>
    (await db.query(`SELECT * FROM ${identifier(table)}`)).rows.map((row) =>
      Object.fromEntries(
        Object.entries(row).map(([key, value]) => [
          key,
          numeric.has(table + '.' + key) && value != null
            ? Number(value)
            : value,
        ])
      )
    );
  const normalize = (row) =>
    Object.fromEntries(
      Object.entries(row).map(([key, value]) => [
        key,
        value instanceof Date
          ? /date$/.test(key)
            ? value.toISOString().slice(0, 10)
            : value.toISOString()
          : value,
      ])
    );
  const related = async (table, row, target, hint) => {
    const fk = hint?.replace('!inner', '');
    const explicit = {
      markets: 'market_id',
      users: 'gebietsleiter_id',
      gebietsleiter: 'gebietsleiter_id',
      wellen: 'welle_id',
      products: 'product_id',
      fb_fragebogen: 'fragebogen_id',
      fb_modules: 'module_id',
      fb_questions: 'question_id',
      fb_responses: 'response_id',
      vorverkauf_wellen: 'wave_id',
    };
    const local = fk && fk !== 'inner' ? fk : explicit[target];
    const all = await tableRows(target);
    if (local && local in row)
      return all.find((r) => String(r.id) === String(row[local])) || null;
    const child = {
      nara_incentive_items: 'submission_id',
      fb_answers: 'response_id',
      fb_response_answers: 'response_id',
      vorverkauf_items: 'vorverkauf_entry_id',
      fb_module_questions: 'module_id',
      fb_fragebogen_modules: 'fragebogen_id',
      fb_fragebogen_markets: 'fragebogen_id',
    }[target];
    if (!child) throw Error(`Unexpected relation ${table}->${target}`);
    return all.filter((r) => String(r[child]) === String(row.id));
  };
  async function project(table, row, selection) {
    const out = {};
    for (const col of parts(selection)) {
      if (col === '*') {
        Object.assign(out, normalize(row));
        continue;
      }
      const relation = col.match(/^([\w:!]+)\s*\((.*)\)$/s);
      if (relation) {
        let [alias, target] = relation[1].split(':');
        target = target || alias;
        const [name, hint] = target.split('!');
        const values = await related(table, row, name, hint);
        out[alias] = Array.isArray(values)
          ? await Promise.all(values.map((v) => project(name, v, relation[2])))
          : values
            ? await project(name, values, relation[2])
            : null;
        continue;
      }
      if (!(col in row)) throw Error(`Missing fixture column ${table}.${col}`);
      out[col] = normalize({ [col]: row[col] })[col];
    }
    return out;
  }
  const filter = (row, key, raw) => {
    const value = String(raw),
      actual = normalize(row)[key];
    if (value.startsWith('eq.')) return String(actual) === value.slice(3);
    if (value.startsWith('neq.'))
      return actual != null && String(actual) !== value.slice(4);
    if (value.startsWith('not.eq.'))
      return actual != null && String(actual) !== value.slice(7);
    if (value.startsWith('in.('))
      return value
        .slice(4, -1)
        .split(',')
        .map((s) => s.replace(/^"|"$/g, ''))
        .includes(String(actual));
    if (value === 'is.null') return actual == null;
    if (value === 'not.is.null') return actual != null;
    if (value === 'is.false') return actual === false;
    if (value === 'is.true') return actual === true;
    if (value.startsWith('gte.')) return actual >= value.slice(4);
    if (value.startsWith('lte.')) return actual <= value.slice(4);
    if (value.startsWith('gt.')) return actual > value.slice(3);
    if (value.startsWith('lt.')) return actual < value.slice(3);
    if (value.startsWith('cs.{'))
      return value
        .slice(4, -1)
        .split(',')
        .every((v) => (actual || []).includes(v));
    throw Error('Unexpected fixture filter ' + key + '=' + value);
  };
  const boolean = (row, expr) =>
    parts(expr).some((term) => {
      if (term.startsWith('and('))
        return parts(term.slice(4, -1)).every((t) => {
          const i = t.indexOf('.');
          return filter(row, t.slice(0, i), t.slice(i + 1));
        });
      const i = term.indexOf('.');
      return filter(row, term.slice(0, i), term.slice(i + 1));
    });
  return async (req, res) => {
    calls.push({ table: req.params.table, select: req.query.select });
    try {
      const table = req.params.table;
      let rows = await tableRows(table);
      for (const [key, value] of Object.entries(req.query)) {
        if (['select', 'order', 'limit', 'offset'].includes(key)) continue;
        if (key === 'or') {
          rows = rows.filter((r) => boolean(r, String(value).slice(1, -1)));
          continue;
        }
        for (const v of Array.isArray(value) ? value : [value])
          rows = rows.filter((r) => filter(r, key, v));
      }
      const total = rows.length;
      if (req.query.order)
        for (const order of String(req.query.order).split(',').reverse()) {
          const [key, direction] = order.split('.');
          rows.sort(
            (a, b) =>
              (a[key] < b[key] ? -1 : a[key] > b[key] ? 1 : 0) *
              (direction === 'desc' ? -1 : 1)
          );
        }
      const offset = Number(req.query.offset || 0);
      rows = rows.slice(
        offset,
        offset + Math.min(Number(req.query.limit || 1000), rowCap)
      );
      const out = await Promise.all(
        rows.map((r) => project(table, r, String(req.query.select || '*')))
      );
      res.setHeader(
        'Content-Range',
        `0-${Math.max(0, out.length - 1)}/${total}`
      );
      res.json(
        req.headers.accept?.includes('application/vnd.pgrst.object')
          ? (out[0] ?? null)
          : out
      );
    } catch (e) {
      errors.push(e.message);
      res.status(400).json({ message: e.message, code: e.code });
    }
  };
}
