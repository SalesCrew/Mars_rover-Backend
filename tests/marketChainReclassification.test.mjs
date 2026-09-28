import assert from 'node:assert/strict';
import test from 'node:test';
import { readFileSync } from 'node:fs';
import { PGlite } from '@electric-sql/pglite';

const correction = readFileSync(new URL('../sql/reclassify_lezanimo_wau_miau_markets.sql', import.meta.url), 'utf8');
const lezanimoIds = ['65165', '534', '16783'];
const wauMiauIds = ['23765', '20507', '70368', '72285', '75131', '72283', '649', '68462'];

async function fixture() {
  const db = new PGlite();
  await db.exec('CREATE TABLE markets (id text PRIMARY KEY, name text, chain text, banner text, subgroup text, is_active boolean, frequency integer)');
  for (const [ids, subgroup] of [[lezanimoIds, 'Lezanimo'], [wauMiauIds, 'Wau Miau']]) {
    for (const id of ids) {
      await db.query('INSERT INTO markets VALUES ($1, $2, $3, $3, $4, true, 12)', [id, `${subgroup} Beispiel`, 'Zoofachhandel', subgroup]);
    }
  }
  await db.exec("INSERT INTO markets VALUES ('other', 'Anderer Zoofachhandel', 'Zoofachhandel', 'Zoofachhandel', 'Andere', true, 24)");
  return db;
}

test('changes exactly the approved market chains and is idempotent', async () => {
  const db = await fixture();
  try {
    const before = (await db.query('SELECT * FROM markets ORDER BY id')).rows;
    await db.exec(correction);
    const after = (await db.query('SELECT * FROM markets ORDER BY id')).rows;
    assert.deepEqual(after, before.map(row => ({
      ...row,
      chain: lezanimoIds.includes(row.id) ? 'Lezanimo' : wauMiauIds.includes(row.id) ? 'Wau,Miau' : row.chain
    })));
    await db.exec(correction);
    assert.deepEqual((await db.query('SELECT * FROM markets ORDER BY id')).rows, after);
  } finally {
    await db.close();
  }
});

test('an unexpected classification rolls back every earlier update', async () => {
  const db = await fixture();
  try {
    await db.exec("UPDATE markets SET subgroup = 'Unerwartet' WHERE id = '75131'");
    const before = (await db.query('SELECT * FROM markets ORDER BY id')).rows;
    await assert.rejects(db.exec(correction), /no longer matches the verified classification/);
    await db.exec('ROLLBACK');
    assert.deepEqual((await db.query('SELECT * FROM markets ORDER BY id')).rows, before);
  } finally {
    await db.close();
  }
});
