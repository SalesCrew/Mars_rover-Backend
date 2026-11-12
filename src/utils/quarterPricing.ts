export type CatalogItemType = 'display' | 'kartonware' | 'einzelprodukt';
export const catalogTypes: CatalogItemType[] = ['display', 'kartonware', 'einzelprodukt'];
export const viennaQuarter = (date = new Date()) => {
  const parts = new Intl.DateTimeFormat('en-CA', { timeZone: 'Europe/Vienna', year: 'numeric', month: 'numeric' }).formatToParts(date);
  return { year: Number(parts.find(p => p.type === 'year')!.value), quarter: Math.ceil(Number(parts.find(p => p.type === 'month')!.value) / 3) };
};
export class CatalogError extends Error { status = 422; }
export const catalogErrorMessage = (error: unknown): string | null => error instanceof CatalogError ? error.message
  : (error as any)?.code === '22023' ? 'Artikel, VE oder Quartalspreis ungültig. Bitte die Welle neu laden und erneut prüfen.' : null;

export async function loadQuarterCatalog(client: any, year: number, quarter: number) {
  const rows: any[] = [];
  for (let from = 0; ; from += 500) {
    const { data, error } = await client.from('quarter_price_catalog').select('*').eq('year', year).eq('quarter', quarter).order('catalog_key').range(from, from + 499);
    if (error) throw error;
    rows.push(...(data || []));
    if (!data || data.length < 500) break;
  }
  return rows;
}

// Resolve selection from authoritative catalog, ignoring caller name/price/VE.
// Old unlinked lines may be retained when editing; new text-only lines are refused.
export async function prepareWaveCatalog(client: any, body: any, existingWaveId?: string) {
  const types = [['display', 'displays', 'wellen_displays'], ['kartonware', 'kartonwareItems', 'wellen_kartonware'], ['einzelprodukt', 'einzelproduktItems', 'wellen_einzelprodukte']] as const;
  const { year, quarter } = viennaQuarter();
  const rows = await loadQuarterCatalog(client, year, quarter);
  const byKey = new Map(rows.map(p => [p.catalog_key, p]));
  for (const [type, field, table] of types) {
    let existing: any[] = [];
    if (existingWaveId) {
      const result = await client.from(table).select('id, name, item_value, artikel_nr, catalog_key, catalog_ve_size, catalog_order_price').eq('welle_id', existingWaveId);
      if (result.error) throw result.error;
      existing = result.data || [];
    }
    for (const item of body[field] || []) {
      const previous = existing.find(p => p.id === item.id || (!item.id && p.name === item.name));
      if (previous?.catalog_key && item.catalogKey === previous.catalog_key) {
        Object.assign(item, { name: previous.name, artikelNr: previous.artikel_nr, itemValue: previous.item_value,
          catalogVeSize: previous.catalog_ve_size, catalogOrderPrice: previous.catalog_order_price });
        continue;
      }
      if (previous && item.catalogKey && item.catalogKey !== previous.catalog_key) {
        const result = await client.from('wellen_submissions').select('id').eq('welle_id', existingWaveId).eq('item_type', type).eq('item_id', previous.id).limit(1);
        if (result.error) throw result.error;
        if (result.data?.length) throw new CatalogError('Dieser Artikel hat bereits Buchungen. Bitte einen neuen Katalogartikel als zusätzliche Zeile anlegen.');
      }
      if (!item.catalogKey) {
        if (previous && !previous.catalog_key) {
          // Editing targets/photos must never revalue an unlinked old wave.
          Object.assign(item, { name: previous.name, itemValue: previous.item_value, artikelNr: previous.artikel_nr });
          continue;
        }
        throw new CatalogError('Bitte alle neuen Artikel über die Artikelnummer im Quartalskatalog auswählen.');
      }
      const product = byKey.get(item.catalogKey);
      if (!product || product.item_type !== type) throw new CatalogError(`Für den ausgewählten Artikel fehlt ein freigegebener Preis in Q${quarter} ${year}.`);
      Object.assign(item, {
        name: product.name, artikelNr: product.artikel_nr, catalogKey: product.catalog_key,
        catalogVeSize: product.units_per_ve, catalogOrderPrice: Number(product.price_per_order),
        itemValue: Number(product.price_per_order) / (type === 'einzelprodukt' ? product.units_per_ve : 1),
      });
      if (previous) {
        // Changing a definition must never silently assign today's price to its history.
        item.itemValue = previous.item_value;
      }
    }
  }
  return !existingWaveId;
}

export const catalogItemMeta = (row: any) => ({
  catalogKey: row.catalog_key || null,
  artikelNr: row.artikel_nr || null,
  catalogVeSize: row.catalog_ve_size || null,
  catalogOrderPrice: row.catalog_order_price == null ? null : Number(row.catalog_order_price),
});

export const catalogItemColumns = (item: any) => item.catalogKey ? ({
  catalog_key: item.catalogKey, artikel_nr: item.artikelNr,
  catalog_ve_size: item.catalogVeSize, catalog_order_price: item.catalogOrderPrice,
}) : {};

// A stored booking price always wins over the mutable wave definition. Missing
// legacy prices can fall back for visibility, but must be marked as not frozen.
export const submissionPrice = (submission: any, wavePrice?: number | null): number | null => {
  if (submission.value_per_unit != null) return Number(submission.value_per_unit);
  return wavePrice != null ? Number(wavePrice) : null;
};
