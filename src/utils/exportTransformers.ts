// Data transformation functions for Excel export
// Handles joining tables, grouping nested data, and formatting for Excel output

import { SupabaseClient } from '@supabase/supabase-js';

export interface ExportRow {
  [key: string]: any;
}

export interface TransformOptions {
  columns: string[];
  filters?: {
    dateRange?: { start: string; end: string };
    glIds?: string[];
    welleIds?: string[];
  };
  expandPaletteProducts?: boolean;
}

interface SubmissionWithMeta extends ExportRow {
  _isParent?: boolean;
  _isChild?: boolean;
  _groupId?: string | null;
  _isMultiline?: boolean;
  _productDetails?: Array<{
    created_at: any;
    welle_name: any;
    gl_name: any;
    market_name: any;
    market_chain: any;
    market_address: any;
    market_postal_code: any;
    market_city: any;
    containerName: string | null;
    containerType: any;
    productName: string;
    quantity: any;
    valuePerUnit: any;
    totalValue: number;
  }>;
}

// Transform wellen_submissions with joins to wellen, gebietsleiter, markets, and items
export async function transformWellenSubmissions(
  client: SupabaseClient,
  options: TransformOptions
): Promise<ExportRow[]> {
  const { columns, filters, expandPaletteProducts = false } = options;

  // Fetch submissions with date filter
  let allSubs: any[] = [];
  let subFrom = 0;
  const subPageSize = 1000;
  let subHasMore = true;
  while (subHasMore) {
    let query = client
      .from('wellen_submissions')
      .select('*')
      .order('created_at', { ascending: false })
      .range(subFrom, subFrom + subPageSize - 1);

    if (filters?.dateRange?.start) {
      query = query.gte('created_at', filters.dateRange.start);
    }
    if (filters?.dateRange?.end) {
      query = query.lte('created_at', filters.dateRange.end + 'T23:59:59');
    }
    if (filters?.glIds && filters.glIds.length > 0) {
      query = query.in('gebietsleiter_id', filters.glIds);
    }
    if (filters?.welleIds && filters.welleIds.length > 0) {
      query = query.in('welle_id', filters.welleIds);
    }

    const { data, error } = await query;
    if (error) throw error;
    if (data && data.length > 0) {
      allSubs = [...allSubs, ...data];
      subFrom += subPageSize;
      subHasMore = data.length === subPageSize;
    } else {
      subHasMore = false;
    }
  }
  const submissions = allSubs;
  if (submissions.length === 0) return [];

  // Fetch related data
  const welleIds = [...new Set(submissions.map(s => s.welle_id))];
  const glIds = [...new Set(submissions.map(s => s.gebietsleiter_id))];
  const marketIds = [...new Set(submissions.map(s => s.market_id))];

  const [wellenData, glData, marketData] = await Promise.all([
    client.from('wellen').select('id, name').in('id', welleIds),
    client.from('gebietsleiter').select('id, name, email').in('id', glIds),
    client.from('markets').select('id, name, chain, city, address, postal_code').in('id', marketIds)
  ]);

  const wellenMap = new Map((wellenData.data || []).map(w => [w.id, w]));
  const glMap = new Map((glData.data || []).map(g => [g.id, g]));
  const marketMap = new Map((marketData.data || []).map(m => [m.id, m]));

  // Fetch item names based on type
  const displayIds = submissions.filter(s => s.item_type === 'display').map(s => s.item_id);
  const kartonwareIds = submissions.filter(s => s.item_type === 'kartonware').map(s => s.item_id);
  const einzelproduktIds = submissions.filter(s => s.item_type === 'einzelprodukt').map(s => s.item_id);
  const paletteProductIds = submissions.filter(s => s.item_type === 'palette').map(s => s.item_id);
  const schutteProductIds = submissions.filter(s => s.item_type === 'schuette').map(s => s.item_id);

  console.log(`📊 Fetching item names:`, {
    displays: displayIds.length,
    kartonware: kartonwareIds.length,
    einzelprodukt: einzelproduktIds.length,
    paletteProducts: paletteProductIds.length,
    schutteProducts: schutteProductIds.length
  });

  const [displaysData, kartonwareData, einzelprodukteData, paletteProductsData, schutteProductsData] = await Promise.all([
    displayIds.length > 0 ? client.from('wellen_displays').select('id, name, item_value').in('id', displayIds) : { data: [] },
    kartonwareIds.length > 0 ? client.from('wellen_kartonware').select('id, name, item_value').in('id', kartonwareIds) : { data: [] },
    einzelproduktIds.length > 0 ? client.from('wellen_einzelprodukte').select('id, name, item_value').in('id', einzelproduktIds) : { data: [] },
    paletteProductIds.length > 0 ? client.from('wellen_paletten_products').select('id, name, palette_id').in('id', paletteProductIds) : { data: [] },
    schutteProductIds.length > 0 ? client.from('wellen_schuetten_products').select('id, name, schuette_id').in('id', schutteProductIds) : { data: [] }
  ]);

  console.log(`✅ Fetched items:`, {
    displays: displaysData.data?.length || 0,
    kartonware: kartonwareData.data?.length || 0,
    einzelprodukt: einzelprodukteData.data?.length || 0,
    paletteProducts: paletteProductsData.data?.length || 0,
    schutteProducts: schutteProductsData.data?.length || 0
  });

  // Fetch palette and schuette names
  const paletteIds = [...new Set((paletteProductsData.data || []).map(p => p.palette_id))];
  const schutteIds = [...new Set((schutteProductsData.data || []).map(s => s.schuette_id))];

  const [palettesData, schuettenData] = await Promise.all([
    paletteIds.length > 0 ? client.from('wellen_paletten').select('id, name').in('id', paletteIds) : { data: [] },
    schutteIds.length > 0 ? client.from('wellen_schuetten').select('id, name').in('id', schutteIds) : { data: [] }
  ]);

  const itemNameMap = new Map<string, { name: string; container: string | null; itemValue: number | null }>([
    ...(displaysData.data || []).map(d => [d.id, { name: d.name, container: null, itemValue: d.item_value || null }]),
    ...(kartonwareData.data || []).map(k => [k.id, { name: k.name, container: null, itemValue: k.item_value || null }]),
    ...(einzelprodukteData.data || []).map(e => [e.id, { name: e.name, container: null, itemValue: e.item_value || null }])
  ] as [string, { name: string; container: string | null; itemValue: number | null }][]);

  const paletteMap = new Map((palettesData.data || []).map(p => [p.id, p.name]));
  const schutteMap = new Map((schuettenData.data || []).map(s => [s.id, s.name]));

  // Add palette/schuette products with container info
  (paletteProductsData.data || []).forEach(p => {
    itemNameMap.set(p.id, { name: p.name, container: paletteMap.get(p.palette_id) || 'Unbekannte Palette', itemValue: null });
  });
  (schutteProductsData.data || []).forEach(s => {
    itemNameMap.set(s.id, { name: s.name, container: schutteMap.get(s.schuette_id) || 'Unbekannte Schütte', itemValue: null });
  });

  // Group submissions by timestamp + container
  const grouped = new Map<string, typeof submissions>();
  submissions.forEach(sub => {
    const item = itemNameMap.get(sub.item_id);
    if ((sub.item_type === 'palette' || sub.item_type === 'schuette') && item?.container) {
      // Create group key: timestamp + container name + market
      const timestamp = new Date(sub.created_at).toISOString().slice(0, 16); // Minute precision
      const groupKey = `${timestamp}|${sub.market_id}|${item.container}`;
      
      if (!grouped.has(groupKey)) {
        grouped.set(groupKey, []);
      }
      grouped.get(groupKey)!.push(sub);
    }
  });

  // Create rows with grouping
  const rows: SubmissionWithMeta[] = [];
  const processedIds = new Set<string>();

  submissions.forEach(sub => {
    if (processedIds.has(sub.id)) return;

    const welle = wellenMap.get(sub.welle_id);
    const gl = glMap.get(sub.gebietsleiter_id);
    const market = marketMap.get(sub.market_id);
    const item = itemNameMap.get(sub.item_id);

    // Check if this is part of a palette/schuette group
    if ((sub.item_type === 'palette' || sub.item_type === 'schuette') && item?.container) {
      const timestamp = new Date(sub.created_at).toISOString().slice(0, 16);
      const groupKey = `${timestamp}|${sub.market_id}|${item.container}`;
      const groupSubmissions = grouped.get(groupKey) || [];

      if (groupSubmissions.length > 0 && !processedIds.has(groupSubmissions[0].id)) {
        const totalValue = groupSubmissions.reduce((sum, s) => sum + (s.quantity * (s.value_per_unit || 0)), 0);
        
        if (expandPaletteProducts) {
          // EXPANDED MODE: Parent row + indented children
          const parentRow: SubmissionWithMeta = {
            submission_id: groupSubmissions.map(s => s.id).join(','),
            created_at: sub.created_at,
            welle_name: welle?.name || 'Unbekannt',
            gl_name: gl?.name || 'Unbekannt',
            gl_email: gl?.email || '',
            market_name: market?.name || 'Unbekannt',
            market_chain: market?.chain || '',
            market_address: market?.address || '',
            market_postal_code: market?.postal_code || '',
            market_city: market?.city || '',
            market_id: sub.market_id,
            item_type: sub.item_type,
            item_name: item.container,
            container_name: '',
            quantity: 1,
            value_per_unit: totalValue,
            total_value: totalValue,
            photo_url: sub.photo_url || '',
            delivery_photo_url: sub.delivery_photo_url || '',
            _isParent: true,
            _groupId: groupKey
          };
          rows.push(parentRow);

          // Add child rows (products)
          groupSubmissions.forEach(childSub => {
            const childItem = itemNameMap.get(childSub.item_id);
            const childRow: SubmissionWithMeta = {
              submission_id: childSub.id,
              created_at: childSub.created_at,
              welle_name: '',
              gl_name: '',
              gl_email: '',
              market_name: '',
              market_chain: '',
              market_address: '',
              market_postal_code: '',
              market_city: '',
              market_id: '',
              item_type: '',
              item_name: `└─ ${childItem?.name || 'Unbekannt'}`,
              container_name: '',
              quantity: childSub.quantity,
              value_per_unit: childSub.value_per_unit || 0,
              total_value: (childSub.quantity * (childSub.value_per_unit || 0)),
              photo_url: '',
              delivery_photo_url: '',
              _isChild: true,
              _groupId: groupKey
            };
            rows.push(childRow);
            processedIds.add(childSub.id);
          });
        } else {
          // COMPACT MODE: Single row with multi-line formatted product list
          const productsLines = groupSubmissions
            .map((s, idx) => {
              const pItem = itemNameMap.get(s.item_id);
              const isLast = idx === groupSubmissions.length - 1;
              const symbol = isLast ? '└' : '├';
              const value = (s.quantity * (s.value_per_unit || 0));
              return `${symbol} ${pItem?.name || 'Unbekannt'} (${s.quantity}×) - €${value.toFixed(2)}`;
            })
            .join('\n');

          const formattedItemName = `${productsLines}\nTotal: €${totalValue.toFixed(2)}`;

          const compactRow: SubmissionWithMeta = {
            submission_id: groupSubmissions.map(s => s.id).join(','),
            created_at: sub.created_at,
            welle_name: welle?.name || 'Unbekannt',
            gl_name: gl?.name || 'Unbekannt',
            gl_email: gl?.email || '',
            market_name: market?.name || 'Unbekannt',
            market_chain: market?.chain || '',
            market_address: market?.address || '',
            market_postal_code: market?.postal_code || '',
            market_city: market?.city || '',
            market_id: sub.market_id,
            item_type: sub.item_type,
            item_name: formattedItemName,
            container_name: item.container,
            quantity: 1,
            value_per_unit: totalValue,
            total_value: totalValue,
            photo_url: sub.photo_url || '',
            delivery_photo_url: sub.delivery_photo_url || '',
            _isMultiline: true,
            _productDetails: groupSubmissions.map(s => ({
              created_at: s.created_at,
              welle_name: welle?.name || 'Unbekannt',
              gl_name: gl?.name || 'Unbekannt',
              market_name: market?.name || 'Unbekannt',
              market_chain: market?.chain || '',
              market_address: market?.address || '',
              market_postal_code: market?.postal_code || '',
              market_city: market?.city || '',
              containerName: item.container,
              containerType: sub.item_type,
              productName: itemNameMap.get(s.item_id)?.name || 'Unbekannt',
              quantity: s.quantity,
              valuePerUnit: s.value_per_unit || 0,
              totalValue: s.quantity * (s.value_per_unit || 0)
            }))
          };
          rows.push(compactRow);
          groupSubmissions.forEach(s => processedIds.add(s.id));
        }
      }
      } else {
        // Regular item (display, kartonware, einzelprodukt)
        // Use item_value from wave definition if available (for value-based waves)
        const valuePerUnit = item?.itemValue || sub.value_per_unit || 0;
        
        const row: SubmissionWithMeta = {
          submission_id: sub.id,
          created_at: sub.created_at,
          welle_name: welle?.name || 'Unbekannt',
          gl_name: gl?.name || 'Unbekannt',
          gl_email: gl?.email || '',
          market_name: market?.name || 'Unbekannt',
          market_chain: market?.chain || '',
          market_address: market?.address || '',
          market_postal_code: market?.postal_code || '',
          market_city: market?.city || '',
          market_id: sub.market_id,
          item_type: sub.item_type,
          item_name: item?.name || 'Unbekannt',
          container_name: item?.container || '',
          quantity: sub.quantity,
          value_per_unit: valuePerUnit,
          total_value: (sub.quantity * valuePerUnit),
          photo_url: sub.photo_url || '',
          delivery_photo_url: sub.delivery_photo_url || ''
        };
        rows.push(row);
        processedIds.add(sub.id);
      }
  });

  // Filter to requested columns and preserve metadata
  return rows.map(row => {
    const filteredRow: any = {};
    columns.forEach(col => {
      if (row.hasOwnProperty(col)) {
        filteredRow[col] = row[col];
      }
    });
    // ALWAYS preserve metadata for Excel formatting
    filteredRow._isParent = row._isParent || false;
    filteredRow._isChild = row._isChild || false;
    filteredRow._groupId = row._groupId || null;
    filteredRow._isMultiline = row._isMultiline || false;
    filteredRow._productDetails = row._productDetails || null;
    return filteredRow;
  });
}

// Transform markets data
export async function transformMarkets(
  client: SupabaseClient,
  options: TransformOptions
): Promise<ExportRow[]> {
  const { columns, filters } = options;

  let allMkts: any[] = [];
  let mktFrom = 0;
  const mktPageSize = 1000;
  let mktHasMore = true;
  while (mktHasMore) {
    let query = client
      .from('markets')
      .select('*')
      .order('name', { ascending: true })
      .range(mktFrom, mktFrom + mktPageSize - 1);

    if (filters?.dateRange?.start) {
      query = query.gte('created_at', filters.dateRange.start);
    }
    if (filters?.dateRange?.end) {
      query = query.lte('created_at', filters.dateRange.end + 'T23:59:59');
    }
    if (filters?.glIds && filters.glIds.length > 0) {
      query = query.in('gebietsleiter_id', filters.glIds);
    }

    const { data, error } = await query;
    if (error) throw error;
    if (data && data.length > 0) {
      allMkts = [...allMkts, ...data];
      mktFrom += mktPageSize;
      mktHasMore = data.length === mktPageSize;
    } else {
      mktHasMore = false;
    }
  }
  const markets = allMkts;
  if (markets.length === 0) return [];

  const rows: ExportRow[] = markets.map(market => {
    const row: ExportRow = {
      id: market.id,
      internal_id: market.internal_id,
      name: market.name,
      chain: market.chain || '',
      address: market.address || '',
      city: market.city || '',
      postal_code: market.postal_code || '',
      gebietsleiter_name: market.gebietsleiter_name || '',
      gebietsleiter_email: market.gebietsleiter_email || '',
      gebietsleiter_id: market.gebietsleiter_id || '',
      frequency: market.frequency || 0,
      current_visits: market.current_visits || 0,
      last_visit_date: market.last_visit_date || '',
      is_active: market.is_active ? 'Aktiv' : 'Inaktiv',
      phone: market.phone || '',
      email: market.email || '',
      channel: market.channel || '',
      banner: market.banner || '',
      subgroup: market.subgroup || '',
      created_at: market.created_at
    };

    const filteredRow: ExportRow = {};
    columns.forEach(col => {
      if (row.hasOwnProperty(col)) {
        filteredRow[col] = row[col];
      }
    });

    return filteredRow;
  });

  return rows;
}

// Transform vorverkauf entries with items
export async function transformVorverkaufEntries(
  client: SupabaseClient,
  options: TransformOptions
): Promise<ExportRow[]> {
  const { columns, filters } = options;

  let allEntries: any[] = [];
  let entFrom = 0;
  const entPageSize = 1000;
  let entHasMore = true;
  while (entHasMore) {
    let query = client
      .from('vorverkauf_entries')
      .select('*')
      .order('created_at', { ascending: false })
      .range(entFrom, entFrom + entPageSize - 1);

    if (filters?.dateRange?.start) {
      query = query.gte('created_at', filters.dateRange.start);
    }
    if (filters?.dateRange?.end) {
      query = query.lte('created_at', filters.dateRange.end + 'T23:59:59');
    }
    if (filters?.glIds && filters.glIds.length > 0) {
      query = query.in('gebietsleiter_id', filters.glIds);
    }

    const { data, error } = await query;
    if (error) throw error;
    if (data && data.length > 0) {
      allEntries = [...allEntries, ...data];
      entFrom += entPageSize;
      entHasMore = data.length === entPageSize;
    } else {
      entHasMore = false;
    }
  }
  const entries = allEntries;
  if (entries.length === 0) return [];

  // Fetch related data
  const glIds = [...new Set(entries.map(e => e.gebietsleiter_id))];
  const marketIds = [...new Set(entries.map(e => e.market_id))];
  const entryIds = entries.map(e => e.id);

  const [glData, marketData, itemsData] = await Promise.all([
    client.from('gebietsleiter').select('id, name, email').in('id', glIds),
    client.from('markets').select('id, name, chain, city, address, postal_code').in('id', marketIds),
    client.from('vorverkauf_items').select('*, products(name)').in('vorverkauf_entry_id', entryIds)
  ]);

  const glMap = new Map((glData.data || []).map(g => [g.id, g]));
  const marketMap = new Map((marketData.data || []).map(m => [m.id, m]));

  // Group items by entry
  const itemsByEntry = new Map<string, any[]>();
  (itemsData.data || []).forEach(item => {
    if (!itemsByEntry.has(item.vorverkauf_entry_id)) {
      itemsByEntry.set(item.vorverkauf_entry_id, []);
    }
    itemsByEntry.get(item.vorverkauf_entry_id)!.push(item);
  });

  const rows: ExportRow[] = entries.map(entry => {
    const gl = glMap.get(entry.gebietsleiter_id);
    const market = marketMap.get(entry.market_id);
    const items = itemsByEntry.get(entry.id) || [];

    const productsSummary = items
      .map(item => `${item.products?.name || 'Unbekannt'} (${item.quantity}× ${item.item_type === 'replace' ? 'Ersatz' : 'Entnahme'})`)
      .join(', ');

    const productsJson = JSON.stringify(items.map(item => ({
      name: item.products?.name || 'Unbekannt',
      quantity: item.quantity,
      type: item.item_type
    })));

    const row: ExportRow = {
      id: entry.id,
      created_at: entry.created_at,
      gl_name: gl?.name || 'Unbekannt',
      gl_email: gl?.email || '',
      market_name: market?.name || 'Unbekannt',
      market_chain: market?.chain || '',
      market_address: market?.address || '',
      market_postal_code: market?.postal_code || '',
      market_city: market?.city || '',
      reason: entry.reason,
      status: entry.status === 'completed' ? 'Abgeschlossen' : 'Ausstehend',
      notes: entry.notes || '',
      products_summary: productsSummary,
      products_json: productsJson
    };

    const filteredRow: ExportRow = {};
    columns.forEach(col => {
      if (row.hasOwnProperty(col)) {
        filteredRow[col] = row[col];
      }
    });

    return filteredRow;
  });

  return rows;
}

// Transform action_history data
export async function transformActionHistory(
  client: SupabaseClient,
  options: TransformOptions
): Promise<ExportRow[]> {
  const { columns, filters } = options;

  let query = client
    .from('action_history')
    .select('*')
    .order('timestamp', { ascending: false });

  if (filters?.dateRange?.start) {
    query = query.gte('timestamp', filters.dateRange.start);
  }
  if (filters?.dateRange?.end) {
    query = query.lte('timestamp', filters.dateRange.end + 'T23:59:59');
  }

  const { data: actions, error } = await query;
  if (error) throw error;
  if (!actions || actions.length === 0) return [];

  const rows: ExportRow[] = actions.map(action => {
    const row: ExportRow = {
      timestamp: action.timestamp,
      action_type: action.action_type === 'assign' ? 'Zuweisen' : 
                   action.action_type === 'swap' ? 'Tauschen' : 'Entfernen',
      market_id: action.market_id || '',
      market_chain: action.market_chain,
      market_address: action.market_address,
      market_city: action.market_city || '',
      market_postal_code: action.market_postal_code || '',
      target_gl: action.target_gl,
      previous_gl: action.previous_gl || '',
      performed_by: action.performed_by || '',
      notes: action.notes || ''
    };

    const filteredRow: ExportRow = {};
    columns.forEach(col => {
      if (row.hasOwnProperty(col)) {
        filteredRow[col] = row[col];
      }
    });

    return filteredRow;
  });

  return rows;
}

// Transform gebietsleiter data with aggregated metrics
export async function transformGebietsleiter(
  client: SupabaseClient,
  options: TransformOptions
): Promise<ExportRow[]> {
  const { columns, filters } = options;

  let glQuery = client
    .from('gebietsleiter')
    .select('*')
    .eq('is_active', true)
    .order('name', { ascending: true });

  if (filters?.dateRange?.start) {
    glQuery = glQuery.gte('created_at', filters.dateRange.start);
  }
  if (filters?.dateRange?.end) {
    glQuery = glQuery.lte('created_at', filters.dateRange.end + 'T23:59:59');
  }

  const { data: gls, error } = await glQuery;

  if (error) throw error;
  if (!gls || gls.length === 0) return [];

  const glIds = gls.map(gl => gl.id);

  // Fetch market visits for each GL (paginated to avoid Supabase 1000-row cap)
  let allMarkets: any[] = [];
  let from = 0;
  const pageSize = 1000;
  let hasMore = true;
  while (hasMore) {
    const { data } = await client
      .from('markets')
      .select('gebietsleiter_id, current_visits')
      .in('gebietsleiter_id', glIds)
      .range(from, from + pageSize - 1);
    if (data && data.length > 0) {
      allMarkets = [...allMarkets, ...data];
      from += pageSize;
      hasMore = data.length === pageSize;
    } else {
      hasMore = false;
    }
  }

  const visitCounts = new Map<string, number>();
  allMarkets.forEach(m => {
    const current = visitCounts.get(m.gebietsleiter_id) || 0;
    visitCounts.set(m.gebietsleiter_id, current + (m.current_visits || 0));
  });

  // Fetch submissions for performance metrics (paginated)
  let allSubmissions: any[] = [];
  from = 0;
  hasMore = true;
  while (hasMore) {
    const { data } = await client
      .from('wellen_submissions')
      .select('gebietsleiter_id, item_type, quantity, value_per_unit')
      .in('gebietsleiter_id', glIds)
      .range(from, from + pageSize - 1);
    if (data && data.length > 0) {
      allSubmissions = [...allSubmissions, ...data];
      from += pageSize;
      hasMore = data.length === pageSize;
    } else {
      hasMore = false;
    }
  }

  // Calculate metrics per GL
  const displayCounts = new Map<string, number>();
  const kartonwareCounts = new Map<string, number>();
  const paletteValues = new Map<string, number>();
  const schutteValues = new Map<string, number>();

  allSubmissions.forEach(sub => {
    switch (sub.item_type) {
      case 'display':
        const dCount = displayCounts.get(sub.gebietsleiter_id) || 0;
        displayCounts.set(sub.gebietsleiter_id, dCount + sub.quantity);
        break;
      case 'kartonware':
        const kCount = kartonwareCounts.get(sub.gebietsleiter_id) || 0;
        kartonwareCounts.set(sub.gebietsleiter_id, kCount + sub.quantity);
        break;
      case 'palette':
        const pValue = paletteValues.get(sub.gebietsleiter_id) || 0;
        paletteValues.set(sub.gebietsleiter_id, pValue + (sub.quantity * (sub.value_per_unit || 0)));
        break;
      case 'schuette':
        const sValue = schutteValues.get(sub.gebietsleiter_id) || 0;
        schutteValues.set(sub.gebietsleiter_id, sValue + (sub.quantity * (sub.value_per_unit || 0)));
        break;
    }
  });

  const rows: ExportRow[] = gls.map(gl => {
    const row: ExportRow = {
      id: gl.id,
      name: gl.name,
      email: gl.email,
      phone: gl.phone || '',
      address: gl.address || '',
      city: gl.city || '',
      postal_code: gl.postal_code || '',
      is_active: gl.is_active ? 'Aktiv' : 'Inaktiv',
      total_visits: visitCounts.get(gl.id) || 0,
      display_count: displayCounts.get(gl.id) || 0,
      kartonware_count: kartonwareCounts.get(gl.id) || 0,
      paletten_value: paletteValues.get(gl.id) || 0,
      schuetten_value: schutteValues.get(gl.id) || 0,
      created_at: gl.created_at,
      profile_picture_url: gl.profile_picture_url || ''
    };

    const filteredRow: ExportRow = {};
    columns.forEach(col => {
      if (row.hasOwnProperty(col)) {
        filteredRow[col] = row[col];
      }
    });

    return filteredRow;
  });

  return rows;
}

// ============================================================================
// SINGLE WAVE MATRIX EXPORT
// ============================================================================

export interface SingleWaveItem {
  id: string;
  name: string;
  type: 'display' | 'kartonware' | 'einzelprodukt' | 'palette_product' | 'schuette_product';
  pricePerUnit: number;
  parentId: string | null;
  parentName: string | null;
  parentType: string | null;
  colorGroup: number;
  isParent: boolean;
}

export interface SingleWaveResult {
  waveName: string;
  goalType: string;
  items: SingleWaveItem[];
  parentItems: Array<{ id: string; name: string; type: string; colorGroup: number }>;
  markets: Array<{ id: string; name: string }>;
  matrix: Record<string, Record<string, number>>; // itemId -> marketId -> quantity
  valueMatrix: Record<string, Record<string, number>>; // itemId -> marketId -> total value (from submissions)
  parentMatrix: Record<string, Record<string, number>>; // parentId -> marketId -> quantity
}

const PASTEL_COLORS = [
  'FFD4E4F7', // light blue
  'FFFDE2D4', // light orange
  'FFD4F7E4', // light green
  'FFF4D4F7', // light purple
  'FFF7F0D4', // light yellow
  'FFD4F7F4', // light teal
  'FFF7D4D4', // light red
  'FFE8D4F7', // light violet
  'FFD4F0F7', // light cyan
  'FFF7E8D4', // light peach
];

export async function transformSingleWaveExport(
  client: SupabaseClient,
  welleId: string
): Promise<SingleWaveResult> {
  // 1. Fetch wave
  const { data: welle, error: welleError } = await client
    .from('wellen')
    .select('id, name, goal_type')
    .eq('id', welleId)
    .single();

  if (welleError || !welle) throw new Error('Welle nicht gefunden');

  // 2. Fetch all item types in parallel
  const [displaysRes, kartonwareRes, einzelprodukteRes, palettenRes, schuettenRes] = await Promise.all([
    client.from('wellen_displays').select('id, name, item_value').eq('welle_id', welleId).order('display_order'),
    client.from('wellen_kartonware').select('id, name, item_value').eq('welle_id', welleId).order('kartonware_order'),
    client.from('wellen_einzelprodukte').select('id, name, item_value').eq('welle_id', welleId).order('einzelprodukt_order'),
    client.from('wellen_paletten').select('id, name, size, palette_order').eq('welle_id', welleId).order('palette_order'),
    client.from('wellen_schuetten').select('id, name, size, schuette_order').eq('welle_id', welleId).order('schuette_order'),
  ]);

  const displays = displaysRes.data || [];
  const kartonware = kartonwareRes.data || [];
  const einzelprodukte = einzelprodukteRes.data || [];
  const paletten = palettenRes.data || [];
  const schuetten = schuettenRes.data || [];

  // Fetch palette + schuette products
  const paletteIds = paletten.map(p => p.id);
  const schutteIds = schuetten.map(s => s.id);

  const [paletteProdsRes, schutteProdsRes] = await Promise.all([
    paletteIds.length > 0
      ? client.from('wellen_paletten_products').select('id, name, palette_id, value_per_ve').in('palette_id', paletteIds).order('product_order')
      : { data: [] },
    schutteIds.length > 0
      ? client.from('wellen_schuetten_products').select('id, name, schuette_id, value_per_ve').in('schuette_id', schutteIds).order('product_order')
      : { data: [] },
  ]);

  const paletteProducts = paletteProdsRes.data || [];
  const schutteProducts = schutteProdsRes.data || [];

  // 3. Fetch markets in this wave
  const { data: welleMarketsData } = await client
    .from('wellen_markets')
    .select('market_id')
    .eq('welle_id', welleId);

  const welleMarketIds = (welleMarketsData || []).map(wm => wm.market_id);

  let markets: Array<{ id: string; name: string }> = [];
  if (welleMarketIds.length > 0) {
    const { data: marketsData } = await client
      .from('markets')
      .select('id, name')
      .in('id', welleMarketIds)
      .order('name');
    markets = (marketsData || []).map(m => ({ id: m.id, name: m.name }));
  }

  // 4. Fetch all submissions for this wave (paginated) -- include value_per_unit and created_at
  let allSubs: any[] = [];
  let subFrom = 0;
  const pageSize = 1000;
  let hasMore = true;
  while (hasMore) {
    const { data, error } = await client
      .from('wellen_submissions')
      .select('item_type, item_id, market_id, quantity, value_per_unit, created_at')
      .eq('welle_id', welleId)
      .range(subFrom, subFrom + pageSize - 1);
    if (error) throw error;
    if (data && data.length > 0) {
      allSubs = [...allSubs, ...data];
      subFrom += pageSize;
      hasMore = data.length === pageSize;
    } else {
      hasMore = false;
    }
  }

  // 5. Assign color groups -- one consistent order for both children and parents
  // Order: paletten first, then schuetten, then displays, then kartonware
  let colorIdx = 0;
  const parentColorMap = new Map<string, number>();

  paletten.forEach(p => { parentColorMap.set(p.id, colorIdx); colorIdx++; });
  schuetten.forEach(s => { parentColorMap.set(s.id, colorIdx); colorIdx++; });
  displays.forEach(d => { parentColorMap.set(d.id, colorIdx); colorIdx++; });
  kartonware.forEach(k => { parentColorMap.set(k.id, colorIdx); colorIdx++; });

  // 6. Build items list: children grouped by parent (same order), then standalone einzelprodukte
  const items: SingleWaveItem[] = [];

  // Palette children -- grouped by palette parent
  paletten.forEach(pal => {
    const children = paletteProducts.filter(pp => pp.palette_id === pal.id);
    children.forEach(pp => {
      items.push({
        id: pp.id,
        name: pp.name,
        type: 'palette_product',
        pricePerUnit: pp.value_per_ve || 0,
        parentId: pp.palette_id,
        parentName: pal.name,
        parentType: 'palette',
        colorGroup: parentColorMap.get(pal.id) ?? -1,
        isParent: false,
      });
    });
  });

  // Schuette children -- grouped by schuette parent
  schuetten.forEach(sch => {
    const children = schutteProducts.filter(sp => sp.schuette_id === sch.id);
    children.forEach(sp => {
      items.push({
        id: sp.id,
        name: sp.name,
        type: 'schuette_product',
        pricePerUnit: sp.value_per_ve || 0,
        parentId: sp.schuette_id,
        parentName: sch.name,
        parentType: 'schuette',
        colorGroup: parentColorMap.get(sch.id) ?? -1,
        isParent: false,
      });
    });
  });

  // Displays (standalone, no children -- treated as items with their own color + value)
  displays.forEach(d => {
    items.push({
      id: d.id,
      name: d.name,
      type: 'display',
      pricePerUnit: d.item_value || 0,
      parentId: null,
      parentName: null,
      parentType: null,
      colorGroup: parentColorMap.get(d.id) ?? -1,
      isParent: false,
    });
  });

  // Kartonware (standalone, no children -- treated as items with their own color + value)
  kartonware.forEach(k => {
    items.push({
      id: k.id,
      name: k.name,
      type: 'kartonware',
      pricePerUnit: k.item_value || 0,
      parentId: null,
      parentName: null,
      parentType: null,
      colorGroup: parentColorMap.get(k.id) ?? -1,
      isParent: false,
    });
  });

  // Einzelprodukte (standalone, no parent -- no color)
  einzelprodukte.forEach(ep => {
    items.push({
      id: ep.id,
      name: ep.name,
      type: 'einzelprodukt',
      pricePerUnit: ep.item_value || 0,
      parentId: null,
      parentName: null,
      parentType: null,
      colorGroup: -1,
      isParent: false,
    });
  });

  // Parent items -- only palette/schuette containers (displays/kartonware are already in items)
  const parentItems: SingleWaveResult['parentItems'] = [];

  paletten.forEach(p => {
    parentItems.push({ id: p.id, name: p.name, type: 'palette', colorGroup: parentColorMap.get(p.id) ?? -1 });
  });
  schuetten.forEach(s => {
    parentItems.push({ id: s.id, name: s.name, type: 'schuette', colorGroup: parentColorMap.get(s.id) ?? -1 });
  });

  // 7. Build quantity + value matrices
  const matrix: Record<string, Record<string, number>> = {};
  const valueMatrix: Record<string, Record<string, number>> = {}; // itemId -> marketId -> total value
  const parentMatrix: Record<string, Record<string, number>> = {};

  const paletteProductParentMap = new Map<string, string>();
  paletteProducts.forEach(pp => paletteProductParentMap.set(pp.id, pp.palette_id));
  const schutteProductParentMap = new Map<string, string>();
  schutteProducts.forEach(sp => schutteProductParentMap.set(sp.id, sp.schuette_id));

  // Fallback price map: item_id -> price from definition (used when submission has no value_per_unit)
  const itemPriceMap = new Map<string, number>();
  displays.forEach(d => itemPriceMap.set(d.id, d.item_value || 0));
  kartonware.forEach(k => itemPriceMap.set(k.id, k.item_value || 0));
  einzelprodukte.forEach(ep => itemPriceMap.set(ep.id, ep.item_value || 0));
  paletteProducts.forEach(pp => itemPriceMap.set(pp.id, pp.value_per_ve || 0));
  schutteProducts.forEach(sp => itemPriceMap.set(sp.id, sp.value_per_ve || 0));

  allSubs.forEach(sub => {
    const { item_type, item_id, market_id, quantity, value_per_unit } = sub;

    let unitPrice = 0;
    if (item_type === 'display' || item_type === 'kartonware' || item_type === 'einzelprodukt') {
      unitPrice = itemPriceMap.get(item_id) || 0;
    } else if (item_type === 'palette' || item_type === 'schuette') {
      unitPrice = value_per_unit != null && value_per_unit > 0
        ? value_per_unit
        : (itemPriceMap.get(item_id) || 0);
    }
    const subValue = quantity * unitPrice;

    if (!matrix[item_id]) matrix[item_id] = {};
    matrix[item_id][market_id] = (matrix[item_id][market_id] || 0) + quantity;

    if (!valueMatrix[item_id]) valueMatrix[item_id] = {};
    valueMatrix[item_id][market_id] = (valueMatrix[item_id][market_id] || 0) + subValue;
  });

  // Recover orphaned submissions (products deleted from wave after submissions were made)
  const allItemIds = new Set(items.map(i => i.id));
  const orphanedItemIds = new Set<string>();
  allSubs.forEach(sub => {
    if (!allItemIds.has(sub.item_id)) orphanedItemIds.add(sub.item_id);
  });

  if (orphanedItemIds.size > 0) {
    const orphanIds = Array.from(orphanedItemIds);
    const [palOrphan, schOrphan, dispOrphan, kartOrphan, epOrphan] = await Promise.all([
      client.from('wellen_paletten_products').select('id, name, value_per_ve').in('id', orphanIds),
      client.from('wellen_schuetten_products').select('id, name, value_per_ve').in('id', orphanIds),
      client.from('wellen_displays').select('id, name, item_value').in('id', orphanIds),
      client.from('wellen_kartonware').select('id, name, item_value').in('id', orphanIds),
      client.from('wellen_einzelprodukte').select('id, name, item_value').in('id', orphanIds),
    ]);

    const orphanNameMap = new Map<string, { name: string; price: number }>();
    (palOrphan.data || []).forEach(p => orphanNameMap.set(p.id, { name: p.name, price: p.value_per_ve || 0 }));
    (schOrphan.data || []).forEach(s => orphanNameMap.set(s.id, { name: s.name, price: s.value_per_ve || 0 }));
    (dispOrphan.data || []).forEach(d => orphanNameMap.set(d.id, { name: d.name, price: d.item_value || 0 }));
    (kartOrphan.data || []).forEach(k => orphanNameMap.set(k.id, { name: k.name, price: k.item_value || 0 }));
    (epOrphan.data || []).forEach(e => orphanNameMap.set(e.id, { name: e.name, price: e.item_value || 0 }));

    for (const oid of orphanIds) {
      const info = orphanNameMap.get(oid);
      items.push({
        id: oid,
        name: info?.name || `Gelöschtes Produkt (${oid.slice(0, 8)})`,
        type: 'einzelprodukt',
        pricePerUnit: info?.price || 0,
        parentId: null,
        parentName: null,
        parentType: null,
        colorGroup: -1,
        isParent: false,
      });
    }
  }

  // Calculate parent container quantities for palette/schuette
  if (paletteProducts.length > 0 || schutteProducts.length > 0) {
    const containerCounts = new Map<string, Map<string, Set<string>>>();
    allSubs.forEach(sub => {
      if (sub.item_type !== 'palette' && sub.item_type !== 'schuette') return;
      const parentId = sub.item_type === 'palette'
        ? paletteProductParentMap.get(sub.item_id)
        : schutteProductParentMap.get(sub.item_id);
      if (!parentId) return;

      const timestamp = new Date(sub.created_at).toISOString().slice(0, 16);
      const groupKey = `${timestamp}|${parentId}`;

      if (!containerCounts.has(parentId)) containerCounts.set(parentId, new Map());
      const marketMap = containerCounts.get(parentId)!;
      if (!marketMap.has(sub.market_id)) marketMap.set(sub.market_id, new Set());
      marketMap.get(sub.market_id)!.add(groupKey);
    });

    containerCounts.forEach((marketMap, parentId) => {
      if (!parentMatrix[parentId]) parentMatrix[parentId] = {};
      marketMap.forEach((groups, marketId) => {
        parentMatrix[parentId][marketId] = groups.size;
      });
    });
  }

  return {
    waveName: welle.name,
    goalType: welle.goal_type,
    items,
    parentItems,
    markets,
    matrix,
    valueMatrix,
    parentMatrix,
  };
}

// Main transformer that routes to specific dataset transformers
export async function transformDataset(
  client: SupabaseClient,
  datasetId: string,
  options: TransformOptions
): Promise<ExportRow[]> {
  switch (datasetId) {
    case 'wellen_submissions':
      return transformWellenSubmissions(client, options);
    case 'markets':
      return transformMarkets(client, options);
    case 'vorverkauf_entries':
      return transformVorverkaufEntries(client, options);
    case 'action_history':
      return transformActionHistory(client, options);
    case 'gebietsleiter':
      return transformGebietsleiter(client, options);
    default:
      throw new Error(`Unknown dataset: ${datasetId}`);
  }
}
