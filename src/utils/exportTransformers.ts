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
  let query = client
    .from('wellen_submissions')
    .select('*')
    .order('created_at', { ascending: false });

  if (filters?.dateRange?.start) {
    query = query.gte('created_at', filters.dateRange.start);
  }
  if (filters?.dateRange?.end) {
    query = query.lte('created_at', filters.dateRange.end);
  }
  if (filters?.glIds && filters.glIds.length > 0) {
    query = query.in('gebietsleiter_id', filters.glIds);
  }
  if (filters?.welleIds && filters.welleIds.length > 0) {
    query = query.in('welle_id', filters.welleIds);
  }

  const { data: submissions, error } = await query;
  if (error) throw error;
  if (!submissions || submissions.length === 0) return [];

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

  let query = client
    .from('markets')
    .select('*')
    .order('name', { ascending: true });

  if (filters?.glIds && filters.glIds.length > 0) {
    query = query.in('gebietsleiter_id', filters.glIds);
  }

  const { data: markets, error } = await query;
  if (error) throw error;
  if (!markets || markets.length === 0) return [];

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

  let query = client
    .from('vorverkauf_entries')
    .select('*')
    .order('created_at', { ascending: false });

  if (filters?.dateRange?.start) {
    query = query.gte('created_at', filters.dateRange.start);
  }
  if (filters?.dateRange?.end) {
    query = query.lte('created_at', filters.dateRange.end);
  }
  if (filters?.glIds && filters.glIds.length > 0) {
    query = query.in('gebietsleiter_id', filters.glIds);
  }

  const { data: entries, error } = await query;
  if (error) throw error;
  if (!entries || entries.length === 0) return [];

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
    query = query.lte('timestamp', filters.dateRange.end);
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
  const { columns } = options;

  const { data: gls, error } = await client
    .from('gebietsleiter')
    .select('*')
    .eq('is_active', true)
    .order('name', { ascending: true });

  if (error) throw error;
  if (!gls || gls.length === 0) return [];

  const glIds = gls.map(gl => gl.id);

  // Fetch market visits for each GL
  const { data: markets } = await client
    .from('markets')
    .select('gebietsleiter_id, current_visits')
    .in('gebietsleiter_id', glIds);

  const visitCounts = new Map<string, number>();
  (markets || []).forEach(m => {
    const current = visitCounts.get(m.gebietsleiter_id) || 0;
    visitCounts.set(m.gebietsleiter_id, current + (m.current_visits || 0));
  });

  // Fetch submissions for performance metrics
  const { data: submissions } = await client
    .from('wellen_submissions')
    .select('gebietsleiter_id, item_type, quantity, value_per_unit')
    .in('gebietsleiter_id', glIds);

  // Calculate metrics per GL
  const displayCounts = new Map<string, number>();
  const kartonwareCounts = new Map<string, number>();
  const paletteValues = new Map<string, number>();
  const schutteValues = new Map<string, number>();

  (submissions || []).forEach(sub => {
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
