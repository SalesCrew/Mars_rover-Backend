import { Router, Request, Response } from 'express';
import { supabase, createFreshClient } from '../config/supabase';
import { AuthRequest, getAuthenticatedGlId, requireAdmin } from '../middleware/auth';
import { sendInternalError } from '../utils/httpErrors';

const router = Router();

const MARKET_WRITE_FIELDS = [
  'id',
  'internal_id',
  'name',
  'address',
  'city',
  'postal_code',
  'chain',
  'phone',
  'email',
  'gebietsleiter_id',
  'gebietsleiter_name',
  'gebietsleiter_email',
  'channel',
  'banner',
  'branch',
  'maingroup',
  'subgroup',
  'frequency',
  'current_visits',
  'visit_day',
  'visit_duration',
  'last_visit_date',
  'customer_type',
  'is_active',
  'is_completed',
  'latitude',
  'longitude',
  'market_tel',
  'market_email',
  'mars_fil'
] as const;

const MARKET_SELECT_COLUMNS = 'id, internal_id, name, address, city, postal_code, chain, phone, email, gebietsleiter_id, gebietsleiter_name, gebietsleiter_email, channel, banner, branch, maingroup, subgroup, frequency, current_visits, visit_day, visit_duration, last_visit_date, customer_type, is_active, is_completed, latitude, longitude, market_tel, market_email, mars_fil';
const MARKET_HISTORY_WELLEN_SUBMISSION_SELECT = 'id, welle_id, gebietsleiter_id, item_type, item_id, quantity, value_per_unit, created_at, wellen(name), gebietsleiter(name)';
const MARKET_HISTORY_VORVERKAUF_SUBMISSION_SELECT = 'id, gebietsleiter_id, notes, created_at, gebietsleiter(name), vorverkauf_wellen(name)';
const MARKET_HISTORY_VORVERKAUF_PRODUCT_SELECT = 'quantity, reason, products(name)';
const MARKET_HISTORY_VORVERKAUF_ENTRY_SELECT = 'id, gebietsleiter_id, reason, notes, created_at, gebietsleiter(name)';
const MARKET_HISTORY_VORVERKAUF_ITEM_SELECT = 'quantity, item_type, products(name)';

const logMarketDbError = (context: string, error: any) => {
  console.error(`${context}: ${error?.code || 'database_error'}`);
};

const relatedName = (value: any): string | undefined => {
  const row = Array.isArray(value) ? value[0] : value;
  return typeof row?.name === 'string' ? row.name : undefined;
};

const pickMarketWriteFields = (input: unknown): Record<string, any> => {
  if (!input || typeof input !== 'object' || Array.isArray(input)) {
    return {};
  }

  const source = input as Record<string, any>;
  return MARKET_WRITE_FIELDS.reduce<Record<string, any>>((payload, field) => {
    if (Object.prototype.hasOwnProperty.call(source, field) && source[field] !== undefined) {
      payload[field] = source[field];
    }
    return payload;
  }, {});
};

/**
 * GET /api/markets
 * Get all markets
 */
router.get('/', async (req: Request, res: Response) => {
  try {
    console.log('📋 Fetching all markets...');
    
    const freshClient = createFreshClient();
    
    // Fetch ALL markets using pagination (Supabase has 1000 row limit per request)
    let allMarkets: any[] = [];
    let from = 0;
    const pageSize = 1000;
    let hasMore = true;

    while (hasMore) {
      const { data, error } = await freshClient
        .from('markets')
        .select(MARKET_SELECT_COLUMNS)
        .order('name', { ascending: true })
        .range(from, from + pageSize - 1);

      if (error) {
        logMarketDbError('Error fetching markets page', error);
        throw error;
      }

      if (data && data.length > 0) {
        allMarkets = [...allMarkets, ...data];
        from += pageSize;
        hasMore = data.length === pageSize; // If we got less than pageSize, we're done
      } else {
        hasMore = false;
      }
    }

    console.log(`✅ Fetched ${allMarkets.length} markets`);
    res.json(allMarkets);
  } catch (error: any) {
    console.error('Error fetching markets:');
    sendInternalError(res);
  }
});

/**
 * POST /api/markets/backfill-gl-ids
 * Backfill gebietsleiter_id for markets that have gebietsleiter_name but no gebietsleiter_id
 * Uses fuzzy matching (case insensitive, ignores dashes, extra spaces, etc.)
 * MUST be defined BEFORE /:id routes to avoid being caught by parameter matching
 */
router.post('/backfill-gl-ids', requireAdmin, async (req: Request, res: Response) => {
  try {
    console.log('🔄 Starting GL ID backfill...');
    
    const freshClient = createFreshClient();

    // Helper function to normalize names for matching
    const normalizeName = (name: string | null | undefined): string => {
      if (!name) return '';
      return name
        .toLowerCase()
        .replace(/[-–—]/g, ' ')  // Replace dashes with spaces
        .replace(/\s+/g, ' ')     // Collapse multiple spaces
        .replace(/[^a-z0-9äöüß\s]/g, '') // Remove special chars except umlauts
        .trim();
    };

    // Fetch all GLs
    const { data: gls, error: glError } = await freshClient
      .from('gebietsleiter')
      .select('id, name, email')
      .eq('is_active', true);

    if (glError) throw glError;

    console.log(`📋 Found ${gls?.length || 0} active GLs`);

    // Create a map of normalized names to GL data
    const glNameMap = new Map<string, { id: string; name: string; email: string }>();
    const glEmailMap = new Map<string, { id: string; name: string; email: string }>();
    for (const gl of gls || []) {
      const normalizedName = normalizeName(gl.name);
      glNameMap.set(normalizedName, { id: gl.id, name: gl.name, email: gl.email });
      // Also create email map for fallback matching
      if (gl.email) {
        glEmailMap.set(gl.email.toLowerCase().trim(), { id: gl.id, name: gl.name, email: gl.email });
      }
    }

    // Fetch markets with no gebietsleiter_id (they might have gebietsleiter_name or gebietsleiter_email)
    const { data: marketsToUpdate, error: marketsError } = await freshClient
      .from('markets')
      .select('id, gebietsleiter_name, gebietsleiter_email, gebietsleiter_id')
      .or('gebietsleiter_id.is.null,gebietsleiter_id.eq.');

    if (marketsError) throw marketsError;

    console.log(`📋 Found ${marketsToUpdate?.length || 0} markets needing GL ID backfill`);

    let updated = 0;
    let notFound = 0;
    const unmatchedNames = new Set<string>();
    const unmatchedMarketIds: string[] = [];

    for (const market of marketsToUpdate || []) {
      let matchedGL = null;
      
      // First try: match by name
      if (market.gebietsleiter_name) {
        const normalizedMarketGL = normalizeName(market.gebietsleiter_name);
        matchedGL = glNameMap.get(normalizedMarketGL);
      }
      
      // Second try: match by email if name didn't match
      if (!matchedGL && market.gebietsleiter_email) {
        const normalizedEmail = market.gebietsleiter_email.toLowerCase().trim();
        matchedGL = glEmailMap.get(normalizedEmail);
      }

      if (matchedGL) {
        // Update the market with the GL ID and email
        const { error: updateError } = await freshClient
          .from('markets')
          .update({
            gebietsleiter_id: matchedGL.id,
            gebietsleiter_email: matchedGL.email
          })
          .eq('id', market.id);

        if (updateError) {
          console.error(`  ❌ Failed to update market ${market.id}:`);
        } else {
          updated++;
        }
      } else {
        notFound++;
        unmatchedMarketIds.push(market.id);
        if (market.gebietsleiter_name) {
          unmatchedNames.add(market.gebietsleiter_name);
        }
      }
    }

    console.log(`✅ Backfill complete: ${updated} updated, ${notFound} not matched`);
    if (unmatchedNames.size > 0) {
      console.log(`Unmatched GL name count: ${unmatchedNames.size}`);
    }

    // Fetch detailed info for unmatched markets
    const unmatchedMarkets: Array<{ id: string; name: string; glName: string | null; glEmail: string | null }> = [];
    if (unmatchedMarketIds.length > 0) {
      const { data: unmatchedMarketsData } = await freshClient
        .from('markets')
        .select('id, name, gebietsleiter_name, gebietsleiter_email')
        .in('id', unmatchedMarketIds);
      
      for (const m of unmatchedMarketsData || []) {
        unmatchedMarkets.push({
          id: m.id,
          name: m.name || m.id,
          glName: m.gebietsleiter_name,
          glEmail: m.gebietsleiter_email
        });
      }
    }
    
    console.log(`📋 Returning ${unmatchedMarkets.length} unmatched markets for manual assignment`);

    res.json({
      success: true,
      updated,
      notMatched: notFound,
      unmatchedNames: Array.from(unmatchedNames),
      unmatchedMarkets
    });
  } catch (error: any) {
    console.error('❌ Error during GL ID backfill:');
    sendInternalError(res);
  }
});

/**
 * GET /api/markets/find-duplicates
 * Find markets that might be duplicates (same normalized content but different IDs)
 * MUST be defined BEFORE /:id routes to avoid being caught by parameter matching
 */
router.get('/find-duplicates', requireAdmin, async (req: Request, res: Response) => {
  try {
    console.log('🔍 Scanning for duplicate markets...');
    
    const freshClient = createFreshClient();

    // Helper function to normalize strings for comparison
    const normalize = (str: string | null | undefined): string => {
      if (!str) return '';
      return str
        .toLowerCase()
        .normalize('NFD').replace(/[\u0300-\u036f]/g, '') // Remove diacritics
        .replace(/ä/g, 'ae').replace(/ö/g, 'oe').replace(/ü/g, 'ue').replace(/ß/g, 'ss') // German chars
        .replace(/[-–—]/g, ' ')  // Replace dashes with spaces
        .replace(/[^a-z0-9\s]/g, '') // Remove special chars
        .replace(/\s+/g, ' ')     // Collapse multiple spaces
        .trim();
    };

    // Fetch ALL markets
    let allMarkets: any[] = [];
    let from = 0;
    const pageSize = 1000;
    let hasMore = true;

    while (hasMore) {
      const { data, error } = await freshClient
        .from('markets')
        .select('id, name, address, city, postal_code, chain, gebietsleiter_name, gebietsleiter_id')
        .order('name', { ascending: true })
        .range(from, from + pageSize - 1);

      if (error) throw error;

      if (data && data.length > 0) {
        allMarkets = [...allMarkets, ...data];
        from += pageSize;
        hasMore = data.length === pageSize;
      } else {
        hasMore = false;
      }
    }

    console.log(`📋 Total markets in DB: ${allMarkets.length}`);

    // Create a map to group by normalized key
    const duplicateGroups = new Map<string, any[]>();

    for (const market of allMarkets) {
      // Create a normalized key from name + address + city + chain
      const normalizedName = normalize(market.name);
      const normalizedAddress = normalize(market.address);
      const normalizedCity = normalize(market.city);
      const normalizedChain = normalize(market.chain);
      
      // Key: combination of all normalized fields
      const key = `${normalizedName}|${normalizedAddress}|${normalizedCity}|${normalizedChain}`;
      
      if (!duplicateGroups.has(key)) {
        duplicateGroups.set(key, []);
      }
      duplicateGroups.get(key)!.push({
        id: market.id,
        name: market.name,
        address: market.address,
        city: market.city,
        postal_code: market.postal_code,
        chain: market.chain,
        gebietsleiter_name: market.gebietsleiter_name,
        gebietsleiter_id: market.gebietsleiter_id,
        normalizedKey: key
      });
    }

    // Filter to only groups with more than 1 market (actual duplicates)
    const duplicates: any[] = [];
    for (const [key, markets] of duplicateGroups) {
      if (markets.length > 1) {
        duplicates.push({
          normalizedKey: key,
          count: markets.length,
          markets: markets
        });
      }
    }

    // Sort by count (most duplicates first)
    duplicates.sort((a, b) => b.count - a.count);

    console.log(`🔍 Found ${duplicates.length} duplicate groups (${duplicates.reduce((sum, d) => sum + d.count, 0)} total duplicate entries)`);

    res.json({
      totalMarketsInDb: allMarkets.length,
      uniqueMarkets: duplicateGroups.size,
      duplicateGroups: duplicates.length,
      duplicateEntries: duplicates.reduce((sum, d) => sum + d.count, 0),
      extraEntries: duplicates.reduce((sum, d) => sum + d.count - 1, 0),
      duplicates: duplicates
    });
  } catch (error: any) {
    console.error('Error finding duplicates:');
    sendInternalError(res);
  }
});

/**
 * GET /api/markets/:id
 * Get a single market by ID
 */
router.get('/:id', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    console.log(`📋 Fetching market ${id}...`);
    
    const freshClient = createFreshClient();

    const { data, error } = await freshClient
      .from('markets')
      .select(MARKET_SELECT_COLUMNS)
      .eq('id', id)
      .single();

    if (error) {
      logMarketDbError('Error fetching market row', error);
      throw error;
    }

    if (!data) {
      return res.status(404).json({ error: 'Market not found' });
    }

    console.log(`✅ Fetched market ${id}`);
    res.json(data);
  } catch (error: any) {
    console.error('Error fetching market:');
    sendInternalError(res);
  }
});

/**
 * POST /api/markets
 * Create a new market
 */
router.post('/', requireAdmin, async (req: Request, res: Response) => {
  try {
    console.log('➕ Creating new market...');
    
    const freshClient = createFreshClient();
    
    const marketPayload = pickMarketWriteFields(req.body);
    if (Object.keys(marketPayload).length === 0) {
      return res.status(400).json({ error: 'No valid market fields provided' });
    }

    const { data, error } = await freshClient
      .from('markets')
      .insert(marketPayload)
      .select(MARKET_SELECT_COLUMNS)
      .single();

    if (error) {
      logMarketDbError('Error creating market row', error);
      throw error;
    }

    console.log(`✅ Created market ${data?.id}`);
    res.status(201).json(data);
  } catch (error: any) {
    console.error('Error creating market:');
    sendInternalError(res);
  }
});

/**
 * POST /api/markets/import-mars-fil
 * Bulk update only mars_fil by matching market internal_id
 */
router.post('/import-mars-fil', requireAdmin, async (req: Request, res: Response) => {
  try {
    const entries: Array<{ id: string; mars_fil: string }> = req.body;

    if (!Array.isArray(entries) || entries.length === 0) {
      return res.status(400).json({ error: 'Invalid request: entries array required' });
    }

    console.log(`📥 Updating mars_fil for ${entries.length} markets...`);
    const freshClient = createFreshClient();

    let updated = 0;
    for (const entry of entries) {
      if (!entry.id || !entry.mars_fil) continue;
      const { error } = await freshClient
        .from('markets')
        .update({ mars_fil: entry.mars_fil })
        .eq('internal_id', entry.id);
      if (!error) updated++;
    }

    console.log(`✅ Updated mars_fil for ${updated} markets`);
    res.json({ success: updated, failed: entries.length - updated });
  } catch (error: any) {
    console.error('Error updating mars_fil:');
    sendInternalError(res);
  }
});

/**
 * POST /api/markets/import
 * Bulk import markets
 */
router.post('/import', requireAdmin, async (req: Request, res: Response) => {
  try {
    const markets = req.body;

    if (!Array.isArray(markets) || markets.length === 0) {
      return res.status(400).json({ error: 'Invalid request: markets array required' });
    }

    console.log(`📥 Importing ${markets.length} markets...`);
    
    const freshClient = createFreshClient();

    const marketPayloads = markets
      .map(pickMarketWriteFields)
      .filter((market) => Object.keys(market).length > 0);

    if (marketPayloads.length === 0) {
      return res.status(400).json({ error: 'No valid market rows provided' });
    }

    const { data, error } = await freshClient
      .from('markets')
      .upsert(marketPayloads, {
        onConflict: 'id',
        ignoreDuplicates: false 
      })
      .select(MARKET_SELECT_COLUMNS);

    if (error) {
      logMarketDbError('Error importing market rows', error);
      throw error;
    }

    console.log(`✅ Successfully imported ${data?.length || 0} markets`);
    res.json({
      success: data?.length || 0,
      failed: markets.length - (data?.length || 0),
    });
  } catch (error: any) {
    console.error('Error importing markets:');
    sendInternalError(res);
  }
});

/**
 * PUT /api/markets/:id
 * Update a market
 */
router.put('/:id', requireAdmin, async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    console.log(`✏️ Updating market ${id}...`);
    
    const freshClient = createFreshClient();

    const marketPayload = pickMarketWriteFields(req.body);
    delete marketPayload.id;

    if (Object.keys(marketPayload).length === 0) {
      return res.status(400).json({ error: 'No valid market fields provided' });
    }

    const { data, error } = await freshClient
      .from('markets')
      .update(marketPayload)
      .eq('id', id)
      .select(MARKET_SELECT_COLUMNS)
      .single();

    if (error) {
      logMarketDbError('Error updating market row', error);
      throw error;
    }

    console.log(`✅ Updated market ${id}`);
    res.json(data);
  } catch (error: any) {
    console.error('Error updating market:');
    sendInternalError(res);
  }
});

/**
 * POST /api/markets/:id/visit
 * Record a visit to a market (increments current_visits if not already visited today)
 */
router.post('/:id/visit', async (req: AuthRequest, res: Response) => {
  try {
    const { id } = req.params;
    const { gl_id } = req.body; // Optional for admins: track which GL visited
    const effectiveGlId = req.user?.role === 'admin' ? gl_id : getAuthenticatedGlId(req.user);
    
    console.log(`📍 Recording visit for market ${id}...`);
    
    const freshClient = createFreshClient();

    // Get current market data
    const { data: market, error: fetchError } = await freshClient
      .from('markets')
      .select('current_visits, last_visit_date')
      .eq('id', id)
      .single();

    if (fetchError) {
      console.error('Error fetching market:');
      throw fetchError;
    }

    if (!market) {
      return res.status(404).json({ error: 'Market not found' });
    }

    const today = new Date().toISOString().split('T')[0]; // YYYY-MM-DD
    const lastVisit = market.last_visit_date;

    // Check if already visited today
    if (lastVisit === today) {
      console.log(`ℹ️ Market ${id} already visited today, not incrementing`);
      return res.json({ 
        message: 'Already visited today',
        current_visits: market.current_visits,
        last_visit_date: lastVisit,
        incremented: false
      });
    }

    // Increment visit count and update last visit date
    const newVisitCount = (market.current_visits || 0) + 1;
    
    const { data: updated, error: updateError } = await freshClient
      .from('markets')
      .update({
        current_visits: newVisitCount,
        last_visit_date: today
      })
      .eq('id', id)
      .select(MARKET_SELECT_COLUMNS)
      .single();

    if (updateError) {
      console.error('Error updating market visit:');
      throw updateError;
    }

    await freshClient
      .from('market_visits')
      .upsert({
        market_id: id,
        gebietsleiter_id: effectiveGlId || null,
        visit_date: today,
        source: 'manual'
      }, { onConflict: 'market_id,visit_date', ignoreDuplicates: true });

    console.log(`✅ Recorded visit for market ${id}: ${newVisitCount} total visits`);
    res.json({
      message: 'Visit recorded',
      current_visits: newVisitCount,
      last_visit_date: today,
      incremented: true
    });
  } catch (error: any) {
    console.error('Error recording market visit:');
    sendInternalError(res);
  }
});

/**
 * GET /api/markets/:id/history
 * Get all activities/history for a specific market
 */
router.get('/:id/history', async (req: AuthRequest, res: Response) => {
  try {
    const { id } = req.params;
    const requestedGlId = req.query.gl_id as string | undefined;
    const glId = req.user?.role === 'admin' ? requestedGlId : getAuthenticatedGlId(req.user);
    console.log('Fetching market history');
    
    const freshClient = createFreshClient();
    const activities: any[] = [];

    // 1. Get Vorbesteller submissions from wellen_submissions table (individual actions)
    let wellenQuery = freshClient
      .from('wellen_submissions')
      .select(MARKET_HISTORY_WELLEN_SUBMISSION_SELECT)
      .eq('market_id', id)
      .order('created_at', { ascending: false });
    if (glId) wellenQuery = wellenQuery.eq('gebietsleiter_id', glId);
    const { data: submissionsData } = await wellenQuery;
    
    if (submissionsData && submissionsData.length > 0) {
      // Get item names from correct tables
      const displayIds = submissionsData.filter(s => s.item_type === 'display').map(s => s.item_id);
      const kartonwareIds = submissionsData.filter(s => s.item_type === 'kartonware').map(s => s.item_id);
      const paletteProductIds = submissionsData.filter(s => s.item_type === 'palette').map(s => s.item_id);
      const schutteProductIds = submissionsData.filter(s => s.item_type === 'schuette').map(s => s.item_id);
      
      const [displaysRes, kartonwareRes, paletteProductsRes, schutteProductsRes] = await Promise.all([
        displayIds.length > 0 ? freshClient.from('wellen_displays').select('id, name').in('id', displayIds) : { data: [] },
        kartonwareIds.length > 0 ? freshClient.from('wellen_kartonware').select('id, name').in('id', kartonwareIds) : { data: [] },
        paletteProductIds.length > 0 ? freshClient.from('wellen_paletten_products').select('id, name, palette_id, value_per_ve').in('id', paletteProductIds) : { data: [] },
        schutteProductIds.length > 0 ? freshClient.from('wellen_schuetten_products').select('id, name, schuette_id, value_per_ve').in('id', schutteProductIds) : { data: [] }
      ]);
      
      const displays = displaysRes.data || [];
      const kartonware = kartonwareRes.data || [];
      const paletteProducts = paletteProductsRes.data || [];
      const schutteProducts = schutteProductsRes.data || [];
      
      // Fetch parent palette/schuette names
      const paletteParentIds = [...new Set((paletteProducts || []).map((p: any) => p.palette_id))].filter(Boolean);
      const schutteParentIds = [...new Set((schutteProducts || []).map((p: any) => p.schuette_id))].filter(Boolean);
      
      const [palettesRes, schuttenRes] = await Promise.all([
        paletteParentIds.length > 0 ? freshClient.from('wellen_paletten').select('id, name').in('id', paletteParentIds) : { data: [] },
        schutteParentIds.length > 0 ? freshClient.from('wellen_schuetten').select('id, name').in('id', schutteParentIds) : { data: [] }
      ]);
      
      const palettes = palettesRes.data || [];
      const schutten = schuttenRes.data || [];
      
      // Process display/kartonware entries (standard, no grouping needed)
      for (const s of submissionsData.filter(sub => sub.item_type === 'display' || sub.item_type === 'kartonware')) {
        let itemName = 'Unbekannt';
        if (s.item_type === 'display') {
          itemName = displays.find((d: any) => d.id === s.item_id)?.name || 'Display';
        } else if (s.item_type === 'kartonware') {
          itemName = kartonware.find((k: any) => k.id === s.item_id)?.name || 'Kartonware';
        }
        
        activities.push({
          id: s.id,
          type: 'vorbesteller',
          date: s.created_at,
          glName: relatedName(s.gebietsleiter) || 'Unbekannt',
          glId: s.gebietsleiter_id,
          details: {
            welleName: relatedName(s.wellen) || 'Unbekannt',
            itemType: s.item_type,
            itemName,
            quantity: s.quantity
          }
        });
      }
      
      // Group palette submissions by parent palette (within same welle and time window)
      const paletteSubmissions = submissionsData.filter(sub => sub.item_type === 'palette');
      const paletteGroups = new Map<string, any[]>();
      
      for (const sub of paletteSubmissions) {
        const product = paletteProducts.find((p: any) => p.id === sub.item_id);
        const parentId = product?.palette_id || 'unknown';
        const timeBucket = Math.floor(new Date(sub.created_at).getTime() / (5 * 60 * 1000));
        const key = `${sub.welle_id}|${parentId}|${timeBucket}`;
        
        if (!paletteGroups.has(key)) {
          paletteGroups.set(key, []);
        }
        paletteGroups.get(key)!.push({ ...sub, product });
      }
      
      for (const [, subs] of paletteGroups) {
        const firstSub = subs[0];
        const parentPalette = palettes.find((p: any) => p.id === firstSub.product?.palette_id);
        
        const products = subs.map((sub: any) => ({
          id: sub.item_id,
          name: sub.product?.name || 'Produkt',
          quantity: sub.quantity,
          valuePerUnit: sub.value_per_unit || sub.product?.value_per_ve || 0
        }));
        
        const totalValue = products.reduce((sum: number, p: any) => sum + (p.quantity * p.valuePerUnit), 0);
        
        activities.push({
          id: subs.map((s: any) => s.id).join(','),
          type: 'vorbesteller',
          date: firstSub.created_at,
          glName: relatedName(firstSub.gebietsleiter) || 'Unbekannt',
          glId: firstSub.gebietsleiter_id,
          details: {
            welleName: relatedName(firstSub.wellen) || 'Unbekannt',
            itemType: 'palette',
            itemName: parentPalette?.name || 'Palette',
            parentId: firstSub.product?.palette_id,
            products,
            totalValue,
            quantity: 1
          }
        });
      }
      
      // Group schuette submissions by parent schuette (within same welle and time window)
      const schutteSubmissions = submissionsData.filter(sub => sub.item_type === 'schuette');
      const schutteGroups = new Map<string, any[]>();
      
      for (const sub of schutteSubmissions) {
        const product = schutteProducts.find((p: any) => p.id === sub.item_id);
        const parentId = product?.schuette_id || 'unknown';
        const timeBucket = Math.floor(new Date(sub.created_at).getTime() / (5 * 60 * 1000));
        const key = `${sub.welle_id}|${parentId}|${timeBucket}`;
        
        if (!schutteGroups.has(key)) {
          schutteGroups.set(key, []);
        }
        schutteGroups.get(key)!.push({ ...sub, product });
      }
      
      for (const [, subs] of schutteGroups) {
        const firstSub = subs[0];
        const parentSchutte = schutten.find((s: any) => s.id === firstSub.product?.schuette_id);
        
        const products = subs.map((sub: any) => ({
          id: sub.item_id,
          name: sub.product?.name || 'Produkt',
          quantity: sub.quantity,
          valuePerUnit: sub.value_per_unit || sub.product?.value_per_ve || 0
        }));
        
        const totalValue = products.reduce((sum: number, p: any) => sum + (p.quantity * p.valuePerUnit), 0);
        
        activities.push({
          id: subs.map((s: any) => s.id).join(','),
          type: 'vorbesteller',
          date: firstSub.created_at,
          glName: relatedName(firstSub.gebietsleiter) || 'Unbekannt',
          glId: firstSub.gebietsleiter_id,
          details: {
            welleName: relatedName(firstSub.wellen) || 'Unbekannt',
            itemType: 'schuette',
            itemName: parentSchutte?.name || 'Schütte',
            parentId: firstSub.product?.schuette_id,
            products,
            totalValue,
            quantity: 1
          }
        });
      }
    }

    // 2. Get Vorverkauf submissions
    let vvQuery = freshClient
      .from('vorverkauf_submissions')
      .select(MARKET_HISTORY_VORVERKAUF_SUBMISSION_SELECT)
      .eq('market_id', id)
      .order('created_at', { ascending: false });
    if (glId) vvQuery = vvQuery.eq('gebietsleiter_id', glId);
    const { data: vorverkaufData } = await vvQuery;
    
    if (vorverkaufData) {
      // Get products for each submission
      for (const sub of vorverkaufData) {
        const { data: products } = await freshClient
          .from('vorverkauf_submission_products')
          .select(MARKET_HISTORY_VORVERKAUF_PRODUCT_SELECT)
          .eq('submission_id', sub.id);
        
        activities.push({
          id: sub.id,
          type: 'vorverkauf',
          date: sub.created_at,
          glName: relatedName(sub.gebietsleiter) || 'Unbekannt',
          glId: sub.gebietsleiter_id,
          details: {
            welleName: relatedName(sub.vorverkauf_wellen) || 'Vorverkauf',
            products: (products || []).map((p: any) => ({
              name: relatedName(p.products) || 'Produkt',
              quantity: p.quantity,
              reason: p.reason
            })),
            notes: sub.notes
          }
        });
      }
    }

    // 3. Get Produkttausch entries (vorverkauf_entries)
    let ptQuery = freshClient
      .from('vorverkauf_entries')
      .select(MARKET_HISTORY_VORVERKAUF_ENTRY_SELECT)
      .eq('market_id', id)
      .order('created_at', { ascending: false });
    if (glId) ptQuery = ptQuery.eq('gebietsleiter_id', glId);
    const { data: produkttauschData } = await ptQuery;
    
    if (produkttauschData) {
      for (const entry of produkttauschData) {
        // Get items for this entry
        const { data: items } = await freshClient
          .from('vorverkauf_items')
          .select(MARKET_HISTORY_VORVERKAUF_ITEM_SELECT)
          .eq('vorverkauf_entry_id', entry.id);
        
        activities.push({
          id: entry.id,
          type: 'produkttausch',
          date: entry.created_at,
          glName: relatedName(entry.gebietsleiter) || 'Unbekannt',
          glId: entry.gebietsleiter_id,
          details: {
            reason: entry.reason,
            items: (items || []).map((i: any) => ({
              name: relatedName(i.products) || 'Produkt',
              quantity: i.quantity,
              itemType: i.item_type
            })),
            notes: entry.notes
          }
        });
      }
    }

    // 4. Get zeiterfassung visit entries for this market
    let zeQuery = freshClient
      .from('fb_zeiterfassung_submissions')
      .select('id, gebietsleiter_id, market_id, besuchszeit_von, besuchszeit_bis, created_at')
      .eq('market_id', id)
      .order('created_at', { ascending: false });
    if (glId) zeQuery = zeQuery.eq('gebietsleiter_id', glId);
    const { data: zeiterfassungData } = await zeQuery;

    if (zeiterfassungData) {
      for (const ze of zeiterfassungData) {
        activities.push({
          id: ze.id,
          type: 'marktbesuch',
          date: ze.created_at,
          glName: '',
          glId: ze.gebietsleiter_id,
          details: {
            besuchszeitVon: ze.besuchszeit_von,
            besuchszeitBis: ze.besuchszeit_bis
          }
        });
      }
    }

    // 5. Get visit-only entries from market_visits (exclude dates already covered by other sources)
    const existingDates = new Set(
      activities.map(a => new Date(a.date).toISOString().split('T')[0])
    );

    let mvQuery = freshClient
      .from('market_visits')
      .select('id, market_id, gebietsleiter_id, visit_date, source, created_at')
      .eq('market_id', id)
      .order('visit_date', { ascending: false });
    if (glId) mvQuery = mvQuery.eq('gebietsleiter_id', glId);
    const { data: marketVisitsData } = await mvQuery;

    if (marketVisitsData) {
      const mvGlIds = [...new Set(marketVisitsData.map(mv => mv.gebietsleiter_id).filter(Boolean))];
      const { data: mvGlDetails } = await freshClient
        .from('gebietsleiter')
        .select('id, name')
        .in('id', mvGlIds.length > 0 ? mvGlIds : ['__none__']);

      for (const mv of marketVisitsData) {
        if (!existingDates.has(mv.visit_date)) {
          const mvGl = (mvGlDetails || []).find((g: any) => g.id === mv.gebietsleiter_id);
          activities.push({
            id: mv.id,
            type: 'marktbesuch',
            date: mv.created_at,
            glName: mvGl?.name || 'Unbekannt',
            glId: mv.gebietsleiter_id,
            details: { source: mv.source }
          });
        }
      }
    }

    // Sort all activities by date (newest first)
    activities.sort((a, b) => new Date(b.date).getTime() - new Date(a.date).getTime());

    console.log(`✅ Fetched ${activities.length} history entries for market ${id}`);
    res.json(activities);
  } catch (error: any) {
    console.error('Error fetching market history:');
    sendInternalError(res);
  }
});

/**
 * GET /api/markets/:id/visit-crm
 * Get compact GL-scoped CRM context for a market visit overlay.
 */
router.get('/:id/visit-crm', async (req: AuthRequest, res: Response) => {
  try {
    const { id: marketId } = req.params;
    const requestedGlId = String(req.query.gl_id || '').trim();
    const glId = req.user?.role === 'admin' ? requestedGlId : (getAuthenticatedGlId(req.user) || '');
    const SECTION_LIMIT = 20;

    if (!glId) {
      return res.status(400).json({ error: 'gl_id is required' });
    }

    const toDateKey = (value?: string | null): string | null => {
      if (!value) return null;
      const date = new Date(value);
      if (Number.isNaN(date.getTime())) return null;
      return date.toISOString().split('T')[0];
    };

    const toTimeLabel = (value?: string | null): string => {
      if (!value) return '';
      const date = new Date(value);
      if (Number.isNaN(date.getTime())) return '';
      return date.toLocaleTimeString('de-DE', { hour: '2-digit', minute: '2-digit' });
    };

    const formatCurrency = (value: number): string =>
      new Intl.NumberFormat('de-DE', { style: 'currency', currency: 'EUR' }).format(value || 0);

    const freshClient = createFreshClient();

    const { data: marketRow, error: marketError } = await freshClient
      .from('markets')
      .select('id, name, chain, address, postal_code, city')
      .eq('id', marketId)
      .single();

    if (marketError) throw marketError;
    if (!marketRow) {
      return res.status(404).json({ error: 'Market not found' });
    }

    // Latest visit context from zeiterfassung (preferred source for comment + time window).
    const { data: latestVisitRows, error: latestVisitError } = await freshClient
      .from('fb_zeiterfassung_submissions')
      .select('id, created_at, besuchszeit_von, besuchszeit_bis, kommentar')
      .eq('market_id', marketId)
      .eq('gebietsleiter_id', glId)
      .order('created_at', { ascending: false })
      .limit(1);
    if (latestVisitError) throw latestVisitError;
    const latestVisit = latestVisitRows?.[0] || null;

    // -----------------------------------------------------------------------
    // Fragebogen section
    // -----------------------------------------------------------------------
    const { data: fbResponses, error: fbResponsesError } = await freshClient
      .from('fb_responses')
      .select(`
        id,
        fragebogen_id,
        status,
        started_at,
        completed_at,
        zeiterfassung_submission_id,
        fragebogen:fb_fragebogen (id, name)
      `)
      .eq('market_id', marketId)
      .eq('gebietsleiter_id', glId)
      .order('started_at', { ascending: false })
      .limit(SECTION_LIMIT);
    if (fbResponsesError) throw fbResponsesError;

    const fbResponseIds = (fbResponses || []).map((r: any) => r.id).filter(Boolean);
    const zeiterfassungIdsFromResponses = Array.from(
      new Set((fbResponses || []).map((r: any) => r.zeiterfassung_submission_id).filter(Boolean))
    );

    const [fbAnswersResult, fbResponseVisitResult] = await Promise.all([
      fbResponseIds.length > 0
        ? freshClient
            .from('fb_response_answers')
            .select(`
              id,
              response_id,
              question_id,
              question_type,
              answer_text,
              answer_numeric,
              answer_boolean,
              answer_json,
              answer_file_url,
              answered_at,
              question:fb_questions!question_id (id, type, question_text, options, matrix_config, likert_scale, slider_config)
            `)
            .in('response_id', fbResponseIds)
            .order('answered_at', { ascending: true })
        : Promise.resolve({ data: [], error: null } as any),
      zeiterfassungIdsFromResponses.length > 0
        ? freshClient
            .from('fb_zeiterfassung_submissions')
            .select('id, kommentar, besuchszeit_von, besuchszeit_bis, created_at')
            .in('id', zeiterfassungIdsFromResponses)
        : Promise.resolve({ data: [], error: null } as any)
    ]);
    if (fbAnswersResult.error) throw fbAnswersResult.error;
    if (fbResponseVisitResult.error) throw fbResponseVisitResult.error;

    const answersByResponseId = new Map<string, any[]>();
    (fbAnswersResult.data || []).forEach((answer: any) => {
      const existing = answersByResponseId.get(answer.response_id) || [];
      existing.push(answer);
      answersByResponseId.set(answer.response_id, existing);
    });

    const responseVisitById = new Map<string, any>(
      (fbResponseVisitResult.data || []).map((row: any) => [row.id, row])
    );

    const mapFragebogenAnswerPreview = (answer: any): string => {
      const qType = answer.question?.type || answer.question_type;

      if (qType === 'yesno') {
        if (answer.answer_boolean === true) return 'Ja';
        if (answer.answer_boolean === false) return 'Nein';
        return '—';
      }

      if (qType === 'single_choice') {
        const options = answer.question?.options || [];
        const selected = options.find((o: any) => o.id === answer.answer_text);
        return selected?.label || answer.answer_text || '—';
      }

      if (qType === 'multiple_choice') {
        const options = answer.question?.options || [];
        const values: string[] = Array.isArray(answer.answer_json) ? answer.answer_json : [];
        if (values.length === 0) return '—';
        return values
          .map((id: string) => options.find((o: any) => o.id === id)?.label || id)
          .join(', ');
      }

      if (qType === 'matrix') {
        const matrix = answer.answer_json && typeof answer.answer_json === 'object' ? answer.answer_json : null;
        if (!matrix) return '—';
        const entries = Object.entries(matrix);
        if (entries.length === 0) return '—';
        const first = entries[0];
        return `${first[0]} → ${first[1]}${entries.length > 1 ? ` (+${entries.length - 1})` : ''}`;
      }

      if (qType === 'likert' || qType === 'open_numeric' || qType === 'slider') {
        return answer.answer_numeric !== null && answer.answer_numeric !== undefined
          ? String(answer.answer_numeric)
          : '—';
      }

      if (qType === 'photo_upload') {
        return answer.answer_file_url ? 'Foto hochgeladen' : '—';
      }

      if (answer.answer_text !== null && answer.answer_text !== undefined && String(answer.answer_text).trim() !== '') {
        return String(answer.answer_text);
      }
      if (answer.answer_json !== null && answer.answer_json !== undefined) {
        return JSON.stringify(answer.answer_json);
      }
      return '—';
    };

    const fragebogenItems = (fbResponses || []).map((response: any) => {
      const responseAnswers = answersByResponseId.get(response.id) || [];
      const answerPreview = responseAnswers.slice(0, 5).map((answer: any) => ({
        question: answer.question?.question_text || 'Frage',
        value: mapFragebogenAnswerPreview(answer)
      }));
      const linkedVisit = response.zeiterfassung_submission_id
        ? responseVisitById.get(response.zeiterfassung_submission_id)
        : null;

      const timestamp = response.completed_at || response.started_at || linkedVisit?.created_at;

      return {
        id: response.id,
        type: 'fragebogen',
        timestamp,
        title: response.fragebogen?.name || 'Fragebogen',
        subtitle: response.status === 'completed' ? 'Abgeschlossen' : 'In Bearbeitung',
        meta: [
          `Status: ${response.status === 'completed' ? 'Abgeschlossen' : 'In Bearbeitung'}`,
          `Antworten: ${responseAnswers.length}`
        ],
        comment: linkedVisit?.kommentar || null,
        details: {
          startedAt: response.started_at,
          completedAt: response.completed_at,
          answerPreview
        }
      };
    });

    // -----------------------------------------------------------------------
    // Vorbesteller section
    // -----------------------------------------------------------------------
    const { data: vorbestellerRows, error: vorbestellerError } = await freshClient
      .from('wellen_submissions')
      .select('id, welle_id, item_type, item_id, quantity, value_per_unit, created_at')
      .eq('market_id', marketId)
      .eq('gebietsleiter_id', glId)
      .order('created_at', { ascending: false })
      .limit(SECTION_LIMIT * 8);
    if (vorbestellerError) throw vorbestellerError;

    const vorbestellerWelleIds = Array.from(new Set((vorbestellerRows || []).map((row: any) => row.welle_id).filter(Boolean)));
    const displayIds = (vorbestellerRows || []).filter((row: any) => row.item_type === 'display').map((row: any) => row.item_id);
    const kartonwareIds = (vorbestellerRows || []).filter((row: any) => row.item_type === 'kartonware').map((row: any) => row.item_id);
    const einzelproduktIds = (vorbestellerRows || []).filter((row: any) => row.item_type === 'einzelprodukt').map((row: any) => row.item_id);
    const paletteProductIds = (vorbestellerRows || []).filter((row: any) => row.item_type === 'palette').map((row: any) => row.item_id);
    const schutteProductIds = (vorbestellerRows || []).filter((row: any) => row.item_type === 'schuette').map((row: any) => row.item_id);

    const [wellenResult, displaysResult, kartonwareResult, paletteProductsResult, schutteProductsResult, einzelprodukteResult, vbPhotoCommentsResult] = await Promise.all([
      vorbestellerWelleIds.length > 0
        ? freshClient.from('wellen').select('id, name').in('id', vorbestellerWelleIds)
        : Promise.resolve({ data: [], error: null } as any),
      displayIds.length > 0
        ? freshClient.from('wellen_displays').select('id, name').in('id', displayIds)
        : Promise.resolve({ data: [], error: null } as any),
      kartonwareIds.length > 0
        ? freshClient.from('wellen_kartonware').select('id, name').in('id', kartonwareIds)
        : Promise.resolve({ data: [], error: null } as any),
      paletteProductIds.length > 0
        ? freshClient.from('wellen_paletten_products').select('id, name').in('id', paletteProductIds)
        : Promise.resolve({ data: [], error: null } as any),
      schutteProductIds.length > 0
        ? freshClient.from('wellen_schuetten_products').select('id, name').in('id', schutteProductIds)
        : Promise.resolve({ data: [], error: null } as any),
      einzelproduktIds.length > 0
        ? freshClient.from('wellen_einzelprodukte').select('id, name').in('id', einzelproduktIds)
        : Promise.resolve({ data: [], error: null } as any),
      freshClient
        .from('wellen_photos')
        .select('created_at, comment')
        .eq('market_id', marketId)
        .eq('gebietsleiter_id', glId)
        .not('comment', 'is', null)
        .order('created_at', { ascending: false })
        .limit(100)
    ]);

    if (wellenResult.error) throw wellenResult.error;
    if (displaysResult.error) throw displaysResult.error;
    if (kartonwareResult.error) throw kartonwareResult.error;
    if (paletteProductsResult.error) throw paletteProductsResult.error;
    if (schutteProductsResult.error) throw schutteProductsResult.error;
    if (einzelprodukteResult.error) throw einzelprodukteResult.error;
    if (vbPhotoCommentsResult.error) throw vbPhotoCommentsResult.error;

    const wellenById = new Map<string, any>((wellenResult.data || []).map((row: any) => [row.id, row]));
    const displaysById = new Map<string, any>((displaysResult.data || []).map((row: any) => [row.id, row]));
    const kartonwareById = new Map<string, any>((kartonwareResult.data || []).map((row: any) => [row.id, row]));
    const paletteProductsById = new Map<string, any>((paletteProductsResult.data || []).map((row: any) => [row.id, row]));
    const schutteProductsById = new Map<string, any>((schutteProductsResult.data || []).map((row: any) => [row.id, row]));
    const einzelprodukteById = new Map<string, any>((einzelprodukteResult.data || []).map((row: any) => [row.id, row]));

    const vbCommentByDate = new Map<string, string>();
    (vbPhotoCommentsResult.data || []).forEach((row: any) => {
      const dateKey = toDateKey(row.created_at);
      if (!dateKey || !row.comment || vbCommentByDate.has(dateKey)) return;
      vbCommentByDate.set(dateKey, row.comment);
    });

    const vorbestellerItems = (vorbestellerRows || [])
      .map((row: any) => {
        let itemName = 'Produkt';
        if (row.item_type === 'display') itemName = displaysById.get(row.item_id)?.name || 'Display';
        if (row.item_type === 'kartonware') itemName = kartonwareById.get(row.item_id)?.name || 'Kartonware';
        if (row.item_type === 'einzelprodukt') itemName = einzelprodukteById.get(row.item_id)?.name || 'Einzelprodukt';
        if (row.item_type === 'palette') itemName = paletteProductsById.get(row.item_id)?.name || 'Palettenprodukt';
        if (row.item_type === 'schuette') itemName = schutteProductsById.get(row.item_id)?.name || 'Schüttenprodukt';

        const welleName = wellenById.get(row.welle_id)?.name || 'Vorbesteller';
        const totalValue = (row.value_per_unit || 0) * (row.quantity || 0);
        const dateKey = toDateKey(row.created_at);

        return {
          id: row.id,
          type: 'vorbesteller',
          timestamp: row.created_at,
          title: welleName,
          subtitle: `${itemName} · ${row.quantity}x`,
          meta: [
            `Typ: ${row.item_type}`,
            row.value_per_unit ? `Wert: ${formatCurrency(totalValue)}` : ''
          ].filter(Boolean),
          comment: dateKey ? vbCommentByDate.get(dateKey) || null : null,
          details: {
            itemType: row.item_type,
            itemName,
            quantity: row.quantity,
            valuePerUnit: row.value_per_unit
          }
        };
      })
      .sort((a: any, b: any) => new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime())
      .slice(0, SECTION_LIMIT);

    // -----------------------------------------------------------------------
    // Vorverkauf section (wave-based submissions)
    // -----------------------------------------------------------------------
    const { data: vorverkaufRows, error: vorverkaufError } = await freshClient
      .from('vorverkauf_submissions')
      .select('id, vorverkauf_welle_id, notes, created_at')
      .eq('market_id', marketId)
      .eq('gebietsleiter_id', glId)
      .order('created_at', { ascending: false })
      .limit(SECTION_LIMIT);
    if (vorverkaufError) throw vorverkaufError;

    const vorverkaufIds = (vorverkaufRows || []).map((row: any) => row.id).filter(Boolean);
    const vorverkaufWelleIds = Array.from(
      new Set((vorverkaufRows || []).map((row: any) => row.vorverkauf_welle_id).filter(Boolean))
    );

    const [vorverkaufWellenResult, vorverkaufProductsResult] = await Promise.all([
      vorverkaufWelleIds.length > 0
        ? freshClient.from('vorverkauf_wellen').select('id, name').in('id', vorverkaufWelleIds)
        : Promise.resolve({ data: [], error: null } as any),
      vorverkaufIds.length > 0
        ? freshClient
            .from('vorverkauf_submission_products')
            .select('submission_id, product_id, quantity, reason')
            .in('submission_id', vorverkaufIds)
        : Promise.resolve({ data: [], error: null } as any)
    ]);
    if (vorverkaufWellenResult.error) throw vorverkaufWellenResult.error;
    if (vorverkaufProductsResult.error) throw vorverkaufProductsResult.error;

    const vorverkaufWelleById = new Map<string, any>((vorverkaufWellenResult.data || []).map((row: any) => [row.id, row]));
    const vorverkaufProductIds = Array.from(
      new Set((vorverkaufProductsResult.data || []).map((row: any) => row.product_id).filter(Boolean))
    );
    const { data: vorverkaufProductRows, error: vorverkaufProductRowsError } = vorverkaufProductIds.length > 0
      ? await freshClient.from('products').select('id, name').in('id', vorverkaufProductIds)
      : { data: [], error: null } as any;
    if (vorverkaufProductRowsError) throw vorverkaufProductRowsError;
    const vorverkaufProductById = new Map<string, any>((vorverkaufProductRows || []).map((row: any) => [row.id, row]));

    const vorverkaufProductsBySubmissionId = new Map<string, any[]>();
    (vorverkaufProductsResult.data || []).forEach((row: any) => {
      const existing = vorverkaufProductsBySubmissionId.get(row.submission_id) || [];
      existing.push(row);
      vorverkaufProductsBySubmissionId.set(row.submission_id, existing);
    });

    const vorverkaufItems = (vorverkaufRows || []).map((row: any) => {
      const products = vorverkaufProductsBySubmissionId.get(row.id) || [];
      const productMeta = products.slice(0, 4).map((p: any) =>
        `${vorverkaufProductById.get(p.product_id)?.name || 'Produkt'} (${p.quantity}x${p.reason ? ` · ${p.reason}` : ''})`
      );

      return {
        id: row.id,
        type: 'vorverkauf',
        timestamp: row.created_at,
        title: vorverkaufWelleById.get(row.vorverkauf_welle_id)?.name || 'Vorverkauf',
        subtitle: `${products.length} Produkte`,
        meta: productMeta,
        comment: row.notes || null,
        details: {
          products: products.map((p: any) => ({
            name: vorverkaufProductById.get(p.product_id)?.name || 'Produkt',
            quantity: p.quantity,
            reason: p.reason
          }))
        }
      };
    });

    // -----------------------------------------------------------------------
    // Produkttausch section (direct entries in vorverkauf_entries)
    // -----------------------------------------------------------------------
    const { data: produkttauschRows, error: produkttauschError } = await freshClient
      .from('vorverkauf_entries')
      .select('id, reason, notes, status, created_at')
      .eq('market_id', marketId)
      .eq('gebietsleiter_id', glId)
      .order('created_at', { ascending: false })
      .limit(SECTION_LIMIT);
    if (produkttauschError) throw produkttauschError;

    const produkttauschIds = (produkttauschRows || []).map((row: any) => row.id).filter(Boolean);
    const { data: produkttauschItemsRows, error: produkttauschItemsError } = produkttauschIds.length > 0
      ? await freshClient
          .from('vorverkauf_items')
          .select('vorverkauf_entry_id, product_id, quantity, item_type')
          .in('vorverkauf_entry_id', produkttauschIds)
      : { data: [], error: null } as any;
    if (produkttauschItemsError) throw produkttauschItemsError;

    const produkttauschProductIds = Array.from(
      new Set((produkttauschItemsRows || []).map((row: any) => row.product_id).filter(Boolean))
    );
    const { data: produkttauschProductRows, error: produkttauschProductRowsError } = produkttauschProductIds.length > 0
      ? await freshClient.from('products').select('id, name').in('id', produkttauschProductIds)
      : { data: [], error: null } as any;
    if (produkttauschProductRowsError) throw produkttauschProductRowsError;
    const produkttauschProductById = new Map<string, any>((produkttauschProductRows || []).map((row: any) => [row.id, row]));

    const produkttauschItemsByEntryId = new Map<string, any[]>();
    (produkttauschItemsRows || []).forEach((row: any) => {
      const existing = produkttauschItemsByEntryId.get(row.vorverkauf_entry_id) || [];
      existing.push(row);
      produkttauschItemsByEntryId.set(row.vorverkauf_entry_id, existing);
    });

    const produkttauschItems = (produkttauschRows || []).map((row: any) => {
      const items = produkttauschItemsByEntryId.get(row.id) || [];
      const takeOutCount = items.filter((i: any) => i.item_type === 'take_out').length;
      const replaceCount = items.filter((i: any) => i.item_type === 'replace').length;

      return {
        id: row.id,
        type: 'produkttausch',
        timestamp: row.created_at,
        title: 'Produkttausch',
        subtitle: `Entnommen: ${takeOutCount} · Ersetzt: ${replaceCount}`,
        meta: [`Status: ${row.status || 'completed'}`],
        comment: row.notes || null,
        details: {
          reason: row.reason,
          takeOut: items
            .filter((i: any) => i.item_type === 'take_out')
            .map((i: any) => ({ name: produkttauschProductById.get(i.product_id)?.name || 'Produkt', quantity: i.quantity })),
          replace: items
            .filter((i: any) => i.item_type === 'replace')
            .map((i: any) => ({ name: produkttauschProductById.get(i.product_id)?.name || 'Produkt', quantity: i.quantity }))
        }
      };
    });

    // -----------------------------------------------------------------------
    // NARA section
    // -----------------------------------------------------------------------
    const { data: naraRows, error: naraError } = await freshClient
      .from('nara_incentive_submissions')
      .select('id, created_at')
      .eq('market_id', marketId)
      .eq('gebietsleiter_id', glId)
      .order('created_at', { ascending: false })
      .limit(SECTION_LIMIT);
    if (naraError) throw naraError;

    const naraIds = (naraRows || []).map((row: any) => row.id).filter(Boolean);
    const { data: naraItemRows, error: naraItemsError } = naraIds.length > 0
      ? await freshClient
          .from('nara_incentive_items')
          .select('submission_id, product_id, quantity')
          .in('submission_id', naraIds)
      : { data: [], error: null } as any;
    if (naraItemsError) throw naraItemsError;

    const naraProductIds = Array.from(new Set((naraItemRows || []).map((row: any) => row.product_id).filter(Boolean)));
    const { data: naraProductRows, error: naraProductRowsError } = naraProductIds.length > 0
      ? await freshClient.from('products').select('id, name, price').in('id', naraProductIds)
      : { data: [], error: null } as any;
    if (naraProductRowsError) throw naraProductRowsError;
    const naraProductById = new Map<string, any>((naraProductRows || []).map((row: any) => [row.id, row]));

    const naraItemsBySubmissionId = new Map<string, any[]>();
    (naraItemRows || []).forEach((row: any) => {
      const existing = naraItemsBySubmissionId.get(row.submission_id) || [];
      existing.push(row);
      naraItemsBySubmissionId.set(row.submission_id, existing);
    });

    const naraItems = (naraRows || []).map((row: any) => {
      const items = naraItemsBySubmissionId.get(row.id) || [];
      const totalValue = items.reduce(
        (sum: number, item: any) => sum + (item.quantity || 0) * (naraProductById.get(item.product_id)?.price || 0),
        0
      );

      return {
        id: row.id,
        type: 'nara',
        timestamp: row.created_at,
        title: 'NARA-Incentive',
        subtitle: `${items.length} Produkte`,
        meta: [`Gesamtwert: ${formatCurrency(totalValue)}`],
        comment: null,
        details: {
          totalValue,
          products: items.map((item: any) => ({
            name: naraProductById.get(item.product_id)?.name || 'Produkt',
            quantity: item.quantity,
            lineTotal: (item.quantity || 0) * (naraProductById.get(item.product_id)?.price || 0)
          }))
        }
      };
    });

    const sectionEntries: Array<{ section: string; item: any }> = [
      ...fragebogenItems.map((item: any) => ({ section: 'fragebogen', item })),
      ...vorbestellerItems.map((item: any) => ({ section: 'vorbesteller', item })),
      ...vorverkaufItems.map((item: any) => ({ section: 'vorverkauf', item })),
      ...produkttauschItems.map((item: any) => ({ section: 'produkttausch', item })),
      ...naraItems.map((item: any) => ({ section: 'nara', item }))
    ];

    const latestSectionTimestamp = sectionEntries
      .map((entry) => entry.item?.timestamp)
      .filter(Boolean)
      .sort((a: string, b: string) => new Date(b).getTime() - new Date(a).getTime())[0];

    const lastVisitDate = toDateKey(latestVisit?.created_at) || toDateKey(latestSectionTimestamp) || null;

    const actionsOnLastVisit = lastVisitDate
      ? sectionEntries
          .filter((entry) => toDateKey(entry.item?.timestamp) === lastVisitDate)
          .sort((a, b) => new Date(b.item.timestamp).getTime() - new Date(a.item.timestamp).getTime())
          .map((entry) => ({
            section: entry.section,
            ...entry.item
          }))
      : [];

    const sections = {
      fragebogen: {
        count: fragebogenItems.length,
        latest: fragebogenItems[0] || null,
        items: fragebogenItems
      },
      vorbesteller: {
        count: vorbestellerItems.length,
        latest: vorbestellerItems[0] || null,
        items: vorbestellerItems
      },
      vorverkauf: {
        count: vorverkaufItems.length,
        latest: vorverkaufItems[0] || null,
        items: vorverkaufItems
      },
      produkttausch: {
        count: produkttauschItems.length,
        latest: produkttauschItems[0] || null,
        items: produkttauschItems
      },
      nara: {
        count: naraItems.length,
        latest: naraItems[0] || null,
        items: naraItems
      }
    };

    res.json({
      market: {
        id: marketRow.id,
        name: marketRow.name,
        chain: marketRow.chain || '',
        addressLine: marketRow.address || '',
        postalCode: marketRow.postal_code || '',
        city: marketRow.city || '',
        address: [marketRow.address, [marketRow.postal_code, marketRow.city].filter(Boolean).join(' ')]
          .filter(Boolean)
          .join(', ')
      },
      lastVisit: {
        date: lastVisitDate,
        visitComment: latestVisit?.kommentar || null,
        visitWindow: latestVisit
          ? {
              from: latestVisit.besuchszeit_von || null,
              to: latestVisit.besuchszeit_bis || null
            }
          : null,
        timestamp: latestVisit?.created_at || latestSectionTimestamp || null,
        label: latestVisit?.created_at ? toTimeLabel(latestVisit.created_at) : '',
        actionsOnLastVisit
      },
      sections
    });
  } catch (error: any) {
    console.error('Error fetching market visit CRM context:');
    sendInternalError(res);
  }
});

/**
 * DELETE /api/markets/:id
 * Delete a market
 */
router.delete('/:id', requireAdmin, async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    console.log(`🗑️ Deleting market ${id}...`);
    
    const freshClient = createFreshClient();

    const { error } = await freshClient
      .from('markets')
      .delete()
      .eq('id', id);

    if (error) {
      logMarketDbError('Error deleting market row', error);
      throw error;
    }

    console.log(`✅ Deleted market ${id}`);
    res.status(204).send();
  } catch (error: any) {
    console.error('Error deleting market:');
    sendInternalError(res);
  }
});

/**
 * POST /api/markets/sync-visits
 * Recalculate all market visits from historical DB data
 * Sources: vorverkauf_submissions, vorverkauf_entries, wellen_submissions
 * Same market + same day = 1 visit
 */
router.post('/sync-visits', requireAdmin, async (_req: Request, res: Response) => {
  try {
    console.log('🔄 Syncing market visits from historical data...');
    
    const freshClient = createFreshClient();

    // 1. Get all vorverkauf_submissions (Vorverkauf waves)
    const { data: vorverkaufSubmissions } = await freshClient
      .from('vorverkauf_submissions')
      .select('market_id, created_at');

    // 2. Get all vorverkauf_entries (Produkttausch)
    const { data: vorverkaufEntries } = await freshClient
      .from('vorverkauf_entries')
      .select('market_id, created_at');

    // 3. Get all wellen_submissions (Vorbesteller) - individual submissions with market_id
    const { data: wellenSubmissions } = await freshClient
      .from('wellen_submissions')
      .select('market_id, created_at');

    // Build a map of market_id -> Set of unique dates
    const marketVisitDates: Record<string, Set<string>> = {};

    const addVisit = (marketId: string, dateStr: string) => {
      if (!marketId) return;
      const date = new Date(dateStr).toISOString().split('T')[0]; // YYYY-MM-DD
      if (!marketVisitDates[marketId]) {
        marketVisitDates[marketId] = new Set();
      }
      marketVisitDates[marketId].add(date);
    };

    // Add vorverkauf submissions
    for (const sub of (vorverkaufSubmissions || [])) {
      addVisit(sub.market_id, sub.created_at);
    }

    // Add vorverkauf entries (produkttausch)
    for (const entry of (vorverkaufEntries || [])) {
      addVisit(entry.market_id, entry.created_at);
    }

    // Add wellen submissions (vorbesteller) - individual submissions
    for (const sub of (wellenSubmissions || [])) {
      if (sub.market_id) {
        addVisit(sub.market_id, sub.created_at);
      }
    }

    // Calculate totals and find most recent date
    const updates: { marketId: string; visits: number; lastDate: string }[] = [];
    
    for (const [marketId, dates] of Object.entries(marketVisitDates)) {
      const sortedDates = Array.from(dates).sort();
      const lastDate = sortedDates[sortedDates.length - 1];
      updates.push({
        marketId,
        visits: dates.size,
        lastDate
      });
    }

    // Update markets in batches
    let updatedCount = 0;
    for (const update of updates) {
      const { error } = await freshClient
        .from('markets')
        .update({
          current_visits: update.visits,
          last_visit_date: update.lastDate
        })
        .eq('id', update.marketId);
      
      if (!error) {
        updatedCount++;
      }
    }

    console.log(`✅ Synced visits for ${updatedCount} markets`);
    res.json({
      message: 'Visits synced successfully',
      marketsUpdated: updatedCount,
      totalUniqueVisits: updates.reduce((sum, u) => sum + u.visits, 0)
    });
  } catch (error: any) {
    console.error('Error syncing market visits:');
    sendInternalError(res);
  }
});

export default router;
