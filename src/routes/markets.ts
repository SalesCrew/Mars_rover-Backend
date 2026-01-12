import { Router, Request, Response } from 'express';
import { supabase, createFreshClient } from '../config/supabase';

const router = Router();

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
        .select('*')
        .order('name', { ascending: true })
        .range(from, from + pageSize - 1);

      if (error) {
        console.error('Supabase error:', error);
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
    console.error('Error fetching markets:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

/**
 * POST /api/markets/backfill-gl-ids
 * Backfill gebietsleiter_id for markets that have gebietsleiter_name but no gebietsleiter_id
 * Uses fuzzy matching (case insensitive, ignores dashes, extra spaces, etc.)
 * MUST be defined BEFORE /:id routes to avoid being caught by parameter matching
 */
router.post('/backfill-gl-ids', async (req: Request, res: Response) => {
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
      console.log(`  GL: "${gl.name}" -> normalized: "${normalizedName}", email: "${gl.email}"`);
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
        if (matchedGL) {
          console.log(`  ✓ Matched by name: "${market.gebietsleiter_name}" -> GL ${matchedGL.name}`);
        }
      }
      
      // Second try: match by email if name didn't match
      if (!matchedGL && market.gebietsleiter_email) {
        const normalizedEmail = market.gebietsleiter_email.toLowerCase().trim();
        matchedGL = glEmailMap.get(normalizedEmail);
        if (matchedGL) {
          console.log(`  ✓ Matched by email: "${market.gebietsleiter_email}" -> GL ${matchedGL.name}`);
        }
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
          console.error(`  ❌ Failed to update market ${market.id}:`, updateError);
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
      console.log(`⚠️ Unmatched GL names:`, Array.from(unmatchedNames));
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
    console.error('❌ Error during GL ID backfill:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
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
      .select('*')
      .eq('id', id)
      .single();

    if (error) {
      console.error('Supabase error:', error);
      throw error;
    }

    if (!data) {
      return res.status(404).json({ error: 'Market not found' });
    }

    console.log(`✅ Fetched market ${id}`);
    res.json(data);
  } catch (error: any) {
    console.error('Error fetching market:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

/**
 * POST /api/markets
 * Create a new market
 */
router.post('/', async (req: Request, res: Response) => {
  try {
    console.log('➕ Creating new market...');
    
    const freshClient = createFreshClient();
    
    const { data, error } = await freshClient
      .from('markets')
      .insert(req.body)
      .select()
      .single();

    if (error) {
      console.error('Supabase error:', error);
      throw error;
    }

    console.log(`✅ Created market ${data?.id}`);
    res.status(201).json(data);
  } catch (error: any) {
    console.error('Error creating market:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

/**
 * POST /api/markets/import
 * Bulk import markets
 */
router.post('/import', async (req: Request, res: Response) => {
  try {
    const markets = req.body;

    if (!Array.isArray(markets) || markets.length === 0) {
      return res.status(400).json({ error: 'Invalid request: markets array required' });
    }

    console.log(`📥 Importing ${markets.length} markets...`);
    
    const freshClient = createFreshClient();

    const { data, error } = await freshClient
      .from('markets')
      .upsert(markets, { 
        onConflict: 'id',
        ignoreDuplicates: false 
      })
      .select();

    if (error) {
      console.error('Supabase error:', error);
      throw error;
    }

    console.log(`✅ Successfully imported ${data?.length || 0} markets`);
    res.json({
      success: data?.length || 0,
      failed: markets.length - (data?.length || 0),
    });
  } catch (error: any) {
    console.error('Error importing markets:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

/**
 * PUT /api/markets/:id
 * Update a market
 */
router.put('/:id', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    console.log(`✏️ Updating market ${id}...`);
    
    const freshClient = createFreshClient();

    const { data, error } = await freshClient
      .from('markets')
      .update(req.body)
      .eq('id', id)
      .select()
      .single();

    if (error) {
      console.error('Supabase error:', error);
      throw error;
    }

    console.log(`✅ Updated market ${id}`);
    res.json(data);
  } catch (error: any) {
    console.error('Error updating market:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

/**
 * POST /api/markets/:id/visit
 * Record a visit to a market (increments current_visits if not already visited today)
 */
router.post('/:id/visit', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    const { gl_id } = req.body; // Optional: track which GL visited
    
    console.log(`📍 Recording visit for market ${id}...`);
    
    const freshClient = createFreshClient();

    // Get current market data
    const { data: market, error: fetchError } = await freshClient
      .from('markets')
      .select('current_visits, last_visit_date')
      .eq('id', id)
      .single();

    if (fetchError) {
      console.error('Error fetching market:', fetchError);
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
      .select()
      .single();

    if (updateError) {
      console.error('Error updating market visit:', updateError);
      throw updateError;
    }

    console.log(`✅ Recorded visit for market ${id}: ${newVisitCount} total visits`);
    res.json({
      message: 'Visit recorded',
      current_visits: newVisitCount,
      last_visit_date: today,
      incremented: true
    });
  } catch (error: any) {
    console.error('Error recording market visit:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

/**
 * GET /api/markets/:id/history
 * Get all activities/history for a specific market
 */
router.get('/:id/history', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    console.log(`📜 Fetching history for market ${id}...`);
    
    const freshClient = createFreshClient();
    const activities: any[] = [];

    // 1. Get Vorbesteller submissions from wellen_submissions table (individual actions)
    const { data: submissionsData } = await freshClient
      .from('wellen_submissions')
      .select('*, wellen(name), gebietsleiter(name)')
      .eq('market_id', id)
      .order('created_at', { ascending: false });
    
    if (submissionsData && submissionsData.length > 0) {
      // Get item names
      const displayIds = submissionsData.filter(s => s.item_type === 'display').map(s => s.item_id);
      const kartonwareIds = submissionsData.filter(s => s.item_type === 'kartonware').map(s => s.item_id);
      const paletteIds = submissionsData.filter(s => s.item_type === 'palette').map(s => s.item_id);
      const schutteIds = submissionsData.filter(s => s.item_type === 'schuette').map(s => s.item_id);
      
      const [displaysRes, kartonwareRes, palettenRes, schuttenRes] = await Promise.all([
        displayIds.length > 0 ? freshClient.from('wellen_displays').select('id, name').in('id', displayIds) : { data: [] },
        kartonwareIds.length > 0 ? freshClient.from('wellen_kartonware').select('id, name').in('id', kartonwareIds) : { data: [] },
        paletteIds.length > 0 ? freshClient.from('wellen_paletten').select('id, name').in('id', paletteIds) : { data: [] },
        schutteIds.length > 0 ? freshClient.from('wellen_schuetten').select('id, name').in('id', schutteIds) : { data: [] }
      ]);
      
      const displays = displaysRes.data || [];
      const kartonware = kartonwareRes.data || [];
      const paletten = palettenRes.data || [];
      const schutten = schuttenRes.data || [];
      
      for (const s of submissionsData) {
        let itemName = 'Unbekannt';
        if (s.item_type === 'display') {
          itemName = displays.find((d: any) => d.id === s.item_id)?.name || 'Display';
        } else if (s.item_type === 'kartonware') {
          itemName = kartonware.find((k: any) => k.id === s.item_id)?.name || 'Kartonware';
        } else if (s.item_type === 'palette') {
          itemName = paletten.find((pl: any) => pl.id === s.item_id)?.name || 'Palette';
        } else if (s.item_type === 'schuette') {
          itemName = schutten.find((sc: any) => sc.id === s.item_id)?.name || 'Schütte';
        }
        
        activities.push({
          id: s.id,
          type: 'vorbesteller',
          date: s.created_at,
          glName: s.gebietsleiter?.name || 'Unbekannt',
          glId: s.gebietsleiter_id,
          details: {
            welleName: s.wellen?.name || 'Unbekannt',
            itemType: s.item_type,
            itemName,
            quantity: s.quantity
          }
        });
      }
    }

    // 2. Get Vorverkauf submissions
    const { data: vorverkaufData } = await freshClient
      .from('vorverkauf_submissions')
      .select('*, gebietsleiter(name), vorverkauf_wellen(name)')
      .eq('market_id', id)
      .order('created_at', { ascending: false });
    
    if (vorverkaufData) {
      // Get products for each submission
      for (const sub of vorverkaufData) {
        const { data: products } = await freshClient
          .from('vorverkauf_submission_products')
          .select('*, products(name)')
          .eq('submission_id', sub.id);
        
        activities.push({
          id: sub.id,
          type: 'vorverkauf',
          date: sub.created_at,
          glName: sub.gebietsleiter?.name || 'Unbekannt',
          glId: sub.gebietsleiter_id,
          details: {
            welleName: sub.vorverkauf_wellen?.name || 'Vorverkauf',
            products: (products || []).map((p: any) => ({
              name: p.products?.name || 'Produkt',
              quantity: p.quantity,
              reason: p.reason
            })),
            notes: sub.notes
          }
        });
      }
    }

    // 3. Get market visit history from markets table
    const { data: marketData } = await freshClient
      .from('markets')
      .select('last_visit_date, current_visits, gebietsleiter_name')
      .eq('id', id)
      .single();
    
    if (marketData && marketData.last_visit_date) {
      activities.push({
        id: `visit-${id}`,
        type: 'marktbesuch',
        date: marketData.last_visit_date,
        glName: marketData.gebietsleiter_name || 'Unbekannt',
        glId: null,
        details: {
          visitCount: marketData.current_visits || 1
        }
      });
    }

    // Sort all activities by date (newest first)
    activities.sort((a, b) => new Date(b.date).getTime() - new Date(a.date).getTime());

    console.log(`✅ Fetched ${activities.length} history entries for market ${id}`);
    res.json(activities);
  } catch (error: any) {
    console.error('Error fetching market history:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

/**
 * DELETE /api/markets/:id
 * Delete a market
 */
router.delete('/:id', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    console.log(`🗑️ Deleting market ${id}...`);
    
    const freshClient = createFreshClient();

    const { error } = await freshClient
      .from('markets')
      .delete()
      .eq('id', id);

    if (error) {
      console.error('Supabase error:', error);
      throw error;
    }

    console.log(`✅ Deleted market ${id}`);
    res.status(204).send();
  } catch (error: any) {
    console.error('Error deleting market:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

/**
 * POST /api/markets/sync-visits
 * Recalculate all market visits from historical DB data
 * Sources: vorverkauf_submissions, vorverkauf_entries, wellen_gl_progress
 * Same market + same day = 1 visit
 */
router.post('/sync-visits', async (_req: Request, res: Response) => {
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
    console.error('Error syncing market visits:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

export default router;

