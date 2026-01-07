import { Router, Request, Response } from 'express';
import { supabase } from '../config/supabase';

const router = Router();

/**
 * GET /api/markets
 * Get all markets
 */
router.get('/', async (req: Request, res: Response) => {
  try {
    console.log('📋 Fetching all markets...');
    
    // Fetch ALL markets using pagination (Supabase has 1000 row limit per request)
    let allMarkets: any[] = [];
    let from = 0;
    const pageSize = 1000;
    let hasMore = true;

    while (hasMore) {
      const { data, error } = await supabase
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
 * GET /api/markets/:id
 * Get a single market by ID
 */
router.get('/:id', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    console.log(`📋 Fetching market ${id}...`);

    const { data, error } = await supabase
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
    
    const { data, error } = await supabase
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

    const { data, error } = await supabase
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

    const { data, error } = await supabase
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

    // Get current market data
    const { data: market, error: fetchError } = await supabase
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
    
    const { data: updated, error: updateError } = await supabase
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
 * DELETE /api/markets/:id
 * Delete a market
 */
router.delete('/:id', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    console.log(`🗑️ Deleting market ${id}...`);

    const { error } = await supabase
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
 * POST /api/markets/backfill-gl-ids
 * Backfill gebietsleiter_id for markets that have gebietsleiter_name but no gebietsleiter_id
 * Uses fuzzy matching (case insensitive, ignores dashes, extra spaces, etc.)
 */
router.post('/backfill-gl-ids', async (req: Request, res: Response) => {
  try {
    console.log('🔄 Starting GL ID backfill...');

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
    const { data: gls, error: glError } = await supabase
      .from('gebietsleiter')
      .select('id, name, email')
      .eq('is_active', true);

    if (glError) throw glError;

    console.log(`📋 Found ${gls?.length || 0} active GLs`);

    // Create a map of normalized names to GL data
    const glMap = new Map<string, { id: string; name: string; email: string }>();
    for (const gl of gls || []) {
      const normalizedName = normalizeName(gl.name);
      glMap.set(normalizedName, { id: gl.id, name: gl.name, email: gl.email });
      console.log(`  GL: "${gl.name}" -> normalized: "${normalizedName}"`);
    }

    // Fetch markets with gebietsleiter_name but no gebietsleiter_id
    const { data: marketsToUpdate, error: marketsError } = await supabase
      .from('markets')
      .select('id, gebietsleiter_name, gebietsleiter_id')
      .not('gebietsleiter_name', 'is', null)
      .or('gebietsleiter_id.is.null,gebietsleiter_id.eq.');

    if (marketsError) throw marketsError;

    console.log(`📋 Found ${marketsToUpdate?.length || 0} markets needing GL ID backfill`);

    let updated = 0;
    let notFound = 0;
    const unmatchedNames = new Set<string>();

    for (const market of marketsToUpdate || []) {
      const normalizedMarketGL = normalizeName(market.gebietsleiter_name);
      const matchedGL = glMap.get(normalizedMarketGL);

      if (matchedGL) {
        // Update the market with the GL ID and email
        const { error: updateError } = await supabase
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
        unmatchedNames.add(market.gebietsleiter_name);
      }
    }

    console.log(`✅ Backfill complete: ${updated} updated, ${notFound} not matched`);
    if (unmatchedNames.size > 0) {
      console.log(`⚠️ Unmatched GL names:`, Array.from(unmatchedNames));
    }

    res.json({
      success: true,
      updated,
      notMatched: notFound,
      unmatchedNames: Array.from(unmatchedNames)
    });
  } catch (error: any) {
    console.error('❌ Error during GL ID backfill:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

export default router;

