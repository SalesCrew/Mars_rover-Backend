import { Router, Request, Response } from 'express';
import { supabase } from '../config/supabase';

const router = Router();

// ============================================================================
// GET ALL VORVERKAUF ENTRIES (with GL and market info)
// ============================================================================
router.get('/', async (req: Request, res: Response) => {
  try {
    console.log('📦 Fetching all vorverkauf entries...');

    // Get query params for filtering
    const { glId, search } = req.query;

    // Fetch all entries
    let query = supabase
      .from('vorverkauf_entries')
      .select('*')
      .order('created_at', { ascending: false });

    // Apply GL filter if specified
    if (glId && typeof glId === 'string') {
      query = query.eq('gebietsleiter_id', glId);
    }

    const { data: entries, error: entriesError } = await query;

    if (entriesError) throw entriesError;

    if (!entries || entries.length === 0) {
      return res.json([]);
    }

    // Get all related data
    const glIds = [...new Set(entries.map(e => e.gebietsleiter_id))];
    const marketIds = [...new Set(entries.map(e => e.market_id))];
    const entryIds = entries.map(e => e.id);

    const [glsResult, marketsResult, itemsResult] = await Promise.all([
      glIds.length > 0 ? supabase.from('gebietsleiter').select('id, name').in('id', glIds) : { data: [] },
      marketIds.length > 0 ? supabase.from('markets').select('id, name, chain, address, city, postal_code').in('id', marketIds) : { data: [] },
      entryIds.length > 0 ? supabase.from('vorverkauf_items').select('*').in('vorverkauf_entry_id', entryIds) : { data: [] }
    ]);

    const gls = glsResult.data || [];
    const markets = marketsResult.data || [];
    const items = itemsResult.data || [];

    // Get product info for items
    const productIds = [...new Set(items.map(i => i.product_id).filter(Boolean))];
    let products: any[] = [];
    if (productIds.length > 0) {
      console.log('Looking up product IDs:', productIds);
      const { data, error: productError } = await supabase.from('products').select('*').in('id', productIds);
      if (productError) console.error('Product lookup error:', productError);
      console.log('Found products:', data?.length || 0);
      products = data || [];
    }

    // Build response
    let response = entries.map(entry => {
      const gl = gls.find((g: any) => g.id === entry.gebietsleiter_id);
      const market = markets.find((m: any) => m.id === entry.market_id);
      const entryItems = items
        .filter(i => i.vorverkauf_entry_id === entry.id)
        .map(item => {
          const product = products.find((p: any) => String(p.id) === String(item.product_id));
          return {
            id: item.id,
            productId: item.product_id,
            productName: product?.name || product?.productName || 'Unknown',
            productBrand: product?.brand || product?.productBrand || '',
            productSize: product?.size || product?.weight || product?.content || '',
            quantity: item.quantity,
            itemType: item.item_type || 'take_out'
          };
        });

      return {
        id: entry.id,
        glId: entry.gebietsleiter_id,
        glName: gl?.name || 'Unknown',
        marketId: entry.market_id,
        marketName: market?.name || 'Unknown',
        marketChain: market?.chain || '',
        marketAddress: market?.address || '',
        marketCity: market?.city || '',
        reason: entry.reason,
        notes: entry.notes,
        items: entryItems,
        totalItems: entryItems.reduce((sum, i) => sum + i.quantity, 0),
        createdAt: entry.created_at
      };
    });

    // Apply search filter if specified (search in GL name, market name, product names)
    if (search && typeof search === 'string') {
      const searchLower = search.toLowerCase();
      response = response.filter(entry => 
        entry.glName.toLowerCase().includes(searchLower) ||
        entry.marketName.toLowerCase().includes(searchLower) ||
        entry.marketChain.toLowerCase().includes(searchLower) ||
        entry.items.some(item => 
          item.productName.toLowerCase().includes(searchLower) ||
          item.productBrand.toLowerCase().includes(searchLower)
        )
      );
    }

    console.log(`✅ Fetched ${response.length} vorverkauf entries`);
    res.json(response);
  } catch (error: any) {
    console.error('❌ Error fetching vorverkauf entries:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

// ============================================================================
// GET SINGLE VORVERKAUF ENTRY
// ============================================================================
router.get('/:id', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;

    const { data: entry, error: entryError } = await supabase
      .from('vorverkauf_entries')
      .select('*')
      .eq('id', id)
      .single();

    if (entryError) throw entryError;
    if (!entry) {
      return res.status(404).json({ error: 'Entry not found' });
    }

    // Get related data
    const [glResult, marketResult, itemsResult] = await Promise.all([
      supabase.from('gebietsleiter').select('id, name').eq('id', entry.gebietsleiter_id).single(),
      supabase.from('markets').select('id, name, chain, address, city, postal_code').eq('id', entry.market_id).single(),
      supabase.from('vorverkauf_items').select('*').eq('vorverkauf_entry_id', entry.id)
    ]);

    const gl = glResult.data;
    const market = marketResult.data;
    const items = itemsResult.data || [];

    // Get product info
    const productIds = items.map(i => i.product_id);
    let products: any[] = [];
    if (productIds.length > 0) {
      const { data } = await supabase.from('products').select('id, name, brand, size').in('id', productIds);
      products = data || [];
    }

    const response = {
      id: entry.id,
      glId: entry.gebietsleiter_id,
      glName: gl?.name || 'Unknown',
      marketId: entry.market_id,
      marketName: market?.name || 'Unknown',
      marketChain: market?.chain || '',
      marketAddress: market?.address || '',
      marketCity: market?.city || '',
      reason: entry.reason,
      notes: entry.notes,
      items: items.map(item => {
        const product = products.find((p: any) => p.id === item.product_id);
        return {
          id: item.id,
          productId: item.product_id,
          productName: product?.name || product?.productName || 'Unknown',
          productBrand: product?.brand || product?.productBrand || '',
          productSize: product?.size || product?.weight || product?.content || '',
          quantity: item.quantity,
          itemType: item.item_type || 'take_out'
        };
      }),
      createdAt: entry.created_at
    };

    res.json(response);
  } catch (error: any) {
    console.error('❌ Error fetching vorverkauf entry:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

// ============================================================================
// CREATE VORVERKAUF ENTRY
// ============================================================================
router.post('/', async (req: Request, res: Response) => {
  try {
    const { gebietsleiter_id, market_id, reason, notes, items, take_out_items, replace_items } = req.body;

    console.log('📦 Creating vorverkauf entry...');
    console.log('Request body:', JSON.stringify(req.body, null, 2));

    // Support both old format (items) and new format (take_out_items + replace_items)
    let allItems: any[] = [];
    
    if (take_out_items || replace_items) {
      // New format with item types
      if (take_out_items && Array.isArray(take_out_items)) {
        allItems.push(...take_out_items.map((item: any) => ({ ...item, item_type: 'take_out' })));
      }
      if (replace_items && Array.isArray(replace_items)) {
        allItems.push(...replace_items.map((item: any) => ({ ...item, item_type: 'replace' })));
      }
    } else if (items && Array.isArray(items)) {
      // Old format - all items without type
      allItems = items.map((item: any) => ({ ...item, item_type: item.type || 'take_out' }));
    }

    if (!gebietsleiter_id || !market_id || !reason || allItems.length === 0) {
      return res.status(400).json({ error: 'Missing required fields: gebietsleiter_id, market_id, reason, items' });
    }

    // Create the main entry
    const { data: entry, error: entryError } = await supabase
      .from('vorverkauf_entries')
      .insert({
        gebietsleiter_id,
        market_id,
        reason,
        notes: notes || null
      })
      .select()
      .single();

    if (entryError) {
      console.error('Error creating entry:', entryError);
      throw entryError;
    }

    // Create items
    const itemsToInsert = allItems.map((item: any) => ({
      vorverkauf_entry_id: entry.id,
      product_id: item.product_id,
      quantity: item.quantity || 1,
      item_type: item.item_type || 'take_out'
    }));

    console.log('Items to insert:', itemsToInsert);

    const { error: itemsError } = await supabase
      .from('vorverkauf_items')
      .insert(itemsToInsert);

    if (itemsError) {
      console.error('Error creating items:', itemsError);
      throw itemsError;
    }

    console.log(`✅ Created vorverkauf entry with ${allItems.length} items`);
    res.status(201).json({
      message: 'Vorverkauf entry created successfully',
      id: entry.id,
      itemsCount: allItems.length
    });
  } catch (error: any) {
    console.error('❌ Error creating vorverkauf entry:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

// ============================================================================
// DELETE VORVERKAUF ENTRY
// ============================================================================
router.delete('/:id', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;

    console.log(`🗑️ Deleting vorverkauf entry ${id}...`);

    // Items will be deleted automatically due to CASCADE
    const { error } = await supabase
      .from('vorverkauf_entries')
      .delete()
      .eq('id', id);

    if (error) throw error;

    console.log(`✅ Deleted vorverkauf entry ${id}`);
    res.json({ message: 'Entry deleted successfully' });
  } catch (error: any) {
    console.error('❌ Error deleting vorverkauf entry:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

// ============================================================================
// GET VORVERKAUF STATISTICS
// ============================================================================
router.get('/stats/summary', async (req: Request, res: Response) => {
  try {
    const { data: entries, error } = await supabase
      .from('vorverkauf_entries')
      .select('id, reason, created_at');

    if (error) throw error;

    const { data: items } = await supabase
      .from('vorverkauf_items')
      .select('quantity');

    const totalEntries = entries?.length || 0;
    const totalItems = (items || []).reduce((sum, i) => sum + i.quantity, 0);
    const byReason = {
      OOS: entries?.filter(e => e.reason === 'OOS').length || 0,
      Listungslücke: entries?.filter(e => e.reason === 'Listungslücke').length || 0,
      Platzierung: entries?.filter(e => e.reason === 'Platzierung').length || 0
    };

    res.json({
      totalEntries,
      totalItems,
      byReason
    });
  } catch (error: any) {
    console.error('❌ Error fetching vorverkauf stats:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

export default router;
