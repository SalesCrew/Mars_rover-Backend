import { Router, Request, Response, NextFunction } from 'express';
import { supabase, createFreshClient } from '../config/supabase';
import { AuthRequest, getAuthenticatedGlId, requireAdmin, requireOwnedRowOrAdmin, requireSelfOrAdmin } from '../middleware/auth';
import { sendInternalError } from '../utils/httpErrors';

const router = Router();
const VORVERKAUF_ENTRY_SELECT = 'id, gebietsleiter_id, market_id, reason, notes, status, created_at';
const VORVERKAUF_ITEM_SELECT = 'id, vorverkauf_entry_id, product_id, quantity, item_type';
const VORVERKAUF_PRODUCT_SELECT = 'id, name, weight, content, price';

const requireVorverkaufItemOwnerOrAdmin = async (req: AuthRequest, res: Response, next: NextFunction) => {
  try {
    if (req.user?.role === 'admin') {
      return next();
    }

    const { itemId } = req.params;
    const freshClient = createFreshClient();

    const { data: item, error: itemError } = await freshClient
      .from('vorverkauf_items')
      .select('vorverkauf_entry_id')
      .eq('id', itemId)
      .maybeSingle();

    if (itemError) {
      throw itemError;
    }

    if (!item?.vorverkauf_entry_id) {
      return res.status(404).json({ error: 'Item not found' });
    }

    const { data: entry, error: entryError } = await freshClient
      .from('vorverkauf_entries')
      .select('gebietsleiter_id')
      .eq('id', item.vorverkauf_entry_id)
      .maybeSingle();

    if (entryError) {
      throw entryError;
    }

    if (!entry) {
      return res.status(404).json({ error: 'Entry not found' });
    }

    if (entry.gebietsleiter_id !== getAuthenticatedGlId(req.user)) {
      return res.status(403).json({ error: 'Forbidden' });
    }

    return next();
  } catch (error) {
    console.error('Error checking vorverkauf item ownership:');
    return res.status(500).json({ error: 'Authorization check failed' });
  }
};

// ============================================================================
// GET ALL VORVERKAUF ENTRIES (with GL and market info)
// ============================================================================
router.get('/', async (req: AuthRequest, res: Response) => {
  try {
    console.log('Fetching all vorverkauf entries...');
    
    const freshClient = createFreshClient();

    // Get query params for filtering
    const { glId, search } = req.query;

    // Fetch all entries
    let query = freshClient
      .from('vorverkauf_entries')
      .select(VORVERKAUF_ENTRY_SELECT)
      .order('created_at', { ascending: false });

    // Apply GL filter if specified
    if (req.user?.role !== 'admin') {
      query = query.eq('gebietsleiter_id', getAuthenticatedGlId(req.user));
    } else if (glId && typeof glId === 'string') {
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
      glIds.length > 0 ? freshClient.from('gebietsleiter').select('id, name').in('id', glIds) : { data: [] },
      marketIds.length > 0 ? freshClient.from('markets').select('id, name, chain, address, city, postal_code').in('id', marketIds) : { data: [] },
      entryIds.length > 0 ? freshClient.from('vorverkauf_items').select(VORVERKAUF_ITEM_SELECT).in('vorverkauf_entry_id', entryIds) : { data: [] }
    ]);

    const gls = glsResult.data || [];
    const markets = marketsResult.data || [];
    const items = itemsResult.data || [];

    // Get product info for items
    const productIds = [...new Set(items.map(i => i.product_id).filter(Boolean))];
    let products: any[] = [];
    if (productIds.length > 0) {
      const { data, error: productError } = await freshClient.from('products').select(VORVERKAUF_PRODUCT_SELECT).in('id', productIds);
      if (productError) console.error('Product lookup error:');
      console.log(`Found ${data?.length || 0} products for vorverkauf entry enrichment`);
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
            productPrice: product?.price || 0,
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
        marketPostalCode: market?.postal_code || '',
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
    console.error('❌ Error fetching vorverkauf entries:');
    sendInternalError(res);
  }
});

// ============================================================================
// GET SINGLE VORVERKAUF ENTRY
// ============================================================================
router.get('/:id', requireOwnedRowOrAdmin('vorverkauf_entries'), async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    
    const freshClient = createFreshClient();

    const { data: entry, error: entryError } = await freshClient
      .from('vorverkauf_entries')
      .select(VORVERKAUF_ENTRY_SELECT)
      .eq('id', id)
      .single();

    if (entryError) throw entryError;
    if (!entry) {
      return res.status(404).json({ error: 'Entry not found' });
    }

    // Get related data
    const [glResult, marketResult, itemsResult] = await Promise.all([
      freshClient.from('gebietsleiter').select('id, name').eq('id', entry.gebietsleiter_id).single(),
      freshClient.from('markets').select('id, name, chain, address, city, postal_code').eq('id', entry.market_id).single(),
      freshClient.from('vorverkauf_items').select(VORVERKAUF_ITEM_SELECT).eq('vorverkauf_entry_id', entry.id)
    ]);

    const gl = glResult.data;
    const market = marketResult.data;
    const items = itemsResult.data || [];

    // Get product info
    const productIds = items.map(i => i.product_id);
    let products: any[] = [];
    if (productIds.length > 0) {
      const { data } = await freshClient.from('products').select(VORVERKAUF_PRODUCT_SELECT).in('id', productIds);
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
    console.error('❌ Error fetching vorverkauf entry:');
    sendInternalError(res);
  }
});

// ============================================================================
// CREATE VORVERKAUF ENTRY
// ============================================================================
router.post('/', async (req: AuthRequest, res: Response) => {
  try {
    const { gebietsleiter_id, market_id, reason, notes, items, take_out_items, replace_items, status, skipVisitUpdate } = req.body;
    const effectiveGlId = req.user?.role === 'admin' ? gebietsleiter_id : getAuthenticatedGlId(req.user);

    console.log('Creating vorverkauf entry...');
    
    // Validate status
    const entryStatus = status === 'pending' ? 'pending' : 'completed';
    
    const freshClient = createFreshClient();

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

    if (!effectiveGlId || !market_id || !reason || allItems.length === 0) {
      return res.status(400).json({ error: 'Missing required fields: gebietsleiter_id, market_id, reason, items' });
    }

    // Create the main entry
    const { data: entry, error: entryError } = await freshClient
      .from('vorverkauf_entries')
      .insert({
        gebietsleiter_id: effectiveGlId,
        market_id,
        reason,
        notes: notes || null,
        status: entryStatus
      })
      .select(VORVERKAUF_ENTRY_SELECT)
      .single();

    if (entryError) {
      console.error('Error creating entry:');
      throw entryError;
    }

    // Create items
    const itemsToInsert = allItems.map((item: any) => ({
      vorverkauf_entry_id: entry.id,
      product_id: item.product_id,
      quantity: item.quantity || 1,
      item_type: item.item_type || 'take_out'
    }));

    const { error: itemsError } = await freshClient
      .from('vorverkauf_items')
      .insert(itemsToInsert);

    if (itemsError) {
      console.error('Error creating items:');
      throw itemsError;
    }

    // Update market visit count (multiple actions same day = 1 visit) - skip if user chose not to
    if (!skipVisitUpdate) {
      const today = new Date().toISOString().split('T')[0];
      const { data: market } = await freshClient
        .from('markets')
        .select('last_visit_date, current_visits')
        .eq('id', market_id)
        .single();

      if (market && market.last_visit_date !== today) {
        await freshClient
          .from('markets')
          .update({
            current_visits: (market.current_visits || 0) + 1,
            last_visit_date: today
          })
          .eq('id', market_id);
        console.log('Recorded vorverkauf market visit');
      }

      await freshClient
        .from('market_visits')
        .upsert({
          market_id,
          gebietsleiter_id: effectiveGlId,
          visit_date: today,
          source: 'vorverkauf'
        }, { onConflict: 'market_id,visit_date', ignoreDuplicates: true });
    } else {
      console.log('Skipping vorverkauf visit update by user choice');
    }

    console.log(`✅ Created vorverkauf entry with ${allItems.length} items (status: ${entryStatus})`);
    res.status(201).json({
      message: 'Vorverkauf entry created successfully',
      id: entry.id,
      itemsCount: allItems.length,
      status: entryStatus
    });
  } catch (error: any) {
    console.error('❌ Error creating vorverkauf entry:');
    sendInternalError(res);
  }
});

// ============================================================================
// GET PENDING ENTRIES FOR GL
// ============================================================================
router.get('/pending/:glId', requireSelfOrAdmin(req => req.params.glId), async (req: Request, res: Response) => {
  try {
    const { glId } = req.params;
    console.log('Fetching pending vorverkauf entries for GL profile');
    
    const freshClient = createFreshClient();

    // Fetch pending entries for this GL
    const { data: entries, error: entriesError } = await freshClient
      .from('vorverkauf_entries')
      .select(VORVERKAUF_ENTRY_SELECT)
      .eq('gebietsleiter_id', glId)
      .eq('status', 'pending')
      .order('created_at', { ascending: false });

    if (entriesError) throw entriesError;

    if (!entries || entries.length === 0) {
      return res.json([]);
    }

    // Get market info and items
    const marketIds = [...new Set(entries.map(e => e.market_id))];
    const entryIds = entries.map(e => e.id);

    const [marketsResult, itemsResult] = await Promise.all([
      marketIds.length > 0 ? freshClient.from('markets').select('id, name, chain, address, city, postal_code').in('id', marketIds) : { data: [] },
      entryIds.length > 0 ? freshClient.from('vorverkauf_items').select(VORVERKAUF_ITEM_SELECT).in('vorverkauf_entry_id', entryIds) : { data: [] }
    ]);

    const markets = marketsResult.data || [];
    const items = itemsResult.data || [];

    // Get product info
    const productIds = [...new Set(items.map(i => i.product_id).filter(Boolean))];
    let products: any[] = [];
    if (productIds.length > 0) {
      const { data } = await freshClient.from('products').select(VORVERKAUF_PRODUCT_SELECT).in('id', productIds);
      products = data || [];
    }

    // Build response
    const response = entries.map(entry => {
      const market = markets.find((m: any) => m.id === entry.market_id);
      const entryItems = items.filter(i => i.vorverkauf_entry_id === entry.id);
      const takeOutItems = entryItems.filter(i => i.item_type === 'take_out');
      const replaceItems = entryItems.filter(i => i.item_type === 'replace');

      const takeOutProducts = takeOutItems.map(item => {
        const product = products.find((p: any) => String(p.id) === String(item.product_id));
        return {
          id: item.id,
          productId: item.product_id,
          name: product?.name || 'Unknown',
          quantity: item.quantity,
          price: product?.price || 0
        };
      });

      const replaceProducts = replaceItems.map(item => {
        const product = products.find((p: any) => String(p.id) === String(item.product_id));
        return {
          id: item.id,
          productId: item.product_id,
          name: product?.name || 'Unknown',
          quantity: item.quantity,
          price: product?.price || 0
        };
      });

      const takeOutValue = takeOutProducts.reduce((sum, p) => sum + (p.price * p.quantity), 0);
      const replaceValue = replaceProducts.reduce((sum, p) => sum + (p.price * p.quantity), 0);

      return {
        id: entry.id,
        marketId: entry.market_id,
        marketName: market?.name || 'Unknown',
        marketChain: market?.chain || '',
        marketAddress: market?.address || '',
        marketCity: market?.city || '',
        marketPostalCode: market?.postal_code || '',
        takeOutCount: takeOutItems.reduce((sum, i) => sum + i.quantity, 0),
        replaceCount: replaceItems.reduce((sum, i) => sum + i.quantity, 0),
        takeOutProducts,
        replaceProducts,
        takeOutValue,
        replaceValue,
        notes: entry.notes,
        createdAt: entry.created_at
      };
    });

    console.log(`Found ${response.length} pending vorverkauf entries`);
    res.json(response);
  } catch (error: any) {
    console.error('❌ Error fetching pending entries:');
    sendInternalError(res);
  }
});

// ============================================================================
// FULFILL PENDING ENTRY
// ============================================================================
router.put('/:id/fulfill', requireOwnedRowOrAdmin('vorverkauf_entries'), async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    console.log('Fulfilling pending vorverkauf entry');
    
    const freshClient = createFreshClient();

    // Get the entry first
    const { data: entry, error: getError } = await freshClient
      .from('vorverkauf_entries')
      .select(VORVERKAUF_ENTRY_SELECT)
      .eq('id', id)
      .single();

    if (getError) throw getError;
    if (!entry) {
      return res.status(404).json({ error: 'Entry not found' });
    }
    if (entry.status !== 'pending') {
      return res.status(400).json({ error: 'Entry is not pending' });
    }

    // Update status to completed
    const { error: updateError } = await freshClient
      .from('vorverkauf_entries')
      .update({ 
        status: 'completed',
        updated_at: new Date().toISOString()
      })
      .eq('id', id);

    if (updateError) throw updateError;

    // Update market visit count (multiple actions same day = 1 visit)
    const today = new Date().toISOString().split('T')[0];
    const { data: market } = await freshClient
      .from('markets')
      .select('last_visit_date, current_visits')
      .eq('id', entry.market_id)
      .single();

    if (market && market.last_visit_date !== today) {
      await freshClient
        .from('markets')
        .update({
          current_visits: (market.current_visits || 0) + 1,
          last_visit_date: today
        })
        .eq('id', entry.market_id);
      console.log('Recorded vorverkauf pending-entry market visit');
    }

    await freshClient
      .from('market_visits')
      .upsert({
        market_id: entry.market_id,
        gebietsleiter_id: entry.gebietsleiter_id || null,
        visit_date: today,
        source: 'vorverkauf'
      }, { onConflict: 'market_id,visit_date', ignoreDuplicates: true });

    console.log('Fulfilled pending vorverkauf entry');
    res.json({ 
      message: 'Entry fulfilled successfully',
      id: entry.id 
    });
  } catch (error: any) {
    console.error('❌ Error fulfilling entry:');
    sendInternalError(res);
  }
});

// ============================================================================
// DELETE VORVERKAUF ENTRY
// ============================================================================
router.delete('/:id', requireOwnedRowOrAdmin('vorverkauf_entries'), async (req: Request, res: Response) => {
  try {
    const { id } = req.params;

    console.log('Deleting vorverkauf entry');
    
    const freshClient = createFreshClient();

    // Items will be deleted automatically due to CASCADE
    const { error } = await freshClient
      .from('vorverkauf_entries')
      .delete()
      .eq('id', id);

    if (error) throw error;

    console.log('Deleted vorverkauf entry');
    res.json({ message: 'Entry deleted successfully' });
  } catch (error: any) {
    console.error('❌ Error deleting vorverkauf entry:');
    sendInternalError(res);
  }
});

// ============================================================================
// UPDATE ITEM QUANTITY
// ============================================================================
router.put('/items/:itemId', requireVorverkaufItemOwnerOrAdmin, async (req: Request, res: Response) => {
  try {
    const { itemId } = req.params;
    const { quantity } = req.body;

    if (typeof quantity !== 'number' || quantity < 1) {
      return res.status(400).json({ error: 'Invalid quantity' });
    }

    const freshClient = createFreshClient();

    const { error } = await freshClient
      .from('vorverkauf_items')
      .update({ quantity })
      .eq('id', itemId);

    if (error) throw error;

    res.json({ message: 'Item updated', itemId, newQuantity: quantity });
  } catch (error: any) {
    console.error('Error updating vorverkauf item:');
    sendInternalError(res);
  }
});

// ============================================================================
// SUBMIT VORVERKAUF (GL - no wave required)
// ============================================================================
router.post('/submit', async (req: AuthRequest, res: Response) => {
  try {
    const { gebietsleiter_id, market_id, products, notes } = req.body;
    const effectiveGlId = req.user?.role === 'admin' ? gebietsleiter_id : getAuthenticatedGlId(req.user);

    console.log('Submitting vorverkauf (direct, no wave)...');

    if (!effectiveGlId || !market_id || !products || products.length === 0) {
      return res.status(400).json({ 
        error: 'gebietsleiter_id, market_id, and products are required' 
      });
    }
    
    const freshClient = createFreshClient();

    // Group products by reason to create multiple entries if needed
    // Or we could just use the most common reason - let's use the first one for the entry level
    const primaryReason = products[0]?.reason || 'OOS';

    // Create the main entry
    const { data: entry, error: entryError } = await freshClient
      .from('vorverkauf_entries')
      .insert({
        gebietsleiter_id: effectiveGlId,
        market_id,
        reason: primaryReason,
        notes: notes || null,
        status: 'completed'
      })
      .select(VORVERKAUF_ENTRY_SELECT)
      .single();

    if (entryError) {
      console.error('Error creating entry:');
      throw entryError;
    }

    const itemsToInsert = products.map((p: { productId: string; quantity: number; reason: string }) => ({
      vorverkauf_entry_id: entry.id,
      product_id: p.productId,
      quantity: p.quantity || 1,
      item_type: 'take_out'
    }));

    const { error: itemsError } = await freshClient
      .from('vorverkauf_items')
      .insert(itemsToInsert);

    if (itemsError) {
      console.error('Error creating items:');
      throw itemsError;
    }

    // Update market visit count
    const today = new Date().toISOString().split('T')[0];
    const { data: market } = await freshClient
      .from('markets')
      .select('last_visit_date, current_visits')
      .eq('id', market_id)
      .single();

    if (market && market.last_visit_date !== today) {
      await freshClient
        .from('markets')
        .update({
          current_visits: (market.current_visits || 0) + 1,
          last_visit_date: today
        })
        .eq('id', market_id);
      console.log('Recorded direct vorverkauf market visit');
    }

    await freshClient
      .from('market_visits')
      .upsert({
        market_id,
        gebietsleiter_id: effectiveGlId,
        visit_date: today,
        source: 'vorverkauf'
      }, { onConflict: 'market_id,visit_date', ignoreDuplicates: true });

    console.log(`✅ Created vorverkauf entry with ${products.length} products`);
    res.status(201).json({
      id: entry.id,
      itemsCount: products.length
    });
  } catch (error: any) {
    console.error('❌ Error submitting vorverkauf:');
    sendInternalError(res);
  }
});

// ============================================================================
// GET VORVERKAUF STATISTICS
// ============================================================================
router.get('/stats/summary', requireAdmin, async (req: Request, res: Response) => {
  try {
    const freshClient = createFreshClient();
    
    const { data: entries, error } = await freshClient
      .from('vorverkauf_entries')
      .select('id, reason, created_at');

    if (error) throw error;

    const { data: items } = await freshClient
      .from('vorverkauf_items')
      .select('quantity');

    const totalEntries = entries?.length || 0;
    const totalItems = (items || []).reduce((sum, i) => sum + i.quantity, 0);
    const takeOutItems = (items || []).filter((i: any) => i.item_type === 'take_out').reduce((sum, i) => sum + i.quantity, 0);
    const replaceItems = (items || []).filter((i: any) => i.item_type === 'replace').reduce((sum, i) => sum + i.quantity, 0);

    res.json({
      totalEntries,
      totalItems,
      takeOutItems,
      replaceItems
    });
  } catch (error: any) {
    console.error('❌ Error fetching vorverkauf stats:');
    sendInternalError(res);
  }
});

export default router;
