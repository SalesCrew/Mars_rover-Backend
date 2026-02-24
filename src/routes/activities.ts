import { Router, Request, Response } from 'express';
import { supabase, createFreshClient } from '../config/supabase';

const router = Router();

interface Activity {
  id: string;
  type: 'vorbestellung' | 'vorverkauf' | 'produkttausch_pending' | 'nara_incentive';
  glId: string;
  glName: string;
  marketId: string;
  marketChain: string;
  marketAddress: string;
  marketCity: string;
  action: string;
  details: any;
  createdAt: string;
  status?: 'pending' | 'completed';
}

// ============================================================================
// GET ALL ACTIVITIES (combined vorbestellungen & vorverkäufe)
// ============================================================================
router.get('/', async (req: Request, res: Response) => {
  try {
    const limit = parseInt(req.query.limit as string) || 50;
    const offset = parseInt(req.query.offset as string) || 0;
    
    console.log('📊 Fetching combined activities...');
    
    const freshClient = createFreshClient();
    
    // Fetch vorbestellungen (wellen_submissions - individual market submissions)
    const { data: progressData, error: progressError } = await freshClient
      .from('wellen_submissions')
      .select('*')
      .order('created_at', { ascending: false })
      .range(0, 100);
    
    if (progressError) console.error('Progress error:', progressError);
    
    // Fetch vorverkäufe
    const { data: vorverkaufData, error: vorverkaufError } = await freshClient
      .from('vorverkauf_entries')
      .select('*')
      .order('created_at', { ascending: false })
      .range(0, 100);
    
    if (vorverkaufError) console.error('Vorverkauf error:', vorverkaufError);
    
    // Get all related data
    const glIds = [
      ...new Set([
        ...(progressData || []).map(p => p.gebietsleiter_id),
        ...(vorverkaufData || []).map(v => v.gebietsleiter_id)
      ])
    ].filter(Boolean);
    
    const marketIds = [
      ...new Set([
        ...(progressData || []).map(p => p.market_id),
        ...(vorverkaufData || []).map(v => v.market_id)
      ])
    ].filter(Boolean);
    
    const welleIds = [...new Set((progressData || []).map(p => p.welle_id))].filter(Boolean);
    const displayIds = (progressData || []).filter(p => p.item_type === 'display').map(p => p.item_id);
    const kartonwareIds = (progressData || []).filter(p => p.item_type === 'kartonware').map(p => p.item_id);
    const paletteProductIds = (progressData || []).filter(p => p.item_type === 'palette').map(p => p.item_id);
    const schutteProductIds = (progressData || []).filter(p => p.item_type === 'schuette').map(p => p.item_id);
    
    // Fetch related data
    const [glsResult, marketsResult, wellenResult, displaysResult, kartonwareResult, paletteProductsResult, schutteProductsResult] = await Promise.all([
      glIds.length > 0 ? freshClient.from('gebietsleiter').select('id, name').in('id', glIds) : { data: [] },
      marketIds.length > 0 ? freshClient.from('markets').select('id, name, chain, address, city').in('id', marketIds) : { data: [] },
      welleIds.length > 0 ? freshClient.from('wellen').select('id, name').in('id', welleIds) : { data: [] },
      displayIds.length > 0 ? freshClient.from('wellen_displays').select('id, name').in('id', displayIds) : { data: [] },
      kartonwareIds.length > 0 ? freshClient.from('wellen_kartonware').select('id, name').in('id', kartonwareIds) : { data: [] },
      paletteProductIds.length > 0 ? freshClient.from('wellen_paletten_products').select('id, name, palette_id').in('id', paletteProductIds) : { data: [] },
      schutteProductIds.length > 0 ? freshClient.from('wellen_schuetten_products').select('id, name, schuette_id').in('id', schutteProductIds) : { data: [] }
    ]);
    
    const gls = glsResult.data || [];
    const markets = marketsResult.data || [];
    const wellen = wellenResult.data || [];
    const displays = displaysResult.data || [];
    const kartonware = kartonwareResult.data || [];
    const paletteProducts = paletteProductsResult.data || [];
    const schutteProducts = schutteProductsResult.data || [];
    
    // Fetch parent palette/schuette names
    const paletteIds = [...new Set((paletteProducts || []).map((p: any) => p.palette_id))].filter(Boolean);
    const schutteIds = [...new Set((schutteProducts || []).map((p: any) => p.schuette_id))].filter(Boolean);
    
    const [palettesResult, schuttenResult] = await Promise.all([
      paletteIds.length > 0 ? freshClient.from('wellen_paletten').select('id, name').in('id', paletteIds) : { data: [] },
      schutteIds.length > 0 ? freshClient.from('wellen_schuetten').select('id, name').in('id', schutteIds) : { data: [] }
    ]);
    
    const palettes = palettesResult.data || [];
    const schutten = schuttenResult.data || [];
    
    // Fetch vorverkauf items
    const vorverkaufEntryIds = (vorverkaufData || []).map(v => v.id);
    const vorverkaufItemsResult = vorverkaufEntryIds.length > 0 
      ? await freshClient.from('vorverkauf_items').select('*').in('vorverkauf_entry_id', vorverkaufEntryIds)
      : { data: [] };
    const vorverkaufItems = vorverkaufItemsResult.data || [];
    
    // Fetch product info for vorverkauf items
    const vorverkaufProductIds = [...new Set(vorverkaufItems.map(i => i.product_id))].filter(Boolean);
    const vorverkaufProductsResult = vorverkaufProductIds.length > 0
      ? await freshClient.from('products').select('*').in('id', vorverkaufProductIds)
      : { data: [] };
    const vorverkaufProducts = vorverkaufProductsResult.data || [];
    
    // Get welle markets for progress entries
    let welleMarkets: any[] = [];
    if (welleIds.length > 0) {
      const { data } = await freshClient
        .from('wellen_markets')
        .select('welle_id, market_id')
        .in('welle_id', welleIds);
      welleMarkets = data || [];
      
      // Get those market details
      const progressMarketIds = [...new Set(welleMarkets.map(wm => wm.market_id))];
      if (progressMarketIds.length > 0) {
        const { data: progressMarketsData } = await freshClient
          .from('markets')
          .select('id, name, chain, address, city')
          .in('id', progressMarketIds);
        if (progressMarketsData) {
          markets.push(...progressMarketsData.filter(m => !markets.find((em: any) => em.id === m.id)));
        }
      }
    }
    
    // Transform vorbestellungen (from wellen_submissions)
    // For displays and kartonware: keep as individual entries
    // For palette/schuette: group by parent palette_id/schuette_id within same market visit
    
    const displayKartonwareActivities: Activity[] = [];
    const paletteSubmissions: any[] = [];
    const schutteSubmissions: any[] = [];
    
    for (const p of (progressData || [])) {
      if (p.item_type === 'display' || p.item_type === 'kartonware') {
        const gl = gls.find((g: any) => g.id === p.gebietsleiter_id);
        const welle = wellen.find((w: any) => w.id === p.welle_id);
        const market = markets.find((m: any) => m.id === p.market_id);
        
        const item = p.item_type === 'display' 
          ? displays.find((d: any) => d.id === p.item_id)
          : kartonware.find((k: any) => k.id === p.item_id);
        
        const itemTypeLabel = p.item_type === 'display' ? 'Display' : 'Kartonware';
        
        displayKartonwareActivities.push({
          id: p.id,
          type: 'vorbestellung' as const,
          glId: p.gebietsleiter_id,
          glName: gl?.name || 'Unknown',
          marketId: p.market_id || '',
          marketChain: market?.chain || 'Unknown',
          marketAddress: market?.address || '',
          marketCity: market?.city || market?.name || '',
          action: `+${p.quantity} ${itemTypeLabel.toLowerCase()}`,
          details: {
            welleId: p.welle_id,
            welleName: welle?.name || 'Unknown',
            itemId: p.item_id,
            itemName: item?.name || itemTypeLabel,
            itemType: p.item_type,
            itemTypeLabel,
            quantity: p.quantity,
            valuePerUnit: p.value_per_unit || null,
            marketName: market?.name || 'Unknown'
          },
          createdAt: p.created_at
        });
      } else if (p.item_type === 'palette') {
        const product = paletteProducts.find((pp: any) => pp.id === p.item_id);
        paletteSubmissions.push({ ...p, product, parentId: product?.palette_id });
      } else if (p.item_type === 'schuette') {
        const product = schutteProducts.find((sp: any) => sp.id === p.item_id);
        schutteSubmissions.push({ ...p, product, parentId: product?.schuette_id });
      }
    }
    
    // Group palette submissions by parent palette within same market visit (5 min window)
    const groupedPaletteActivities: Activity[] = [];
    const paletteGroups = new Map<string, any[]>();
    
    for (const sub of paletteSubmissions) {
      // Create a grouping key: market_id + gl_id + welle_id + parent_id + time_bucket (5 min)
      const timeBucket = Math.floor(new Date(sub.created_at).getTime() / (5 * 60 * 1000));
      const key = `${sub.market_id}|${sub.gebietsleiter_id}|${sub.welle_id}|${sub.parentId}|${timeBucket}`;
      
      if (!paletteGroups.has(key)) {
        paletteGroups.set(key, []);
      }
      paletteGroups.get(key)!.push(sub);
    }
    
    for (const [, subs] of paletteGroups) {
      const firstSub = subs[0];
      const gl = gls.find((g: any) => g.id === firstSub.gebietsleiter_id);
      const welle = wellen.find((w: any) => w.id === firstSub.welle_id);
      const market = markets.find((m: any) => m.id === firstSub.market_id);
      const parentPalette = palettes.find((pal: any) => pal.id === firstSub.parentId);
      
      // Build products array with all submissions in this group
      const products = subs.map(sub => ({
        submissionId: sub.id,
        productId: sub.item_id,
        productName: sub.product?.name || 'Produkt',
        quantity: sub.quantity,
        valuePerUnit: sub.value_per_unit || 0
      }));
      
      const totalValue = products.reduce((sum: number, p: any) => sum + (p.quantity * p.valuePerUnit), 0);
      
      groupedPaletteActivities.push({
        id: subs.map((s: any) => s.id).join(','), // Composite ID for grouped items
        type: 'vorbestellung' as const,
        glId: firstSub.gebietsleiter_id,
        glName: gl?.name || 'Unknown',
        marketId: firstSub.market_id || '',
        marketChain: market?.chain || 'Unknown',
        marketAddress: market?.address || '',
        marketCity: market?.city || market?.name || '',
        action: `+1 palette`,
        details: {
          welleId: firstSub.welle_id,
          welleName: welle?.name || 'Unknown',
          itemType: 'palette',
          itemTypeLabel: 'Palette',
          parentId: firstSub.parentId,
          parentName: parentPalette?.name || 'Palette',
          products,
          totalValue,
          marketName: market?.name || 'Unknown'
        },
        createdAt: firstSub.created_at
      });
    }
    
    // Group schuette submissions by parent schuette within same market visit (5 min window)
    const groupedSchutteActivities: Activity[] = [];
    const schutteGroups = new Map<string, any[]>();
    
    for (const sub of schutteSubmissions) {
      const timeBucket = Math.floor(new Date(sub.created_at).getTime() / (5 * 60 * 1000));
      const key = `${sub.market_id}|${sub.gebietsleiter_id}|${sub.welle_id}|${sub.parentId}|${timeBucket}`;
      
      if (!schutteGroups.has(key)) {
        schutteGroups.set(key, []);
      }
      schutteGroups.get(key)!.push(sub);
    }
    
    for (const [, subs] of schutteGroups) {
      const firstSub = subs[0];
      const gl = gls.find((g: any) => g.id === firstSub.gebietsleiter_id);
      const welle = wellen.find((w: any) => w.id === firstSub.welle_id);
      const market = markets.find((m: any) => m.id === firstSub.market_id);
      const parentSchutte = schutten.find((sch: any) => sch.id === firstSub.parentId);
      
      // Build products array with all submissions in this group
      const products = subs.map(sub => ({
        submissionId: sub.id,
        productId: sub.item_id,
        productName: sub.product?.name || 'Produkt',
        quantity: sub.quantity,
        valuePerUnit: sub.value_per_unit || 0
      }));
      
      const totalValue = products.reduce((sum: number, p: any) => sum + (p.quantity * p.valuePerUnit), 0);
      
      groupedSchutteActivities.push({
        id: subs.map((s: any) => s.id).join(','), // Composite ID for grouped items
        type: 'vorbestellung' as const,
        glId: firstSub.gebietsleiter_id,
        glName: gl?.name || 'Unknown',
        marketId: firstSub.market_id || '',
        marketChain: market?.chain || 'Unknown',
        marketAddress: market?.address || '',
        marketCity: market?.city || market?.name || '',
        action: `+1 schütte`,
        details: {
          welleId: firstSub.welle_id,
          welleName: welle?.name || 'Unknown',
          itemType: 'schuette',
          itemTypeLabel: 'Schütte',
          parentId: firstSub.parentId,
          parentName: parentSchutte?.name || 'Schütte',
          products,
          totalValue,
          marketName: market?.name || 'Unknown'
        },
        createdAt: firstSub.created_at
      });
    }
    
    // Combine all vorbestellung activities
    const progressActivities: Activity[] = [
      ...displayKartonwareActivities,
      ...groupedPaletteActivities,
      ...groupedSchutteActivities
    ];
    
    // Transform vorverkäufe
    const vorverkaufActivities: Activity[] = (vorverkaufData || []).map(v => {
      const gl = gls.find((g: any) => g.id === v.gebietsleiter_id);
      const market = markets.find((m: any) => m.id === v.market_id);
      const isPending = v.status === 'pending';
      
      // Get items for this entry
      const entryItems = vorverkaufItems.filter(i => i.vorverkauf_entry_id === v.id);
      const takeOutItems = entryItems.filter(i => i.item_type === 'take_out');
      const replaceItems = entryItems.filter(i => i.item_type === 'replace');
      
      // Map items to products
      const takeOutProducts = takeOutItems.map(item => {
        const product = vorverkaufProducts.find((p: any) => String(p.id) === String(item.product_id));
        return {
          id: item.id,
          productId: item.product_id,
          name: product?.name || 'Unknown',
          quantity: item.quantity,
          price: product?.price || 0
        };
      });
      
      const replaceProducts = replaceItems.map(item => {
        const product = vorverkaufProducts.find((p: any) => String(p.id) === String(item.product_id));
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
        id: v.id,
        type: isPending ? 'produkttausch_pending' as const : 'vorverkauf' as const,
        glId: v.gebietsleiter_id,
        glName: gl?.name || 'Unknown',
        marketId: v.market_id,
        marketChain: market?.chain || 'Unknown',
        marketAddress: market?.address || '',
        marketCity: market?.city || '',
        action: isPending ? `Vorgemerkt: ${v.reason}` : `Vorverkauf: ${v.reason}`,
        details: {
          reason: v.reason,
          notes: v.notes,
          status: v.status || 'completed',
          takeOutProducts,
          replaceProducts,
          takeOutValue,
          replaceValue
        },
        createdAt: v.created_at,
        status: v.status || 'completed'
      };
    });
    
    // Fetch NARA incentive submissions
    const { data: naraData, error: naraError } = await freshClient
      .from('nara_incentive_submissions')
      .select(`
        id, gebietsleiter_id, market_id, created_at,
        gebietsleiter ( name ),
        markets ( name, chain, address, city ),
        nara_incentive_items (
          id, product_id, quantity,
          products ( name, price )
        )
      `)
      .order('created_at', { ascending: false })
      .range(0, 100);
    
    if (naraError) console.error('NARA error:', naraError);
    
    const naraActivities: Activity[] = (naraData || []).map((n: any) => {
      const items = (n.nara_incentive_items || []);
      const totalValue = items.reduce((sum: number, i: any) => sum + ((i.products?.price || 0) * i.quantity), 0);
      const totalQuantity = items.reduce((sum: number, i: any) => sum + i.quantity, 0);
      
      return {
        id: n.id,
        type: 'nara_incentive' as const,
        glId: n.gebietsleiter_id,
        glName: n.gebietsleiter?.name || 'Unknown',
        marketId: n.market_id || '',
        marketChain: n.markets?.chain || 'Unknown',
        marketAddress: n.markets?.address || '',
        marketCity: n.markets?.city || '',
        action: `NaRa-Incentive: ${totalQuantity} Produkte`,
        details: {
          marketName: n.markets?.name || 'Unknown',
          totalValue,
          totalQuantity,
          items: items.map((i: any) => ({
            productName: i.products?.name || 'Unbekannt',
            quantity: i.quantity,
            price: i.products?.price || 0
          }))
        },
        createdAt: n.created_at
      };
    });
    
    // Combine and sort by date
    const allActivities = [...progressActivities, ...vorverkaufActivities, ...naraActivities]
      .sort((a, b) => new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime())
      .slice(offset, offset + limit);
    
    console.log(`✅ Fetched ${allActivities.length} activities (${progressActivities.length} vorbestellungen, ${vorverkaufActivities.length} vorverkäufe, ${naraActivities.length} nara-incentive)`);
    res.json(allActivities);
  } catch (error: any) {
    console.error('❌ Error fetching activities:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

// ============================================================================
// UPDATE VORBESTELLUNG (wellen_submissions)
// ============================================================================
router.put('/vorbestellung/:id', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    const { current_number } = req.body;
    
    console.log(`📝 Updating vorbestellung submission ${id} to quantity ${current_number}...`);
    
    const freshClient = createFreshClient();
    
    const { data, error } = await freshClient
      .from('wellen_submissions')
      .update({ quantity: current_number })
      .eq('id', id)
      .select()
      .single();
    
    if (error) throw error;
    
    console.log(`✅ Updated vorbestellung submission ${id}`);
    res.json(data);
  } catch (error: any) {
    console.error('❌ Error updating vorbestellung submission:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

// ============================================================================
// UPDATE GROUPED PRODUCT (for palette/schuette products)
// ============================================================================
router.put('/vorbestellung/product/:submissionId', async (req: Request, res: Response) => {
  try {
    const { submissionId } = req.params;
    const { quantity } = req.body;
    
    console.log(`📝 Updating product submission ${submissionId} to quantity ${quantity}...`);
    
    const freshClient = createFreshClient();
    
    // Get the current submission
    const { data: oldSubmission, error: fetchError } = await freshClient
      .from('wellen_submissions')
      .select('welle_id, gebietsleiter_id, item_type, item_id, quantity')
      .eq('id', submissionId)
      .single();
    
    if (fetchError) throw fetchError;
    
    const quantityDiff = quantity - (oldSubmission.quantity || 0);
    
    // Update the submission
    const { data, error } = await freshClient
      .from('wellen_submissions')
      .update({ quantity })
      .eq('id', submissionId)
      .select()
      .single();
    
    if (error) throw error;
    
    console.log(`✅ Updated product submission ${submissionId}`);
    res.json(data);
  } catch (error: any) {
    console.error('❌ Error updating product submission:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

// ============================================================================
// UPDATE VORVERKAUF
// ============================================================================
router.put('/vorverkauf/:id', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    const { reason, notes } = req.body;
    
    console.log(`📝 Updating vorverkauf ${id}...`);
    
    const freshClient = createFreshClient();
    
    const updateData: any = {};
    if (reason !== undefined) updateData.reason = reason;
    if (notes !== undefined) updateData.notes = notes;
    
    const { data, error } = await freshClient
      .from('vorverkauf_entries')
      .update(updateData)
      .eq('id', id)
      .select()
      .single();
    
    if (error) throw error;
    
    console.log(`✅ Updated vorverkauf ${id}`);
    res.json(data);
  } catch (error: any) {
    console.error('❌ Error updating vorverkauf:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

// ============================================================================
// DELETE ACTIVITY
// ============================================================================
router.delete('/:type/:id', async (req: Request, res: Response) => {
  try {
    const { type, id } = req.params;
    
    console.log(`🗑️ Deleting ${type} ${id}...`);
    
    const freshClient = createFreshClient();
    
    if (type === 'vorbestellung') {
      // Handle composite IDs (for grouped palette/schuette activities)
      const ids = id.includes(',') ? id.split(',') : [id];
      
      for (const submissionId of ids) {
        // Get the submission details before deleting
        const { data: submission, error: fetchError } = await freshClient
          .from('wellen_submissions')
          .select('welle_id, gebietsleiter_id, item_type, item_id, quantity')
          .eq('id', submissionId)
          .single();
        
        if (fetchError) {
          console.warn(`Could not fetch submission details for ${submissionId}:`, fetchError.message);
          continue;
        }
        
        // Delete from wellen_submissions
        const { error } = await freshClient
          .from('wellen_submissions')
          .delete()
          .eq('id', submissionId);
        
        if (error) {
          console.error(`Error deleting submission ${submissionId}:`, error.message);
          continue;
        }
        
      }
      
      console.log(`✅ Deleted ${ids.length} submission(s)`);
    } else if (type === 'vorverkauf' || type === 'produkttausch_pending') {
      // Delete from vorverkauf_entries (Produkttausch - both completed and pending)
      const { error } = await freshClient
        .from('vorverkauf_entries')
        .delete()
        .eq('id', id);
      
      if (error) throw error;
    } else if (type === 'produkttausch') {
      // Alias for vorverkauf_entries
      const { error } = await freshClient
        .from('vorverkauf_entries')
        .delete()
        .eq('id', id);
      
      if (error) throw error;
    } else if (type === 'nara_incentive') {
      const { error } = await freshClient
        .from('nara_incentive_submissions')
        .delete()
        .eq('id', id);
      
      if (error) throw error;
    } else {
      return res.status(400).json({ error: 'Invalid type' });
    }
    
    console.log(`✅ Deleted ${type} ${id}`);
    res.json({ message: 'Deleted successfully' });
  } catch (error: any) {
    console.error('❌ Error deleting activity:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

export default router;
