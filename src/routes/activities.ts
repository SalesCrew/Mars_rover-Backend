import { Router, Request, Response } from 'express';
import { supabase } from '../config/supabase';

const router = Router();

interface Activity {
  id: string;
  type: 'vorbestellung' | 'vorverkauf';
  glId: string;
  glName: string;
  marketId: string;
  marketChain: string;
  marketAddress: string;
  marketCity: string;
  action: string;
  details: any;
  createdAt: string;
}

// ============================================================================
// GET ALL ACTIVITIES (combined vorbestellungen & vorverkäufe)
// ============================================================================
router.get('/', async (req: Request, res: Response) => {
  try {
    const limit = parseInt(req.query.limit as string) || 50;
    const offset = parseInt(req.query.offset as string) || 0;
    
    console.log('📊 Fetching combined activities...');
    
    // Fetch vorbestellungen (wellen_gl_progress)
    const { data: progressData, error: progressError } = await supabase
      .from('wellen_gl_progress')
      .select('*')
      .order('updated_at', { ascending: false })
      .range(0, 100);
    
    if (progressError) console.error('Progress error:', progressError);
    
    // Fetch vorverkäufe
    const { data: vorverkaufData, error: vorverkaufError } = await supabase
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
        ...(vorverkaufData || []).map(v => v.market_id)
      ])
    ].filter(Boolean);
    
    const welleIds = [...new Set((progressData || []).map(p => p.welle_id))].filter(Boolean);
    const displayIds = (progressData || []).filter(p => p.item_type === 'display').map(p => p.item_id);
    const kartonwareIds = (progressData || []).filter(p => p.item_type === 'kartonware').map(p => p.item_id);
    
    // Fetch related data
    const [glsResult, marketsResult, wellenResult, displaysResult, kartonwareResult] = await Promise.all([
      glIds.length > 0 ? supabase.from('gebietsleiter').select('id, name').in('id', glIds) : { data: [] },
      marketIds.length > 0 ? supabase.from('markets').select('id, name, chain, address, city').in('id', marketIds) : { data: [] },
      welleIds.length > 0 ? supabase.from('wellen').select('id, name').in('id', welleIds) : { data: [] },
      displayIds.length > 0 ? supabase.from('wellen_displays').select('id, name').in('id', displayIds) : { data: [] },
      kartonwareIds.length > 0 ? supabase.from('wellen_kartonware').select('id, name').in('id', kartonwareIds) : { data: [] }
    ]);
    
    const gls = glsResult.data || [];
    const markets = marketsResult.data || [];
    const wellen = wellenResult.data || [];
    const displays = displaysResult.data || [];
    const kartonware = kartonwareResult.data || [];
    
    // Get welle markets for progress entries
    let welleMarkets: any[] = [];
    if (welleIds.length > 0) {
      const { data } = await supabase
        .from('wellen_markets')
        .select('welle_id, market_id')
        .in('welle_id', welleIds);
      welleMarkets = data || [];
      
      // Get those market details
      const progressMarketIds = [...new Set(welleMarkets.map(wm => wm.market_id))];
      if (progressMarketIds.length > 0) {
        const { data: progressMarketsData } = await supabase
          .from('markets')
          .select('id, name, chain, address, city')
          .in('id', progressMarketIds);
        if (progressMarketsData) {
          markets.push(...progressMarketsData.filter(m => !markets.find((em: any) => em.id === m.id)));
        }
      }
    }
    
    // Transform vorbestellungen
    const progressActivities: Activity[] = (progressData || []).map(p => {
      const gl = gls.find((g: any) => g.id === p.gebietsleiter_id);
      const welle = wellen.find((w: any) => w.id === p.welle_id);
      const item = p.item_type === 'display' 
        ? displays.find((d: any) => d.id === p.item_id)
        : kartonware.find((k: any) => k.id === p.item_id);
      
      // Find a market for this welle
      const welleMarket = welleMarkets.find(wm => wm.welle_id === p.welle_id);
      const market = welleMarket ? markets.find((m: any) => m.id === welleMarket.market_id) : null;
      
      const itemName = item?.name || (p.item_type === 'display' ? 'Display' : 'Kartonware');
      
      return {
        id: p.id,
        type: 'vorbestellung' as const,
        glId: p.gebietsleiter_id,
        glName: gl?.name || 'Unknown',
        marketId: market?.id || '',
        marketChain: market?.chain || welle?.name || 'Welle',
        marketAddress: market?.address || '',
        marketCity: market?.city || '',
        action: `+${p.current_number} ${p.item_type === 'display' ? 'Display' : 'Kartonware'}`,
        details: {
          welleId: p.welle_id,
          welleName: welle?.name || 'Unknown',
          itemId: p.item_id,
          itemName,
          itemType: p.item_type,
          quantity: p.current_number
        },
        createdAt: p.updated_at || p.created_at
      };
    });
    
    // Transform vorverkäufe
    const vorverkaufActivities: Activity[] = (vorverkaufData || []).map(v => {
      const gl = gls.find((g: any) => g.id === v.gebietsleiter_id);
      const market = markets.find((m: any) => m.id === v.market_id);
      
      return {
        id: v.id,
        type: 'vorverkauf' as const,
        glId: v.gebietsleiter_id,
        glName: gl?.name || 'Unknown',
        marketId: v.market_id,
        marketChain: market?.chain || 'Unknown',
        marketAddress: market?.address || '',
        marketCity: market?.city || '',
        action: `Vorverkauf: ${v.reason}`,
        details: {
          reason: v.reason,
          notes: v.notes
        },
        createdAt: v.created_at
      };
    });
    
    // Combine and sort by date
    const allActivities = [...progressActivities, ...vorverkaufActivities]
      .sort((a, b) => new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime())
      .slice(offset, offset + limit);
    
    console.log(`✅ Fetched ${allActivities.length} activities (${progressActivities.length} vorbestellungen, ${vorverkaufActivities.length} vorverkäufe)`);
    res.json(allActivities);
  } catch (error: any) {
    console.error('❌ Error fetching activities:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

// ============================================================================
// UPDATE VORBESTELLUNG (wellen_gl_progress)
// ============================================================================
router.put('/vorbestellung/:id', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    const { current_number } = req.body;
    
    console.log(`📝 Updating vorbestellung ${id}...`);
    
    const { data, error } = await supabase
      .from('wellen_gl_progress')
      .update({ current_number, updated_at: new Date().toISOString() })
      .eq('id', id)
      .select()
      .single();
    
    if (error) throw error;
    
    console.log(`✅ Updated vorbestellung ${id}`);
    res.json(data);
  } catch (error: any) {
    console.error('❌ Error updating vorbestellung:', error);
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
    
    const updateData: any = {};
    if (reason !== undefined) updateData.reason = reason;
    if (notes !== undefined) updateData.notes = notes;
    
    const { data, error } = await supabase
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
    
    if (type === 'vorbestellung') {
      const { error } = await supabase
        .from('wellen_gl_progress')
        .delete()
        .eq('id', id);
      
      if (error) throw error;
    } else if (type === 'vorverkauf') {
      const { error } = await supabase
        .from('vorverkauf_entries')
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
