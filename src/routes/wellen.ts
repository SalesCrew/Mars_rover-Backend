import { Router, Request, Response } from 'express';
import { supabase } from '../config/supabase';

const router = Router();

// ============================================================================
// DASHBOARD: GET CHAIN AVERAGES
// ============================================================================
router.get('/dashboard/chain-averages', async (req: Request, res: Response) => {
  try {
    console.log('📊 Fetching chain averages...');

    // Chain groupings
    const chains = {
      billa: ['Adeg', 'Billa+', 'BILLA+', 'BILLA Plus', 'BILLA+ Privat', 'BILLA Plus Privat', 'BILLA Privat'],
      spar: ['Spar', 'SPAR Privat Popovic', 'Spar Gourmet', 'Eurospar'],
      interspar: ['Interspar'],
      hagebau: ['Hagebau']
    };

    const chainAverages = await Promise.all([
      // BILLA AVERAGE
      (async () => {
        const chainTypes = chains.billa;
        
        // Get all markets of this chain type
        const { data: markets, error: marketsError } = await supabase
          .from('markets')
          .select('id')
          .in('chain', chainTypes);
        
        if (marketsError) throw marketsError;
        
        const marketIds = (markets || []).map(m => m.id);
        const totalMarkets = marketIds.length;
        
        if (totalMarkets === 0) {
          return {
            chainName: 'Billa',
            chainColor: 'linear-gradient(135deg, #FED304, #F9C80E)',
            goalType: 'percentage' as const,
            goalPercentage: 80,
            totalMarkets: 0,
            marketsWithProgress: 0,
            currentPercentage: 0
          };
        }
        
        // Get all wellen assigned to these markets
        const { data: welleMarkets } = await supabase
          .from('wellen_markets')
          .select('welle_id, market_id')
          .in('market_id', marketIds);
        
        const welleIds = [...new Set((welleMarkets || []).map(wm => wm.welle_id))];
        
        if (welleIds.length === 0) {
          return {
            chainName: 'Billa',
            chainColor: 'linear-gradient(135deg, #FED304, #F9C80E)',
            goalType: 'percentage' as const,
            goalPercentage: 80,
            totalMarkets,
            marketsWithProgress: 0,
            currentPercentage: 0
          };
        }
        
        // Get all displays and kartonware for these wellen
        const { data: displays } = await supabase
          .from('wellen_displays')
          .select('id, target_number, welle_id')
          .in('welle_id', welleIds);
        
        const { data: kartonware } = await supabase
          .from('wellen_kartonware')
          .select('id, target_number, welle_id')
          .in('welle_id', welleIds);
        
        // Get all progress for these items
        const { data: displayProgress } = await supabase
          .from('wellen_gl_progress')
          .select('current_number, item_id, gebietsleiter_id')
          .eq('item_type', 'display')
          .in('welle_id', welleIds);
        
        const { data: kartonwareProgress } = await supabase
          .from('wellen_gl_progress')
          .select('current_number, item_id, gebietsleiter_id')
          .eq('item_type', 'kartonware')
          .in('welle_id', welleIds);
        
        // Calculate totals
        const totalTarget = 
          (displays || []).reduce((sum, d) => sum + d.target_number, 0) +
          (kartonware || []).reduce((sum, k) => sum + k.target_number, 0);
        
        const totalCurrent = 
          (displayProgress || []).reduce((sum, p) => sum + p.current_number, 0) +
          (kartonwareProgress || []).reduce((sum, p) => sum + p.current_number, 0);
        
        // Count unique markets with progress
        const marketsWithProgressSet = new Set<string>();
        for (const progress of [...(displayProgress || []), ...(kartonwareProgress || [])]) {
          // Find the welle_id for this progress item
          const display = (displays || []).find(d => d.id === progress.item_id);
          const kw = (kartonware || []).find(k => k.id === progress.item_id);
          const welleId = display?.welle_id || kw?.welle_id;
          
          if (welleId) {
            // Find markets assigned to this welle
            const welleMarketsForProgress = (welleMarkets || [])
              .filter(wm => wm.welle_id === welleId)
              .map(wm => wm.market_id);
            welleMarketsForProgress.forEach(mid => marketsWithProgressSet.add(mid));
          }
        }
        
        const currentPercentage = totalTarget > 0 
          ? Math.round((totalCurrent / totalTarget) * 100 * 100) / 100 
          : 0;
        
        return {
          chainName: 'Billa',
          chainColor: 'linear-gradient(135deg, #FED304, #F9C80E)',
          goalType: 'percentage' as const,
          goalPercentage: 80,
          totalMarkets,
          marketsWithProgress: marketsWithProgressSet.size,
          currentPercentage
        };
      })(),
      
      // SPAR AVERAGE
      (async () => {
        const chainTypes = chains.spar;
        
        const { data: markets, error: marketsError } = await supabase
          .from('markets')
          .select('id')
          .in('chain', chainTypes);
        
        if (marketsError) throw marketsError;
        
        const marketIds = (markets || []).map(m => m.id);
        const totalMarkets = marketIds.length;
        
        if (totalMarkets === 0) {
          return {
            chainName: 'Spar',
            chainColor: 'linear-gradient(135deg, #EF4444, #DC2626)',
            goalType: 'percentage' as const,
            goalPercentage: 60,
            totalMarkets: 0,
            marketsWithProgress: 0,
            currentPercentage: 0
          };
        }
        
        const { data: welleMarkets } = await supabase
          .from('wellen_markets')
          .select('welle_id, market_id')
          .in('market_id', marketIds);
        
        const welleIds = [...new Set((welleMarkets || []).map(wm => wm.welle_id))];
        
        if (welleIds.length === 0) {
          return {
            chainName: 'Spar',
            chainColor: 'linear-gradient(135deg, #EF4444, #DC2626)',
            goalType: 'percentage' as const,
            goalPercentage: 60,
            totalMarkets,
            marketsWithProgress: 0,
            currentPercentage: 0
          };
        }
        
        const { data: displays } = await supabase
          .from('wellen_displays')
          .select('id, target_number, welle_id')
          .in('welle_id', welleIds);
        
        const { data: kartonware } = await supabase
          .from('wellen_kartonware')
          .select('id, target_number, welle_id')
          .in('welle_id', welleIds);
        
        const { data: displayProgress } = await supabase
          .from('wellen_gl_progress')
          .select('current_number, item_id, gebietsleiter_id')
          .eq('item_type', 'display')
          .in('welle_id', welleIds);
        
        const { data: kartonwareProgress } = await supabase
          .from('wellen_gl_progress')
          .select('current_number, item_id, gebietsleiter_id')
          .eq('item_type', 'kartonware')
          .in('welle_id', welleIds);
        
        const totalTarget = 
          (displays || []).reduce((sum, d) => sum + d.target_number, 0) +
          (kartonware || []).reduce((sum, k) => sum + k.target_number, 0);
        
        const totalCurrent = 
          (displayProgress || []).reduce((sum, p) => sum + p.current_number, 0) +
          (kartonwareProgress || []).reduce((sum, p) => sum + p.current_number, 0);
        
        const marketsWithProgressSet = new Set<string>();
        for (const progress of [...(displayProgress || []), ...(kartonwareProgress || [])]) {
          const display = (displays || []).find(d => d.id === progress.item_id);
          const kw = (kartonware || []).find(k => k.id === progress.item_id);
          const welleId = display?.welle_id || kw?.welle_id;
          
          if (welleId) {
            const welleMarketsForProgress = (welleMarkets || [])
              .filter(wm => wm.welle_id === welleId)
              .map(wm => wm.market_id);
            welleMarketsForProgress.forEach(mid => marketsWithProgressSet.add(mid));
          }
        }
        
        const currentPercentage = totalTarget > 0 
          ? Math.round((totalCurrent / totalTarget) * 100 * 100) / 100 
          : 0;
        
        return {
          chainName: 'Spar',
          chainColor: 'linear-gradient(135deg, #EF4444, #DC2626)',
          goalType: 'percentage' as const,
          goalPercentage: 60,
          totalMarkets,
          marketsWithProgress: marketsWithProgressSet.size,
          currentPercentage
        };
      })(),
      
      // INTERSPAR AVERAGE
      (async () => {
        const chainTypes = chains.interspar;
        
        const { data: markets, error: marketsError } = await supabase
          .from('markets')
          .select('id')
          .in('chain', chainTypes);
        
        if (marketsError) throw marketsError;
        
        const marketIds = (markets || []).map(m => m.id);
        const totalMarkets = marketIds.length;
        
        if (totalMarkets === 0) {
          return {
            chainName: 'Interspar',
            chainColor: 'linear-gradient(135deg, #DC2626, #991B1B)',
            goalType: 'value' as const,
            goalValue: 0,
            currentValue: 0,
            totalValue: 0,
            totalMarkets: 0,
            marketsWithProgress: 0
          };
        }
        
        const { data: welleMarkets } = await supabase
          .from('wellen_markets')
          .select('welle_id, market_id')
          .in('market_id', marketIds);
        
        const welleIds = [...new Set((welleMarkets || []).map(wm => wm.welle_id))];
        
        if (welleIds.length === 0) {
          return {
            chainName: 'Interspar',
            chainColor: 'linear-gradient(135deg, #DC2626, #991B1B)',
            goalType: 'value' as const,
            goalValue: 0,
            currentValue: 0,
            totalValue: 0,
            totalMarkets,
            marketsWithProgress: 0
          };
        }
        
        // Get wellen to sum goal values
        const { data: wellen } = await supabase
          .from('wellen')
          .select('goal_value')
          .in('id', welleIds)
          .eq('goal_type', 'value');
        
        const goalValue = (wellen || []).reduce((sum, w) => sum + (w.goal_value || 0), 0);
        
        const { data: displays } = await supabase
          .from('wellen_displays')
          .select('id, target_number, item_value, welle_id')
          .in('welle_id', welleIds);
        
        const { data: kartonware } = await supabase
          .from('wellen_kartonware')
          .select('id, target_number, item_value, welle_id')
          .in('welle_id', welleIds);
        
        const { data: displayProgress } = await supabase
          .from('wellen_gl_progress')
          .select('current_number, item_id, gebietsleiter_id')
          .eq('item_type', 'display')
          .in('welle_id', welleIds);
        
        const { data: kartonwareProgress } = await supabase
          .from('wellen_gl_progress')
          .select('current_number, item_id, gebietsleiter_id')
          .eq('item_type', 'kartonware')
          .in('welle_id', welleIds);
        
        // Calculate total value (target * item_value)
        const totalValue = 
          (displays || []).reduce((sum, d) => sum + (d.target_number * (d.item_value || 0)), 0) +
          (kartonware || []).reduce((sum, k) => sum + (k.target_number * (k.item_value || 0)), 0);
        
        // Calculate current value (current * item_value)
        let currentValue = 0;
        for (const progress of (displayProgress || [])) {
          const display = (displays || []).find(d => d.id === progress.item_id);
          if (display) {
            currentValue += progress.current_number * (display.item_value || 0);
          }
        }
        for (const progress of (kartonwareProgress || [])) {
          const kw = (kartonware || []).find(k => k.id === progress.item_id);
          if (kw) {
            currentValue += progress.current_number * (kw.item_value || 0);
          }
        }
        
        const marketsWithProgressSet = new Set<string>();
        for (const progress of [...(displayProgress || []), ...(kartonwareProgress || [])]) {
          const display = (displays || []).find(d => d.id === progress.item_id);
          const kw = (kartonware || []).find(k => k.id === progress.item_id);
          const welleId = display?.welle_id || kw?.welle_id;
          
          if (welleId) {
            const welleMarketsForProgress = (welleMarkets || [])
              .filter(wm => wm.welle_id === welleId)
              .map(wm => wm.market_id);
            welleMarketsForProgress.forEach(mid => marketsWithProgressSet.add(mid));
          }
        }
        
        return {
          chainName: 'Interspar',
          chainColor: 'linear-gradient(135deg, #DC2626, #991B1B)',
          goalType: 'value' as const,
          goalValue: Math.round(goalValue * 100) / 100,
          currentValue: Math.round(currentValue * 100) / 100,
          totalValue: Math.round(totalValue * 100) / 100,
          totalMarkets,
          marketsWithProgress: marketsWithProgressSet.size
        };
      })(),
      
      // HAGEBAU AVERAGE
      (async () => {
        const chainTypes = chains.hagebau;
        
        const { data: markets, error: marketsError } = await supabase
          .from('markets')
          .select('id')
          .in('chain', chainTypes);
        
        if (marketsError) throw marketsError;
        
        const marketIds = (markets || []).map(m => m.id);
        const totalMarkets = marketIds.length;
        
        if (totalMarkets === 0) {
          return {
            chainName: 'Hagebau',
            chainColor: 'linear-gradient(135deg, #06B6D4, #0891B2)',
            goalType: 'value' as const,
            goalValue: 0,
            currentValue: 0,
            totalValue: 0,
            totalMarkets: 0,
            marketsWithProgress: 0
          };
        }
        
        const { data: welleMarkets } = await supabase
          .from('wellen_markets')
          .select('welle_id, market_id')
          .in('market_id', marketIds);
        
        const welleIds = [...new Set((welleMarkets || []).map(wm => wm.welle_id))];
        
        if (welleIds.length === 0) {
          return {
            chainName: 'Hagebau',
            chainColor: 'linear-gradient(135deg, #06B6D4, #0891B2)',
            goalType: 'value' as const,
            goalValue: 0,
            currentValue: 0,
            totalValue: 0,
            totalMarkets,
            marketsWithProgress: 0
          };
        }
        
        const { data: wellen } = await supabase
          .from('wellen')
          .select('goal_value')
          .in('id', welleIds)
          .eq('goal_type', 'value');
        
        const goalValue = (wellen || []).reduce((sum, w) => sum + (w.goal_value || 0), 0);
        
        const { data: displays } = await supabase
          .from('wellen_displays')
          .select('id, target_number, item_value, welle_id')
          .in('welle_id', welleIds);
        
        const { data: kartonware } = await supabase
          .from('wellen_kartonware')
          .select('id, target_number, item_value, welle_id')
          .in('welle_id', welleIds);
        
        const { data: displayProgress } = await supabase
          .from('wellen_gl_progress')
          .select('current_number, item_id, gebietsleiter_id')
          .eq('item_type', 'display')
          .in('welle_id', welleIds);
        
        const { data: kartonwareProgress } = await supabase
          .from('wellen_gl_progress')
          .select('current_number, item_id, gebietsleiter_id')
          .eq('item_type', 'kartonware')
          .in('welle_id', welleIds);
        
        const totalValue = 
          (displays || []).reduce((sum, d) => sum + (d.target_number * (d.item_value || 0)), 0) +
          (kartonware || []).reduce((sum, k) => sum + (k.target_number * (k.item_value || 0)), 0);
        
        let currentValue = 0;
        for (const progress of (displayProgress || [])) {
          const display = (displays || []).find(d => d.id === progress.item_id);
          if (display) {
            currentValue += progress.current_number * (display.item_value || 0);
          }
        }
        for (const progress of (kartonwareProgress || [])) {
          const kw = (kartonware || []).find(k => k.id === progress.item_id);
          if (kw) {
            currentValue += progress.current_number * (kw.item_value || 0);
          }
        }
        
        const marketsWithProgressSet = new Set<string>();
        for (const progress of [...(displayProgress || []), ...(kartonwareProgress || [])]) {
          const display = (displays || []).find(d => d.id === progress.item_id);
          const kw = (kartonware || []).find(k => k.id === progress.item_id);
          const welleId = display?.welle_id || kw?.welle_id;
          
          if (welleId) {
            const welleMarketsForProgress = (welleMarkets || [])
              .filter(wm => wm.welle_id === welleId)
              .map(wm => wm.market_id);
            welleMarketsForProgress.forEach(mid => marketsWithProgressSet.add(mid));
          }
        }
        
        return {
          chainName: 'Hagebau',
          chainColor: 'linear-gradient(135deg, #06B6D4, #0891B2)',
          goalType: 'value' as const,
          goalValue: Math.round(goalValue * 100) / 100,
          currentValue: Math.round(currentValue * 100) / 100,
          totalValue: Math.round(totalValue * 100) / 100,
          totalMarkets,
          marketsWithProgress: marketsWithProgressSet.size
        };
      })()
    ]);

    console.log(`✅ Fetched chain averages for ${chainAverages.length} chains`);
    res.json(chainAverages);
  } catch (error: any) {
    console.error('❌ Error fetching chain averages:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

// ============================================================================
// DASHBOARD: GET WAVES PROGRESS
// ============================================================================
router.get('/dashboard/waves', async (req: Request, res: Response) => {
  try {
    console.log('📊 Fetching waves for dashboard...');

    const today = new Date();
    const threeDaysAgo = new Date(today.getTime() - 3 * 24 * 60 * 60 * 1000);

    // Fetch active and recently finished waves
    const { data: wellen, error: wellenError } = await supabase
      .from('wellen')
      .select('*')
      .or(`status.eq.active,and(status.eq.past,end_date.gte.${threeDaysAgo.toISOString().split('T')[0]})`)
      .order('start_date', { ascending: false });

    if (wellenError) throw wellenError;

    const wavesProgress = await Promise.all(
      (wellen || []).map(async (welle) => {
        // Fetch displays
        const { data: displays } = await supabase
          .from('wellen_displays')
          .select('id, target_number, item_value')
          .eq('welle_id', welle.id);

        // Fetch kartonware
        const { data: kartonware } = await supabase
          .from('wellen_kartonware')
          .select('id, target_number, item_value')
          .eq('welle_id', welle.id);

        // Fetch assigned markets count
        const { data: welleMarkets } = await supabase
          .from('wellen_markets')
          .select('market_id')
          .eq('welle_id', welle.id);

        // Fetch progress
        const { data: progressData } = await supabase
          .from('wellen_gl_progress')
          .select('current_number, item_type, item_id, gebietsleiter_id')
          .eq('welle_id', welle.id);

        // Calculate display aggregates
        let displayCount = 0;
        let displayTarget = 0;
        for (const display of (displays || [])) {
          displayTarget += display.target_number;
          const progress = (progressData || [])
            .filter(p => p.item_type === 'display' && p.item_id === display.id)
            .reduce((sum, p) => sum + p.current_number, 0);
          displayCount += progress;
        }

        // Calculate kartonware aggregates
        let kartonwareCount = 0;
        let kartonwareTarget = 0;
        for (const kw of (kartonware || [])) {
          kartonwareTarget += kw.target_number;
          const progress = (progressData || [])
            .filter(p => p.item_type === 'kartonware' && p.item_id === kw.id)
            .reduce((sum, p) => sum + p.current_number, 0);
          kartonwareCount += progress;
        }

        // Calculate current value for value-based goals
        let currentValue = 0;
        if (welle.goal_type === 'value') {
          for (const progress of (progressData || [])) {
            if (progress.item_type === 'display') {
              const display = (displays || []).find(d => d.id === progress.item_id);
              if (display) {
                currentValue += progress.current_number * (display.item_value || 0);
              }
            } else if (progress.item_type === 'kartonware') {
              const kw = (kartonware || []).find(k => k.id === progress.item_id);
              if (kw) {
                currentValue += progress.current_number * (kw.item_value || 0);
              }
            }
          }
        }

        // Count unique participating GLs
        const participatingGLs = new Set((progressData || []).map(p => p.gebietsleiter_id)).size;

        // Determine status
        const status = welle.status === 'past' ? 'finished' : welle.status;

        return {
          id: welle.id,
          name: welle.name,
          startDate: welle.start_date,
          endDate: welle.end_date,
          status,
          goalType: welle.goal_type,
          goalPercentage: welle.goal_percentage,
          goalValue: welle.goal_value,
          currentValue: Math.round(currentValue * 100) / 100,
          displayCount,
          displayTarget,
          kartonwareCount,
          kartonwareTarget,
          assignedMarkets: (welleMarkets || []).length,
          participatingGLs
        };
      })
    );

    console.log(`✅ Fetched ${wavesProgress.length} waves for dashboard`);
    res.json(wavesProgress);
  } catch (error: any) {
    console.error('❌ Error fetching waves:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

// ============================================================================
// GET ALL WELLEN
// ============================================================================
router.get('/', async (req: Request, res: Response) => {
  try {
    console.log('📋 Fetching all wellen...');
    
    // Fetch all wellen
    const { data: wellen, error: wellenError } = await supabase
      .from('wellen')
      .select('*')
      .order('created_at', { ascending: false });

    if (wellenError) throw wellenError;

    // For each welle, fetch related data
    const wellenWithDetails = await Promise.all(
      (wellen || []).map(async (welle) => {
        // Fetch displays
        const { data: displays } = await supabase
          .from('wellen_displays')
          .select('*')
          .eq('welle_id', welle.id)
          .order('display_order', { ascending: true });

        // Fetch kartonware
        const { data: kartonware } = await supabase
          .from('wellen_kartonware')
          .select('*')
          .eq('welle_id', welle.id)
          .order('kartonware_order', { ascending: true });

        // Fetch KW days
        const { data: kwDays } = await supabase
          .from('wellen_kw_days')
          .select('*')
          .eq('welle_id', welle.id)
          .order('kw_order', { ascending: true });

        // Fetch assigned market IDs
        const { data: welleMarkets } = await supabase
          .from('wellen_markets')
          .select('market_id')
          .eq('welle_id', welle.id);

        // Calculate progress aggregates
        const { data: progressData } = await supabase
          .from('wellen_gl_progress')
          .select('current_number, item_type, item_id, gebietsleiter_id')
          .eq('welle_id', welle.id);

        const uniqueGLs = new Set((progressData || []).map(p => p.gebietsleiter_id)).size;

        // Derive types based on what displays/kartonware exist
        const types: ('display' | 'kartonware')[] = [];
        if (displays && displays.length > 0) types.push('display');
        if (kartonware && kartonware.length > 0) types.push('kartonware');

        return {
          id: welle.id,
          name: welle.name,
          image: welle.image_url,
          startDate: welle.start_date,
          endDate: welle.end_date,
          types, // Derived from displays/kartonware
          status: welle.status,
          goalType: welle.goal_type,
          goalPercentage: welle.goal_percentage,
          goalValue: welle.goal_value,
          displayCount: displays?.length || 0,
          kartonwareCount: kartonware?.length || 0,
          displays: (displays || []).map(d => ({
            id: d.id,
            name: d.name,
            targetNumber: d.target_number,
            currentNumber: (progressData || [])
              .filter(p => p.item_type === 'display' && p.item_id === d.id)
              .reduce((sum, p) => sum + p.current_number, 0),
            picture: d.picture_url,
            itemValue: d.item_value
          })),
          kartonwareItems: (kartonware || []).map(k => ({
            id: k.id,
            name: k.name,
            targetNumber: k.target_number,
            currentNumber: (progressData || [])
              .filter(p => p.item_type === 'kartonware' && p.item_id === k.id)
              .reduce((sum, p) => sum + p.current_number, 0),
            picture: k.picture_url,
            itemValue: k.item_value
          })),
          kwDays: (kwDays || []).map(kw => ({
            kw: kw.kw,
            days: kw.days
          })),
          assignedMarketIds: (welleMarkets || []).map(wm => wm.market_id),
          participatingGLs: uniqueGLs,
          totalGLs: 45 // TODO: Get actual total from users table
        };
      })
    );

    console.log(`✅ Fetched ${wellenWithDetails.length} wellen`);
    res.json(wellenWithDetails);
  } catch (error: any) {
    console.error('❌ Error fetching wellen:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

// ============================================================================
// GET SINGLE WELLE BY ID
// ============================================================================
router.get('/:id', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    console.log(`📄 Fetching welle ${id}...`);

    const { data: welle, error: welleError } = await supabase
      .from('wellen')
      .select('*')
      .eq('id', id)
      .single();

    if (welleError) throw welleError;
    if (!welle) {
      return res.status(404).json({ error: 'Welle not found' });
    }

    // Fetch related data (same as GET ALL logic)
    const { data: displays } = await supabase
      .from('wellen_displays')
      .select('*')
      .eq('welle_id', id)
      .order('display_order', { ascending: true });

    const { data: kartonware } = await supabase
      .from('wellen_kartonware')
      .select('*')
      .eq('welle_id', id)
      .order('kartonware_order', { ascending: true });

    const { data: kwDays } = await supabase
      .from('wellen_kw_days')
      .select('*')
      .eq('welle_id', id)
      .order('kw_order', { ascending: true });

    const { data: welleMarkets } = await supabase
      .from('wellen_markets')
      .select('market_id')
      .eq('welle_id', id);

    const { data: progressData } = await supabase
      .from('wellen_gl_progress')
      .select('current_number, item_type, item_id, gebietsleiter_id')
      .eq('welle_id', id);

    const uniqueGLs = new Set((progressData || []).map(p => p.gebietsleiter_id)).size;

    // Derive types based on what displays/kartonware exist
    const types: ('display' | 'kartonware')[] = [];
    if (displays && displays.length > 0) types.push('display');
    if (kartonware && kartonware.length > 0) types.push('kartonware');

    const welleWithDetails = {
      id: welle.id,
      name: welle.name,
      image: welle.image_url,
      startDate: welle.start_date,
      endDate: welle.end_date,
      types, // Derived from displays/kartonware
      status: welle.status,
      goalType: welle.goal_type,
      goalPercentage: welle.goal_percentage,
      goalValue: welle.goal_value,
      displayCount: displays?.length || 0,
      kartonwareCount: kartonware?.length || 0,
      displays: (displays || []).map(d => ({
        id: d.id,
        name: d.name,
        targetNumber: d.target_number,
        currentNumber: (progressData || [])
          .filter(p => p.item_type === 'display' && p.item_id === d.id)
          .reduce((sum, p) => sum + p.current_number, 0),
        picture: d.picture_url,
        itemValue: d.item_value
      })),
      kartonwareItems: (kartonware || []).map(k => ({
        id: k.id,
        name: k.name,
        targetNumber: k.target_number,
        currentNumber: (progressData || [])
          .filter(p => p.item_type === 'kartonware' && p.item_id === k.id)
          .reduce((sum, p) => sum + p.current_number, 0),
        picture: k.picture_url,
        itemValue: k.item_value
      })),
      kwDays: (kwDays || []).map(kw => ({
        kw: kw.kw,
        days: kw.days
      })),
      assignedMarketIds: (welleMarkets || []).map(wm => wm.market_id),
      participatingGLs: uniqueGLs,
      totalGLs: 45
    };

    console.log(`✅ Fetched welle ${id}`);
    res.json(welleWithDetails);
  } catch (error: any) {
    console.error('❌ Error fetching welle:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

// ============================================================================
// CREATE NEW WELLE
// ============================================================================
router.post('/', async (req: Request, res: Response) => {
  try {
    console.log('➕ Creating new welle...');
    const {
      name,
      image,
      startDate,
      endDate,
      goalType,
      goalPercentage,
      goalValue,
      displays,
      kartonwareItems,
      kwDays,
      assignedMarketIds
    } = req.body;

    // Validate required fields
    if (!name || !startDate || !endDate || !goalType) {
      return res.status(400).json({ error: 'Missing required fields' });
    }

    // Determine initial status based on dates
    const today = new Date().toISOString().split('T')[0];
    let status = 'upcoming';
    if (startDate <= today && endDate >= today) {
      status = 'active';
    } else if (endDate < today) {
      status = 'past';
    }

    // Insert main welle record
    const { data: welle, error: welleError } = await supabase
      .from('wellen')
      .insert({
        name,
        image_url: image,
        start_date: startDate,
        end_date: endDate,
        status,
        goal_type: goalType,
        goal_percentage: goalType === 'percentage' ? goalPercentage : null,
        goal_value: goalType === 'value' ? goalValue : null
      })
      .select()
      .single();

    if (welleError) throw welleError;
    console.log(`✅ Created welle ${welle.id}`);

    // Insert displays
    if (displays && displays.length > 0) {
      const displaysToInsert = displays.map((d: any, index: number) => ({
        welle_id: welle.id,
        name: d.name,
        target_number: d.targetNumber,
        item_value: d.itemValue || null,
        picture_url: d.picture || null,
        display_order: index
      }));

      const { error: displaysError } = await supabase
        .from('wellen_displays')
        .insert(displaysToInsert);

      if (displaysError) throw displaysError;
      console.log(`✅ Created ${displays.length} displays`);
    }

    // Insert kartonware
    if (kartonwareItems && kartonwareItems.length > 0) {
      const kartonwareToInsert = kartonwareItems.map((k: any, index: number) => ({
        welle_id: welle.id,
        name: k.name,
        target_number: k.targetNumber,
        item_value: k.itemValue || null,
        picture_url: k.picture || null,
        kartonware_order: index
      }));

      const { error: kartonwareError } = await supabase
        .from('wellen_kartonware')
        .insert(kartonwareToInsert);

      if (kartonwareError) throw kartonwareError;
      console.log(`✅ Created ${kartonwareItems.length} kartonware items`);
    }

    // Insert KW days
    if (kwDays && kwDays.length > 0) {
      const kwDaysToInsert = kwDays.map((kw: any, index: number) => ({
        welle_id: welle.id,
        kw: kw.kw,
        days: kw.days,
        kw_order: index
      }));

      const { error: kwDaysError } = await supabase
        .from('wellen_kw_days')
        .insert(kwDaysToInsert);

      if (kwDaysError) throw kwDaysError;
      console.log(`✅ Created ${kwDays.length} KW day entries`);
    }

    // Insert market assignments
    if (assignedMarketIds && assignedMarketIds.length > 0) {
      const marketsToInsert = assignedMarketIds.map((marketId: string) => ({
        welle_id: welle.id,
        market_id: marketId
      }));

      const { error: marketsError } = await supabase
        .from('wellen_markets')
        .insert(marketsToInsert);

      if (marketsError) throw marketsError;
      console.log(`✅ Assigned ${assignedMarketIds.length} markets`);
    }

    console.log(`✅ Successfully created welle ${welle.id} with all related data`);
    res.status(201).json({ id: welle.id, message: 'Welle created successfully' });
  } catch (error: any) {
    console.error('❌ Error creating welle:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

// ============================================================================
// UPDATE WELLE
// ============================================================================
router.put('/:id', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    console.log(`✏️ Updating welle ${id}...`);

    const {
      name,
      image,
      startDate,
      endDate,
      goalType,
      goalPercentage,
      goalValue,
      displays,
      kartonwareItems,
      kwDays,
      assignedMarketIds
    } = req.body;

    // Update main welle record
    const { error: welleError } = await supabase
      .from('wellen')
      .update({
        name,
        image_url: image,
        start_date: startDate,
        end_date: endDate,
        goal_type: goalType,
        goal_percentage: goalType === 'percentage' ? goalPercentage : null,
        goal_value: goalType === 'value' ? goalValue : null
      })
      .eq('id', id);

    if (welleError) throw welleError;

    // Delete and recreate displays
    await supabase.from('wellen_displays').delete().eq('welle_id', id);
    if (displays && displays.length > 0) {
      const displaysToInsert = displays.map((d: any, index: number) => ({
        welle_id: id,
        name: d.name,
        target_number: d.targetNumber,
        item_value: d.itemValue || null,
        picture_url: d.picture || null,
        display_order: index
      }));
      await supabase.from('wellen_displays').insert(displaysToInsert);
    }

    // Delete and recreate kartonware
    await supabase.from('wellen_kartonware').delete().eq('welle_id', id);
    if (kartonwareItems && kartonwareItems.length > 0) {
      const kartonwareToInsert = kartonwareItems.map((k: any, index: number) => ({
        welle_id: id,
        name: k.name,
        target_number: k.targetNumber,
        item_value: k.itemValue || null,
        picture_url: k.picture || null,
        kartonware_order: index
      }));
      await supabase.from('wellen_kartonware').insert(kartonwareToInsert);
    }

    // Delete and recreate KW days
    await supabase.from('wellen_kw_days').delete().eq('welle_id', id);
    if (kwDays && kwDays.length > 0) {
      const kwDaysToInsert = kwDays.map((kw: any, index: number) => ({
        welle_id: id,
        kw: kw.kw,
        days: kw.days,
        kw_order: index
      }));
      await supabase.from('wellen_kw_days').insert(kwDaysToInsert);
    }

    // Delete and recreate market assignments
    await supabase.from('wellen_markets').delete().eq('welle_id', id);
    if (assignedMarketIds && assignedMarketIds.length > 0) {
      const marketsToInsert = assignedMarketIds.map((marketId: string) => ({
        welle_id: id,
        market_id: marketId
      }));
      await supabase.from('wellen_markets').insert(marketsToInsert);
    }

    console.log(`✅ Updated welle ${id}`);
    res.json({ message: 'Welle updated successfully' });
  } catch (error: any) {
    console.error('❌ Error updating welle:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

// ============================================================================
// DELETE WELLE
// ============================================================================
router.delete('/:id', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    console.log(`🗑️ Deleting welle ${id}...`);

    // Delete welle (cascade will handle related tables)
    const { error } = await supabase
      .from('wellen')
      .delete()
      .eq('id', id);

    if (error) throw error;

    console.log(`✅ Deleted welle ${id}`);
    res.json({ message: 'Welle deleted successfully' });
  } catch (error: any) {
    console.error('❌ Error deleting welle:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

// ============================================================================
// UPDATE GL PROGRESS (BATCH)
// ============================================================================
router.post('/:id/progress/batch', async (req: Request, res: Response) => {
  try {
    const { id: welleId } = req.params;
    const { gebietsleiter_id, market_id, items, photo_url } = req.body;

    console.log(`📊 Batch updating GL progress for welle ${welleId}...`);

    if (!gebietsleiter_id || !items || !Array.isArray(items)) {
      return res.status(400).json({ error: 'Missing required fields: gebietsleiter_id, items' });
    }

    // Upsert all progress entries
    const progressEntries = items.map((item: any) => ({
      welle_id: welleId,
      gebietsleiter_id,
      item_type: item.item_type,
      item_id: item.item_id,
      current_number: item.current_number
    }));

    const { error } = await supabase
      .from('wellen_gl_progress')
      .upsert(progressEntries, {
        onConflict: 'welle_id,gebietsleiter_id,item_type,item_id'
      });

    if (error) throw error;

    console.log(`✅ Updated ${items.length} progress entries`);
    res.json({ 
      message: 'Progress updated successfully',
      items_updated: items.length 
    });
  } catch (error: any) {
    console.error('❌ Error updating batch progress:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

// ============================================================================
// UPDATE GL PROGRESS
// ============================================================================
router.post('/:id/progress', async (req: Request, res: Response) => {
  try {
    const { id: welleId } = req.params;
    const { gebietsleiter_id, item_type, item_id, current_number } = req.body;

    console.log(`📊 Updating GL progress for welle ${welleId}...`);

    // Upsert progress
    const { error } = await supabase
      .from('wellen_gl_progress')
      .upsert({
        welle_id: welleId,
        gebietsleiter_id,
        item_type,
        item_id,
        current_number
      }, {
        onConflict: 'welle_id,gebietsleiter_id,item_type,item_id'
      });

    if (error) throw error;

    console.log(`✅ Updated progress`);
    res.json({ message: 'Progress updated successfully' });
  } catch (error: any) {
    console.error('❌ Error updating progress:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

// ============================================================================
// GET GL PROGRESS FOR A WELLE
// ============================================================================
router.get('/:id/progress/:glId', async (req: Request, res: Response) => {
  try {
    const { id: welleId, glId } = req.params;
    console.log(`📊 Fetching GL progress for welle ${welleId}, GL ${glId}...`);

    const { data, error } = await supabase
      .from('wellen_gl_progress')
      .select('*')
      .eq('welle_id', welleId)
      .eq('gebietsleiter_id', glId);

    if (error) throw error;

    console.log(`✅ Fetched ${data?.length || 0} progress entries`);
    res.json(data || []);
  } catch (error: any) {
    console.error('❌ Error fetching progress:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

// ============================================================================
// GET ALL PROGRESS FOR A WELLE (ALL GLs)
// ============================================================================
router.get('/:id/all-progress', async (req: Request, res: Response) => {
  try {
    const { id: welleId } = req.params;
    console.log(`📊 Fetching all GL progress for welle ${welleId}...`);

    // Get all progress entries for this welle
    const { data: progressEntries, error: progressError } = await supabase
      .from('wellen_gl_progress')
      .select('*')
      .eq('welle_id', welleId)
      .order('created_at', { ascending: false });

    if (progressError) throw progressError;

    if (!progressEntries || progressEntries.length === 0) {
      return res.json([]);
    }

    // Get GL names
    const glIds = [...new Set(progressEntries.map(p => p.gebietsleiter_id))];
    const { data: gls } = await supabase
      .from('users')
      .select('id, email')
      .in('id', glIds);

    const { data: glDetails } = await supabase
      .from('gebietsleiter')
      .select('id, name')
      .in('id', glIds);

    // Get market names
    const marketIds = [...new Set(progressEntries.map(p => p.market_id).filter(Boolean))];
    const { data: markets } = await supabase
      .from('markets')
      .select('id, name, chain')
      .in('id', marketIds);

    // Get display and kartonware names
    const displayIds = progressEntries.filter(p => p.item_type === 'display').map(p => p.item_id);
    const kartonwareIds = progressEntries.filter(p => p.item_type === 'kartonware').map(p => p.item_id);

    const { data: displays } = await supabase
      .from('wellen_displays')
      .select('id, name, item_value')
      .in('id', displayIds);

    const { data: kartonware } = await supabase
      .from('wellen_kartonware')
      .select('id, name, item_value')
      .in('id', kartonwareIds);

    // Build response
    const response = progressEntries.map(entry => {
      const gl = glDetails?.find(g => g.id === entry.gebietsleiter_id);
      const glUser = gls?.find(u => u.id === entry.gebietsleiter_id);
      const market = markets?.find(m => m.id === entry.market_id);
      const item = entry.item_type === 'display'
        ? displays?.find(d => d.id === entry.item_id)
        : kartonware?.find(k => k.id === entry.item_id);

      const itemValue = item?.item_value || 0;
      const totalValue = entry.current_number * itemValue;

      return {
        id: entry.id,
        glName: gl?.name || 'Unknown',
        glEmail: glUser?.email || '',
        marketName: market?.name || 'Unknown',
        marketChain: market?.chain || '',
        itemType: entry.item_type,
        itemName: item?.name || 'Unknown',
        quantity: entry.current_number,
        value: totalValue,
        timestamp: entry.created_at,
        photoUrl: entry.photo_url
      };
    });

    console.log(`✅ Fetched ${response.length} progress entries for welle ${welleId}`);
    res.json(response);
  } catch (error: any) {
    console.error('❌ Error fetching all progress:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

export default router;
