import { Router, Request, Response } from 'express';
import { supabase } from '../config/supabase';

const router = Router();

// ============================================================================
// DASHBOARD: GET CHAIN AVERAGES
// ============================================================================
router.get('/dashboard/chain-averages', async (req: Request, res: Response) => {
  try {
    // Get GL filter from query params (comma-separated list of GL IDs)
    const glIdsParam = req.query.glIds as string | undefined;
    const glFilter = glIdsParam ? glIdsParam.split(',').filter(Boolean) : [];
    
    console.log('📊 Fetching chain averages...', glFilter.length > 0 ? `Filtering by GLs: ${glFilter.join(', ')}` : '');

    // Chain groupings
    const chains = {
      billa: ['Adeg', 'Billa+', 'BILLA+', 'BILLA Plus', 'BILLA+ Privat', 'BILLA Plus Privat', 'BILLA Privat'],
      spar: ['Spar', 'SPAR Privat Popovic', 'Spar Gourmet', 'Eurospar', 'Interspar'],
      zoofachhandel: ['Zoofachhandel', 'Futterhaus', 'Fressnapf', 'Das Futterhaus'],
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
        
        // Get all progress for these items (optionally filtered by GL)
        let displayProgressQuery = supabase
          .from('wellen_gl_progress')
          .select('current_number, item_id, gebietsleiter_id')
          .eq('item_type', 'display')
          .in('welle_id', welleIds);
        
        let kartonwareProgressQuery = supabase
          .from('wellen_gl_progress')
          .select('current_number, item_id, gebietsleiter_id')
          .eq('item_type', 'kartonware')
          .in('welle_id', welleIds);
        
        // Apply GL filter if specified
        if (glFilter.length > 0) {
          displayProgressQuery = displayProgressQuery.in('gebietsleiter_id', glFilter);
          kartonwareProgressQuery = kartonwareProgressQuery.in('gebietsleiter_id', glFilter);
        }
        
        const { data: displayProgress } = await displayProgressQuery;
        const { data: kartonwareProgress } = await kartonwareProgressQuery;
        
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
        
        // Get progress (optionally filtered by GL)
        let displayProgressQuery = supabase
          .from('wellen_gl_progress')
          .select('current_number, item_id, gebietsleiter_id')
          .eq('item_type', 'display')
          .in('welle_id', welleIds);
        
        let kartonwareProgressQuery = supabase
          .from('wellen_gl_progress')
          .select('current_number, item_id, gebietsleiter_id')
          .eq('item_type', 'kartonware')
          .in('welle_id', welleIds);
        
        if (glFilter.length > 0) {
          displayProgressQuery = displayProgressQuery.in('gebietsleiter_id', glFilter);
          kartonwareProgressQuery = kartonwareProgressQuery.in('gebietsleiter_id', glFilter);
        }
        
        const { data: displayProgress } = await displayProgressQuery;
        const { data: kartonwareProgress } = await kartonwareProgressQuery;
        
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
        const chainTypes = chains.zoofachhandel;
        
        const { data: markets, error: marketsError } = await supabase
          .from('markets')
          .select('id')
          .in('chain', chainTypes);
        
        if (marketsError) throw marketsError;
        
        const marketIds = (markets || []).map(m => m.id);
        const totalMarkets = marketIds.length;
        
        if (totalMarkets === 0) {
          return {
            chainName: 'Zoofachhandel',
            chainColor: 'linear-gradient(135deg, #EC4899, #DB2777)',
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
            chainName: 'Zoofachhandel',
            chainColor: 'linear-gradient(135deg, #EC4899, #DB2777)',
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
        
        // Get progress (optionally filtered by GL)
        let displayProgressQuery = supabase
          .from('wellen_gl_progress')
          .select('current_number, item_id, gebietsleiter_id')
          .eq('item_type', 'display')
          .in('welle_id', welleIds);
        
        let kartonwareProgressQuery = supabase
          .from('wellen_gl_progress')
          .select('current_number, item_id, gebietsleiter_id')
          .eq('item_type', 'kartonware')
          .in('welle_id', welleIds);
        
        if (glFilter.length > 0) {
          displayProgressQuery = displayProgressQuery.in('gebietsleiter_id', glFilter);
          kartonwareProgressQuery = kartonwareProgressQuery.in('gebietsleiter_id', glFilter);
        }
        
        const { data: displayProgress } = await displayProgressQuery;
        const { data: kartonwareProgress } = await kartonwareProgressQuery;
        
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
          chainName: 'Zoofachhandel',
          chainColor: 'linear-gradient(135deg, #EC4899, #DB2777)',
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
        
        // Get progress (optionally filtered by GL)
        let displayProgressQuery = supabase
          .from('wellen_gl_progress')
          .select('current_number, item_id, gebietsleiter_id')
          .eq('item_type', 'display')
          .in('welle_id', welleIds);
        
        let kartonwareProgressQuery = supabase
          .from('wellen_gl_progress')
          .select('current_number, item_id, gebietsleiter_id')
          .eq('item_type', 'kartonware')
          .in('welle_id', welleIds);
        
        if (glFilter.length > 0) {
          displayProgressQuery = displayProgressQuery.in('gebietsleiter_id', glFilter);
          kartonwareProgressQuery = kartonwareProgressQuery.in('gebietsleiter_id', glFilter);
        }
        
        const { data: displayProgress } = await displayProgressQuery;
        const { data: kartonwareProgress } = await kartonwareProgressQuery;
        
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
    // Get GL filter from query params (comma-separated list of GL IDs)
    const glIdsParam = req.query.glIds as string | undefined;
    const glFilter = glIdsParam ? glIdsParam.split(',').filter(Boolean) : [];
    
    console.log('📊 Fetching waves for dashboard...', glFilter.length > 0 ? `Filtering by GLs: ${glFilter.join(', ')}` : '');

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

        // Fetch progress (optionally filtered by GL)
        let progressQuery = supabase
          .from('wellen_gl_progress')
          .select('current_number, item_type, item_id, gebietsleiter_id')
          .eq('welle_id', welle.id);
        
        if (glFilter.length > 0) {
          progressQuery = progressQuery.in('gebietsleiter_id', glFilter);
        }
        
        const { data: progressData } = await progressQuery;

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
// UPDATE GL PROGRESS (BATCH) - Adds to existing values (cumulative)
// ============================================================================
router.post('/:id/progress/batch', async (req: Request, res: Response) => {
  try {
    const { id: welleId } = req.params;
    const { gebietsleiter_id, items } = req.body;

    console.log(`📊 Batch updating GL progress for welle ${welleId}...`);

    if (!gebietsleiter_id || !items || !Array.isArray(items)) {
      return res.status(400).json({ error: 'Missing required fields: gebietsleiter_id, items' });
    }

    // Fetch existing progress for this GL and welle
    const { data: existingProgress } = await supabase
      .from('wellen_gl_progress')
      .select('item_type, item_id, current_number')
      .eq('welle_id', welleId)
      .eq('gebietsleiter_id', gebietsleiter_id);

    // Create a map of existing progress for quick lookup
    const existingMap = new Map<string, number>();
    for (const p of (existingProgress || [])) {
      const key = `${p.item_type}:${p.item_id}`;
      existingMap.set(key, p.current_number || 0);
    }

    // Build upsert entries with cumulative values
    const progressEntries = items.map((item: any) => {
      const key = `${item.item_type}:${item.item_id}`;
      const existingValue = existingMap.get(key) || 0;
      const newTotal = existingValue + item.current_number;
      
      return {
        welle_id: welleId,
        gebietsleiter_id,
        item_type: item.item_type,
        item_id: item.item_id,
        current_number: newTotal
      };
    });

    const { error } = await supabase
      .from('wellen_gl_progress')
      .upsert(progressEntries, {
        onConflict: 'welle_id,gebietsleiter_id,item_type,item_id'
      });

    if (error) throw error;

    console.log(`✅ Updated ${items.length} progress entries (cumulative)`);
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
// UPDATE GL PROGRESS - Adds to existing value (cumulative)
// ============================================================================
router.post('/:id/progress', async (req: Request, res: Response) => {
  try {
    const { id: welleId } = req.params;
    const { gebietsleiter_id, item_type, item_id, current_number } = req.body;

    console.log(`📊 Updating GL progress for welle ${welleId}...`);

    // Fetch existing progress for this specific item
    const { data: existing } = await supabase
      .from('wellen_gl_progress')
      .select('current_number')
      .eq('welle_id', welleId)
      .eq('gebietsleiter_id', gebietsleiter_id)
      .eq('item_type', item_type)
      .eq('item_id', item_id)
      .single();

    const existingValue = existing?.current_number || 0;
    const newTotal = existingValue + current_number;

    // Upsert with cumulative value
    const { error } = await supabase
      .from('wellen_gl_progress')
      .upsert({
        welle_id: welleId,
        gebietsleiter_id,
        item_type,
        item_id,
        current_number: newTotal
      }, {
        onConflict: 'welle_id,gebietsleiter_id,item_type,item_id'
      });

    if (error) throw error;

    console.log(`✅ Updated progress entry (cumulative: ${existingValue} + ${current_number} = ${newTotal})`);
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
// GET ALL PROGRESS FOR A WELLE (ALL GLs) - For detailed view
// ============================================================================
router.get('/:id/all-progress', async (req: Request, res: Response) => {
  try {
    const { id: welleId } = req.params;
    
    const { data: progressEntries, error: progressError } = await supabase
      .from('wellen_gl_progress')
      .select('*')
      .eq('welle_id', welleId)
      .order('created_at', { ascending: false });

    if (progressError) {
      return res.status(500).json({ error: progressError.message });
    }

    if (!progressEntries || progressEntries.length === 0) {
      return res.json([]);
    }

    const glIds = [...new Set(progressEntries.map(p => p.gebietsleiter_id).filter(Boolean))];
    const marketIds = [...new Set(progressEntries.map(p => p.market_id).filter(Boolean))];
    const displayIds = progressEntries.filter(p => p.item_type === 'display').map(p => p.item_id).filter(Boolean);
    const kartonwareIds = progressEntries.filter(p => p.item_type === 'kartonware').map(p => p.item_id).filter(Boolean);

    const [glsResult, glDetailsResult, marketsResult, displaysResult, kartonwareResult] = await Promise.all([
      glIds.length > 0 ? supabase.from('users').select('id, email').in('id', glIds) : { data: [] },
      glIds.length > 0 ? supabase.from('gebietsleiter').select('id, name').in('id', glIds) : { data: [] },
      marketIds.length > 0 ? supabase.from('markets').select('id, name, chain').in('id', marketIds) : { data: [] },
      displayIds.length > 0 ? supabase.from('wellen_displays').select('id, name, item_value').in('id', displayIds) : { data: [] },
      kartonwareIds.length > 0 ? supabase.from('wellen_kartonware').select('id, name, item_value').in('id', kartonwareIds) : { data: [] }
    ]);

    const gls = glsResult.data || [];
    const glDetails = glDetailsResult.data || [];
    const markets = marketsResult.data || [];
    const displays = displaysResult.data || [];
    const kartonware = kartonwareResult.data || [];

    const response = progressEntries.map(entry => {
      const gl = glDetails.find((g: any) => g.id === entry.gebietsleiter_id);
      const glUser = gls.find((u: any) => u.id === entry.gebietsleiter_id);
      const market = markets.find((m: any) => m.id === entry.market_id);
      const item = entry.item_type === 'display'
        ? displays.find((d: any) => d.id === entry.item_id)
        : kartonware.find((k: any) => k.id === entry.item_id);

      return {
        id: entry.id,
        glName: gl?.name || 'Unknown',
        glEmail: glUser?.email || '',
        marketName: market?.name || 'Unknown',
        marketChain: market?.chain || '',
        itemType: entry.item_type,
        itemName: item?.name || 'Unknown',
        quantity: entry.current_number,
        value: entry.current_number * (item?.item_value || 0),
        timestamp: entry.created_at,
        photoUrl: entry.photo_url
      };
    });

    res.json(response);
  } catch (error: any) {
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

// ============================================================================
// GET GL CHAIN PERFORMANCE - For GL detail modal charts
// ============================================================================
router.get('/gl/:glId/chain-performance', async (req: Request, res: Response) => {
  try {
    const { glId } = req.params;
    
    // Chain groupings (same as dashboard)
    const chainGroups = {
      billa: ['Adeg', 'Billa+', 'BILLA+', 'BILLA Plus', 'BILLA+ Privat', 'BILLA Plus Privat', 'BILLA Privat'],
      spar: ['Spar', 'SPAR Privat Popovic', 'Spar Gourmet', 'Eurospar', 'Interspar'],
      zoofachhandel: ['Zoofachhandel', 'Futterhaus', 'Fressnapf', 'Das Futterhaus'],
      hagebau: ['Hagebau']
    };

    // Get all progress entries for this GL
    const { data: allProgress, error: progressError } = await supabase
      .from('wellen_gl_progress')
      .select('*')
      .eq('gebietsleiter_id', glId)
      .order('created_at', { ascending: true });

    if (progressError) throw progressError;

    if (!allProgress || allProgress.length === 0) {
      return res.json({
        billa: { kwData: [], current: { displays: 0, kartonware: 0 }, goal: { displays: 0, kartonware: 0 } },
        spar: { kwData: [], current: { displays: 0, kartonware: 0 }, goal: { displays: 0, kartonware: 0 } },
        zoofachhandel: { kwData: [], current: { displays: 0, kartonware: 0 }, goal: { displays: 0, kartonware: 0 } },
        hagebau: { kwData: [], current: { displays: 0, kartonware: 0 }, goal: { displays: 0, kartonware: 0 } }
      });
    }

    // Get market info to determine chain (from direct market_id or via welle's assigned markets)
    const marketIds = [...new Set(allProgress.map(p => p.market_id).filter(Boolean))];
    let markets: any[] = [];
    if (marketIds.length > 0) {
      const { data } = await supabase.from('markets').select('id, chain').in('id', marketIds);
      markets = data || [];
    }

    // Get welle info for goals and to determine chain when market_id is null
    const welleIds = [...new Set(allProgress.map(p => p.welle_id).filter(Boolean))];
    let displays: any[] = [];
    let kartonware: any[] = [];
    let welleMarkets: any[] = [];
    let wellen: any[] = [];
    
    if (welleIds.length > 0) {
      const [displaysResult, kartonwareResult, welleMarketsResult, wellenResult] = await Promise.all([
        supabase.from('wellen_displays').select('id, target_number, welle_id').in('welle_id', welleIds),
        supabase.from('wellen_kartonware').select('id, target_number, welle_id').in('welle_id', welleIds),
        supabase.from('wellen_markets').select('welle_id, market_id').in('welle_id', welleIds),
        supabase.from('wellen').select('id, name').in('id', welleIds)
      ]);
      displays = displaysResult.data || [];
      kartonware = kartonwareResult.data || [];
      welleMarkets = welleMarketsResult.data || [];
      wellen = wellenResult.data || [];
    }

    // Get all markets that are assigned to these wellen (for fallback chain detection)
    const welleMarketIds = [...new Set(welleMarkets.map(wm => wm.market_id))];
    if (welleMarketIds.length > 0) {
      const { data: additionalMarkets } = await supabase.from('markets').select('id, chain').in('id', welleMarketIds);
      if (additionalMarkets) {
        // Merge with existing markets, avoiding duplicates
        const existingIds = new Set(markets.map(m => m.id));
        for (const m of additionalMarkets) {
          if (!existingIds.has(m.id)) {
            markets.push(m);
          }
        }
      }
    }

    // Get number of GLs for goal calculation
    const { count: glCount } = await supabase.from('gebietsleiter').select('id', { count: 'exact', head: true });
    const numGLs = glCount || 1;

    // Helper to get calendar week from date
    const getKW = (dateStr: string) => {
      const date = new Date(dateStr);
      const startOfYear = new Date(date.getFullYear(), 0, 1);
      const days = Math.floor((date.getTime() - startOfYear.getTime()) / (24 * 60 * 60 * 1000));
      const weekNumber = Math.ceil((days + startOfYear.getDay() + 1) / 7);
      return `KW ${weekNumber}`;
    };

    // Helper to get chain group for a market
    const getChainGroupByMarket = (marketId: string): string | null => {
      const market = markets.find(m => m.id === marketId);
      if (!market) return null;
      
      for (const [group, chains] of Object.entries(chainGroups)) {
        if (chains.includes(market.chain)) return group;
      }
      return null;
    };

    // Helper to get chain group for a welle (based on its assigned markets)
    const getChainGroupByWelle = (welleId: string): string | null => {
      const welleMarketsForWelle = welleMarkets.filter(wm => wm.welle_id === welleId);
      for (const wm of welleMarketsForWelle) {
        const chainGroup = getChainGroupByMarket(wm.market_id);
        if (chainGroup) return chainGroup;
      }
      return null;
    };

    // Process progress by chain group and KW
    const chainData: Record<string, { 
      kwProgress: Record<string, { displays: number; kartonware: number }>;
      totalDisplays: number;
      totalKartonware: number;
      goalDisplays: number;
      goalKartonware: number;
    }> = {
      billa: { kwProgress: {}, totalDisplays: 0, totalKartonware: 0, goalDisplays: 0, goalKartonware: 0 },
      spar: { kwProgress: {}, totalDisplays: 0, totalKartonware: 0, goalDisplays: 0, goalKartonware: 0 },
      zoofachhandel: { kwProgress: {}, totalDisplays: 0, totalKartonware: 0, goalDisplays: 0, goalKartonware: 0 },
      hagebau: { kwProgress: {}, totalDisplays: 0, totalKartonware: 0, goalDisplays: 0, goalKartonware: 0 }
    };

    // Aggregate progress by chain and KW
    for (const progress of allProgress) {
      // Try to get chain from market_id first, then fall back to welle's assigned markets
      let chainGroup = progress.market_id ? getChainGroupByMarket(progress.market_id) : null;
      if (!chainGroup) {
        chainGroup = getChainGroupByWelle(progress.welle_id);
      }
      
      if (!chainGroup || !chainData[chainGroup]) continue;

      const kw = getKW(progress.created_at);
      
      if (!chainData[chainGroup].kwProgress[kw]) {
        chainData[chainGroup].kwProgress[kw] = { displays: 0, kartonware: 0 };
      }

      if (progress.item_type === 'display') {
        chainData[chainGroup].kwProgress[kw].displays += progress.current_number;
        chainData[chainGroup].totalDisplays += progress.current_number;
      } else if (progress.item_type === 'kartonware') {
        chainData[chainGroup].kwProgress[kw].kartonware += progress.current_number;
        chainData[chainGroup].totalKartonware += progress.current_number;
      }
    }

    // Calculate goals per GL (total goals / number of GLs)
    const totalDisplayGoal = displays.reduce((sum, d) => sum + (d.target_number || 0), 0);
    const totalKartonwareGoal = kartonware.reduce((sum, k) => sum + (k.target_number || 0), 0);
    const glDisplayGoal = Math.ceil(totalDisplayGoal / numGLs);
    const glKartonwareGoal = Math.ceil(totalKartonwareGoal / numGLs);

    // Set goals for each chain (proportional based on typical chain distribution)
    chainData.billa.goalDisplays = Math.ceil(glDisplayGoal * 0.35);
    chainData.billa.goalKartonware = Math.ceil(glKartonwareGoal * 0.35);
    chainData.spar.goalDisplays = Math.ceil(glDisplayGoal * 0.30);
    chainData.spar.goalKartonware = Math.ceil(glKartonwareGoal * 0.30);
    chainData.zoofachhandel.goalDisplays = Math.ceil(glDisplayGoal * 0.20);
    chainData.zoofachhandel.goalKartonware = Math.ceil(glKartonwareGoal * 0.20);
    chainData.hagebau.goalDisplays = Math.ceil(glDisplayGoal * 0.15);
    chainData.hagebau.goalKartonware = Math.ceil(glKartonwareGoal * 0.15);

    // Convert to response format
    const formatChainResponse = (chain: string) => {
      const data = chainData[chain];
      const kwEntries = Object.entries(data.kwProgress)
        .sort((a, b) => {
          const kwA = parseInt(a[0].replace('KW ', ''));
          const kwB = parseInt(b[0].replace('KW ', ''));
          return kwA - kwB;
        });

      // Calculate cumulative values for chart
      let cumulativeDisplays = 0;
      let cumulativeKartonware = 0;
      const kwData = kwEntries.map(([kw, values]) => {
        cumulativeDisplays += values.displays;
        cumulativeKartonware += values.kartonware;
        return {
          kw,
          displays: cumulativeDisplays,
          kartonware: cumulativeKartonware
        };
      });

      return {
        kwData,
        current: { displays: data.totalDisplays, kartonware: data.totalKartonware },
        goal: { displays: data.goalDisplays, kartonware: data.goalKartonware }
      };
    };

    res.json({
      billa: formatChainResponse('billa'),
      spar: formatChainResponse('spar'),
      zoofachhandel: formatChainResponse('zoofachhandel'),
      hagebau: formatChainResponse('hagebau')
    });
  } catch (error: any) {
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

export default router;
