import { Router, Request, Response } from 'express';
import { supabase, createFreshClient } from '../config/supabase';
import * as XLSX from 'xlsx';

const router = Router();

// ============================================================================
// KW (Calendar Week) HELPER FUNCTIONS
// ============================================================================

// Get current day abbreviation in German
const getCurrentDayAbbr = (): string => {
  const days = ['SO', 'MO', 'DI', 'MI', 'DO', 'FR', 'SA'];
  return days[new Date().getDay()];
};

// Get current calendar week number
const getCurrentKWNumber = (): number => {
  const now = new Date();
  const startOfYear = new Date(now.getFullYear(), 0, 1);
  const days = Math.floor((now.getTime() - startOfYear.getTime()) / (24 * 60 * 60 * 1000));
  return Math.ceil((days + startOfYear.getDay() + 1) / 7);
};

// Extract just the number from a KW string (handles "KW2", "KW 2", "2", etc.)
const extractKWNumber = (kwString: string): number => {
  const match = kwString.match(/(\d+)/);
  return match ? parseInt(match[1], 10) : -1;
};

// Day order for comparison (MO=1, DI=2, ... SO=7)
const getDayOrder = (day: string): number => {
  const dayOrder: { [key: string]: number } = { 'MO': 1, 'DI': 2, 'MI': 3, 'DO': 4, 'FR': 5, 'SA': 6, 'SO': 7 };
  return dayOrder[day.toUpperCase()] || 0;
};

// Check if a wave with KW days is currently in its active selling period
const isWaveInKWSellPeriod = (kwDays: Array<{ kw: string; days: string[] }> | null): 'before' | 'active' | 'after' => {
  if (!kwDays || kwDays.length === 0) return 'active'; // No KW days = use regular status
  
  const currentKW = getCurrentKWNumber();
  const currentDay = getCurrentDayAbbr();
  const currentDayOrder = getDayOrder(currentDay);
  
  // Find min and max KW numbers
  let minKW = Infinity, maxKW = -Infinity;
  let minDayInMinKW = Infinity, maxDayInMaxKW = -Infinity;
  
  for (const kwDay of kwDays) {
    const kwNum = extractKWNumber(kwDay.kw);
    if (kwNum === -1) continue;
    
    if (kwNum < minKW) {
      minKW = kwNum;
      minDayInMinKW = Math.min(...kwDay.days.map(d => getDayOrder(d)));
    } else if (kwNum === minKW) {
      minDayInMinKW = Math.min(minDayInMinKW, ...kwDay.days.map(d => getDayOrder(d)));
    }
    
    if (kwNum > maxKW) {
      maxKW = kwNum;
      maxDayInMaxKW = Math.max(...kwDay.days.map(d => getDayOrder(d)));
    } else if (kwNum === maxKW) {
      maxDayInMaxKW = Math.max(maxDayInMaxKW, ...kwDay.days.map(d => getDayOrder(d)));
    }
  }
  
  if (minKW === Infinity || maxKW === -Infinity) return 'active';
  
  // Check if before first selling day
  if (currentKW < minKW || (currentKW === minKW && currentDayOrder < minDayInMinKW)) {
    return 'before';
  }
  
  // Check if after last selling day
  if (currentKW > maxKW || (currentKW === maxKW && currentDayOrder > maxDayInMaxKW)) {
    return 'after';
  }
  
  // We're within the selling period
  return 'active';
};

// ============================================================================
// DASHBOARD: GET CHAIN AVERAGES
// ============================================================================
router.get('/dashboard/chain-averages', async (req: Request, res: Response) => {
  try {
    // Use fresh client to avoid caching issues with wellen queries
    const freshClient = createFreshClient();
    
    // Get filters from query params
    const glIdsParam = req.query.glIds as string | undefined;
    const startDate = req.query.startDate as string | undefined;
    const endDate = req.query.endDate as string | undefined;
    const itemType = req.query.itemType as 'displays' | 'kartonware' | undefined;
    
    const glFilter = glIdsParam ? glIdsParam.split(',').filter(Boolean) : [];
    const isNoneSelected = glFilter.includes('__none__');
   
    console.log('📊 Fetching chain averages...', {
      glFilter: glFilter.length > 0 ? (isNoneSelected ? 'NONE' : glFilter.join(', ')) : 'ALL',
      dateRange: startDate && endDate ? `${startDate} to ${endDate}` : 'ALL',
      itemType: itemType || 'ALL'
    });

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
        
        // If no GLs selected (__none__), return 0 progress
        if (isNoneSelected) {
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
        
        // Get all markets of this chain type
        const { data: markets, error: marketsError } = await freshClient
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
        
        // Get all wellen assigned to these markets (optionally filtered by date)
        let wellenQuery = freshClient
          .from('wellen')
          .select('id, start_date, end_date');
        
        if (startDate) {
          wellenQuery = wellenQuery.gte('end_date', startDate);
        }
        if (endDate) {
          wellenQuery = wellenQuery.lte('start_date', endDate);
        }
        
        const { data: filteredWellen } = await wellenQuery;
        const filteredWellenIds = (filteredWellen || []).map(w => w.id);
        
        if (filteredWellenIds.length === 0) {
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
        
        const { data: welleMarkets } = await freshClient
          .from('wellen_markets')
          .select('welle_id, market_id')
          .in('market_id', marketIds)
          .in('welle_id', filteredWellenIds);
        
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
        
        // Get displays and kartonware for these wellen (filtered by item type)
        let displays: any[] = [];
        let kartonware: any[] = [];
        
        if (!itemType || itemType === 'displays') {
          const { data } = await freshClient
            .from('wellen_displays')
            .select('id, target_number, welle_id')
            .in('welle_id', welleIds);
          displays = data || [];
        }
        
        if (!itemType || itemType === 'kartonware') {
          const { data } = await freshClient
            .from('wellen_kartonware')
            .select('id, target_number, welle_id')
            .in('welle_id', welleIds);
          kartonware = data || [];
        }
        
        // Get progress (filtered by GL and item type)
        let displayProgress: any[] = [];
        let kartonwareProgress: any[] = [];
        
        if (!itemType || itemType === 'displays') {
          let query = freshClient
            .from('wellen_gl_progress')
            .select('current_number, item_id, gebietsleiter_id')
            .eq('item_type', 'display')
            .in('welle_id', welleIds);
          
          if (glFilter.length > 0) {
            query = query.in('gebietsleiter_id', glFilter);
          }
          
          const { data } = await query;
          displayProgress = data || [];
        }
        
        if (!itemType || itemType === 'kartonware') {
          let query = freshClient
            .from('wellen_gl_progress')
            .select('current_number, item_id, gebietsleiter_id')
            .eq('item_type', 'kartonware')
            .in('welle_id', welleIds);
          
          if (glFilter.length > 0) {
            query = query.in('gebietsleiter_id', glFilter);
          }
          
          const { data } = await query;
          kartonwareProgress = data || [];
        }
        
        // Calculate totals
        const totalTarget = 
          displays.reduce((sum, d) => sum + d.target_number, 0) +
          kartonware.reduce((sum, k) => sum + k.target_number, 0);
        
        const totalCurrent = 
          displayProgress.reduce((sum, p) => sum + p.current_number, 0) +
          kartonwareProgress.reduce((sum, p) => sum + p.current_number, 0);
        
        // Count unique markets with progress
        const marketsWithProgressSet = new Set<string>();
        for (const progress of [...displayProgress, ...kartonwareProgress]) {
          const display = displays.find(d => d.id === progress.item_id);
          const kw = kartonware.find(k => k.id === progress.item_id);
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
        
        // If no GLs selected, return 0 progress
        if (isNoneSelected) {
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
        
        const { data: markets, error: marketsError } = await freshClient
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
        
        // Get wellen filtered by date range
        let wellenQuery = freshClient.from('wellen').select('id');
        if (startDate) wellenQuery = wellenQuery.gte('end_date', startDate);
        if (endDate) wellenQuery = wellenQuery.lte('start_date', endDate);
        const { data: filteredWellen } = await wellenQuery;
        const filteredWellenIds = (filteredWellen || []).map(w => w.id);
        
        if (filteredWellenIds.length === 0) {
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
        
        const { data: welleMarkets } = await freshClient
          .from('wellen_markets')
          .select('welle_id, market_id')
          .in('market_id', marketIds)
          .in('welle_id', filteredWellenIds);
        
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
        
        // Get items filtered by type
        let displays: any[] = [];
        let kartonware: any[] = [];
        
        if (!itemType || itemType === 'displays') {
          const { data } = await freshClient.from('wellen_displays').select('id, target_number, welle_id').in('welle_id', welleIds);
          displays = data || [];
        }
        if (!itemType || itemType === 'kartonware') {
          const { data } = await freshClient.from('wellen_kartonware').select('id, target_number, welle_id').in('welle_id', welleIds);
          kartonware = data || [];
        }
        
        // Get progress
        let displayProgress: any[] = [];
        let kartonwareProgress: any[] = [];
        
        if (!itemType || itemType === 'displays') {
          let query = freshClient.from('wellen_gl_progress').select('current_number, item_id, gebietsleiter_id').eq('item_type', 'display').in('welle_id', welleIds);
          if (glFilter.length > 0) query = query.in('gebietsleiter_id', glFilter);
          const { data } = await query;
          displayProgress = data || [];
        }
        if (!itemType || itemType === 'kartonware') {
          let query = freshClient.from('wellen_gl_progress').select('current_number, item_id, gebietsleiter_id').eq('item_type', 'kartonware').in('welle_id', welleIds);
          if (glFilter.length > 0) query = query.in('gebietsleiter_id', glFilter);
          const { data } = await query;
          kartonwareProgress = data || [];
        }
        
        const totalTarget = displays.reduce((sum, d) => sum + d.target_number, 0) + kartonware.reduce((sum, k) => sum + k.target_number, 0);
        const totalCurrent = displayProgress.reduce((sum, p) => sum + p.current_number, 0) + kartonwareProgress.reduce((sum, p) => sum + p.current_number, 0);
        
        const marketsWithProgressSet = new Set<string>();
        for (const progress of [...displayProgress, ...kartonwareProgress]) {
          const display = displays.find(d => d.id === progress.item_id);
          const kw = kartonware.find(k => k.id === progress.item_id);
          const welleId = display?.welle_id || kw?.welle_id;
          if (welleId) {
            (welleMarkets || []).filter(wm => wm.welle_id === welleId).forEach(wm => marketsWithProgressSet.add(wm.market_id));
          }
        }
        
        const currentPercentage = totalTarget > 0 ? Math.round((totalCurrent / totalTarget) * 100 * 100) / 100 : 0;
        
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
      
      // ZOOFACHHANDEL AVERAGE
      (async () => {
        const chainTypes = chains.zoofachhandel;
        
        // If no GLs selected, return 0 progress
        if (isNoneSelected) {
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
        
        // Get ALL markets of this chain type (for total count)
        const { data: markets, error: marketsError } = await freshClient
          .from('markets')
          .select('id, gebietsleiter_id')
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
        
        // Get wellen filtered by date range
        let wellenQuery = freshClient.from('wellen').select('id, goal_value, goal_type');
        if (startDate) wellenQuery = wellenQuery.gte('end_date', startDate);
        if (endDate) wellenQuery = wellenQuery.lte('start_date', endDate);
        const { data: filteredWellen } = await wellenQuery;
        const filteredWellenIds = (filteredWellen || []).map(w => w.id);
        
        if (filteredWellenIds.length === 0) {
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
        
        const { data: welleMarkets } = await freshClient
          .from('wellen_markets')
          .select('welle_id, market_id')
          .in('market_id', marketIds)
          .in('welle_id', filteredWellenIds);
        
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
        
        // Get unique market IDs that are in the waves (wave markets)
        const waveMarketIds = [...new Set((welleMarkets || []).map(wm => wm.market_id))];
        const totalWaveMarkets = waveMarketIds.length;
        
        // Calculate GL's markets that are in the wave
        let glWaveMarketCount = totalWaveMarkets; // Default to all if no GL filter
        if (glFilter.length > 0) {
          // Find markets in the wave that belong to this GL
          const glWaveMarkets = waveMarketIds.filter(marketId => {
            const market = markets?.find(m => m.id === marketId);
            return market && glFilter.includes(market.gebietsleiter_id);
          });
          glWaveMarketCount = glWaveMarkets.length;
        }
        
        // Sum goal values from filtered wellen (total goal)
        const totalGoalValue = (filteredWellen || [])
          .filter(w => welleIds.includes(w.id) && w.goal_type === 'value')
          .reduce((sum, w) => sum + (w.goal_value || 0), 0);
        
        // Calculate GL's proportional goal based on their market share IN THE WAVE
        // Formula: GL Goal = Wave Goal × (GL's Markets in Wave / Total Markets in Wave)
        const glGoalRatio = totalWaveMarkets > 0 ? glWaveMarketCount / totalWaveMarkets : 0;
        const goalValue = glFilter.length > 0 ? totalGoalValue * glGoalRatio : totalGoalValue;
        
        // Get items filtered by type
        let displays: any[] = [];
        let kartonware: any[] = [];
        
        if (!itemType || itemType === 'displays') {
          const { data } = await freshClient.from('wellen_displays').select('id, target_number, item_value, welle_id').in('welle_id', welleIds);
          displays = data || [];
        }
        if (!itemType || itemType === 'kartonware') {
          const { data } = await freshClient.from('wellen_kartonware').select('id, target_number, item_value, welle_id').in('welle_id', welleIds);
          kartonware = data || [];
        }
        
        // Get palette and schuette products for value calculation
        const { data: paletteProductsZoo } = await freshClient
          .from('wellen_paletten_products')
          .select('id, value_per_ve, palette_id')
          .in('palette_id', (await freshClient.from('wellen_paletten').select('id').in('welle_id', welleIds)).data?.map((p: any) => p.id) || []);
        
        const { data: schutteProductsZoo } = await freshClient
          .from('wellen_schuetten_products')
          .select('id, value_per_ve, schuette_id')
          .in('schuette_id', (await freshClient.from('wellen_schuetten').select('id').in('welle_id', welleIds)).data?.map((s: any) => s.id) || []);
        
        // Get progress
        let displayProgress: any[] = [];
        let kartonwareProgress: any[] = [];
        let paletteProgressZoo: any[] = [];
        let schutteProgressZoo: any[] = [];
        
        if (!itemType || itemType === 'displays') {
          let query = freshClient.from('wellen_gl_progress').select('current_number, item_id, gebietsleiter_id, value_per_unit').eq('item_type', 'display').in('welle_id', welleIds);
          if (glFilter.length > 0) query = query.in('gebietsleiter_id', glFilter);
          const { data } = await query;
          displayProgress = data || [];
        }
        if (!itemType || itemType === 'kartonware') {
          let query = freshClient.from('wellen_gl_progress').select('current_number, item_id, gebietsleiter_id, value_per_unit').eq('item_type', 'kartonware').in('welle_id', welleIds);
          if (glFilter.length > 0) query = query.in('gebietsleiter_id', glFilter);
          const { data } = await query;
          kartonwareProgress = data || [];
        }
        
        // Get palette progress (always include for value-based chains)
        {
          let query = freshClient.from('wellen_gl_progress').select('current_number, item_id, gebietsleiter_id, value_per_unit, welle_id').eq('item_type', 'palette').in('welle_id', welleIds);
          if (glFilter.length > 0) query = query.in('gebietsleiter_id', glFilter);
          const { data } = await query;
          paletteProgressZoo = data || [];
        }
        
        // Get schuette progress (always include for value-based chains)
        {
          let query = freshClient.from('wellen_gl_progress').select('current_number, item_id, gebietsleiter_id, value_per_unit, welle_id').eq('item_type', 'schuette').in('welle_id', welleIds);
          if (glFilter.length > 0) query = query.in('gebietsleiter_id', glFilter);
          const { data } = await query;
          schutteProgressZoo = data || [];
        }
        
        // Calculate total value (target * item_value)
        const totalValue = 
          displays.reduce((sum, d) => sum + (d.target_number * (d.item_value || 0)), 0) +
          kartonware.reduce((sum, k) => sum + (k.target_number * (k.item_value || 0)), 0);
        
        // Calculate current value (current * item_value)
        let currentValue = 0;
        for (const progress of displayProgress) {
          const display = displays.find(d => d.id === progress.item_id);
          if (display) currentValue += progress.current_number * (display.item_value || 0);
        }
        for (const progress of kartonwareProgress) {
          const kw = kartonware.find(k => k.id === progress.item_id);
          if (kw) currentValue += progress.current_number * (kw.item_value || 0);
        }
        
        // Add palette progress values
        for (const progress of paletteProgressZoo) {
          const product = (paletteProductsZoo || []).find((p: any) => p.id === progress.item_id);
          const valuePerUnit = progress.value_per_unit || product?.value_per_ve || 0;
          currentValue += progress.current_number * valuePerUnit;
        }
        
        // Add schuette progress values
        for (const progress of schutteProgressZoo) {
          const product = (schutteProductsZoo || []).find((p: any) => p.id === progress.item_id);
          const valuePerUnit = progress.value_per_unit || product?.value_per_ve || 0;
          currentValue += progress.current_number * valuePerUnit;
        }
        
        const marketsWithProgressSet = new Set<string>();
        for (const progress of [...displayProgress, ...kartonwareProgress, ...paletteProgressZoo, ...schutteProgressZoo]) {
          const display = displays.find(d => d.id === progress.item_id);
          const kw = kartonware.find(k => k.id === progress.item_id);
          const welleId = display?.welle_id || kw?.welle_id || (progress as any).welle_id;
          if (welleId) {
            (welleMarkets || []).filter(wm => wm.welle_id === welleId).forEach(wm => marketsWithProgressSet.add(wm.market_id));
          }
        }
        
        return {
          chainName: 'Zoofachhandel',
          chainColor: 'linear-gradient(135deg, #EC4899, #DB2777)',
          goalType: 'value' as const,
          goalValue: Math.round(goalValue * 100) / 100,
          currentValue: Math.round(currentValue * 100) / 100,
          totalValue: Math.round(totalValue * 100) / 100,
          // Return GL's wave market count when filtering, otherwise total wave markets
          totalMarkets: glFilter.length > 0 ? glWaveMarketCount : totalWaveMarkets,
          marketsWithProgress: marketsWithProgressSet.size
        };
      })(),
      
      // HAGEBAU AVERAGE
      (async () => {
        const chainTypes = chains.hagebau;
        
        // If no GLs selected, return 0 progress
        if (isNoneSelected) {
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
        
        // Get ALL markets of this chain type (including gebietsleiter_id for GL filtering)
        const { data: markets, error: marketsError } = await freshClient
          .from('markets')
          .select('id, gebietsleiter_id')
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
        
        // Get wellen filtered by date range
        let wellenQuery = freshClient.from('wellen').select('id, goal_value, goal_type');
        if (startDate) wellenQuery = wellenQuery.gte('end_date', startDate);
        if (endDate) wellenQuery = wellenQuery.lte('start_date', endDate);
        const { data: filteredWellen } = await wellenQuery;
        const filteredWellenIds = (filteredWellen || []).map(w => w.id);
        
        if (filteredWellenIds.length === 0) {
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
        
        const { data: welleMarkets } = await freshClient
          .from('wellen_markets')
          .select('welle_id, market_id')
          .in('market_id', marketIds)
          .in('welle_id', filteredWellenIds);
        
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
        
        // Get unique market IDs that are in the waves (wave markets)
        const waveMarketIds = [...new Set((welleMarkets || []).map(wm => wm.market_id))];
        const totalWaveMarkets = waveMarketIds.length;
        
        // Calculate GL's markets that are in the wave
        let glWaveMarketCount = totalWaveMarkets; // Default to all if no GL filter
        if (glFilter.length > 0) {
          // Find markets in the wave that belong to this GL
          const glWaveMarkets = waveMarketIds.filter(marketId => {
            const market = markets?.find(m => m.id === marketId);
            return market && glFilter.includes(market.gebietsleiter_id);
          });
          glWaveMarketCount = glWaveMarkets.length;
        }
        
        // Sum goal values from filtered wellen (total goal)
        const totalGoalValue = (filteredWellen || [])
          .filter(w => welleIds.includes(w.id) && w.goal_type === 'value')
          .reduce((sum, w) => sum + (w.goal_value || 0), 0);
        
        // Calculate GL's proportional goal based on their market share IN THE WAVE
        // Formula: GL Goal = Wave Goal × (GL's Markets in Wave / Total Markets in Wave)
        const glGoalRatio = totalWaveMarkets > 0 ? glWaveMarketCount / totalWaveMarkets : 0;
        const goalValue = glFilter.length > 0 ? totalGoalValue * glGoalRatio : totalGoalValue;
        
        // Get items filtered by type
        let displays: any[] = [];
        let kartonware: any[] = [];
        
        if (!itemType || itemType === 'displays') {
          const { data } = await freshClient.from('wellen_displays').select('id, target_number, item_value, welle_id').in('welle_id', welleIds);
          displays = data || [];
        }
        if (!itemType || itemType === 'kartonware') {
          const { data } = await freshClient.from('wellen_kartonware').select('id, target_number, item_value, welle_id').in('welle_id', welleIds);
          kartonware = data || [];
        }
        
        // Get palette and schuette products for value calculation
        const { data: paletteProducts } = await freshClient
          .from('wellen_paletten_products')
          .select('id, value_per_ve, palette_id')
          .in('palette_id', (await freshClient.from('wellen_paletten').select('id').in('welle_id', welleIds)).data?.map((p: any) => p.id) || []);
        
        const { data: schutteProducts } = await freshClient
          .from('wellen_schuetten_products')
          .select('id, value_per_ve, schuette_id')
          .in('schuette_id', (await freshClient.from('wellen_schuetten').select('id').in('welle_id', welleIds)).data?.map((s: any) => s.id) || []);
        
        // Get progress
        let displayProgress: any[] = [];
        let kartonwareProgress: any[] = [];
        let paletteProgress: any[] = [];
        let schutteProgress: any[] = [];
        
        if (!itemType || itemType === 'displays') {
          let query = freshClient.from('wellen_gl_progress').select('current_number, item_id, gebietsleiter_id, value_per_unit').eq('item_type', 'display').in('welle_id', welleIds);
          if (glFilter.length > 0) query = query.in('gebietsleiter_id', glFilter);
          const { data } = await query;
          displayProgress = data || [];
        }
        if (!itemType || itemType === 'kartonware') {
          let query = freshClient.from('wellen_gl_progress').select('current_number, item_id, gebietsleiter_id, value_per_unit').eq('item_type', 'kartonware').in('welle_id', welleIds);
          if (glFilter.length > 0) query = query.in('gebietsleiter_id', glFilter);
          const { data } = await query;
          kartonwareProgress = data || [];
        }
        
        // Get palette progress (always include for value-based chains)
        {
          let query = freshClient.from('wellen_gl_progress').select('current_number, item_id, gebietsleiter_id, value_per_unit, welle_id').eq('item_type', 'palette').in('welle_id', welleIds);
          if (glFilter.length > 0) query = query.in('gebietsleiter_id', glFilter);
          const { data } = await query;
          paletteProgress = data || [];
        }
        
        // Get schuette progress (always include for value-based chains)
        {
          let query = freshClient.from('wellen_gl_progress').select('current_number, item_id, gebietsleiter_id, value_per_unit, welle_id').eq('item_type', 'schuette').in('welle_id', welleIds);
          if (glFilter.length > 0) query = query.in('gebietsleiter_id', glFilter);
          const { data } = await query;
          schutteProgress = data || [];
        }
        
        const totalValue = 
          displays.reduce((sum, d) => sum + (d.target_number * (d.item_value || 0)), 0) +
          kartonware.reduce((sum, k) => sum + (k.target_number * (k.item_value || 0)), 0);
        
        let currentValue = 0;
        for (const progress of displayProgress) {
          const display = displays.find(d => d.id === progress.item_id);
          if (display) currentValue += progress.current_number * (display.item_value || 0);
        }
        for (const progress of kartonwareProgress) {
          const kw = kartonware.find(k => k.id === progress.item_id);
          if (kw) currentValue += progress.current_number * (kw.item_value || 0);
        }
        
        // Add palette progress values
        for (const progress of paletteProgress) {
          const product = (paletteProducts || []).find((p: any) => p.id === progress.item_id);
          const valuePerUnit = progress.value_per_unit || product?.value_per_ve || 0;
          currentValue += progress.current_number * valuePerUnit;
        }
        
        // Add schuette progress values
        for (const progress of schutteProgress) {
          const product = (schutteProducts || []).find((p: any) => p.id === progress.item_id);
          const valuePerUnit = progress.value_per_unit || product?.value_per_ve || 0;
          currentValue += progress.current_number * valuePerUnit;
        }
        
        const marketsWithProgressSet = new Set<string>();
        for (const progress of [...displayProgress, ...kartonwareProgress, ...paletteProgress, ...schutteProgress]) {
          const display = displays.find(d => d.id === progress.item_id);
          const kw = kartonware.find(k => k.id === progress.item_id);
          const welleId = display?.welle_id || kw?.welle_id || (progress as any).welle_id;
          if (welleId) {
            (welleMarkets || []).filter(wm => wm.welle_id === welleId).forEach(wm => marketsWithProgressSet.add(wm.market_id));
          }
        }
        
        return {
          chainName: 'Hagebau',
          chainColor: 'linear-gradient(135deg, #06B6D4, #0891B2)',
          goalType: 'value' as const,
          goalValue: Math.round(goalValue * 100) / 100,
          currentValue: Math.round(currentValue * 100) / 100,
          totalValue: Math.round(totalValue * 100) / 100,
          // Return GL's wave market count when filtering, otherwise total wave markets
          totalMarkets: glFilter.length > 0 ? glWaveMarketCount : totalWaveMarkets,
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
    // Get filters from query params
    const glIdsParam = req.query.glIds as string | undefined;
    const itemType = req.query.itemType as 'displays' | 'kartonware' | undefined;
    
    const glFilter = glIdsParam ? glIdsParam.split(',').filter(Boolean) : [];
    const isNoneSelected = glFilter.includes('__none__');
    
    console.log('📊 Fetching waves for dashboard...', {
      glFilter: glFilter.length > 0 ? (isNoneSelected ? 'NONE' : glFilter.join(', ')) : 'ALL',
      itemType: itemType || 'ALL'
    });
    
    // If no GLs selected, return empty waves (or waves with 0 progress)
    if (isNoneSelected) {
      res.json([]);
      return;
    }

    const today = new Date();
    const threeDaysAgo = new Date(today.getTime() - 3 * 24 * 60 * 60 * 1000);

    // Use fresh client to avoid caching issues
    const freshClient = createFreshClient();

    // Fetch active, upcoming, and recently finished waves
    const { data: wellen, error: wellenError } = await freshClient
      .from('wellen')
      .select('*')
      .or(`status.eq.active,status.eq.upcoming,and(status.eq.past,end_date.gte.${threeDaysAgo.toISOString().split('T')[0]})`)
      .order('start_date', { ascending: false });


    if (wellenError) throw wellenError;

    const wavesProgress = await Promise.all(
      (wellen || []).map(async (welle) => {
        // Fetch displays (conditionally based on item type filter) - use fresh client
        let displays: any[] = [];
        if (!itemType || itemType === 'displays') {
          const { data } = await freshClient
            .from('wellen_displays')
            .select('id, target_number, item_value')
            .eq('welle_id', welle.id);
          displays = data || [];
        }

        // Fetch kartonware (conditionally based on item type filter) - use fresh client
        let kartonware: any[] = [];
        if (!itemType || itemType === 'kartonware') {
          const { data } = await freshClient
            .from('wellen_kartonware')
            .select('id, target_number, item_value')
            .eq('welle_id', welle.id);
          kartonware = data || [];
        }

        // Fetch einzelprodukte for value calculation
        const { data: einzelprodukteData } = await freshClient
          .from('wellen_einzelprodukte')
          .select('id, target_number, item_value')
          .eq('welle_id', welle.id);
        const einzelprodukte = einzelprodukteData || [];

        // Fetch KW days for this wave (for Vorbesteller status calculation)
        const { data: kwDaysData } = await freshClient
          .from('wellen_kw_days')
          .select('kw, days')
          .eq('welle_id', welle.id)
          .order('kw_order', { ascending: true });
        
        // Fetch assigned markets with gebietsleiter info - use fresh client
        const { data: welleMarkets } = await freshClient
          .from('wellen_markets')
          .select('market_id')
          .eq('welle_id', welle.id);
        
        const waveMarketIds = (welleMarkets || []).map(wm => wm.market_id);
        const totalWaveMarkets = waveMarketIds.length;
        
        // Get GL's markets in the wave for proportional goal calculation
        let glWaveMarketCount = totalWaveMarkets; // Default to all if no GL filter
        if (glFilter.length > 0 && waveMarketIds.length > 0) {
          const { data: marketsData } = await freshClient
            .from('markets')
            .select('id, gebietsleiter_id')
            .in('id', waveMarketIds);
          
          const glWaveMarkets = (marketsData || []).filter(m => 
            glFilter.includes(m.gebietsleiter_id)
          );
          glWaveMarketCount = glWaveMarkets.length;
        }
        
        // Calculate GL's proportional ratio
        const glGoalRatio = totalWaveMarkets > 0 ? glWaveMarketCount / totalWaveMarkets : 0;

        // Fetch progress (optionally filtered by GL and item type) - use fresh client
        let progressData: any[] = [];
        
        if (!itemType || itemType === 'displays') {
          let query = freshClient
            .from('wellen_gl_progress')
            .select('current_number, item_type, item_id, gebietsleiter_id')
            .eq('welle_id', welle.id)
            .eq('item_type', 'display');
          
          if (glFilter.length > 0) {
            query = query.in('gebietsleiter_id', glFilter);
          }
          
          const { data } = await query;
          progressData = [...progressData, ...(data || [])];
        }
        
        if (!itemType || itemType === 'kartonware') {
          let query = freshClient
            .from('wellen_gl_progress')
            .select('current_number, item_type, item_id, gebietsleiter_id')
            .eq('welle_id', welle.id)
            .eq('item_type', 'kartonware');
          
          if (glFilter.length > 0) {
            query = query.in('gebietsleiter_id', glFilter);
          }
          
          const { data } = await query;
          progressData = [...progressData, ...(data || [])];
        }

        // Fetch palette and schuette progress for value calculation
        {
          let query = freshClient
            .from('wellen_gl_progress')
            .select('current_number, item_type, item_id, gebietsleiter_id, value_per_unit')
            .eq('welle_id', welle.id)
            .in('item_type', ['palette', 'schuette']);
          
          if (glFilter.length > 0) {
            query = query.in('gebietsleiter_id', glFilter);
          }
          
          const { data } = await query;
          progressData = [...progressData, ...(data || [])];
        }

        // Fetch einzelprodukt progress
        {
          let query = freshClient
            .from('wellen_gl_progress')
            .select('current_number, item_type, item_id, gebietsleiter_id, value_per_unit')
            .eq('welle_id', welle.id)
            .eq('item_type', 'einzelprodukt');
          
          if (glFilter.length > 0) {
            query = query.in('gebietsleiter_id', glFilter);
          }
          
          const { data } = await query;
          progressData = [...progressData, ...(data || [])];
        }

        // Calculate display aggregates
        let displayCount = 0;
        let displayTarget = 0;
        for (const display of displays) {
          displayTarget += display.target_number;
          const progress = progressData
            .filter(p => p.item_type === 'display' && p.item_id === display.id)
            .reduce((sum, p) => sum + p.current_number, 0);
          displayCount += progress;
        }

        // Calculate kartonware aggregates
        let kartonwareCount = 0;
        let kartonwareTarget = 0;
        for (const kw of kartonware) {
          kartonwareTarget += kw.target_number;
          const progress = progressData
            .filter(p => p.item_type === 'kartonware' && p.item_id === kw.id)
            .reduce((sum, p) => sum + p.current_number, 0);
          kartonwareCount += progress;
        }

        // Calculate current value for value-based goals
        let currentValue = 0;
        if (welle.goal_type === 'value') {
          for (const progress of progressData) {
            if (progress.item_type === 'display') {
              const display = displays.find(d => d.id === progress.item_id);
              if (display) {
                currentValue += progress.current_number * (display.item_value || 0);
              }
            } else if (progress.item_type === 'kartonware') {
              const kw = kartonware.find(k => k.id === progress.item_id);
              if (kw) {
                currentValue += progress.current_number * (kw.item_value || 0);
              }
            } else if (progress.item_type === 'palette' || progress.item_type === 'schuette') {
              // For palette/schuette, use stored value_per_unit
              currentValue += progress.current_number * (progress.value_per_unit || 0);
            } else if (progress.item_type === 'einzelprodukt') {
              const ep = einzelprodukte.find(e => e.id === progress.item_id);
              if (ep) {
                currentValue += progress.current_number * (ep.item_value || 0);
              }
            }
          }
        }

        // Count unique participating GLs
        const participatingGLs = new Set((progressData || []).map(p => p.gebietsleiter_id)).size;

        // Determine status - for Vorbesteller waves with kw_days, use KW-based status
        let status = welle.status === 'past' ? 'finished' : welle.status;
        
        // If wave has kw_days (Vorbesteller), override status based on KW selling period
        if (kwDaysData && Array.isArray(kwDaysData) && kwDaysData.length > 0) {
          const kwStatus = isWaveInKWSellPeriod(kwDaysData);
          if (kwStatus === 'before') {
            status = 'upcoming';
          } else if (kwStatus === 'active') {
            status = 'active';
          } else if (kwStatus === 'after') {
            status = 'finished';
          }
        }
        
        // Apply proportional goals when GL filter is active
        // Formula: GL Target = Total Target × (GL's Markets in Wave / Total Markets in Wave)
        const proportionalDisplayTarget = glFilter.length > 0 
          ? Math.ceil(displayTarget * glGoalRatio) 
          : displayTarget;
        const proportionalKartonwareTarget = glFilter.length > 0 
          ? Math.ceil(kartonwareTarget * glGoalRatio) 
          : kartonwareTarget;
        const proportionalGoalValue = glFilter.length > 0 && welle.goal_value
          ? Math.round(welle.goal_value * glGoalRatio * 100) / 100
          : welle.goal_value;

        return {
          id: welle.id,
          name: welle.name,
          startDate: welle.start_date,
          endDate: welle.end_date,
          status,
          goalType: welle.goal_type,
          goalPercentage: welle.goal_percentage,
          goalValue: proportionalGoalValue,
          currentValue: Math.round(currentValue * 100) / 100,
          displayCount,
          displayTarget: proportionalDisplayTarget,
          kartonwareCount,
          kartonwareTarget: proportionalKartonwareTarget,
          assignedMarkets: glFilter.length > 0 ? glWaveMarketCount : totalWaveMarkets,
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
    // Use a fresh client to avoid any potential caching issues
    const freshClient = createFreshClient();
    
    // Fetch all wellen
    const { data: wellen, error: wellenError } = await freshClient
      .from('wellen')
      .select('*')
      .order('created_at', { ascending: false });

    if (wellenError) {
      console.error('❌ Wellen query error:', wellenError);
      throw wellenError;
    }
    
    console.log(`✅ Fetched ${wellen?.length || 0} wellen`);

    // For each welle, fetch related data (all using fresh client)
    const wellenWithDetails = await Promise.all(
      (wellen || []).map(async (welle) => {
        // Fetch displays
        const { data: displays } = await freshClient
          .from('wellen_displays')
          .select('*')
          .eq('welle_id', welle.id)
          .order('display_order', { ascending: true });

        // Fetch kartonware
        const { data: kartonware } = await freshClient
          .from('wellen_kartonware')
          .select('*')
          .eq('welle_id', welle.id)
          .order('kartonware_order', { ascending: true });

        // Fetch einzelprodukte
        const { data: einzelprodukte } = await freshClient
          .from('wellen_einzelprodukte')
          .select('*')
          .eq('welle_id', welle.id)
          .order('einzelprodukt_order', { ascending: true });

        // Fetch palettes with their products
        const { data: paletten, error: palError } = await freshClient
          .from('wellen_paletten')
          .select('*')
          .eq('welle_id', welle.id)
          .order('palette_order', { ascending: true });
        console.log(`🎨 Fetched palettes for welle ${welle.id}:`, paletten?.length || 0, 'error:', palError?.message || 'none');

        // Fetch palette products for each palette
        const palettenWithProducts = await Promise.all(
          (paletten || []).map(async (palette) => {
            const { data: products } = await freshClient
              .from('wellen_paletten_products')
              .select('*')
              .eq('palette_id', palette.id)
              .order('product_order', { ascending: true });
            
            return {
              ...palette,
              products: products || []
            };
          })
        );

        // Fetch schütten with their products
        const { data: schuetten, error: schError } = await freshClient
          .from('wellen_schuetten')
          .select('*')
          .eq('welle_id', welle.id)
          .order('schuette_order', { ascending: true });
        console.log(`📦 Fetched schuetten for welle ${welle.id}:`, schuetten?.length || 0, 'error:', schError?.message || 'none');

        // Fetch schütte products for each schütte
        const schuettenWithProducts = await Promise.all(
          (schuetten || []).map(async (schuette) => {
            const { data: products } = await freshClient
              .from('wellen_schuetten_products')
              .select('*')
              .eq('schuette_id', schuette.id)
              .order('product_order', { ascending: true });
            
            return {
              ...schuette,
              products: products || []
            };
          })
        );

        // Fetch KW days
        const { data: kwDays } = await freshClient
          .from('wellen_kw_days')
          .select('*')
          .eq('welle_id', welle.id)
          .order('kw_order', { ascending: true });

        // Fetch assigned market IDs
        const { data: welleMarkets } = await freshClient
          .from('wellen_markets')
          .select('market_id')
          .eq('welle_id', welle.id);
        
        // Count unique GLs that have markets assigned in this wave
        let glsWithMarketsInWave = 0;
        const waveMarketIds = (welleMarkets || []).map(wm => wm.market_id);
        if (waveMarketIds.length > 0) {
          const { data: marketsWithGL } = await freshClient
            .from('markets')
            .select('gebietsleiter_id')
            .in('id', waveMarketIds)
            .not('gebietsleiter_id', 'is', null);
          
          const uniqueGLsInWave = new Set((marketsWithGL || []).map(m => m.gebietsleiter_id));
          glsWithMarketsInWave = uniqueGLsInWave.size;
        }

        // Calculate progress aggregates
        const { data: progressData } = await freshClient
          .from('wellen_gl_progress')
          .select('current_number, item_type, item_id, gebietsleiter_id, value_per_unit')
          .eq('welle_id', welle.id);

        const uniqueGLs = new Set((progressData || []).map(p => p.gebietsleiter_id)).size;

        // Derive types based on what items exist
        const types: ('display' | 'kartonware' | 'palette' | 'schuette' | 'einzelprodukt')[] = [];
        if (displays && displays.length > 0) types.push('display');
        if (kartonware && kartonware.length > 0) types.push('kartonware');
        if (palettenWithProducts && palettenWithProducts.length > 0) types.push('palette');
        if (schuettenWithProducts && schuettenWithProducts.length > 0) types.push('schuette');
        if (einzelprodukte && einzelprodukte.length > 0) types.push('einzelprodukt');

        // Determine status - for Vorbesteller waves with kw_days, use KW-based status (same logic as dashboard)
        let finalStatus = welle.status;
        if (kwDays && Array.isArray(kwDays) && kwDays.length > 0) {
          const kwStatus = isWaveInKWSellPeriod(kwDays);
          if (kwStatus === 'before') {
            finalStatus = 'upcoming';
          } else if (kwStatus === 'active') {
            finalStatus = 'active';
          } else if (kwStatus === 'after') {
            finalStatus = 'past';
          }
        }

        return {
          id: welle.id,
          name: welle.name,
          image: welle.image_url,
          startDate: welle.start_date,
          endDate: welle.end_date,
          types, // Derived from displays/kartonware/palettes/schuetten
          status: finalStatus,
          goalType: welle.goal_type,
          goalPercentage: welle.goal_percentage,
          goalValue: welle.goal_value,
          displayCount: displays?.length || 0,
          kartonwareCount: kartonware?.length || 0,
          paletteCount: palettenWithProducts?.length || 0,
          schutteCount: schuettenWithProducts?.length || 0,
          einzelproduktCount: einzelprodukte?.length || 0,
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
          paletteItems: palettenWithProducts.map(p => ({
            id: p.id,
            name: p.name,
            size: p.size,
            picture: p.picture_url,
            products: p.products.map((prod: any) => ({
              id: prod.id,
              name: prod.name,
              valuePerVE: prod.value_per_ve,
              ve: prod.ve,
              ean: prod.ean
            }))
          })),
          schutteItems: schuettenWithProducts.map(s => ({
            id: s.id,
            name: s.name,
            size: s.size,
            picture: s.picture_url,
            products: s.products.map((prod: any) => ({
              id: prod.id,
              name: prod.name,
              valuePerVE: prod.value_per_ve,
              ve: prod.ve,
              ean: prod.ean
            }))
          })),
          einzelproduktItems: (einzelprodukte || []).map(e => ({
            id: e.id,
            name: e.name,
            targetNumber: e.target_number,
            currentNumber: (progressData || [])
              .filter(p => p.item_type === 'einzelprodukt' && p.item_id === e.id)
              .reduce((sum, p) => sum + p.current_number, 0),
            picture: e.picture_url,
            itemValue: e.item_value
          })),
          kwDays: (kwDays || []).map(kw => ({
            kw: kw.kw,
            days: kw.days
          })),
          assignedMarketIds: (welleMarkets || []).map(wm => wm.market_id),
          participatingGLs: uniqueGLs,
          totalGLs: glsWithMarketsInWave // Number of GLs that have markets in this wave
        };
      })
    );

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

    const { data: einzelprodukte } = await supabase
      .from('wellen_einzelprodukte')
      .select('*')
      .eq('welle_id', id)
      .order('einzelprodukt_order', { ascending: true });

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
      .select('current_number, item_type, item_id, gebietsleiter_id, value_per_unit')
      .eq('welle_id', id);

    const uniqueGLs = new Set((progressData || []).map(p => p.gebietsleiter_id)).size;

    // Derive types based on what items exist
    const types: ('display' | 'kartonware' | 'einzelprodukt')[] = [];
    if (displays && displays.length > 0) types.push('display');
    if (kartonware && kartonware.length > 0) types.push('kartonware');
    if (einzelprodukte && einzelprodukte.length > 0) types.push('einzelprodukt');

    const welleWithDetails = {
      id: welle.id,
      name: welle.name,
      image: welle.image_url,
      startDate: welle.start_date,
      endDate: welle.end_date,
      types, // Derived from displays/kartonware/einzelprodukte
      status: welle.status,
      goalType: welle.goal_type,
      goalPercentage: welle.goal_percentage,
      goalValue: welle.goal_value,
      displayCount: displays?.length || 0,
      kartonwareCount: kartonware?.length || 0,
      einzelproduktCount: einzelprodukte?.length || 0,
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
      einzelproduktItems: (einzelprodukte || []).map(e => ({
        id: e.id,
        name: e.name,
        targetNumber: e.target_number,
        currentNumber: (progressData || [])
          .filter(p => p.item_type === 'einzelprodukt' && p.item_id === e.id)
          .reduce((sum, p) => sum + p.current_number, 0),
        picture: e.picture_url,
        itemValue: e.item_value
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
      paletteItems,
      schutteItems,
      einzelproduktItems,
      kwDays,
      assignedMarketIds
    } = req.body;

    console.log('📦 Received data:', {
      displays: displays?.length || 0,
      kartonwareItems: kartonwareItems?.length || 0,
      paletteItems: paletteItems?.length || 0,
      schutteItems: schutteItems?.length || 0,
      einzelproduktItems: einzelproduktItems?.length || 0,
      paletteItemsData: JSON.stringify(paletteItems),
      schutteItemsData: JSON.stringify(schutteItems)
    });

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

    // Insert einzelprodukte
    if (einzelproduktItems && einzelproduktItems.length > 0) {
      const einzelprodukteToInsert = einzelproduktItems.map((e: any, index: number) => ({
        welle_id: welle.id,
        name: e.name,
        target_number: e.targetNumber,
        item_value: e.itemValue || null,
        picture_url: e.picture || null,
        einzelprodukt_order: index
      }));

      const { error: einzelproduktError } = await supabase
        .from('wellen_einzelprodukte')
        .insert(einzelprodukteToInsert);

      if (einzelproduktError) throw einzelproduktError;
      console.log(`✅ Created ${einzelproduktItems.length} einzelprodukt items`);
    }

    // Insert palettes with their products
    if (paletteItems && paletteItems.length > 0) {
      console.log(`📦 Inserting ${paletteItems.length} palettes...`);
      for (let index = 0; index < paletteItems.length; index++) {
        const p = paletteItems[index];
        console.log(`  Palette ${index}: ${p.name}, products: ${p.products?.length || 0}`);
        
        // Insert palette - use fresh client
        const freshClient = createFreshClient();
        const { data: palette, error: paletteError } = await freshClient
          .from('wellen_paletten')
          .insert({
            welle_id: welle.id,
            name: p.name,
            size: p.size || null,
            picture_url: p.picture || null,
            palette_order: index
          })
          .select()
          .single();

        if (paletteError) {
          console.error('❌ Palette insert error:', paletteError);
          throw paletteError;
        }
        console.log(`  ✅ Palette inserted with id: ${palette.id}`);

        // Insert palette products
        if (p.products && p.products.length > 0) {
          const productsToInsert = p.products.map((prod: any, prodIndex: number) => ({
            palette_id: palette.id,
            name: prod.name,
            value_per_ve: parseFloat(prod.value) || 0,
            ve: parseInt(prod.ve) || 0,
            ean: prod.ean || null,
            product_order: prodIndex
          }));

          const { error: productsError } = await freshClient
            .from('wellen_paletten_products')
            .insert(productsToInsert);

          if (productsError) throw productsError;
        }
      }
      console.log(`✅ Created ${paletteItems.length} palettes with products`);
    }

    // Insert schütten with their products
    if (schutteItems && schutteItems.length > 0) {
      console.log(`📦 Inserting ${schutteItems.length} schütten...`);
      for (let index = 0; index < schutteItems.length; index++) {
        const s = schutteItems[index];
        console.log(`  Schütte ${index}: ${s.name}, products: ${s.products?.length || 0}`);
        
        // Insert schütte - use fresh client
        const freshClient = createFreshClient();
        const { data: schuette, error: schutteError } = await freshClient
          .from('wellen_schuetten')
          .insert({
            welle_id: welle.id,
            name: s.name,
            size: s.size || null,
            picture_url: s.picture || null,
            schuette_order: index
          })
          .select()
          .single();

        if (schutteError) {
          console.error('❌ Schütte insert error:', schutteError);
          throw schutteError;
        }
        console.log(`  ✅ Schütte inserted with id: ${schuette.id}`);

        // Insert schütte products
        if (s.products && s.products.length > 0) {
          const productsToInsert = s.products.map((prod: any, prodIndex: number) => ({
            schuette_id: schuette.id,
            name: prod.name,
            value_per_ve: parseFloat(prod.value) || 0,
            ve: parseInt(prod.ve) || 0,
            ean: prod.ean || null,
            product_order: prodIndex
          }));

          const { error: productsError } = await freshClient
            .from('wellen_schuetten_products')
            .insert(productsToInsert);

          if (productsError) throw productsError;
        }
      }
      console.log(`✅ Created ${schutteItems.length} schütten with products`);
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
    
    const freshClient = createFreshClient();

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
      paletteItems,
      schutteItems,
      einzelproduktItems,
      kwDays,
      assignedMarketIds
    } = req.body;

    // Update main welle record
    const { error: welleError } = await freshClient
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

    // Update displays - preserve IDs to keep progress links
    const { data: existingDisplays } = await freshClient
      .from('wellen_displays')
      .select('id, name')
      .eq('welle_id', id);
    
    const existingDisplayMap = new Map((existingDisplays || []).map(d => [d.name, d.id]));
    const newDisplayNames = new Set((displays || []).map((d: any) => d.name));
    
    // Delete displays that are no longer in the list
    const displaysToDelete = (existingDisplays || []).filter(d => !newDisplayNames.has(d.name)).map(d => d.id);
    if (displaysToDelete.length > 0) {
      await freshClient.from('wellen_displays').delete().in('id', displaysToDelete);
    }
    
    // Update or insert displays
    if (displays && displays.length > 0) {
      for (let index = 0; index < displays.length; index++) {
        const d = displays[index];
        const existingId = existingDisplayMap.get(d.name);
        
        if (existingId) {
          // Update existing display (preserves ID for progress)
          await freshClient.from('wellen_displays').update({
            target_number: d.targetNumber,
            item_value: d.itemValue || null,
            picture_url: d.picture || null,
            display_order: index
          }).eq('id', existingId);
        } else {
          // Insert new display
          await freshClient.from('wellen_displays').insert({
            welle_id: id,
            name: d.name,
            target_number: d.targetNumber,
            item_value: d.itemValue || null,
            picture_url: d.picture || null,
            display_order: index
          });
        }
      }
    }

    // Update kartonware - preserve IDs to keep progress links
    const { data: existingKartonware } = await freshClient
      .from('wellen_kartonware')
      .select('id, name')
      .eq('welle_id', id);
    
    const existingKartonwareMap = new Map((existingKartonware || []).map(k => [k.name, k.id]));
    const newKartonwareNames = new Set((kartonwareItems || []).map((k: any) => k.name));
    
    // Delete kartonware that is no longer in the list
    const kartonwareToDelete = (existingKartonware || []).filter(k => !newKartonwareNames.has(k.name)).map(k => k.id);
    if (kartonwareToDelete.length > 0) {
      await freshClient.from('wellen_kartonware').delete().in('id', kartonwareToDelete);
    }
    
    // Update or insert kartonware
    if (kartonwareItems && kartonwareItems.length > 0) {
      for (let index = 0; index < kartonwareItems.length; index++) {
        const k = kartonwareItems[index];
        const existingId = existingKartonwareMap.get(k.name);
        
        if (existingId) {
          // Update existing kartonware (preserves ID for progress)
          await freshClient.from('wellen_kartonware').update({
            target_number: k.targetNumber,
            item_value: k.itemValue || null,
            picture_url: k.picture || null,
            kartonware_order: index
          }).eq('id', existingId);
        } else {
          // Insert new kartonware
          await freshClient.from('wellen_kartonware').insert({
            welle_id: id,
            name: k.name,
            target_number: k.targetNumber,
            item_value: k.itemValue || null,
            picture_url: k.picture || null,
            kartonware_order: index
          });
        }
      }
    }

    // Update einzelprodukte - preserve IDs to keep progress links
    const { data: existingEinzelprodukte } = await freshClient
      .from('wellen_einzelprodukte')
      .select('id, name')
      .eq('welle_id', id);
    
    const existingEinzelproduktMap = new Map((existingEinzelprodukte || []).map(e => [e.name, e.id]));
    const newEinzelproduktNames = new Set((einzelproduktItems || []).map((e: any) => e.name));
    
    // Delete einzelprodukte that are no longer in the list
    const einzelprodukteToDelete = (existingEinzelprodukte || []).filter(e => !newEinzelproduktNames.has(e.name)).map(e => e.id);
    if (einzelprodukteToDelete.length > 0) {
      await freshClient.from('wellen_einzelprodukte').delete().in('id', einzelprodukteToDelete);
    }
    
    // Update or insert einzelprodukte
    if (einzelproduktItems && einzelproduktItems.length > 0) {
      for (let index = 0; index < einzelproduktItems.length; index++) {
        const e = einzelproduktItems[index];
        const existingId = existingEinzelproduktMap.get(e.name);
        
        if (existingId) {
          // Update existing einzelprodukt (preserves ID for progress)
          await freshClient.from('wellen_einzelprodukte').update({
            target_number: e.targetNumber,
            item_value: e.itemValue || null,
            picture_url: e.picture || null,
            einzelprodukt_order: index
          }).eq('id', existingId);
        } else {
          // Insert new einzelprodukt
          await freshClient.from('wellen_einzelprodukte').insert({
            welle_id: id,
            name: e.name,
            target_number: e.targetNumber,
            item_value: e.itemValue || null,
            picture_url: e.picture || null,
            einzelprodukt_order: index
          });
        }
      }
    }

    // Smart update palettes - preserve IDs for existing items
    const { data: existingPalettes } = await freshClient
      .from('wellen_paletten')
      .select('id')
      .eq('welle_id', id);
    
    const existingPaletteIds = (existingPalettes || []).map(p => p.id);
    const incomingPaletteIds = (paletteItems || []).filter((p: any) => p.id).map((p: any) => p.id);
    
    // Delete palettes that are no longer in the incoming list (and their products)
    const palettesToDelete = existingPaletteIds.filter(existingId => !incomingPaletteIds.includes(existingId));
    if (palettesToDelete.length > 0) {
      await freshClient.from('wellen_paletten_products').delete().in('palette_id', palettesToDelete);
      await freshClient.from('wellen_paletten').delete().in('id', palettesToDelete);
      console.log(`🗑️ Deleted ${palettesToDelete.length} removed palettes`);
    }
    
    if (paletteItems && paletteItems.length > 0) {
      for (let index = 0; index < paletteItems.length; index++) {
        const p = paletteItems[index];
        let paletteId = p.id;
        
        if (paletteId && existingPaletteIds.includes(paletteId)) {
          // UPDATE existing palette (preserve ID)
          await freshClient
            .from('wellen_paletten')
            .update({
              name: p.name,
              size: p.size || null,
              picture_url: p.picture || null,
              palette_order: index
            })
            .eq('id', paletteId);
        } else {
          // INSERT new palette
          const { data: newPalette, error: paletteError } = await freshClient
            .from('wellen_paletten')
            .insert({
              welle_id: id,
              name: p.name,
              size: p.size || null,
              picture_url: p.picture || null,
              palette_order: index
            })
            .select()
            .single();

          if (paletteError) throw paletteError;
          paletteId = newPalette.id;
        }

        // Smart update products for this palette
        const { data: existingProducts } = await freshClient
          .from('wellen_paletten_products')
          .select('id')
          .eq('palette_id', paletteId);
        
        const existingProductIds = (existingProducts || []).map(prod => prod.id);
        const incomingProductIds = (p.products || []).filter((prod: any) => prod.id).map((prod: any) => prod.id);
        
        // Delete products that are no longer in the incoming list
        const productsToDelete = existingProductIds.filter(existingId => !incomingProductIds.includes(existingId));
        if (productsToDelete.length > 0) {
          await freshClient.from('wellen_paletten_products').delete().in('id', productsToDelete);
        }
        
        // Update or insert products
        if (p.products && p.products.length > 0) {
          for (let prodIndex = 0; prodIndex < p.products.length; prodIndex++) {
            const prod = p.products[prodIndex];
            if (prod.id && existingProductIds.includes(prod.id)) {
              // UPDATE existing product (preserve ID)
              await freshClient
                .from('wellen_paletten_products')
                .update({
                  name: prod.name,
                  value_per_ve: parseFloat(prod.value) || 0,
                  ve: parseInt(prod.ve) || 0,
                  ean: prod.ean || null,
                  product_order: prodIndex
                })
                .eq('id', prod.id);
            } else {
              // INSERT new product
              await freshClient
                .from('wellen_paletten_products')
                .insert({
                  palette_id: paletteId,
                  name: prod.name,
                  value_per_ve: parseFloat(prod.value) || 0,
                  ve: parseInt(prod.ve) || 0,
                  ean: prod.ean || null,
                  product_order: prodIndex
                });
            }
          }
        }
      }
      console.log(`✅ Updated ${paletteItems.length} palettes with products (IDs preserved)`);
    }

    // Smart update schütten - preserve IDs for existing items
    const { data: existingSchuetten } = await freshClient
      .from('wellen_schuetten')
      .select('id')
      .eq('welle_id', id);
    
    const existingSchutteIds = (existingSchuetten || []).map(s => s.id);
    const incomingSchutteIds = (schutteItems || []).filter((s: any) => s.id).map((s: any) => s.id);
    
    // Delete schuetten that are no longer in the incoming list (and their products)
    const schuttenToDelete = existingSchutteIds.filter(existingId => !incomingSchutteIds.includes(existingId));
    if (schuttenToDelete.length > 0) {
      await freshClient.from('wellen_schuetten_products').delete().in('schuette_id', schuttenToDelete);
      await freshClient.from('wellen_schuetten').delete().in('id', schuttenToDelete);
      console.log(`🗑️ Deleted ${schuttenToDelete.length} removed schütten`);
    }
    
    if (schutteItems && schutteItems.length > 0) {
      for (let index = 0; index < schutteItems.length; index++) {
        const s = schutteItems[index];
        let schutteId = s.id;
        
        if (schutteId && existingSchutteIds.includes(schutteId)) {
          // UPDATE existing schutte (preserve ID)
          await freshClient
            .from('wellen_schuetten')
            .update({
              name: s.name,
              size: s.size || null,
              picture_url: s.picture || null,
              schuette_order: index
            })
            .eq('id', schutteId);
        } else {
          // INSERT new schutte
          const { data: newSchutte, error: schutteError } = await freshClient
            .from('wellen_schuetten')
            .insert({
              welle_id: id,
              name: s.name,
              size: s.size || null,
              picture_url: s.picture || null,
              schuette_order: index
            })
            .select()
            .single();

          if (schutteError) throw schutteError;
          schutteId = newSchutte.id;
        }

        // Smart update products for this schutte
        const { data: existingProducts } = await freshClient
          .from('wellen_schuetten_products')
          .select('id')
          .eq('schuette_id', schutteId);
        
        const existingProductIds = (existingProducts || []).map(prod => prod.id);
        const incomingProductIds = (s.products || []).filter((prod: any) => prod.id).map((prod: any) => prod.id);
        
        // Delete products that are no longer in the incoming list
        const productsToDelete = existingProductIds.filter(existingId => !incomingProductIds.includes(existingId));
        if (productsToDelete.length > 0) {
          await freshClient.from('wellen_schuetten_products').delete().in('id', productsToDelete);
        }
        
        // Update or insert products
        if (s.products && s.products.length > 0) {
          for (let prodIndex = 0; prodIndex < s.products.length; prodIndex++) {
            const prod = s.products[prodIndex];
            if (prod.id && existingProductIds.includes(prod.id)) {
              // UPDATE existing product (preserve ID)
              await freshClient
                .from('wellen_schuetten_products')
                .update({
                  name: prod.name,
                  value_per_ve: parseFloat(prod.value) || 0,
                  ve: parseInt(prod.ve) || 0,
                  ean: prod.ean || null,
                  product_order: prodIndex
                })
                .eq('id', prod.id);
            } else {
              // INSERT new product
              await freshClient
                .from('wellen_schuetten_products')
                .insert({
                  schuette_id: schutteId,
                  name: prod.name,
                  value_per_ve: parseFloat(prod.value) || 0,
                  ve: parseInt(prod.ve) || 0,
                  ean: prod.ean || null,
                  product_order: prodIndex
                });
            }
          }
        }
      }
      console.log(`✅ Updated ${schutteItems.length} schütten with products (IDs preserved)`);
    }

    // Delete and recreate KW days
    await freshClient.from('wellen_kw_days').delete().eq('welle_id', id);
    if (kwDays && kwDays.length > 0) {
      const kwDaysToInsert = kwDays.map((kw: any, index: number) => ({
        welle_id: id,
        kw: kw.kw,
        days: kw.days,
        kw_order: index
      }));
      await freshClient.from('wellen_kw_days').insert(kwDaysToInsert);
    }

    // Delete and recreate market assignments
    await freshClient.from('wellen_markets').delete().eq('welle_id', id);
    if (assignedMarketIds && assignedMarketIds.length > 0) {
      const marketsToInsert = assignedMarketIds.map((marketId: string) => ({
        welle_id: id,
        market_id: marketId
      }));
      await freshClient.from('wellen_markets').insert(marketsToInsert);
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
    const { gebietsleiter_id, market_id, items, skipVisitUpdate } = req.body;

    console.log(`📊 Batch updating GL progress for welle ${welleId}...${skipVisitUpdate ? ' (skipping visit update)' : ''}`);
    
    const freshClient = createFreshClient();

    if (!gebietsleiter_id || !items || !Array.isArray(items)) {
      return res.status(400).json({ error: 'Missing required fields: gebietsleiter_id, items' });
    }

    // Fetch existing progress for this GL and welle
    const { data: existingProgress, error: fetchError } = await freshClient
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
      
      const entry: any = {
        welle_id: welleId,
        gebietsleiter_id,
        market_id: market_id || null, // Save market_id for tracking
        item_type: item.item_type,
        item_id: item.item_id,
        current_number: newTotal
      };
      
      // Store value_per_unit for palette/schuette products
      if ((item.item_type === 'palette' || item.item_type === 'schuette') && item.value_per_unit !== undefined) {
        entry.value_per_unit = item.value_per_unit;
      }
      
      return entry;
    });

    const { error } = await freshClient
      .from('wellen_gl_progress')
      .upsert(progressEntries, {
        onConflict: 'welle_id,gebietsleiter_id,item_type,item_id'
      });

    if (error) throw error;

    // Log each submission separately for history/audit (each action is a new row)
    if (market_id) {
      const submissionLogs = items.map((item: any) => ({
        welle_id: welleId,
        gebietsleiter_id,
        market_id,
        item_type: item.item_type,
        item_id: item.item_id,
        quantity: item.current_number,
        value_per_unit: item.value_per_unit || null
      }));

      const { error: logError } = await freshClient
        .from('wellen_submissions')
        .insert(submissionLogs);

      if (logError) {
        console.warn('⚠️ Could not log submissions:', logError.message);
        // Don't fail the whole request if logging fails
      } else {
        console.log(`📝 Logged ${submissionLogs.length} submissions for market ${market_id}`);
      }
    }

    // Update market visit count (if market_id is provided and not skipping)
    if (market_id && !skipVisitUpdate) {
      const today = new Date().toISOString().split('T')[0];
      const { data: market } = await freshClient
        .from('markets')
        .select('last_visit_date, current_visits')
        .eq('id', market_id)
        .single();

      // Only increment if not already visited today (multiple actions same day = 1 visit)
      if (market && market.last_visit_date !== today) {
        await freshClient
          .from('markets')
          .update({
            current_visits: (market.current_visits || 0) + 1,
            last_visit_date: today
          })
          .eq('id', market_id);
        console.log(`📍 Recorded visit for market ${market_id}`);
      }
    } else if (skipVisitUpdate) {
      console.log(`⏭️ Skipping visit update for market ${market_id} (user chose not to record new visit)`);
    }

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
    const { gebietsleiter_id, market_id, item_type, item_id, current_number, value_per_unit } = req.body;

    console.log(`📊 Updating GL progress for welle ${welleId}...`);
    
    const freshClient = createFreshClient();

    // Fetch existing progress for this specific item
    const { data: existing } = await freshClient
      .from('wellen_gl_progress')
      .select('current_number')
      .eq('welle_id', welleId)
      .eq('gebietsleiter_id', gebietsleiter_id)
      .eq('item_type', item_type)
      .eq('item_id', item_id)
      .single();

    const existingValue = existing?.current_number || 0;
    const newTotal = existingValue + current_number;

    // Build upsert entry
    const progressEntry: any = {
      welle_id: welleId,
      gebietsleiter_id,
      market_id: market_id || null,
      item_type,
      item_id,
      current_number: newTotal
    };
    
    // Store value_per_unit for palette/schuette products
    if ((item_type === 'palette' || item_type === 'schuette') && value_per_unit !== undefined) {
      progressEntry.value_per_unit = value_per_unit;
    }

    // Upsert with cumulative value
    const { error } = await freshClient
      .from('wellen_gl_progress')
      .upsert(progressEntry, {
        onConflict: 'welle_id,gebietsleiter_id,item_type,item_id'
      });

    if (error) throw error;

    // Also log to wellen_submissions for consistency with batch endpoint
    if (market_id) {
      const { error: logError } = await freshClient
        .from('wellen_submissions')
        .insert({
          welle_id: welleId,
          gebietsleiter_id,
          market_id,
          item_type,
          item_id,
          quantity: current_number,
          value_per_unit: value_per_unit || null
        });

      if (logError) {
        console.warn('⚠️ Could not log submission:', logError.message);
      }
    }

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
    
    const freshClient = createFreshClient();

    const { data, error } = await freshClient
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
// Uses wellen_submissions for individual market-specific entries
// ============================================================================
router.get('/:id/all-progress', async (req: Request, res: Response) => {
  try {
    const { id: welleId } = req.params;
    
    const freshClient = createFreshClient();
    
    // Use wellen_submissions for detailed view (individual submissions with market_id)
    const { data: submissions, error: submissionsError } = await freshClient
      .from('wellen_submissions')
      .select('*')
      .eq('welle_id', welleId)
      .order('created_at', { ascending: false });

    if (submissionsError) {
      return res.status(500).json({ error: submissionsError.message });
    }

    if (!submissions || submissions.length === 0) {
      return res.json([]);
    }

    const glIds = [...new Set(submissions.map(p => p.gebietsleiter_id).filter(Boolean))];
    const marketIds = [...new Set(submissions.map(p => p.market_id).filter(Boolean))];
    const displayIds = submissions.filter(p => p.item_type === 'display').map(p => p.item_id).filter(Boolean);
    const kartonwareIds = submissions.filter(p => p.item_type === 'kartonware').map(p => p.item_id).filter(Boolean);
    const einzelproduktIds = submissions.filter(p => p.item_type === 'einzelprodukt').map(p => p.item_id).filter(Boolean);
    const paletteProductIds = submissions.filter(p => p.item_type === 'palette').map(p => p.item_id).filter(Boolean);
    const schutteProductIds = submissions.filter(p => p.item_type === 'schuette').map(p => p.item_id).filter(Boolean);

    const [glsResult, glDetailsResult, marketsResult, displaysResult, kartonwareResult, einzelproduktResult, paletteProductsResult, schutteProductsResult] = await Promise.all([
      glIds.length > 0 ? freshClient.from('users').select('id, email').in('id', glIds) : { data: [] },
      glIds.length > 0 ? freshClient.from('gebietsleiter').select('id, name').in('id', glIds) : { data: [] },
      marketIds.length > 0 ? freshClient.from('markets').select('id, name, chain, address, postal_code, city').in('id', marketIds) : { data: [] },
      displayIds.length > 0 ? freshClient.from('wellen_displays').select('id, name, item_value').in('id', displayIds) : { data: [] },
      kartonwareIds.length > 0 ? freshClient.from('wellen_kartonware').select('id, name, item_value').in('id', kartonwareIds) : { data: [] },
      einzelproduktIds.length > 0 ? freshClient.from('wellen_einzelprodukte').select('id, name, item_value').in('id', einzelproduktIds) : { data: [] },
      paletteProductIds.length > 0 ? freshClient.from('wellen_paletten_products').select('id, name, palette_id, value_per_ve').in('id', paletteProductIds) : { data: [] },
      schutteProductIds.length > 0 ? freshClient.from('wellen_schuetten_products').select('id, name, schuette_id, value_per_ve').in('id', schutteProductIds) : { data: [] }
    ]);

    const gls = glsResult.data || [];
    const glDetails = glDetailsResult.data || [];
    const markets = marketsResult.data || [];
    const displays = displaysResult.data || [];
    const kartonware = kartonwareResult.data || [];
    const einzelprodukte = einzelproduktResult.data || [];
    let paletteProducts = paletteProductsResult.data || [];
    let schutteProducts = schutteProductsResult.data || [];

    // Fetch parent palette/schuette names - direct ID match only, no fallback
    const paletteIds = [...new Set((paletteProducts || []).map((p: any) => p.palette_id))].filter(Boolean);
    const schutteIds = [...new Set((schutteProducts || []).map((p: any) => p.schuette_id))].filter(Boolean);
    
    const [palettesResult, schuttenResult] = await Promise.all([
      paletteIds.length > 0 ? freshClient.from('wellen_paletten').select('id, name').in('id', paletteIds) : { data: [] },
      schutteIds.length > 0 ? freshClient.from('wellen_schuetten').select('id, name').in('id', schutteIds) : { data: [] }
    ]);
    
    const palettes = palettesResult.data || [];
    const schutten = schuttenResult.data || [];

    // Separate entries by type
    const displayKartonwareEinzelproduktEntries = submissions.filter(p => p.item_type === 'display' || p.item_type === 'kartonware' || p.item_type === 'einzelprodukt');
    const paletteEntries = submissions.filter(p => p.item_type === 'palette');
    const schutteEntries = submissions.filter(p => p.item_type === 'schuette');

    // Process display/kartonware/einzelprodukt entries - each submission is separate
    const standardResponses = displayKartonwareEinzelproduktEntries.map(entry => {
      const gl = glDetails.find((g: any) => g.id === entry.gebietsleiter_id);
      const glUser = gls.find((u: any) => u.id === entry.gebietsleiter_id);
      const market = markets.find((m: any) => m.id === entry.market_id);
      let item;
      if (entry.item_type === 'display') {
        item = displays.find((d: any) => d.id === entry.item_id);
      } else if (entry.item_type === 'kartonware') {
        item = kartonware.find((k: any) => k.id === entry.item_id);
      } else {
        item = einzelprodukte.find((e: any) => e.id === entry.item_id);
      }

      return {
        id: entry.id,
        glName: gl?.name || 'Unknown',
        glEmail: glUser?.email || '',
        marketName: market?.name || 'Unknown',
        marketChain: market?.chain || '',
        marketAddress: market?.address || '',
        marketPostalCode: market?.postal_code || '',
        marketCity: market?.city || '',
        itemType: entry.item_type as 'display' | 'kartonware' | 'einzelprodukt',
        itemName: item?.name || 'Unknown',
        quantity: entry.quantity,
        value: entry.quantity * (item?.item_value || entry.value_per_unit || 0),
        timestamp: entry.created_at,
        photoUrl: entry.photo_url
      };
    });

    // Group palette entries by parent palette (per GL, market, AND timestamp for same submission batch)
    const paletteGroups = new Map<string, any[]>();
    for (const entry of paletteEntries) {
      // Direct ID match only - no fallback
      const product = paletteProducts.find((p: any) => p.id === entry.item_id);
      const parentId = product?.palette_id || (palettes.length === 1 ? palettes[0].id : 'unknown');
      // Group by GL + market + palette + timestamp (rounded to same minute for batch grouping)
      const timestampKey = new Date(entry.created_at).toISOString().slice(0, 16);
      const key = `${entry.gebietsleiter_id}|${entry.market_id}|${parentId}|${timestampKey}`;
      if (!paletteGroups.has(key)) {
        paletteGroups.set(key, []);
      }
      paletteGroups.get(key)!.push({ ...entry, product });
    }

    const paletteResponses: any[] = [];
    for (const [, entries] of paletteGroups) {
      const firstEntry = entries[0];
      const gl = glDetails.find((g: any) => g.id === firstEntry.gebietsleiter_id);
      const glUser = gls.find((u: any) => u.id === firstEntry.gebietsleiter_id);
      const market = markets.find((m: any) => m.id === firstEntry.market_id);
      const parentPalette = palettes.find((p: any) => p.id === firstEntry.product?.palette_id);

      const products = entries.map((e: any) => {
        const valueUsed = e.value_per_unit || e.product?.value_per_ve || 0;
        return {
          id: e.item_id,
          name: e.product?.name || 'Produkt',
          quantity: e.quantity,
          valuePerUnit: valueUsed,
          value: e.quantity * valueUsed
        };
      });

      const totalValue = products.reduce((sum: number, p: any) => sum + p.value, 0);

      paletteResponses.push({
        id: entries.map((e: any) => e.id).join(','),
        glName: gl?.name || 'Unknown',
        glEmail: glUser?.email || '',
        marketName: market?.name || 'Unknown',
        marketChain: market?.chain || '',
        marketAddress: market?.address || '',
        marketPostalCode: market?.postal_code || '',
        marketCity: market?.city || '',
        itemType: 'palette' as const,
        itemName: parentPalette?.name || 'Palette',
        parentId: firstEntry.product?.palette_id,
        products,
        quantity: 1,
        value: totalValue,
        timestamp: firstEntry.created_at,
        photoUrl: firstEntry.photo_url
      });
    }

    // Group schuette entries by parent schuette (per GL, market, AND timestamp)
    const schutteGroups = new Map<string, any[]>();
    for (const entry of schutteEntries) {
      // Direct ID match only - no fallback
      const product = schutteProducts.find((p: any) => p.id === entry.item_id);
      const parentId = product?.schuette_id || (schutten.length === 1 ? schutten[0].id : 'unknown');
      const timestampKey = new Date(entry.created_at).toISOString().slice(0, 16);
      const key = `${entry.gebietsleiter_id}|${entry.market_id}|${parentId}|${timestampKey}`;
      if (!schutteGroups.has(key)) {
        schutteGroups.set(key, []);
      }
      schutteGroups.get(key)!.push({ ...entry, product });
    }

    const schutteResponses: any[] = [];
    for (const [, entries] of schutteGroups) {
      const firstEntry = entries[0];
      const gl = glDetails.find((g: any) => g.id === firstEntry.gebietsleiter_id);
      const glUser = gls.find((u: any) => u.id === firstEntry.gebietsleiter_id);
      const market = markets.find((m: any) => m.id === firstEntry.market_id);
      const parentSchutte = schutten.find((s: any) => s.id === firstEntry.product?.schuette_id);

      const products = entries.map((e: any) => {
        const valueUsed = e.value_per_unit || e.product?.value_per_ve || 0;
        return {
          id: e.item_id,
          name: e.product?.name || 'Produkt',
          quantity: e.quantity,
          valuePerUnit: valueUsed,
          value: e.quantity * valueUsed
        };
      });

      const totalValue = products.reduce((sum: number, p: any) => sum + p.value, 0);

      schutteResponses.push({
        id: entries.map((e: any) => e.id).join(','),
        glName: gl?.name || 'Unknown',
        glEmail: glUser?.email || '',
        marketName: market?.name || 'Unknown',
        marketChain: market?.chain || '',
        marketAddress: market?.address || '',
        marketPostalCode: market?.postal_code || '',
        marketCity: market?.city || '',
        itemType: 'schuette' as const,
        itemName: parentSchutte?.name || 'Schütte',
        parentId: firstEntry.product?.schuette_id,
        products,
        quantity: 1,
        value: totalValue,
        timestamp: firstEntry.created_at,
        photoUrl: firstEntry.photo_url
      });
    }

    // Combine and sort by timestamp
    const response = [...standardResponses, ...paletteResponses, ...schutteResponses]
      .sort((a, b) => new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime());

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
    
    const freshClient = createFreshClient();
    
    // Chain groupings (same as dashboard)
    const chainGroups = {
      billa: ['Adeg', 'Billa+', 'BILLA+', 'BILLA Plus', 'BILLA+ Privat', 'BILLA Plus Privat', 'BILLA Privat'],
      spar: ['Spar', 'SPAR Privat Popovic', 'Spar Gourmet', 'Eurospar', 'Interspar'],
      zoofachhandel: ['Zoofachhandel', 'Futterhaus', 'Fressnapf', 'Das Futterhaus'],
      hagebau: ['Hagebau']
    };

    // Get all progress entries for this GL
    const { data: allProgress, error: progressError } = await freshClient
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
      const { data } = await freshClient.from('markets').select('id, chain').in('id', marketIds);
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
        freshClient.from('wellen_displays').select('id, target_number, welle_id').in('welle_id', welleIds),
        freshClient.from('wellen_kartonware').select('id, target_number, welle_id').in('welle_id', welleIds),
        freshClient.from('wellen_markets').select('welle_id, market_id').in('welle_id', welleIds),
        freshClient.from('wellen').select('id, name').in('id', welleIds)
      ]);
      displays = displaysResult.data || [];
      kartonware = kartonwareResult.data || [];
      welleMarkets = welleMarketsResult.data || [];
      wellen = wellenResult.data || [];
    }

    // Get all markets that are assigned to these wellen (for fallback chain detection)
    const welleMarketIds = [...new Set(welleMarkets.map(wm => wm.market_id))];
    if (welleMarketIds.length > 0) {
      const { data: additionalMarkets } = await freshClient.from('markets').select('id, chain').in('id', welleMarketIds);
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

    // Calculate goals using proportional formula:
    // GL Goal = Total Target × (GL's Markets in Wave / Total Markets in Wave)
    
    // Get GL's markets
    const { data: glMarkets } = await freshClient
      .from('markets')
      .select('id, chain')
      .eq('gebietsleiter_id', glId);
    const glMarketIds = new Set((glMarkets || []).map(m => m.id));
    
    // Count GL's markets per chain group
    const glMarketsPerChain: Record<string, number> = { billa: 0, spar: 0, zoofachhandel: 0, hagebau: 0 };
    for (const market of (glMarkets || [])) {
      for (const [group, chains] of Object.entries(chainGroups)) {
        if (chains.includes(market.chain)) {
          glMarketsPerChain[group]++;
          break;
        }
      }
    }
    
    // For each welle, calculate proportional goal for this GL
    const glGoalsPerChain: Record<string, { displays: number; kartonware: number }> = {
      billa: { displays: 0, kartonware: 0 },
      spar: { displays: 0, kartonware: 0 },
      zoofachhandel: { displays: 0, kartonware: 0 },
      hagebau: { displays: 0, kartonware: 0 }
    };
    
    // Process each welle to calculate proportional goals
    for (const welleId of welleIds) {
      // Get total markets in this welle
      const welleMarketsForThis = welleMarkets.filter(wm => wm.welle_id === welleId);
      const totalMarketsInWelle = welleMarketsForThis.length;
      if (totalMarketsInWelle === 0) continue;
      
      // Count how many of the GL's markets are in this welle
      const glMarketsInWelle = welleMarketsForThis.filter(wm => glMarketIds.has(wm.market_id)).length;
      if (glMarketsInWelle === 0) continue;
      
      // Calculate GL's ratio for this welle
      const glRatio = glMarketsInWelle / totalMarketsInWelle;
      
      // Get displays and kartonware targets for this welle
      const welleDisplays = displays.filter(d => d.welle_id === welleId);
      const welleKartonware = kartonware.filter(k => k.welle_id === welleId);
      
      const welleDisplayTarget = welleDisplays.reduce((sum, d) => sum + (d.target_number || 0), 0);
      const welleKartonwareTarget = welleKartonware.reduce((sum, k) => sum + (k.target_number || 0), 0);
      
      // Calculate GL's proportional goal for this welle
      const glDisplayGoalForWelle = Math.round(welleDisplayTarget * glRatio * 10) / 10;
      const glKartonwareGoalForWelle = Math.round(welleKartonwareTarget * glRatio * 10) / 10;
      
      // Determine chain group for this welle's markets (use first market's chain)
      const firstMarketId = welleMarketsForThis[0]?.market_id;
      const chainGroup = firstMarketId ? getChainGroupByMarket(firstMarketId) : null;
      
      if (chainGroup && glGoalsPerChain[chainGroup]) {
        glGoalsPerChain[chainGroup].displays += glDisplayGoalForWelle;
        glGoalsPerChain[chainGroup].kartonware += glKartonwareGoalForWelle;
      }
    }
    
    // Set goals for each chain
    chainData.billa.goalDisplays = Math.round(glGoalsPerChain.billa.displays * 10) / 10;
    chainData.billa.goalKartonware = Math.round(glGoalsPerChain.billa.kartonware * 10) / 10;
    chainData.spar.goalDisplays = Math.round(glGoalsPerChain.spar.displays * 10) / 10;
    chainData.spar.goalKartonware = Math.round(glGoalsPerChain.spar.kartonware * 10) / 10;
    chainData.zoofachhandel.goalDisplays = Math.round(glGoalsPerChain.zoofachhandel.displays * 10) / 10;
    chainData.zoofachhandel.goalKartonware = Math.round(glGoalsPerChain.zoofachhandel.kartonware * 10) / 10;
    chainData.hagebau.goalDisplays = Math.round(glGoalsPerChain.hagebau.displays * 10) / 10;
    chainData.hagebau.goalKartonware = Math.round(glGoalsPerChain.hagebau.kartonware * 10) / 10;

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

// ============================================================================
// IMAGE UPLOAD: Upload image to Supabase Storage
// ============================================================================
router.post('/upload-image', async (req: Request, res: Response) => {
  try {
    const { image, folder, filename } = req.body;
    
    if (!image) {
      return res.status(400).json({ error: 'No image provided' });
    }

    console.log('📷 Uploading image to wellen-images bucket...');

    // Extract base64 data (remove data:image/...;base64, prefix if present)
    let base64Data = image;
    let contentType = 'image/jpeg';
    
    if (image.startsWith('data:')) {
      const matches = image.match(/^data:([^;]+);base64,(.+)$/);
      if (matches) {
        contentType = matches[1];
        base64Data = matches[2];
      }
    }

    // Convert base64 to buffer
    const buffer = Buffer.from(base64Data, 'base64');

    // Generate unique filename
    const timestamp = Date.now();
    const randomStr = Math.random().toString(36).substring(2, 8);
    const extension = contentType.split('/')[1] || 'jpg';
    const finalFilename = filename || `${timestamp}-${randomStr}.${extension}`;
    const filePath = folder ? `${folder}/${finalFilename}` : finalFilename;

    // Upload to Supabase Storage
    const { data, error } = await supabase.storage
      .from('wellen-images')
      .upload(filePath, buffer, {
        contentType,
        upsert: true
      });

    if (error) {
      console.error('❌ Storage upload error:', error);
      return res.status(500).json({ error: error.message });
    }

    // Get public URL
    const { data: urlData } = supabase.storage
      .from('wellen-images')
      .getPublicUrl(data.path);

    console.log('✅ Image uploaded successfully:', urlData.publicUrl);

    res.json({
      success: true,
      path: data.path,
      url: urlData.publicUrl
    });
  } catch (error: any) {
    console.error('❌ Error uploading image:', error);
    res.status(500).json({ error: error.message || 'Failed to upload image' });
  }
});

// ============================================================================
// IMAGE DELETE: Delete image from Supabase Storage
// ============================================================================
router.delete('/delete-image', async (req: Request, res: Response) => {
  try {
    const { path } = req.body;
    
    if (!path) {
      return res.status(400).json({ error: 'No path provided' });
    }

    console.log('🗑️ Deleting image from wellen-images bucket:', path);

    const { error } = await supabase.storage
      .from('wellen-images')
      .remove([path]);

    if (error) {
      console.error('❌ Storage delete error:', error);
      return res.status(500).json({ error: error.message });
    }

    console.log('✅ Image deleted successfully');
    res.json({ success: true });
  } catch (error: any) {
    console.error('❌ Error deleting image:', error);
    res.status(500).json({ error: error.message || 'Failed to delete image' });
  }
});

// ============================================================================
// GET WAVE MARKETS WITH VISIT STATUS - For markets modal
// ============================================================================
router.get('/:id/markets-status', async (req: Request, res: Response) => {
  try {
    const { id: welleId } = req.params;
    
    // Use fresh client to avoid any caching issues
    const freshClient = createFreshClient();

    // Get wave details
    const { data: welle, error: welleError } = await freshClient
      .from('wellen')
      .select('id, name, start_date, end_date')
      .eq('id', welleId)
      .single();

    if (welleError || !welle) {
      return res.status(404).json({ error: 'Wave not found' });
    }

    // Get assigned market IDs from wellen_markets table
    const { data: wellenMarkets, error: wellenMarketsError } = await freshClient
      .from('wellen_markets')
      .select('market_id')
      .eq('welle_id', welleId);

    if (wellenMarketsError) {
      return res.status(500).json({ error: wellenMarketsError.message });
    }

    const assignedMarketIds = (wellenMarkets || []).map(wm => wm.market_id);
    
    console.log(`📊 Wave ${welleId} markets-status: found ${assignedMarketIds.length} assigned markets`);

    if (assignedMarketIds.length === 0) {
      return res.json({ visited: [], notVisited: [], visitedCount: 0, totalCount: 0 });
    }

    // Get all assigned markets (including last_visit_date for checking visited without success)
    const { data: markets, error: marketsError } = await freshClient
      .from('markets')
      .select('id, name, chain, address, city, gebietsleiter_name, last_visit_date')
      .in('id', assignedMarketIds);

    // Get KW days for this wave to determine the selling period
    const { data: kwDaysData } = await freshClient
      .from('wellen_kw_days')
      .select('kw, days')
      .eq('welle_id', welleId)
      .order('kw_order', { ascending: true });

    // Helper to get KW number and day abbreviation from a date
    const getDateKWInfo = (date: Date): { kw: number; day: string } => {
      // Get ISO week number
      const tempDate = new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate()));
      const dayNum = tempDate.getUTCDay() || 7;
      tempDate.setUTCDate(tempDate.getUTCDate() + 4 - dayNum);
      const yearStart = new Date(Date.UTC(tempDate.getUTCFullYear(), 0, 1));
      const kw = Math.ceil((((tempDate.getTime() - yearStart.getTime()) / 86400000) + 1) / 7);
      
      // Get day abbreviation
      const dayIndex = date.getDay(); // 0 = Sunday
      const days = ['So', 'Mo', 'Di', 'Mi', 'Do', 'Fr', 'Sa'];
      
      return { kw, day: days[dayIndex] };
    };

    // Helper to check if a date falls within the KW selling period
    const isDateInKWSellPeriod = (dateStr: string | null): boolean => {
      if (!dateStr) return false;
      const date = new Date(dateStr);
      
      // If no KW days defined, fall back to wave start/end dates
      if (!kwDaysData || kwDaysData.length === 0) {
        const startDate = new Date(welle.start_date);
        const endDate = new Date(welle.end_date);
        endDate.setHours(23, 59, 59, 999); // Include the entire end day
        return date >= startDate && date <= endDate;
      }

      const { kw: visitKW, day: visitDay } = getDateKWInfo(date);

      for (const kwDay of kwDaysData) {
        // Parse KW (e.g., "KW 5" -> 5, "KW5" -> 5, "5" -> 5)
        const kwMatch = kwDay.kw.match(/(\d+)/);
        if (!kwMatch) continue;
        const kwNum = parseInt(kwMatch[1], 10);
        
        // Check if the visit week matches this KW
        if (visitKW === kwNum) {
          // Check if the day matches any of the selling days (case-insensitive)
          const allowedDays = (kwDay.days || []).map((d: string) => d.toLowerCase());
          if (allowedDays.includes(visitDay.toLowerCase())) {
            return true;
          }
        }
      }

      return false;
    };

    if (marketsError) {
      return res.status(500).json({ error: marketsError.message });
    }

    // Get all submissions for this wave (each submission is a visit with progress)
    const { data: submissions, error: submissionsError } = await freshClient
      .from('wellen_submissions')
      .select('*')
      .eq('welle_id', welleId)
      .order('created_at', { ascending: false });

    if (submissionsError) {
      return res.status(500).json({ error: submissionsError.message });
    }

    // Get GL details for submissions
    const glIds = [...new Set((submissions || []).map(s => s.gebietsleiter_id).filter(Boolean))];
    const { data: glDetails } = await freshClient
      .from('gebietsleiter')
      .select('id, name')
      .in('id', glIds.length > 0 ? glIds : ['__none__']);

    // Get item details for submissions
    const displayIds = (submissions || []).filter(s => s.item_type === 'display').map(s => s.item_id).filter(Boolean);
    const kartonwareIds = (submissions || []).filter(s => s.item_type === 'kartonware').map(s => s.item_id).filter(Boolean);
    const paletteProductIds = (submissions || []).filter(s => s.item_type === 'palette').map(s => s.item_id).filter(Boolean);
    const schutteProductIds = (submissions || []).filter(s => s.item_type === 'schuette').map(s => s.item_id).filter(Boolean);
    const einzelproduktIds = (submissions || []).filter(s => s.item_type === 'einzelprodukt').map(s => s.item_id).filter(Boolean);

    const [displaysResult, kartonwareResult, paletteProductsResult, schutteProductsResult, einzelprodukteResult] = await Promise.all([
      displayIds.length > 0 ? freshClient.from('wellen_displays').select('id, name, item_value').in('id', displayIds) : { data: [] },
      kartonwareIds.length > 0 ? freshClient.from('wellen_kartonware').select('id, name, item_value').in('id', kartonwareIds) : { data: [] },
      paletteProductIds.length > 0 ? freshClient.from('wellen_paletten_products').select('id, name, palette_id, value_per_ve').in('id', paletteProductIds) : { data: [] },
      schutteProductIds.length > 0 ? freshClient.from('wellen_schuetten_products').select('id, name, schuette_id, value_per_ve').in('id', schutteProductIds) : { data: [] },
      einzelproduktIds.length > 0 ? freshClient.from('wellen_einzelprodukte').select('id, name, item_value').in('id', einzelproduktIds) : { data: [] }
    ]);

    const displays = displaysResult.data || [];
    const kartonware = kartonwareResult.data || [];
    const paletteProducts = paletteProductsResult.data || [];
    const schutteProducts = schutteProductsResult.data || [];
    const einzelprodukte = einzelprodukteResult.data || [];

    // Get parent palette/schuette names - direct ID match only, no fallback
    const paletteIds = [...new Set(paletteProducts.map((p: any) => p.palette_id))].filter(Boolean);
    const schutteIds = [...new Set(schutteProducts.map((p: any) => p.schuette_id))].filter(Boolean);
    
    const [palettesResult, schuttenResult] = await Promise.all([
      paletteIds.length > 0 ? freshClient.from('wellen_paletten').select('id, name').in('id', paletteIds) : { data: [] },
      schutteIds.length > 0 ? freshClient.from('wellen_schuetten').select('id, name').in('id', schutteIds) : { data: [] }
    ]);
    
    const palettes = palettesResult.data || [];
    const schutten = schuttenResult.data || [];

    // Group submissions by market
    const submissionsByMarket = new Map<string, any[]>();
    for (const sub of (submissions || [])) {
      if (!submissionsByMarket.has(sub.market_id)) {
        submissionsByMarket.set(sub.market_id, []);
      }
      submissionsByMarket.get(sub.market_id)!.push(sub);
    }

    // Build visited markets with their activity details
    const visited: any[] = [];
    const visitedNoSuccess: any[] = [];
    const notVisited: any[] = [];

    for (const market of (markets || [])) {
      const marketSubs = submissionsByMarket.get(market.id) || [];
      
      if (marketSubs.length > 0) {
        // Find the first (most recent) submission's GL and date
        const firstSub = marketSubs[0];
        const gl = (glDetails || []).find((g: any) => g.id === firstSub.gebietsleiter_id);
        
        // Build activity entries grouped by timestamp
        const activities: any[] = [];
        const subsByTimestamp = new Map<string, any[]>();
        
        for (const sub of marketSubs) {
          const tsKey = new Date(sub.created_at).toISOString().slice(0, 16);
          if (!subsByTimestamp.has(tsKey)) {
            subsByTimestamp.set(tsKey, []);
          }
          subsByTimestamp.get(tsKey)!.push(sub);
        }

        for (const [, subs] of subsByTimestamp) {
          const subGl = (glDetails || []).find((g: any) => g.id === subs[0].gebietsleiter_id);
          
          // Separate display/kartonware/einzelprodukt from palette/schuette
          const displayKartonwareEinzelproduktSubs = subs.filter((s: any) => s.item_type === 'display' || s.item_type === 'kartonware' || s.item_type === 'einzelprodukt');
          const paletteSubs = subs.filter((s: any) => s.item_type === 'palette');
          const schuetteSubs = subs.filter((s: any) => s.item_type === 'schuette');
          
          const items: any[] = [];
          
          // Process display/kartonware/einzelprodukt individually
          for (const s of displayKartonwareEinzelproduktSubs) {
            let itemName = 'Unknown';
            let itemValue = 0;
            if (s.item_type === 'display') {
              const d = displays.find((x: any) => x.id === s.item_id);
              itemName = d?.name || 'Display';
              itemValue = (d?.item_value || 0) * s.quantity;
            } else if (s.item_type === 'kartonware') {
              const k = kartonware.find((x: any) => x.id === s.item_id);
              itemName = k?.name || 'Kartonware';
              itemValue = (k?.item_value || 0) * s.quantity;
            } else {
              const e = einzelprodukte.find((x: any) => x.id === s.item_id);
              itemName = e?.name || 'Einzelprodukt';
              itemValue = (e?.item_value || 0) * s.quantity;
            }
            items.push({ type: s.item_type, name: itemName, quantity: s.quantity, value: itemValue });
          }
          
          // Group palette products by parent palette - direct ID match only
          const paletteGroups = new Map<string, any[]>();
          for (const s of paletteSubs) {
            const p = paletteProducts.find((x: any) => x.id === s.item_id);
            const parentId = p?.palette_id || 'unknown';
            if (!paletteGroups.has(parentId)) paletteGroups.set(parentId, []);
            paletteGroups.get(parentId)!.push({ ...s, product: p });
          }
          for (const [parentId, groupSubs] of paletteGroups) {
            const parent = palettes.find((x: any) => x.id === parentId);
            const products = groupSubs.map((gs: any) => ({
              name: gs.product?.name || 'Produkt',
              quantity: gs.quantity,
              value: (gs.value_per_unit || gs.product?.value_per_ve || 0) * gs.quantity
            }));
            const totalValue = products.reduce((sum: number, p: any) => sum + p.value, 0);
            items.push({
              type: 'palette',
              name: parent?.name || (palettes.length > 0 ? palettes[0].name : 'Palette'),
              quantity: 1,
              value: totalValue,
              products
            });
          }
          
          // Group schuette products by parent schuette - direct ID match only
          const schutteGroups = new Map<string, any[]>();
          for (const s of schuetteSubs) {
            const p = schutteProducts.find((x: any) => x.id === s.item_id);
            const parentId = p?.schuette_id || 'unknown';
            if (!schutteGroups.has(parentId)) schutteGroups.set(parentId, []);
            schutteGroups.get(parentId)!.push({ ...s, product: p });
          }
          for (const [parentId, groupSubs] of schutteGroups) {
            const parent = schutten.find((x: any) => x.id === parentId);
            const products = groupSubs.map((gs: any) => ({
              name: gs.product?.name || 'Produkt',
              quantity: gs.quantity,
              value: (gs.value_per_unit || gs.product?.value_per_ve || 0) * gs.quantity
            }));
            const totalValue = products.reduce((sum: number, p: any) => sum + p.value, 0);
            items.push({
              type: 'schuette',
              name: parent?.name || (schutten.length > 0 ? schutten[0].name : 'Schütte'),
              quantity: 1,
              value: totalValue,
              products
            });
          }

          activities.push({
            glName: subGl?.name || 'Unknown',
            timestamp: subs[0].created_at,
            items,
            totalValue: items.reduce((sum: number, i: any) => sum + i.value, 0)
          });
        }

        visited.push({
          id: market.id,
          name: market.name,
          chain: market.chain,
          address: market.address,
          city: market.city,
          gebietsleiter: market.gebietsleiter_name,
          visitedBy: gl?.name || 'Unknown',
          visitedAt: firstSub.created_at,
          activities
        });
      } else {
        // Check if market was visited during the wave's selling period but has no submission
        if (isDateInKWSellPeriod(market.last_visit_date)) {
          visitedNoSuccess.push({
            id: market.id,
            name: market.name,
            chain: market.chain,
            address: market.address,
            city: market.city,
            gebietsleiter: market.gebietsleiter_name,
            lastVisitDate: market.last_visit_date
          });
        } else {
          notVisited.push({
            id: market.id,
            name: market.name,
            chain: market.chain,
            address: market.address,
            city: market.city,
            gebietsleiter: market.gebietsleiter_name
          });
        }
      }
    }

    // Sort visited by most recent visit first
    visited.sort((a, b) => new Date(b.visitedAt).getTime() - new Date(a.visitedAt).getTime());
    // Sort visited without success by most recent visit first
    visitedNoSuccess.sort((a, b) => new Date(b.lastVisitDate).getTime() - new Date(a.lastVisitDate).getTime());
    // Sort not visited alphabetically
    notVisited.sort((a, b) => a.name.localeCompare(b.name));

    res.json({
      visited,
      visitedNoSuccess,
      notVisited,
      visitedCount: visited.length,
      visitedNoSuccessCount: visitedNoSuccess.length,
      totalCount: markets?.length || 0
    });
  } catch (error: any) {
    console.error('Error fetching wave markets status:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

// ============================================================================
// TEMPORARY: EXPORT WELLEN_SUBMISSIONS TO EXCEL
// ============================================================================
router.get('/export/submissions', async (req: Request, res: Response) => {
  try {
    const freshClient = createFreshClient();
    
    // Get all submissions
    const { data: submissions, error: subError } = await freshClient
      .from('wellen_submissions')
      .select('*')
      .order('created_at', { ascending: false });
    
    if (subError) throw subError;
    
    if (!submissions || submissions.length === 0) {
      return res.status(404).json({ error: 'No submissions found' });
    }
    
    // Collect unique IDs
    const welleIds = [...new Set(submissions.map(s => s.welle_id).filter(Boolean))];
    const glIds = [...new Set(submissions.map(s => s.gebietsleiter_id).filter(Boolean))];
    const marketIds = [...new Set(submissions.map(s => s.market_id).filter(Boolean))];
    
    // Fetch wellen names
    const { data: wellen } = await freshClient
      .from('wellen')
      .select('id, name')
      .in('id', welleIds);
    const welleMap = new Map((wellen || []).map(w => [w.id, w.name]));
    
    // Fetch GL names
    const { data: gls } = await freshClient
      .from('gebietsleiter')
      .select('id, name')
      .in('id', glIds);
    const glMap = new Map((gls || []).map(g => [g.id, g.name]));
    
    // Fetch market names
    const { data: markets } = await freshClient
      .from('markets')
      .select('id, name, chain, address, city')
      .in('id', marketIds);
    const marketMap = new Map((markets || []).map(m => [m.id, m]));
    
    // Fetch item names (products, displays, palettes, schuetten)
    const itemIds = [...new Set(submissions.map(s => s.item_id).filter(Boolean))];
    
    // Try to fetch from various product tables
    const { data: wellenDisplays } = await freshClient
      .from('wellen_displays')
      .select('id, name')
      .in('id', itemIds);
    
    const { data: wellenPaletten } = await freshClient
      .from('wellen_paletten')
      .select('id, name')
      .in('id', itemIds);
    
    const { data: wellenSchuetten } = await freshClient
      .from('wellen_schuetten')
      .select('id, name')
      .in('id', itemIds);
    
    const { data: wellenKartonware } = await freshClient
      .from('wellen_kartonware')
      .select('id, name')
      .in('id', itemIds);
    
    // Build item name map
    const itemMap = new Map<string, string>();
    (wellenDisplays || []).forEach(d => itemMap.set(d.id, d.name));
    (wellenPaletten || []).forEach(p => itemMap.set(p.id, p.name));
    (wellenSchuetten || []).forEach(s => itemMap.set(s.id, s.name));
    (wellenKartonware || []).forEach(k => itemMap.set(k.id, k.name));
    
    // Transform submissions to readable format
    const excelData = submissions.map(sub => {
      const market = marketMap.get(sub.market_id);
      return {
        'ID': sub.id,
        'Welle ID': sub.welle_id,
        'Welle Name': welleMap.get(sub.welle_id) || sub.welle_id,
        'Gebietsleiter ID': sub.gebietsleiter_id,
        'Gebietsleiter Name': glMap.get(sub.gebietsleiter_id) || sub.gebietsleiter_id,
        'Market ID': sub.market_id,
        'Market Name': market?.name || sub.market_id,
        'Market Chain': market?.chain || '',
        'Market Address': market?.address || '',
        'Market City': market?.city || '',
        'Item Type': sub.item_type,
        'Item ID': sub.item_id,
        'Item Name': itemMap.get(sub.item_id) || sub.item_id,
        'Quantity': sub.quantity,
        'Value Per Unit': sub.value_per_unit,
        'Created At': sub.created_at
      };
    });
    
    // Create workbook and worksheet
    const ws = XLSX.utils.json_to_sheet(excelData);
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, 'Submissions');
    
    // Set column widths
    ws['!cols'] = [
      { wch: 38 }, // ID
      { wch: 38 }, // Welle ID
      { wch: 30 }, // Welle Name
      { wch: 38 }, // GL ID
      { wch: 25 }, // GL Name
      { wch: 38 }, // Market ID
      { wch: 30 }, // Market Name
      { wch: 15 }, // Market Chain
      { wch: 40 }, // Market Address
      { wch: 20 }, // Market City
      { wch: 12 }, // Item Type
      { wch: 38 }, // Item ID
      { wch: 40 }, // Item Name
      { wch: 10 }, // Quantity
      { wch: 15 }, // Value Per Unit
      { wch: 25 }, // Created At
    ];
    
    // Generate buffer
    const buffer = XLSX.write(wb, { type: 'buffer', bookType: 'xlsx' });
    
    // Set headers and send file
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', 'attachment; filename=wellen_submissions_export.xlsx');
    res.send(buffer);
    
    console.log(`📊 Exported ${submissions.length} submissions to Excel`);
  } catch (error: any) {
    console.error('Error exporting submissions:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

// ============================================================================
// UPDATE INDIVIDUAL SUBMISSION QUANTITY
// ============================================================================
router.put('/submissions/:submissionId', async (req: Request, res: Response) => {
  try {
    const { submissionId } = req.params;
    const { quantity } = req.body;

    if (typeof quantity !== 'number' || quantity < 0) {
      return res.status(400).json({ error: 'Invalid quantity' });
    }

    const freshClient = createFreshClient();

    // Get the original submission to find related progress entry
    const { data: submission, error: fetchError } = await freshClient
      .from('wellen_submissions')
      .select('*')
      .eq('id', submissionId)
      .single();

    if (fetchError || !submission) {
      return res.status(404).json({ error: 'Submission not found' });
    }

    const oldQuantity = submission.quantity;
    const quantityDiff = quantity - oldQuantity;

    // Update the submission
    const { error: updateError } = await freshClient
      .from('wellen_submissions')
      .update({ quantity })
      .eq('id', submissionId);

    if (updateError) {
      return res.status(500).json({ error: updateError.message });
    }

    // Update the aggregated progress entry if it exists
    if (quantityDiff !== 0) {
      const { data: progressEntry } = await freshClient
        .from('wellen_gl_progress')
        .select('id, current_number')
        .eq('welle_id', submission.welle_id)
        .eq('gebietsleiter_id', submission.gebietsleiter_id)
        .eq('item_type', submission.item_type)
        .eq('item_id', submission.item_id)
        .single();

      if (progressEntry) {
        await freshClient
          .from('wellen_gl_progress')
          .update({ current_number: Math.max(0, progressEntry.current_number + quantityDiff) })
          .eq('id', progressEntry.id);
      }
    }

    res.json({ message: 'Submission updated', submissionId, newQuantity: quantity });
  } catch (error) {
    console.error('Error updating submission:', error);
    res.status(500).json({ error: 'Failed to update submission' });
  }
});

// ============================================================================
// DELETE INDIVIDUAL SUBMISSION
// ============================================================================
router.delete('/submissions/:submissionId', async (req: Request, res: Response) => {
  try {
    const { submissionId } = req.params;

    const freshClient = createFreshClient();

    // Get the original submission first
    const { data: submission, error: fetchError } = await freshClient
      .from('wellen_submissions')
      .select('*')
      .eq('id', submissionId)
      .single();

    if (fetchError || !submission) {
      return res.status(404).json({ error: 'Submission not found' });
    }

    // Delete the submission
    const { error: deleteError } = await freshClient
      .from('wellen_submissions')
      .delete()
      .eq('id', submissionId);

    if (deleteError) {
      return res.status(500).json({ error: deleteError.message });
    }

    // Update the aggregated progress entry
    const { data: progressEntry } = await freshClient
      .from('wellen_gl_progress')
      .select('id, current_number')
      .eq('welle_id', submission.welle_id)
      .eq('gebietsleiter_id', submission.gebietsleiter_id)
      .eq('item_type', submission.item_type)
      .eq('item_id', submission.item_id)
      .single();

    if (progressEntry) {
      const newNumber = Math.max(0, progressEntry.current_number - submission.quantity);
      if (newNumber === 0) {
        // Delete the progress entry if count goes to 0
        await freshClient
          .from('wellen_gl_progress')
          .delete()
          .eq('id', progressEntry.id);
      } else {
        await freshClient
          .from('wellen_gl_progress')
          .update({ current_number: newNumber })
          .eq('id', progressEntry.id);
      }
    }

    res.json({ message: 'Submission deleted', submissionId });
  } catch (error) {
    console.error('Error deleting submission:', error);
    res.status(500).json({ error: 'Failed to delete submission' });
  }
});

// ============================================================================
// TEMPORARY: EXPORT WELLEN_GL_PROGRESS TO EXCEL
// ============================================================================
router.get('/export/progress', async (req: Request, res: Response) => {
  try {
    const freshClient = createFreshClient();
    
    // Get all progress entries
    const { data: progress, error: progressError } = await freshClient
      .from('wellen_gl_progress')
      .select('*')
      .order('updated_at', { ascending: false });
    
    if (progressError) throw progressError;
    
    if (!progress || progress.length === 0) {
      return res.status(404).json({ error: 'No progress entries found' });
    }
    
    // Collect unique IDs
    const welleIds = [...new Set(progress.map(p => p.welle_id).filter(Boolean))];
    const glIds = [...new Set(progress.map(p => p.gebietsleiter_id).filter(Boolean))];
    
    // Fetch wellen names
    const { data: wellen } = await freshClient
      .from('wellen')
      .select('id, name')
      .in('id', welleIds);
    const welleMap = new Map((wellen || []).map(w => [w.id, w.name]));
    
    // Fetch GL names
    const { data: gls } = await freshClient
      .from('gebietsleiter')
      .select('id, name')
      .in('id', glIds);
    const glMap = new Map((gls || []).map(g => [g.id, g.name]));
    
    // Fetch item names (products, displays, palettes, schuetten)
    const itemIds = [...new Set(progress.map(p => p.item_id).filter(Boolean))];
    
    // Try to fetch from various product tables
    const { data: wellenDisplays } = await freshClient
      .from('wellen_displays')
      .select('id, name')
      .in('id', itemIds);
    
    const { data: wellenPaletten } = await freshClient
      .from('wellen_paletten')
      .select('id, name')
      .in('id', itemIds);
    
    const { data: wellenSchuetten } = await freshClient
      .from('wellen_schuetten')
      .select('id, name')
      .in('id', itemIds);
    
    const { data: wellenKartonware } = await freshClient
      .from('wellen_kartonware')
      .select('id, name')
      .in('id', itemIds);
    
    // Build item name map
    const itemMap = new Map<string, string>();
    (wellenDisplays || []).forEach(d => itemMap.set(d.id, d.name));
    (wellenPaletten || []).forEach(p => itemMap.set(p.id, p.name));
    (wellenSchuetten || []).forEach(s => itemMap.set(s.id, s.name));
    (wellenKartonware || []).forEach(k => itemMap.set(k.id, k.name));
    
    // Transform progress to readable format
    const excelData = progress.map(p => ({
      'ID': p.id,
      'Welle ID': p.welle_id,
      'Welle Name': welleMap.get(p.welle_id) || p.welle_id,
      'Gebietsleiter ID': p.gebietsleiter_id,
      'Gebietsleiter Name': glMap.get(p.gebietsleiter_id) || p.gebietsleiter_id,
      'Item Type': p.item_type,
      'Item ID': p.item_id,
      'Item Name': itemMap.get(p.item_id) || p.item_id,
      'Current Number': p.current_number,
      'Value Per Unit': p.value_per_unit,
      'Created At': p.created_at,
      'Updated At': p.updated_at
    }));
    
    // Create workbook and worksheet
    const ws = XLSX.utils.json_to_sheet(excelData);
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, 'Progress');
    
    // Set column widths
    ws['!cols'] = [
      { wch: 38 }, // ID
      { wch: 38 }, // Welle ID
      { wch: 30 }, // Welle Name
      { wch: 38 }, // GL ID
      { wch: 25 }, // GL Name
      { wch: 12 }, // Item Type
      { wch: 38 }, // Item ID
      { wch: 40 }, // Item Name
      { wch: 15 }, // Current Number
      { wch: 15 }, // Value Per Unit
      { wch: 25 }, // Created At
      { wch: 25 }, // Updated At
    ];
    
    // Generate buffer
    const buffer = XLSX.write(wb, { type: 'buffer', bookType: 'xlsx' });
    
    // Set headers and send file
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', 'attachment; filename=wellen_gl_progress_export.xlsx');
    res.send(buffer);
    
    console.log(`📊 Exported ${progress.length} progress entries to Excel`);
  } catch (error: any) {
    console.error('Error exporting progress:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

// ============================================================================
// DELIVERY PHOTO ENDPOINTS
// ============================================================================

/**
 * GET /wellen/market/:marketId/pending-photos
 * Get Vorbesteller submissions for a market that don't have delivery photos yet
 * Returns grouped items with palette/schuette parents and their nested products
 */
router.get('/market/:marketId/pending-photos', async (req: Request, res: Response) => {
  try {
    const { marketId } = req.params;
    const freshClient = createFreshClient();

    console.log(`📷 Checking pending delivery photos for market: ${marketId}`);

    // Get submissions without delivery photos for this market
    const { data: submissions, error } = await freshClient
      .from('wellen_submissions')
      .select(`
        id,
        welle_id,
        item_type,
        item_id,
        quantity,
        created_at,
        wellen (
          id,
          name
        )
      `)
      .eq('market_id', marketId)
      .is('delivery_photo_url', null)
      .order('created_at', { ascending: false });

    if (error) {
      console.error('Error fetching pending photos:', error);
      return res.status(500).json({ error: error.message });
    }

    if (!submissions || submissions.length === 0) {
      return res.json([]);
    }

    // Get item IDs by type
    const displayIds = submissions.filter(s => s.item_type === 'display').map(s => s.item_id);
    const kartonwareIds = submissions.filter(s => s.item_type === 'kartonware').map(s => s.item_id);
    const einzelproduktIds = submissions.filter(s => s.item_type === 'einzelprodukt').map(s => s.item_id);
    const paletteProductIds = submissions.filter(s => s.item_type === 'palette').map(s => s.item_id);
    const schutteProductIds = submissions.filter(s => s.item_type === 'schuette').map(s => s.item_id);

    // Fetch item details including parent IDs for palette/schuette products
    const [displays, kartonware, einzelprodukte, paletteProducts, schutteProducts] = await Promise.all([
      displayIds.length > 0 
        ? freshClient.from('wellen_displays').select('id, name').in('id', displayIds)
        : { data: [] },
      kartonwareIds.length > 0 
        ? freshClient.from('wellen_kartonware').select('id, name').in('id', kartonwareIds)
        : { data: [] },
      einzelproduktIds.length > 0 
        ? freshClient.from('wellen_einzelprodukte').select('id, name').in('id', einzelproduktIds)
        : { data: [] },
      paletteProductIds.length > 0 
        ? freshClient.from('wellen_paletten_products').select('id, name, palette_id').in('id', paletteProductIds)
        : { data: [] },
      schutteProductIds.length > 0 
        ? freshClient.from('wellen_schuetten_products').select('id, name, schuette_id').in('id', schutteProductIds)
        : { data: [] }
    ]);

    // Get parent palette/schuette names
    const paletteParentIds = [...new Set((paletteProducts.data || []).map((p: any) => p.palette_id).filter(Boolean))];
    const schutteParentIds = [...new Set((schutteProducts.data || []).map((s: any) => s.schuette_id).filter(Boolean))];

    const [palettes, schuetten] = await Promise.all([
      paletteParentIds.length > 0
        ? freshClient.from('wellen_paletten').select('id, name').in('id', paletteParentIds)
        : { data: [] },
      schutteParentIds.length > 0
        ? freshClient.from('wellen_schuetten').select('id, name').in('id', schutteParentIds)
        : { data: [] }
    ]);

    // Build lookups
    const itemDetails: Record<string, { name: string; parentId?: string }> = {};
    const parentNames: Record<string, string> = {};

    (displays.data || []).forEach((d: any) => { itemDetails[d.id] = { name: d.name }; });
    (kartonware.data || []).forEach((k: any) => { itemDetails[k.id] = { name: k.name }; });
    (einzelprodukte.data || []).forEach((e: any) => { itemDetails[e.id] = { name: e.name }; });
    (paletteProducts.data || []).forEach((p: any) => { itemDetails[p.id] = { name: p.name, parentId: p.palette_id }; });
    (schutteProducts.data || []).forEach((s: any) => { itemDetails[s.id] = { name: s.name, parentId: s.schuette_id }; });
    
    (palettes.data || []).forEach((p: any) => { parentNames[p.id] = p.name; });
    (schuetten.data || []).forEach((s: any) => { parentNames[s.id] = s.name; });

    // Group submissions by created_at timestamp (rounded to minute) to identify "visits"
    const groupedSubmissions: Record<string, any[]> = {};
    submissions.forEach(sub => {
      const timestamp = new Date(sub.created_at).toISOString().slice(0, 16); // YYYY-MM-DDTHH:mm
      if (!groupedSubmissions[timestamp]) {
        groupedSubmissions[timestamp] = [];
      }
      groupedSubmissions[timestamp].push(sub);
    });

    // Get the most recent group (last visit)
    const sortedTimestamps = Object.keys(groupedSubmissions).sort().reverse();
    const lastVisitTimestamp = sortedTimestamps[0];
    const lastVisitSubmissions = groupedSubmissions[lastVisitTimestamp] || [];

    // Group palette/schuette products by their parent container
    const parentGroups: Record<string, { 
      parentId: string; 
      parentName: string; 
      parentType: 'palette' | 'schuette';
      welleName: string;
      products: Array<{ id: string; name: string; quantity: number }>;
      submissionIds: string[];
    }> = {};

    const standaloneItems: any[] = [];

    lastVisitSubmissions.forEach((sub: any) => {
      const details = itemDetails[sub.item_id];
      const itemName = details?.name || `${sub.item_type} (Unknown)`;
      
      if ((sub.item_type === 'palette' || sub.item_type === 'schuette') && details?.parentId) {
        const parentId = details.parentId;
        const key = `${sub.item_type}-${parentId}`;
        
        if (!parentGroups[key]) {
          parentGroups[key] = {
            parentId,
            parentName: parentNames[parentId] || `${sub.item_type === 'palette' ? 'Palette' : 'Schütte'} (Unknown)`,
            parentType: sub.item_type,
            welleName: sub.wellen?.name || 'Unbekannt',
            products: [],
            submissionIds: []
          };
        }
        
        parentGroups[key].products.push({
          id: sub.id,
          name: itemName,
          quantity: sub.quantity
        });
        parentGroups[key].submissionIds.push(sub.id);
      } else {
        // Display, kartonware, einzelprodukt - standalone items
        standaloneItems.push({
          id: sub.id,
          itemName,
          itemType: sub.item_type,
          quantity: sub.quantity,
          welleName: sub.wellen?.name || 'Unbekannt',
          createdAt: sub.created_at
        });
      }
    });

    // Convert parent groups to result format
    const groupedItems = Object.values(parentGroups).map(group => ({
      id: group.submissionIds.join(','), // Comma-separated IDs for all products in this group
      itemName: group.parentName,
      itemType: group.parentType,
      quantity: group.products.reduce((sum, p) => sum + p.quantity, 0),
      welleName: group.welleName,
      createdAt: lastVisitSubmissions[0]?.created_at,
      products: group.products
    }));

    // Combine: grouped items first, then standalone items
    const result = [...groupedItems, ...standaloneItems];

    console.log(`📷 Found ${result.length} pending items (${groupedItems.length} grouped, ${standaloneItems.length} standalone) for market ${marketId}`);
    res.json(result);
  } catch (error: any) {
    console.error('Error fetching pending photos:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

/**
 * POST /wellen/upload-delivery-photo
 * Upload a delivery verification photo and link it to submissions
 */
router.post('/upload-delivery-photo', async (req: Request, res: Response) => {
  try {
    const { submissionIds, imageData } = req.body;

    if (!submissionIds || !Array.isArray(submissionIds) || submissionIds.length === 0) {
      return res.status(400).json({ error: 'submissionIds array is required' });
    }

    if (!imageData) {
      return res.status(400).json({ error: 'imageData is required' });
    }

    console.log(`📷 Uploading delivery photo for ${submissionIds.length} submissions`);

    const freshClient = createFreshClient();

    // Parse base64 image
    let base64Data = imageData;
    let mimeType = 'image/jpeg';

    if (imageData.startsWith('data:')) {
      const matches = imageData.match(/^data:([^;]+);base64,(.+)$/);
      if (matches) {
        mimeType = matches[1];
        base64Data = matches[2];
      }
    }

    // Determine file extension
    const ext = mimeType.includes('png') ? 'png' : mimeType.includes('gif') ? 'gif' : 'jpg';
    const fileName = `delivery_${Date.now()}_${Math.random().toString(36).substring(7)}.${ext}`;

    // Convert base64 to buffer
    const buffer = Buffer.from(base64Data, 'base64');

    // Upload to Supabase Storage - dedicated bucket for delivery photos
    const { data: uploadData, error: uploadError } = await supabase.storage
      .from('vorbesteller-lieferung')
      .upload(fileName, buffer, {
        contentType: mimeType,
        cacheControl: '3600',
        upsert: false
      });

    if (uploadError) {
      console.error('Upload error:', uploadError);
      return res.status(500).json({ error: uploadError.message });
    }

    // Get public URL
    const { data: urlData } = supabase.storage
      .from('vorbesteller-lieferung')
      .getPublicUrl(fileName);

    const publicUrl = urlData.publicUrl;

    // Update all submissions with the delivery photo URL
    const { error: updateError } = await freshClient
      .from('wellen_submissions')
      .update({ delivery_photo_url: publicUrl })
      .in('id', submissionIds);

    if (updateError) {
      console.error('Error updating submissions:', updateError);
      return res.status(500).json({ error: updateError.message });
    }

    console.log(`✅ Delivery photo uploaded and linked to ${submissionIds.length} submissions`);
    res.json({ 
      success: true, 
      photoUrl: publicUrl,
      updatedCount: submissionIds.length 
    });
  } catch (error: any) {
    console.error('Error uploading delivery photo:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

/**
 * POST /wellen/upload-delivery-photos-per-item
 * Upload individual delivery photos per palette/schütte
 * Each photo is linked to the parent and all its child products
 */
router.post('/upload-delivery-photos-per-item', async (req: Request, res: Response) => {
  try {
    const { photos } = req.body;

    if (!photos || !Array.isArray(photos) || photos.length === 0) {
      return res.status(400).json({ error: 'photos array is required' });
    }

    console.log(`📷 Uploading ${photos.length} individual delivery photos`);

    const freshClient = createFreshClient();
    let totalUpdated = 0;

    for (const photoItem of photos) {
      const { submissionId, photoBase64 } = photoItem;

      if (!submissionId || !photoBase64) {
        console.warn('Skipping photo item with missing data');
        continue;
      }

      // Parse base64 image
      let base64Data = photoBase64;
      let mimeType = 'image/jpeg';

      if (photoBase64.startsWith('data:')) {
        const matches = photoBase64.match(/^data:([^;]+);base64,(.+)$/);
        if (matches) {
          mimeType = matches[1];
          base64Data = matches[2];
        }
      }

      // Determine file extension
      const ext = mimeType.includes('png') ? 'png' : mimeType.includes('gif') ? 'gif' : 'jpg';
      const fileName = `delivery_${submissionId}_${Date.now()}_${Math.random().toString(36).substring(7)}.${ext}`;

      // Convert base64 to buffer
      const buffer = Buffer.from(base64Data, 'base64');

      // Upload to Supabase Storage - dedicated bucket for delivery photos
      const { data: uploadData, error: uploadError } = await supabase.storage
        .from('vorbesteller-lieferung')
        .upload(fileName, buffer, {
          contentType: mimeType,
          cacheControl: '3600',
          upsert: false
        });

      if (uploadError) {
        console.error(`Upload error for ${submissionId}:`, uploadError);
        continue;
      }

      // Get public URL
      const { data: urlData } = supabase.storage
        .from('vorbesteller-lieferung')
        .getPublicUrl(fileName);

      const publicUrl = urlData.publicUrl;

      // First, get the parent submission to find its palette/schuette details
      const { data: parentSubmission, error: parentError } = await freshClient
        .from('wellen_submissions')
        .select('id, parent_palette_id, market_id, welle_id')
        .eq('id', submissionId)
        .single();

      if (parentError || !parentSubmission) {
        console.error(`Parent submission not found for ${submissionId}`);
        // Still try to update just this one
        await freshClient
          .from('wellen_submissions')
          .update({ delivery_photo_url: publicUrl })
          .eq('id', submissionId);
        totalUpdated++;
        continue;
      }

      // Check if this is a parent palette/schuette or an individual item
      // If it has a parent_palette_id = null and is palette/schuette type, it's a parent
      // We need to update all children that have parent_palette_id = this submissionId
      const { data: childSubmissions, error: childError } = await freshClient
        .from('wellen_submissions')
        .select('id')
        .eq('parent_palette_id', submissionId);

      const idsToUpdate = [submissionId];
      if (!childError && childSubmissions && childSubmissions.length > 0) {
        idsToUpdate.push(...childSubmissions.map(c => c.id));
      }

      // Update all related submissions with the delivery photo URL
      const { error: updateError } = await freshClient
        .from('wellen_submissions')
        .update({ delivery_photo_url: publicUrl })
        .in('id', idsToUpdate);

      if (updateError) {
        console.error(`Error updating submissions for ${submissionId}:`, updateError);
      } else {
        totalUpdated += idsToUpdate.length;
        console.log(`✅ Photo for ${submissionId} linked to ${idsToUpdate.length} submissions`);
      }
    }

    console.log(`✅ Completed uploading photos. Total updated: ${totalUpdated}`);
    res.json({ 
      success: true, 
      uploadedCount: totalUpdated 
    });
  } catch (error: any) {
    console.error('Error uploading delivery photos per item:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

export default router;
