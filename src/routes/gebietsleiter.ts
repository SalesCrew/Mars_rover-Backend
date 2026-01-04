import { Router, Request, Response } from 'express';
import { supabase } from '../config/supabase';
import bcrypt from 'bcrypt';

const router = Router();

/**
 * GET /api/gebietsleiter
 * Get all gebietsleiter
 */
router.get('/', async (req: Request, res: Response) => {
  try {
    console.log('📋 Fetching all gebietsleiter...');
    
    const { data, error } = await supabase
      .from('gebietsleiter')
      .select('id, name, address, postal_code, city, phone, email, profile_picture_url, created_at, updated_at')
      .order('created_at', { ascending: false });

    if (error) {
      console.error('Supabase error:', error);
      throw error;
    }

    console.log(`✅ Fetched ${data?.length || 0} gebietsleiter`);
    res.json(data || []);
  } catch (error: any) {
    console.error('Error fetching gebietsleiter:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

/**
 * GET /api/gebietsleiter/:id
 * Get a single gebietsleiter by ID
 */
router.get('/:id', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    console.log(`📋 Fetching gebietsleiter ${id}...`);

    const { data, error } = await supabase
      .from('gebietsleiter')
      .select('id, name, address, postal_code, city, phone, email, profile_picture_url, created_at, updated_at')
      .eq('id', id)
      .single();

    if (error) {
      console.error('Supabase error:', error);
      throw error;
    }

    if (!data) {
      return res.status(404).json({ error: 'Gebietsleiter not found' });
    }

    console.log(`✅ Fetched gebietsleiter ${id}`);
    res.json(data);
  } catch (error: any) {
    console.error('Error fetching gebietsleiter:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

/**
 * POST /api/gebietsleiter
 * Create a new gebietsleiter with Supabase Auth account
 */
router.post('/', async (req: Request, res: Response) => {
  try {
    console.log('➕ Creating new gebietsleiter with Supabase Auth...');
    
    const { name, address, postalCode, city, phone, email, password, profilePictureUrl } = req.body;

    // Validate required fields
    if (!name || !address || !postalCode || !city || !phone || !email || !password) {
      return res.status(400).json({ error: 'Missing required fields' });
    }

    // Split name into first and last name
    const nameParts = name.trim().split(' ');
    const firstName = nameParts[0] || name;
    const lastName = nameParts.slice(1).join(' ') || '';

    // Step 1: Create user in Supabase Auth
    console.log('🔐 Creating Supabase Auth user...');
    const { data: authData, error: authError } = await supabase.auth.admin.createUser({
      email,
      password,
      email_confirm: true, // Auto-confirm email
    });

    if (authError || !authData.user) {
      console.error('Supabase Auth error:', authError);
      
      // Check for duplicate email in Auth
      if (authError?.message?.includes('already')) {
        return res.status(409).json({ error: 'Email already exists in authentication system' });
      }
      
      return res.status(400).json({ error: authError?.message || 'Failed to create auth user' });
    }

    const authUserId = authData.user.id;
    console.log(`✅ Created Supabase Auth user: ${authUserId}`);

    // Step 2: Create entry in users table with role 'gl'
    console.log('👤 Creating users table entry...');
    const { error: userError } = await supabase
      .from('users')
      .insert({
        id: authUserId,
        role: 'gl',
        first_name: firstName,
        last_name: lastName,
        gebietsleiter_id: authUserId, // Use auth ID as gebietsleiter_id
      });

    if (userError) {
      console.error('Users table error:', userError);
      // Try to clean up the auth user if users table insert fails
      await supabase.auth.admin.deleteUser(authUserId);
      throw userError;
    }

    console.log('✅ Created users table entry');

    // Step 3: Insert into gebietsleiter table
    console.log('📋 Creating gebietsleiter table entry...');
    const { data, error } = await supabase
      .from('gebietsleiter')
      .insert({
        id: authUserId, // Use same ID as auth user for consistency
        name,
        address,
        postal_code: postalCode,
        city,
        phone,
        email,
        password_hash: 'SUPABASE_AUTH', // Marker that password is in Supabase Auth
        profile_picture_url: profilePictureUrl || null
      })
      .select('id, name, address, postal_code, city, phone, email, profile_picture_url, created_at, updated_at')
      .single();

    if (error) {
      console.error('Gebietsleiter table error:', error);
      
      // Clean up on failure
      await supabase.from('users').delete().eq('id', authUserId);
      await supabase.auth.admin.deleteUser(authUserId);
      
      // Check for unique constraint violation (duplicate email)
      if (error.code === '23505') {
        return res.status(409).json({ error: 'Email already exists' });
      }
      
      throw error;
    }

    console.log(`✅ Created gebietsleiter ${data?.id} with full Supabase Auth integration`);
    res.status(201).json(data);
  } catch (error: any) {
    console.error('Error creating gebietsleiter:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

/**
 * PUT /api/gebietsleiter/:id
 * Update a gebietsleiter
 */
router.put('/:id', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    console.log(`✏️ Updating gebietsleiter ${id}...`);

    const { name, address, postalCode, city, phone, email, password, profilePictureUrl } = req.body;

    const updateData: any = {};
    
    if (name) updateData.name = name;
    if (address) updateData.address = address;
    if (postalCode) updateData.postal_code = postalCode;
    if (city) updateData.city = city;
    if (phone) updateData.phone = phone;
    if (email) updateData.email = email;
    if (profilePictureUrl !== undefined) updateData.profile_picture_url = profilePictureUrl;
    
    // Hash password if provided
    if (password) {
      const saltRounds = 10;
      updateData.password_hash = await bcrypt.hash(password, saltRounds);
    }

    const { data, error } = await supabase
      .from('gebietsleiter')
      .update(updateData)
      .eq('id', id)
      .select('id, name, address, postal_code, city, phone, email, profile_picture_url, created_at, updated_at')
      .single();

    if (error) {
      console.error('Supabase error:', error);
      
      // Check for unique constraint violation (duplicate email)
      if (error.code === '23505') {
        return res.status(409).json({ error: 'Email already exists' });
      }
      
      throw error;
    }

    console.log(`✅ Updated gebietsleiter ${id}`);
    res.json(data);
  } catch (error: any) {
    console.error('Error updating gebietsleiter:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

/**
 * DELETE /api/gebietsleiter/:id
 * Delete a gebietsleiter
 */
router.delete('/:id', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    console.log(`🗑️ Deleting gebietsleiter ${id}...`);

    const { error } = await supabase
      .from('gebietsleiter')
      .delete()
      .eq('id', id);

    if (error) {
      console.error('Supabase error:', error);
      throw error;
    }

    console.log(`✅ Deleted gebietsleiter ${id}`);
    res.status(204).send();
  } catch (error: any) {
    console.error('Error deleting gebietsleiter:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

/**
 * GET /api/gebietsleiter/:id/dashboard-stats
 * Get dashboard statistics for a specific GL
 */
router.get('/:id/dashboard-stats', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    console.log(`📊 Fetching dashboard stats for GL ${id}...`);

    const currentYear = new Date().getFullYear();
    const yearStart = new Date(currentYear, 0, 1).toISOString();

    // 1. Get GL's total vorbesteller value YTD (from wellen_gl_progress)
    const { data: glProgress, error: progressError } = await supabase
      .from('wellen_gl_progress')
      .select('current_number, item_type, item_id')
      .eq('gebietsleiter_id', id)
      .gte('created_at', yearStart);

    if (progressError) console.error('Progress error:', progressError);

    // Get display and kartonware prices to calculate value
    const displayIds = glProgress?.filter(p => p.item_type === 'display').map(p => p.item_id) || [];
    const kartonwareIds = glProgress?.filter(p => p.item_type === 'kartonware').map(p => p.item_id) || [];

    let glYearTotal = 0;

    if (displayIds.length > 0) {
      const { data: displays } = await supabase
        .from('wellen_displays')
        .select('id, price')
        .in('id', displayIds);
      
      if (displays) {
        glProgress?.filter(p => p.item_type === 'display').forEach(p => {
          const display = displays.find(d => d.id === p.item_id);
          if (display) glYearTotal += (display.price || 0) * (p.current_number || 0);
        });
      }
    }

    if (kartonwareIds.length > 0) {
      const { data: kartonware } = await supabase
        .from('wellen_kartonware')
        .select('id, price')
        .in('id', kartonwareIds);
      
      if (kartonware) {
        glProgress?.filter(p => p.item_type === 'kartonware').forEach(p => {
          const item = kartonware.find(k => k.id === p.item_id);
          if (item) glYearTotal += (item.price || 0) * (p.current_number || 0);
        });
      }
    }

    // 2. Get agency average (all GLs' total)
    const { data: allGLs } = await supabase.from('gebietsleiter').select('id');
    const glCount = allGLs?.length || 1;

    const { data: allProgress } = await supabase
      .from('wellen_gl_progress')
      .select('current_number, item_type, item_id, gebietsleiter_id')
      .gte('created_at', yearStart);

    let agencyTotal = 0;
    const allDisplayIds = [...new Set(allProgress?.filter(p => p.item_type === 'display').map(p => p.item_id) || [])];
    const allKartonwareIds = [...new Set(allProgress?.filter(p => p.item_type === 'kartonware').map(p => p.item_id) || [])];

    if (allDisplayIds.length > 0) {
      const { data: displays } = await supabase.from('wellen_displays').select('id, price').in('id', allDisplayIds);
      if (displays && allProgress) {
        allProgress.filter(p => p.item_type === 'display').forEach(p => {
          const display = displays.find(d => d.id === p.item_id);
          if (display) agencyTotal += (display.price || 0) * (p.current_number || 0);
        });
      }
    }

    if (allKartonwareIds.length > 0) {
      const { data: kartonware } = await supabase.from('wellen_kartonware').select('id, price').in('id', allKartonwareIds);
      if (kartonware && allProgress) {
        allProgress.filter(p => p.item_type === 'kartonware').forEach(p => {
          const item = kartonware.find(k => k.id === p.item_id);
          if (item) agencyTotal += (item.price || 0) * (p.current_number || 0);
        });
      }
    }

    const agencyAverage = glCount > 0 ? agencyTotal / glCount : 0;
    const percentageChange = agencyAverage > 0 ? ((glYearTotal - agencyAverage) / agencyAverage) * 100 : 0;

    // 3. Get vorverkauf count for this GL
    const { count: vorverkaufCount } = await supabase
      .from('vorverkauf_entries')
      .select('id', { count: 'exact', head: true })
      .eq('gebietsleiter_id', id);

    // 4. Get vorbestellung count (progress entries count)
    const { count: vorbestellungCount } = await supabase
      .from('wellen_gl_progress')
      .select('id', { count: 'exact', head: true })
      .eq('gebietsleiter_id', id);

    // 5. Get markets visited (markets where GL has any action)
    // Get all markets assigned to this GL
    const { data: assignedMarkets } = await supabase
      .from('gl_markets')
      .select('market_id')
      .eq('gebietsleiter_id', id);

    const assignedMarketIds = assignedMarkets?.map(m => m.market_id) || [];
    const totalAssignedMarkets = assignedMarketIds.length;

    // Get markets where GL has submitted progress
    const { data: progressMarkets } = await supabase
      .from('wellen_gl_progress')
      .select('market_id')
      .eq('gebietsleiter_id', id)
      .not('market_id', 'is', null);

    // Get markets where GL has submitted vorverkauf
    const { data: vorverkaufMarkets } = await supabase
      .from('vorverkauf_entries')
      .select('market_id')
      .eq('gebietsleiter_id', id);

    const visitedMarketIds = new Set([
      ...(progressMarkets?.map(p => p.market_id) || []),
      ...(vorverkaufMarkets?.map(v => v.market_id) || [])
    ]);
    const marketsVisited = visitedMarketIds.size;

    console.log(`✅ Dashboard stats for GL ${id}: yearTotal=${glYearTotal}, vorverkauf=${vorverkaufCount}, vorbestellung=${vorbestellungCount}, markets=${marketsVisited}/${totalAssignedMarkets}`);

    res.json({
      yearTotal: Math.round(glYearTotal),
      percentageChange: Math.round(percentageChange * 10) / 10,
      vorverkaufCount: vorverkaufCount || 0,
      vorbestellungCount: vorbestellungCount || 0,
      marketsVisited,
      totalMarkets: totalAssignedMarkets || marketsVisited || 180 // Fallback if no assignment table
    });
  } catch (error: any) {
    console.error('❌ Error fetching dashboard stats:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

/**
 * GET /api/gebietsleiter/:id/suggested-markets
 * Get suggested markets for today (prioritize those with active vorbesteller)
 */
router.get('/:id/suggested-markets', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    console.log(`📍 Fetching suggested markets for GL ${id}...`);

    const now = new Date();

    // Get active waves with their date ranges
    const { data: activeWaves } = await supabase
      .from('wellen')
      .select('id, name, start_date, end_date')
      .eq('status', 'active');

    // Get markets assigned to this GL (if gl_markets table exists)
    let assignedMarketIds: string[] = [];
    const { data: glMarkets } = await supabase
      .from('gl_markets')
      .select('market_id')
      .eq('gebietsleiter_id', id);

    if (glMarkets && glMarkets.length > 0) {
      assignedMarketIds = glMarkets.map(m => m.market_id);
    }

    // Get markets from active waves
    const waveMarketIds: string[] = [];
    if (activeWaves && activeWaves.length > 0) {
      const waveIds = activeWaves.map(w => w.id);
      const { data: wellenMarkets } = await supabase
        .from('wellen_markets')
        .select('market_id, welle_id')
        .in('welle_id', waveIds);

      if (wellenMarkets) {
        wellenMarkets.forEach(wm => waveMarketIds.push(wm.market_id));
      }
    }

    // Combine market IDs - prioritize wave markets
    const priorityMarketIds = [...new Set(waveMarketIds)];
    const allRelevantMarketIds = assignedMarketIds.length > 0 
      ? assignedMarketIds 
      : priorityMarketIds;

    if (allRelevantMarketIds.length === 0) {
      // Fallback: get any markets
      const { data: anyMarkets } = await supabase
        .from('markets')
        .select('id, name, address, city, postal_code, chain')
        .limit(10);

      return res.json((anyMarkets || []).map(m => ({
        marketId: m.id,
        name: `${m.chain} ${m.name}`.trim(),
        address: `${m.address}, ${m.postal_code} ${m.city}`,
        lastVisitWeeks: 4,
        visits: { current: 0, required: 12 },
        status: 'at-risk',
        hasActiveWave: false
      })));
    }

    // Fetch market details
    const { data: markets } = await supabase
      .from('markets')
      .select('id, name, address, city, postal_code, chain, frequency')
      .in('id', allRelevantMarketIds);

    if (!markets) {
      return res.json([]);
    }

    // Get GL's progress for these markets to calculate visits
    const { data: marketProgress } = await supabase
      .from('wellen_gl_progress')
      .select('market_id, created_at')
      .eq('gebietsleiter_id', id)
      .in('market_id', allRelevantMarketIds);

    // Calculate stats for each market
    const suggestions = markets.map(market => {
      const isInActiveWave = priorityMarketIds.includes(market.id);
      const marketActions = marketProgress?.filter(p => p.market_id === market.id) || [];
      const lastAction = marketActions.length > 0 
        ? new Date(Math.max(...marketActions.map(a => new Date(a.created_at).getTime())))
        : null;
      
      const weeksAgo = lastAction 
        ? Math.floor((now.getTime() - lastAction.getTime()) / (7 * 24 * 60 * 60 * 1000))
        : 8;

      const frequency = market.frequency || 12;
      const currentVisits = marketActions.length;
      
      return {
        marketId: market.id,
        name: `${market.chain} ${market.name}`.trim(),
        address: `${market.address}, ${market.postal_code} ${market.city}`,
        lastVisitWeeks: weeksAgo,
        visits: { current: Math.min(currentVisits, frequency), required: frequency },
        status: (currentVisits >= frequency * 0.5 ? 'on-track' : 'at-risk') as 'on-track' | 'at-risk',
        hasActiveWave: isInActiveWave
      };
    });

    // Sort: prioritize active wave markets, then by weeks since last visit
    suggestions.sort((a, b) => {
      if (a.hasActiveWave && !b.hasActiveWave) return -1;
      if (!a.hasActiveWave && b.hasActiveWave) return 1;
      return b.lastVisitWeeks - a.lastVisitWeeks;
    });

    console.log(`✅ Found ${suggestions.length} suggested markets for GL ${id}`);
    res.json(suggestions.slice(0, 10));
  } catch (error: any) {
    console.error('❌ Error fetching suggested markets:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

export default router;



