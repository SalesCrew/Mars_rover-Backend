import { Router, Request, Response } from 'express';
import { supabase, createFreshClient } from '../config/supabase';
import bcrypt from 'bcrypt';

const router = Router();

/**
 * GET /api/gebietsleiter
 * Get all active gebietsleiter
 */
router.get('/', async (req: Request, res: Response) => {
  try {
    console.log('📋 Fetching all gebietsleiter...');
    
    const freshClient = createFreshClient();
    
    const { data, error } = await freshClient
      .from('gebietsleiter')
      .select('id, name, address, postal_code, city, phone, email, profile_picture_url, is_active, created_at, updated_at')
      .neq('is_active', false) // Filter out inactive/deleted GLs
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
    
    const freshClient = createFreshClient();

    const { data, error } = await freshClient
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
    
    const freshClient = createFreshClient();

    // Step 2: Create entry in users table with role 'gl'
    console.log('👤 Creating users table entry...');
    const { error: userError } = await freshClient
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
    const { data, error } = await freshClient
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
      await freshClient.from('users').delete().eq('id', authUserId);
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
    
    const freshClient = createFreshClient();

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

    const { data, error } = await freshClient
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
 * Deactivate a gebietsleiter - deletes auth user but keeps data for progress tracking
 */
router.delete('/:id', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    console.log(`🗑️ Deactivating gebietsleiter ${id}...`);
    
    const freshClient = createFreshClient();

    // 1. Delete the Supabase Auth user so they can't login anymore
    // The GL id is the same as the auth user id
    const { error: authError } = await supabase.auth.admin.deleteUser(id);
    
    if (authError) {
      console.error('Error deleting auth user:', authError);
      // Continue even if auth deletion fails - the user might already be deleted
      // or might not have an auth account
    } else {
      console.log(`✅ Deleted auth user ${id}`);
    }

    // 2. Mark the GL as inactive instead of deleting the data
    // This preserves progress tracking data
    const { error: updateError } = await freshClient
      .from('gebietsleiter')
      .update({ 
        is_active: false,
        email: `deleted_${Date.now()}_${id.substring(0, 8)}@deleted.local` // Change email to prevent conflicts
      })
      .eq('id', id);

    if (updateError) {
      console.error('Supabase error marking GL as inactive:', updateError);
      // If update fails, try to delete as fallback (old behavior)
      const { error: deleteError } = await freshClient
        .from('gebietsleiter')
        .delete()
        .eq('id', id);
      
      if (deleteError) {
        throw deleteError;
      }
    }

    console.log(`✅ Deactivated gebietsleiter ${id} - data preserved for progress tracking`);
    res.status(204).send();
  } catch (error: any) {
    console.error('Error deactivating gebietsleiter:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

/**
 * POST /api/gebietsleiter/:id/change-password
 * Change password for a gebietsleiter using Supabase Auth
 */
router.post('/:id/change-password', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    const { currentPassword, newPassword } = req.body;
    
    console.log(`🔐 Password change request for gebietsleiter ${id}...`);
    
    const freshClient = createFreshClient();

    if (!currentPassword || !newPassword) {
      return res.status(400).json({ error: 'Current password and new password are required' });
    }

    if (newPassword.length < 6) {
      return res.status(400).json({ error: 'New password must be at least 6 characters long' });
    }

    // Get the gebietsleiter email
    const { data: gl, error: glError } = await freshClient
      .from('gebietsleiter')
      .select('email')
      .eq('id', id)
      .single();

    if (glError || !gl) {
      console.error('GL lookup error:', glError);
      return res.status(404).json({ error: 'Gebietsleiter not found' });
    }

    // Verify current password by attempting to sign in
    const { error: signInError } = await supabase.auth.signInWithPassword({
      email: gl.email,
      password: currentPassword
    });

    if (signInError) {
      console.error('Current password verification failed:', signInError.message);
      return res.status(401).json({ error: 'Aktuelles Passwort ist falsch' });
    }

    // Update the password using admin API
    const { error: updateError } = await supabase.auth.admin.updateUserById(id, {
      password: newPassword
    });

    if (updateError) {
      console.error('Password update error:', updateError);
      return res.status(500).json({ error: 'Failed to update password' });
    }

    console.log(`✅ Password changed successfully for gebietsleiter ${id}`);
    res.json({ success: true, message: 'Password changed successfully' });
  } catch (error: any) {
    console.error('Error changing password:', error);
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
    
    const freshClient = createFreshClient();

    const currentYear = new Date().getFullYear();
    const yearStart = new Date(currentYear, 0, 1).toISOString();

    // 1. Get GL's total vorbesteller value YTD (from wellen_gl_progress)
    const { data: glProgress, error: progressError } = await freshClient
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
      const { data: displays } = await freshClient
        .from('wellen_displays')
        .select('id, item_value')
        .in('id', displayIds);
      
      if (displays) {
        glProgress?.filter(p => p.item_type === 'display').forEach(p => {
          const display = displays.find(d => d.id === p.item_id);
          if (display) glYearTotal += (display.item_value || 0) * (p.current_number || 0);
        });
      }
    }

    if (kartonwareIds.length > 0) {
      const { data: kartonware } = await freshClient
        .from('wellen_kartonware')
        .select('id, item_value')
        .in('id', kartonwareIds);
      
      if (kartonware) {
        glProgress?.filter(p => p.item_type === 'kartonware').forEach(p => {
          const item = kartonware.find(k => k.id === p.item_id);
          if (item) glYearTotal += (item.item_value || 0) * (p.current_number || 0);
        });
      }
    }

    // 2. Get agency average (all GLs' total)
    const { data: allGLs } = await freshClient.from('gebietsleiter').select('id');
    const glCount = allGLs?.length || 1;

    const { data: allProgress } = await freshClient
      .from('wellen_gl_progress')
      .select('current_number, item_type, item_id, gebietsleiter_id')
      .gte('created_at', yearStart);

    let agencyTotal = 0;
    const allDisplayIds = [...new Set(allProgress?.filter(p => p.item_type === 'display').map(p => p.item_id) || [])];
    const allKartonwareIds = [...new Set(allProgress?.filter(p => p.item_type === 'kartonware').map(p => p.item_id) || [])];

    if (allDisplayIds.length > 0) {
      const { data: displays } = await freshClient.from('wellen_displays').select('id, item_value').in('id', allDisplayIds);
      if (displays && allProgress) {
        allProgress.filter(p => p.item_type === 'display').forEach(p => {
          const display = displays.find(d => d.id === p.item_id);
          if (display) agencyTotal += (display.item_value || 0) * (p.current_number || 0);
        });
      }
    }

    if (allKartonwareIds.length > 0) {
      const { data: kartonware } = await freshClient.from('wellen_kartonware').select('id, item_value').in('id', allKartonwareIds);
      if (kartonware && allProgress) {
        allProgress.filter(p => p.item_type === 'kartonware').forEach(p => {
          const item = kartonware.find(k => k.id === p.item_id);
          if (item) agencyTotal += (item.item_value || 0) * (p.current_number || 0);
        });
      }
    }

    const agencyAverage = glCount > 0 ? agencyTotal / glCount : 0;
    const percentageChange = agencyAverage > 0 ? ((glYearTotal - agencyAverage) / agencyAverage) * 100 : 0;

    // 3. Get Vorverkauf count (from vorverkauf_submissions table)
    const { count: vorverkaufCount } = await freshClient
      .from('vorverkauf_submissions')
      .select('id', { count: 'exact', head: true })
      .eq('gebietsleiter_id', id);

    // 4. Get Vorbestellung count (unique submissions - group by market_id + date)
    // Each batch submission to a market on a day counts as 1 Vorbestellung
    const { data: vorbestellerProgress } = await freshClient
      .from('wellen_gl_progress')
      .select('market_id, created_at')
      .eq('gebietsleiter_id', id)
      .not('market_id', 'is', null);

    // Count unique market+date combinations for Vorbestellungen
    const uniqueVorbestellungen = new Set<string>();
    vorbestellerProgress?.forEach(p => {
      const dateStr = new Date(p.created_at).toISOString().split('T')[0];
      uniqueVorbestellungen.add(`${p.market_id}_${dateStr}`);
    });
    const vorbestellungCount = uniqueVorbestellungen.size;

    // 5. Get total markets assigned to this GL (via gebietsleiter_id field in markets table)
    const { count: totalAssignedMarkets } = await freshClient
      .from('markets')
      .select('id', { count: 'exact', head: true })
      .eq('gebietsleiter_id', id)
      .eq('is_active', true);

    // 6. Get unique market visits (any action counts, but same market same day = 1)
    // Sources: market visits (last_visit_date), vorverkauf_submissions, wellen_gl_progress, vorverkauf_entries
    
    // Get markets with last_visit_date for this GL
    const { data: visitedMarketsData } = await freshClient
      .from('markets')
      .select('id, last_visit_date')
      .eq('gebietsleiter_id', id)
      .not('last_visit_date', 'is', null);

    // Get vorbesteller progress (market + date)
    const { data: vorbestellerVisits } = await freshClient
      .from('wellen_gl_progress')
      .select('market_id, created_at')
      .eq('gebietsleiter_id', id)
      .not('market_id', 'is', null);

    // Get vorverkauf submissions (market + date)
    const { data: vorverkaufVisits } = await freshClient
      .from('vorverkauf_submissions')
      .select('market_id, created_at')
      .eq('gebietsleiter_id', id);

    // Get produktersatz entries (market + date)
    const { data: produktersatzVisits } = await freshClient
      .from('vorverkauf_entries')
      .select('market_id, created_at')
      .eq('gebietsleiter_id', id);

    // Count unique market+date combinations across all sources
    const uniqueMarketVisits = new Set<string>();
    
    // Add visits from last_visit_date
    visitedMarketsData?.forEach(m => {
      if (m.last_visit_date) {
        const dateStr = new Date(m.last_visit_date).toISOString().split('T')[0];
        uniqueMarketVisits.add(`${m.id}_${dateStr}`);
      }
    });
    
    // Add vorbesteller visits
    vorbestellerVisits?.forEach(v => {
      const dateStr = new Date(v.created_at).toISOString().split('T')[0];
      uniqueMarketVisits.add(`${v.market_id}_${dateStr}`);
    });
    
    // Add vorverkauf visits
    vorverkaufVisits?.forEach(v => {
      const dateStr = new Date(v.created_at).toISOString().split('T')[0];
      uniqueMarketVisits.add(`${v.market_id}_${dateStr}`);
    });
    
    // Add produktersatz visits
    produktersatzVisits?.forEach(v => {
      const dateStr = new Date(v.created_at).toISOString().split('T')[0];
      uniqueMarketVisits.add(`${v.market_id}_${dateStr}`);
    });

    const marketsVisited = uniqueMarketVisits.size;

    console.log(`✅ Dashboard stats for GL ${id}: yearTotal=${glYearTotal}, vorverkauf=${vorverkaufCount}, vorbestellung=${vorbestellungCount}, markets=${marketsVisited}/${totalAssignedMarkets}`);

    res.json({
      yearTotal: Math.round(glYearTotal),
      percentageChange: Math.round(percentageChange * 10) / 10,
      vorverkaufCount: vorverkaufCount || 0,
      vorbestellungCount: vorbestellungCount,
      marketsVisited,
      totalMarkets: totalAssignedMarkets || 0
    });
  } catch (error: any) {
    console.error('❌ Error fetching dashboard stats:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

// Helper functions for KW+Day matching (same logic as PreorderNotification)
const getCurrentKWNumber = (): number => {
  const now = new Date();
  const startOfYear = new Date(now.getFullYear(), 0, 1);
  const days = Math.floor((now.getTime() - startOfYear.getTime()) / (24 * 60 * 60 * 1000));
  return Math.ceil((days + startOfYear.getDay() + 1) / 7);
};

const getCurrentDayAbbr = (): string => {
  const days = ['SO', 'MO', 'DI', 'MI', 'DO', 'FR', 'SA'];
  return days[new Date().getDay()];
};

const extractKWNumber = (kwString: string): number => {
  const match = kwString.match(/(\d+)/);
  return match ? parseInt(match[1], 10) : -1;
};

/**
 * GET /api/gebietsleiter/:id/suggested-markets
 * Smart market suggestions based on priority scoring:
 * - Vorbesteller (KW+Day match): +200 pts
 * - Frequency overdue: +100 pts
 * - Vorverkauf last 3 days: +80 pts
 * - Frequency soon: +50 pts
 * - Vorverkauf last week: +40 pts
 * - Vorverkauf active: +20 pts
 */
router.get('/:id/suggested-markets', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    console.log(`📍 Fetching smart suggested markets for GL ${id}...`);
    
    const freshClient = createFreshClient();

    const now = new Date();
    const currentKW = getCurrentKWNumber();
    const currentDay = getCurrentDayAbbr();

    // 1. Get ALL markets assigned to this GL (via gebietsleiter_id field)
    const { data: glMarkets } = await freshClient
      .from('markets')
      .select('id, name, address, city, postal_code, chain, frequency, last_visit_date, current_visits')
      .eq('gebietsleiter_id', id)
      .eq('is_active', true);

    if (!glMarkets || glMarkets.length === 0) {
      console.log(`⚠️ No markets assigned to GL ${id}`);
      return res.json([]);
    }

    const marketIds = glMarkets.map(m => m.id);

    // 2. Get active Vorbesteller waves with KW days
    const { data: vorbestellerWaves } = await freshClient
      .from('wellen')
      .select('id, name')
      .eq('status', 'active');

    // Get KW days for active waves
    let vorbestellerKwDays: { welle_id: string; kw: string; days: string[] }[] = [];
    if (vorbestellerWaves && vorbestellerWaves.length > 0) {
      const waveIds = vorbestellerWaves.map(w => w.id);
      const { data: kwDaysData } = await freshClient
        .from('wellen_kw_days')
        .select('welle_id, kw, days')
        .in('welle_id', waveIds);
      if (kwDaysData) {
        vorbestellerKwDays = kwDaysData;
      }
    }

    // Get markets linked to active Vorbesteller waves
    let vorbestellerMarketIds: Set<string> = new Set();
    if (vorbestellerWaves && vorbestellerWaves.length > 0) {
      const waveIds = vorbestellerWaves.map(w => w.id);
      const { data: wellenMarkets } = await freshClient
        .from('wellen_markets')
        .select('market_id, welle_id')
        .in('welle_id', waveIds)
        .in('market_id', marketIds);
      
      if (wellenMarkets) {
        wellenMarkets.forEach(wm => vorbestellerMarketIds.add(wm.market_id));
      }
    }

    // Check if today matches any Vorbesteller KW+Day
    const isVorbestellerToday = vorbestellerKwDays.some(kwDay => {
      const kwNum = extractKWNumber(kwDay.kw);
      const matchesKW = kwNum === currentKW;
      const matchesDay = kwDay.days.some(day => day.toUpperCase() === currentDay);
      return matchesKW && matchesDay;
    });

    // 3. Get active Vorverkauf waves
    const { data: vorverkaufWaves } = await freshClient
      .from('vorverkauf_wellen')
      .select('id, name, start_date, end_date')
      .eq('status', 'active');

    // Get markets linked to active Vorverkauf waves
    let vorverkaufMarketData: Map<string, { endDate: Date; daysUntilEnd: number }> = new Map();
    if (vorverkaufWaves && vorverkaufWaves.length > 0) {
      const waveIds = vorverkaufWaves.map(w => w.id);
      const { data: vvMarkets } = await freshClient
        .from('vorverkauf_wellen_markets')
        .select('market_id, welle_id')
        .in('welle_id', waveIds)
        .in('market_id', marketIds);
      
      if (vvMarkets) {
        vvMarkets.forEach(vvm => {
          const wave = vorverkaufWaves.find(w => w.id === vvm.welle_id);
          if (wave) {
            const endDate = new Date(wave.end_date);
            const daysUntilEnd = Math.floor((endDate.getTime() - now.getTime()) / (1000 * 60 * 60 * 24));
            // Keep the one with the soonest end date
            const existing = vorverkaufMarketData.get(vvm.market_id);
            if (!existing || daysUntilEnd < existing.daysUntilEnd) {
              vorverkaufMarketData.set(vvm.market_id, { endDate, daysUntilEnd });
            }
          }
        });
      }
    }

    // 4. Calculate priority score and reason for each market
    const suggestions = glMarkets.map(market => {
      let priorityScore = 0;
      const reasons: string[] = [];

      const frequency = market.frequency || 12;
      const expectedIntervalDays = 365 / frequency;
      
      // Calculate days since last visit
      let daysSinceVisit = 999; // Never visited = very overdue
      if (market.last_visit_date) {
        const lastVisit = new Date(market.last_visit_date);
        daysSinceVisit = Math.floor((now.getTime() - lastVisit.getTime()) / (1000 * 60 * 60 * 24));
      }

      const weeksAgo = Math.floor(daysSinceVisit / 7);

      // --- VORBESTELLER SCORE (+200) ---
      if (vorbestellerMarketIds.has(market.id) && isVorbestellerToday) {
        priorityScore += 200;
        reasons.push('HEUTE: Vorbesteller');
      }

      // --- FREQUENCY OVERDUE SCORE (+100) ---
      const isOverdue = daysSinceVisit > expectedIntervalDays;
      if (isOverdue) {
        priorityScore += 100;
        reasons.push('Frequenz überfällig');
      }

      // --- VORVERKAUF CLOSING SCORE (+80/+40/+20) ---
      const vvData = vorverkaufMarketData.get(market.id);
      if (vvData) {
        if (vvData.daysUntilEnd <= 3 && vvData.daysUntilEnd >= 0) {
          priorityScore += 80;
          reasons.push('Vorverkauf: letzte 3 Tage');
        } else if (vvData.daysUntilEnd <= 7) {
          priorityScore += 40;
          reasons.push('Vorverkauf: letzte Woche');
        } else {
          priorityScore += 20;
          reasons.push('Vorverkauf aktiv');
        }
      }

      // --- FREQUENCY SOON SCORE (+50) ---
      const isSoon = !isOverdue && daysSinceVisit > (expectedIntervalDays - 7);
      if (isSoon) {
        priorityScore += 50;
        reasons.push('Bald Frequenz fällig');
      }

      // Determine primary reason (highest priority one)
      let priorityReason = reasons.length > 0 ? reasons[0] : 'Regelmäßiger Besuch';
      
      // Combine reasons if multiple high-priority ones
      if (reasons.includes('HEUTE: Vorbesteller') && reasons.includes('Frequenz überfällig')) {
        priorityReason = 'HEUTE: Vorbesteller + Frequenz';
      } else if (reasons.includes('HEUTE: Vorbesteller') && reasons.some(r => r.includes('Vorverkauf'))) {
        priorityReason = 'HEUTE: Vorbesteller + Vorverkauf';
      }

      const currentVisits = market.current_visits || 0;
      
      return {
        marketId: market.id,
        name: `${market.chain} ${market.name}`.trim(),
        address: `${market.address}, ${market.postal_code} ${market.city}`,
        lastVisitWeeks: weeksAgo,
        visits: { current: Math.min(currentVisits, frequency), required: frequency },
        status: (currentVisits >= frequency * 0.5 ? 'on-track' : 'at-risk') as 'on-track' | 'at-risk',
        priorityScore,
        priorityReason
      };
    });

    // Sort by priority score (highest first), then by weeks since last visit
    suggestions.sort((a, b) => {
      if (b.priorityScore !== a.priorityScore) {
        return b.priorityScore - a.priorityScore;
      }
      return b.lastVisitWeeks - a.lastVisitWeeks;
    });

    console.log(`✅ Found ${suggestions.length} suggested markets for GL ${id}, returning top 15`);
    res.json(suggestions.slice(0, 15));
  } catch (error: any) {
    console.error('❌ Error fetching suggested markets:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

/**
 * GET /api/gebietsleiter/:id/profile-stats
 * Get profile statistics for a GL
 */
router.get('/:id/profile-stats', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    console.log(`📊 Fetching profile stats for GL ${id}...`);
    
    const freshClient = createFreshClient();

    const now = new Date();
    const currentMonth = now.getMonth();
    const currentYear = now.getFullYear();
    
    // Get current month start/end
    const currentMonthStart = new Date(currentYear, currentMonth, 1).toISOString();
    const currentMonthEnd = new Date(currentYear, currentMonth + 1, 0, 23, 59, 59).toISOString();
    
    // Get previous month start/end
    const prevMonthStart = new Date(currentYear, currentMonth - 1, 1).toISOString();
    const prevMonthEnd = new Date(currentYear, currentMonth, 0, 23, 59, 59).toISOString();

    // 1. Get GL's total markets
    const { count: totalMarkets } = await freshClient
      .from('markets')
      .select('id', { count: 'exact', head: true })
      .eq('gebietsleiter_id', id)
      .eq('is_active', true);

    // 2. Get unique markets visited this month (from wellen_submissions, vorverkauf_submissions, vorverkauf_entries)
    const [wellenSubs, vorverkaufSubs, produkttauschEntries] = await Promise.all([
      freshClient.from('wellen_submissions').select('market_id, created_at').eq('gebietsleiter_id', id).gte('created_at', currentMonthStart).lte('created_at', currentMonthEnd),
      freshClient.from('vorverkauf_submissions').select('market_id, created_at').eq('gebietsleiter_id', id).gte('created_at', currentMonthStart).lte('created_at', currentMonthEnd),
      freshClient.from('vorverkauf_entries').select('market_id, created_at').eq('gebietsleiter_id', id).gte('created_at', currentMonthStart).lte('created_at', currentMonthEnd)
    ]);

    const currentMonthMarkets = new Set([
      ...(wellenSubs.data || []).map(s => s.market_id),
      ...(vorverkaufSubs.data || []).map(s => s.market_id),
      ...(produkttauschEntries.data || []).map(e => e.market_id)
    ]);
    const monthlyVisits = currentMonthMarkets.size;

    // 3. Get previous month visits for comparison
    const [wellenSubsPrev, vorverkaufSubsPrev, produkttauschEntriesPrev] = await Promise.all([
      freshClient.from('wellen_submissions').select('market_id, created_at').eq('gebietsleiter_id', id).gte('created_at', prevMonthStart).lte('created_at', prevMonthEnd),
      freshClient.from('vorverkauf_submissions').select('market_id, created_at').eq('gebietsleiter_id', id).gte('created_at', prevMonthStart).lte('created_at', prevMonthEnd),
      freshClient.from('vorverkauf_entries').select('market_id, created_at').eq('gebietsleiter_id', id).gte('created_at', prevMonthStart).lte('created_at', prevMonthEnd)
    ]);

    const prevMonthMarkets = new Set([
      ...(wellenSubsPrev.data || []).map(s => s.market_id),
      ...(vorverkaufSubsPrev.data || []).map(s => s.market_id),
      ...(produkttauschEntriesPrev.data || []).map(e => e.market_id)
    ]);
    const prevMonthVisits = prevMonthMarkets.size;
    const monthChangePercent = prevMonthVisits > 0 ? Math.round(((monthlyVisits - prevMonthVisits) / prevMonthVisits) * 100) : 0;

    // 4. Get Vorbesteller success rate (markets with vorbesteller / total markets)
    const { data: vorbestellerProgress } = await freshClient
      .from('wellen_gl_progress')
      .select('welle_id, item_type')
      .eq('gebietsleiter_id', id)
      .gt('current_number', 0);

    const wellenWithProgress = new Set((vorbestellerProgress || []).map(p => p.welle_id));
    
    // Get total markets in wellen for this GL
    let totalWellenMarkets = 0;
    let marketsWithVorbesteller = 0;
    
    if (wellenWithProgress.size > 0) {
      const { data: wellenMarkets } = await freshClient
        .from('wellen_markets')
        .select('market_id, welle_id')
        .in('welle_id', Array.from(wellenWithProgress));
      
      // Filter to only markets that belong to this GL
      const { data: glMarkets } = await freshClient
        .from('markets')
        .select('id')
        .eq('gebietsleiter_id', id);
      
      const glMarketIds = new Set((glMarkets || []).map(m => m.id));
      const relevantWellenMarkets = (wellenMarkets || []).filter(wm => glMarketIds.has(wm.market_id));
      totalWellenMarkets = new Set(relevantWellenMarkets.map(wm => wm.market_id)).size;
      
      // Markets that have vorbesteller progress
      const { data: progressMarkets } = await freshClient
        .from('wellen_submissions')
        .select('market_id')
        .eq('gebietsleiter_id', id);
      
      marketsWithVorbesteller = new Set((progressMarkets || []).map(p => p.market_id)).size;
    }

    const sellInSuccessRate = totalWellenMarkets > 0 ? Math.round((marketsWithVorbesteller / totalWellenMarkets) * 100) : 0;

    // 5. Get previous month sell-in rate for comparison
    const { data: prevVorbestellerProgress } = await freshClient
      .from('wellen_submissions')
      .select('market_id')
      .eq('gebietsleiter_id', id)
      .gte('created_at', prevMonthStart)
      .lte('created_at', prevMonthEnd);

    const prevMonthVorbestellerMarkets = new Set((prevVorbestellerProgress || []).map(p => p.market_id)).size;
    const prevSellInRate = totalWellenMarkets > 0 ? Math.round((prevMonthVorbestellerMarkets / totalWellenMarkets) * 100) : 0;
    const sellInChangePercent = prevSellInRate > 0 ? Math.round(sellInSuccessRate - prevSellInRate) : 0;

    // 6. Get most visited market - combine all submission types
    const allMarketVisits: Record<string, number> = {};
    
    const addMarketVisit = (marketId: string) => {
      if (!marketId) return;
      allMarketVisits[marketId] = (allMarketVisits[marketId] || 0) + 1;
    };
    
    (wellenSubs.data || []).forEach(s => addMarketVisit(s.market_id));
    (wellenSubsPrev.data || []).forEach(s => addMarketVisit(s.market_id));
    (vorverkaufSubs.data || []).forEach(s => addMarketVisit(s.market_id));
    (vorverkaufSubsPrev.data || []).forEach(s => addMarketVisit(s.market_id));
    (produkttauschEntries.data || []).forEach(e => addMarketVisit(e.market_id));
    (produkttauschEntriesPrev.data || []).forEach(e => addMarketVisit(e.market_id));
    
    // Find market with most visits
    let mostVisitedMarket = { name: 'Keine Daten', chain: '', visitCount: 0 };
    
    if (Object.keys(allMarketVisits).length > 0) {
      const sortedMarkets = Object.entries(allMarketVisits).sort((a, b) => b[1] - a[1]);
      const topMarketId = sortedMarkets[0][0];
      const topMarketVisits = sortedMarkets[0][1];
      
      const { data: topMarket } = await freshClient
        .from('markets')
        .select('name, chain')
        .eq('id', topMarketId)
        .single();
      
      if (topMarket) {
        mostVisitedMarket = {
          name: topMarket.name || 'Unbekannt',
          chain: topMarket.chain || '',
          visitCount: topMarketVisits
        };
      }
    }

    // 7. Get this month's Vorverkäufe, Vorbesteller, and Produkttausch counts
    const vorverkaufeCount = (vorverkaufSubs.data || []).length;
    const vorbestellerCount = (wellenSubs.data || []).length;
    const produkttauschCount = (produkttauschEntries.data || []).length;

    // 8. Get top 3 visited markets
    const sortedMarketVisits = Object.entries(allMarketVisits)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 3);
    
    const topMarkets = await Promise.all(
      sortedMarketVisits.map(async ([marketId, visitCount]) => {
        const { data: market } = await freshClient
          .from('markets')
          .select('id, name, chain, address, city, postal_code')
          .eq('id', marketId)
          .single();
        
        // Get last visit date for this market from all sources
        const allVisits = [
          ...(wellenSubs.data || []).filter(s => s.market_id === marketId),
          ...(wellenSubsPrev.data || []).filter(s => s.market_id === marketId),
          ...(vorverkaufSubs.data || []).filter(s => s.market_id === marketId),
          ...(vorverkaufSubsPrev.data || []).filter(s => s.market_id === marketId),
          ...(produkttauschEntries.data || []).filter(e => e.market_id === marketId),
          ...(produkttauschEntriesPrev.data || []).filter(e => e.market_id === marketId)
        ];
        
        const lastVisit = allVisits.length > 0 
          ? allVisits.sort((a, b) => new Date(b.created_at).getTime() - new Date(a.created_at).getTime())[0].created_at
          : null;
        
        return {
          id: market?.id || marketId,
          name: market?.name || 'Unbekannt',
          chain: market?.chain || '',
          address: market?.address || '',
          visitCount,
          lastVisit: lastVisit || ''
        };
      })
    );

    res.json({
      monthlyVisits,
      totalMarkets: totalMarkets || 0,
      monthChangePercent,
      sellInSuccessRate,
      sellInChangePercent,
      mostVisitedMarket,
      vorverkaufeCount,
      vorbestellerCount,
      produkttauschCount,
      topMarkets
    });
  } catch (error: any) {
    console.error('❌ Error fetching profile stats:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

// ============================================================================
// ONBOARDING ENDPOINTS
// ============================================================================

// Check if GL has read a specific onboarding feature
router.get('/:id/onboarding/:featureKey', async (req: Request, res: Response) => {
  try {
    const { id, featureKey } = req.params;
    
    console.log(`📖 Checking onboarding status for GL ${id}, feature: ${featureKey}`);
    
    const freshClient = createFreshClient();
    
    const { data, error } = await freshClient
      .from('gl_onboarding_reads')
      .select('id')
      .eq('gl_id', id)
      .eq('feature_key', featureKey)
      .maybeSingle();
    
    if (error) throw error;
    
    res.json({ hasRead: !!data });
  } catch (error: any) {
    console.error('❌ Error checking onboarding status:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

// Mark an onboarding feature as read
router.post('/:id/onboarding/:featureKey', async (req: Request, res: Response) => {
  try {
    const { id, featureKey } = req.params;
    
    console.log(`✅ Marking onboarding as read for GL ${id}, feature: ${featureKey}`);
    
    const freshClient = createFreshClient();
    
    const { error } = await freshClient
      .from('gl_onboarding_reads')
      .upsert({
        gl_id: id,
        feature_key: featureKey,
        read_at: new Date().toISOString()
      }, {
        onConflict: 'gl_id,feature_key'
      });
    
    if (error) throw error;
    
    res.json({ success: true });
  } catch (error: any) {
    console.error('❌ Error marking onboarding as read:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

export default router;



