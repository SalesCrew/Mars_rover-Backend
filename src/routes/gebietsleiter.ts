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
    console.log('📋 Supabase client check:', !!supabase);
    
    const { data, error, count } = await supabase
      .from('gebietsleiter')
      .select('*', { count: 'exact' });

    console.log('📋 Query result - data:', data?.length, 'error:', error, 'count:', count);

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

export default router;



