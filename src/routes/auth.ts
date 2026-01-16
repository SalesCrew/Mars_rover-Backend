import express, { Router, Request, Response } from 'express';
import { supabase, createFreshClient } from '../config/supabase';

const router: Router = express.Router();

// Request logging
router.use((req, res, next) => {
  console.log(`📨 Auth Route: ${req.method} ${req.path}`);
  next();
});

/**
 * POST /api/auth/login
 * Login using Supabase Auth (no password hash in DB!)
 */
router.post('/login', async (req: Request, res: Response) => {
  console.log('🔐 Login attempt received');
  try {
    const { username, password } = req.body; // username is actually email
    console.log('📧 Login email:', username);

    if (!username || !password) {
      console.log('❌ Missing credentials');
      return res.status(400).json({ error: 'Email and password are required' });
    }

    // Login with Supabase Auth
    console.log('🔄 Calling Supabase Auth...');
    const { data: authData, error: authError } = await supabase.auth.signInWithPassword({
      email: username,
      password: password,
    });

    if (authError || !authData.user) {
      console.log('❌ Supabase Auth error:', authError?.message);
      return res.status(401).json({ error: 'Invalid email or password' });
    }

    console.log('✅ Supabase Auth success, user ID:', authData.user.id);

    // Get user profile from users table
    const freshClient = createFreshClient();
    const { data: profile, error: profileError } = await freshClient
      .from('users')
      .select('*')
      .eq('id', authData.user.id)
      .single();

    if (profileError || !profile) {
      console.log('❌ Profile not found:', profileError?.message);
      return res.status(404).json({ error: 'User profile not found' });
    }

    console.log('✅ Profile found:', profile.role, profile.first_name);

    // Return user data
    res.json({
      user: {
        id: authData.user.id,
        username: authData.user.email,
        email: authData.user.email,
        role: profile.role,
        firstName: profile.first_name,
        lastName: profile.last_name,
        gebietsleiter_id: profile.gebietsleiter_id,
      },
    });
  } catch (error) {
    console.error('Login error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

/**
 * GET /api/auth/me
 * Get current user info (for session restore)
 */
router.get('/me', async (req: Request, res: Response) => {
  console.log('👤 /me endpoint called');
  // For now, just return null - user must log in again
  // In a real app, you'd validate a JWT token here
  res.json({ user: null });
});

/**
 * POST /api/auth/register
 * Create user in Supabase Auth + profile in users table
 */
router.post('/register', async (req: Request, res: Response) => {
  try {
    const { email, password, role, firstName, lastName, gebietsleiter_id } = req.body;

    if (!email || !password || !role || !firstName || !lastName) {
      return res.status(400).json({ error: 'All fields are required' });
    }

    // Create user in Supabase Auth
    const { data: authData, error: authError } = await supabase.auth.admin.createUser({
      email,
      password,
      email_confirm: true, // Auto-confirm
    });

    if (authError || !authData.user) {
      return res.status(400).json({ error: authError?.message || 'Failed to create user' });
    }

    // Create profile in users table
    const freshClient = createFreshClient();
    const { error: profileError } = await freshClient
      .from('users')
      .insert({
        id: authData.user.id,
        role,
        first_name: firstName,
        last_name: lastName,
        gebietsleiter_id: role === 'gl' ? gebietsleiter_id : null,
      });

    if (profileError) {
      console.error('Profile creation error:', profileError);
      return res.status(500).json({ error: 'Failed to create user profile' });
    }

    res.status(201).json({
      user: {
        id: authData.user.id,
        email: authData.user.email,
        role,
        firstName,
        lastName,
        gebietsleiter_id,
      },
    });
  } catch (error) {
    console.error('Registration error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// ============================================================================
// ADMIN MANAGEMENT: Get all admin accounts
// ============================================================================
router.get('/admins', async (req: Request, res: Response) => {
  try {
    const freshClient = createFreshClient();
    
    // Get all admin accounts from users table
    const { data: admins, error } = await freshClient
      .from('users')
      .select('id, first_name, last_name, created_at')
      .eq('role', 'admin')
      .order('created_at', { ascending: true });

    if (error) {
      console.error('Error fetching admins:', error);
      throw error;
    }

    // Get emails from Supabase Auth for each admin
    const adminsWithEmails = await Promise.all((admins || []).map(async (admin) => {
      const { data: authUser } = await supabase.auth.admin.getUserById(admin.id);
      return {
        id: admin.id,
        firstName: admin.first_name,
        lastName: admin.last_name,
        email: authUser?.user?.email || 'No email',
        createdAt: admin.created_at
      };
    }));

    res.json(adminsWithEmails);
  } catch (error) {
    console.error('Error in /admins:', error);
    res.status(500).json({ error: 'Failed to fetch admin accounts' });
  }
});

// ============================================================================
// ADMIN MANAGEMENT: Create new admin account
// ============================================================================
router.post('/create-admin', async (req: Request, res: Response) => {
  try {
    const { firstName, lastName, email, password } = req.body;

    if (!firstName || !lastName || !email || !password) {
      return res.status(400).json({ error: 'All fields are required' });
    }

    // Create user in Supabase Auth
    const { data: authData, error: authError } = await supabase.auth.admin.createUser({
      email,
      password,
      email_confirm: true, // Auto-confirm
    });

    if (authError || !authData.user) {
      return res.status(400).json({ error: authError?.message || 'Failed to create admin account' });
    }

    // Create profile in users table with admin role
    const freshClient = createFreshClient();
    const { error: profileError } = await freshClient
      .from('users')
      .insert({
        id: authData.user.id,
        role: 'admin',
        first_name: firstName,
        last_name: lastName,
      });

    if (profileError) {
      console.error('Profile creation error:', profileError);
      // Try to clean up auth user if profile creation failed
      await supabase.auth.admin.deleteUser(authData.user.id);
      return res.status(500).json({ error: 'Failed to create admin profile' });
    }

    console.log(`✅ Created admin account: ${email}`);
    res.status(201).json({
      id: authData.user.id,
      email: authData.user.email,
      password: password // Echo back for display
    });
  } catch (error) {
    console.error('Error creating admin:', error);
    res.status(500).json({ error: 'Failed to create admin account' });
  }
});

// ============================================================================
// ADMIN MANAGEMENT: Delete admin account
// ============================================================================
router.delete('/admin/:id', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    const requesterId = req.body.requesterId; // ID of admin making the request

    // Prevent self-deletion
    if (id === requesterId) {
      return res.status(400).json({ error: 'Cannot delete your own account' });
    }

    // Check if this is the last admin
    const freshClient = createFreshClient();
    const { count } = await freshClient
      .from('users')
      .select('id', { count: 'exact', head: true })
      .eq('role', 'admin');

    if (count && count <= 1) {
      return res.status(400).json({ error: 'Cannot delete the last admin account' });
    }

    // Delete from Supabase Auth (will cascade to users table)
    const { error: authError } = await supabase.auth.admin.deleteUser(id);

    if (authError) {
      console.error('Error deleting admin from auth:', authError);
      throw authError;
    }

    console.log(`✅ Deleted admin account: ${id}`);
    res.json({ success: true });
  } catch (error) {
    console.error('Error deleting admin:', error);
    res.status(500).json({ error: 'Failed to delete admin account' });
  }
});

// ============================================================================
// ADMIN MANAGEMENT: Change password
// ============================================================================
router.put('/change-password', async (req: Request, res: Response) => {
  try {
    const { userId, currentPassword, newPassword } = req.body;

    if (!userId || !currentPassword || !newPassword) {
      return res.status(400).json({ error: 'All fields are required' });
    }

    if (newPassword.length < 8) {
      return res.status(400).json({ error: 'New password must be at least 8 characters' });
    }

    // Get user email from Supabase Auth (email is stored in auth, not users table)
    const { data: authUser, error: authError } = await supabase.auth.admin.getUserById(userId);

    if (authError || !authUser?.user?.email) {
      console.error('Error fetching user from auth:', authError);
      return res.status(404).json({ error: 'User not found' });
    }

    const userEmail = authUser.user.email;

    // Verify current password by attempting to sign in
    const { error: signInError } = await supabase.auth.signInWithPassword({
      email: userEmail,
      password: currentPassword
    });

    if (signInError) {
      return res.status(401).json({ error: 'Current password is incorrect' });
    }

    // Update password using admin API
    const { error: updateError } = await supabase.auth.admin.updateUserById(userId, {
      password: newPassword
    });

    if (updateError) {
      console.error('Error updating password:', updateError);
      throw updateError;
    }

    console.log(`✅ Password changed for user: ${userId}`);
    res.json({ success: true });
  } catch (error) {
    console.error('Error changing password:', error);
    res.status(500).json({ error: 'Failed to change password' });
  }
});

export default router;
