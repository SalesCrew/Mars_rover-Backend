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

export default router;
