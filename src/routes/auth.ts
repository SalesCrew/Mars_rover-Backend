import express, { Router, Request, Response, NextFunction } from 'express';
import { createHash } from 'crypto';
import { supabase, createFreshClient } from '../config/supabase';
import { AuthRequest, authenticateToken, requireAdmin, requireSelfOrAdmin } from '../middleware/auth';

const router: Router = express.Router();
const USER_PROFILE_SELECT = 'id, role, first_name, last_name, gebietsleiter_id';

const AUTH_RATE_LIMIT_WINDOW_MS = 15 * 60 * 1000;
const AUTH_LOGIN_RATE_LIMIT_MAX_ATTEMPTS = 20;
const AUTH_REFRESH_RATE_LIMIT_MAX_ATTEMPTS = 120;
const AUTH_RATE_LIMIT_MAX_BUCKETS = 10000;
const authRateLimitBuckets = new Map<string, { count: number; resetAt: number }>();

const getAuthRateLimitKey = (req: Request): string => {
  const ip = req.ip || req.socket.remoteAddress || 'unknown';
  const ipHash = createHash('sha256').update(ip).digest('hex');
  const username = typeof req.body?.username === 'string' ? req.body.username.trim().toLowerCase() : '';
  const usernameHash = username ? createHash('sha256').update(username).digest('hex') : '';
  const refreshToken = typeof req.body?.refreshToken === 'string' ? req.body.refreshToken : '';
  const refreshHash = refreshToken ? createHash('sha256').update(refreshToken).digest('hex') : '';
  return `${req.path}:${ipHash}:${usernameHash || refreshHash}`;
};

const authRateLimit = (req: Request, res: Response, next: NextFunction) => {
  const now = Date.now();

  for (const [key, bucket] of authRateLimitBuckets) {
    if (bucket.resetAt <= now) {
      authRateLimitBuckets.delete(key);
    }
  }

  const key = getAuthRateLimitKey(req);
  const bucket = authRateLimitBuckets.get(key) || {
    count: 0,
    resetAt: now + AUTH_RATE_LIMIT_WINDOW_MS,
  };

  bucket.count += 1;
  authRateLimitBuckets.set(key, bucket);

  while (authRateLimitBuckets.size > AUTH_RATE_LIMIT_MAX_BUCKETS) {
    const oldestKey = authRateLimitBuckets.keys().next().value;
    if (!oldestKey) break;
    authRateLimitBuckets.delete(oldestKey);
  }

  const maxAttempts = req.path === '/refresh'
    ? AUTH_REFRESH_RATE_LIMIT_MAX_ATTEMPTS
    : AUTH_LOGIN_RATE_LIMIT_MAX_ATTEMPTS;

  if (bucket.count > maxAttempts) {
    return res.status(429).json({ error: 'Too many authentication attempts. Please try again later.' });
  }

  return next();
};

const ensureActiveGlProfile = async (profile: any): Promise<boolean> => {
  if (profile.role !== 'gl') {
    return true;
  }

  const glId = profile.gebietsleiter_id || profile.id;
  if (!glId) {
    return false;
  }

  const freshClient = createFreshClient();
  const { data, error } = await freshClient
    .from('gebietsleiter')
    .select('id, is_active')
    .eq('id', glId)
    .maybeSingle();

  return !error && !!data && data.is_active !== false;
};

const isValidRole = (role: unknown): role is 'admin' | 'gl' =>
  role === 'admin' || role === 'gl';
const sanitizeLoggedPath = (value: string): string =>
  value
    .replace(/[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}/gi, ':uuid')
    .replace(/\/\d{6,}(?=\/|$)/g, '/:number');

// Request logging
router.use((req, res, next) => {
  console.log(`📨 Auth Route: ${req.method} ${sanitizeLoggedPath(req.path)}`);
  next();
});

/**
 * POST /api/auth/login
 * Login using Supabase Auth (no password hash in DB!)
 */
router.post('/login', authRateLimit, async (req: Request, res: Response) => {
  console.log('🔐 Login attempt received');
  try {
    const { username, password } = req.body; // username is actually email

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
      console.log('Authentication failed');
      return res.status(401).json({ error: 'Invalid email or password' });
    }

    console.log('Supabase Auth success');

    // Get user profile from users table
    const freshClient = createFreshClient();
    const { data: profile, error: profileError } = await freshClient
      .from('users')
      .select(USER_PROFILE_SELECT)
      .eq('id', authData.user.id)
      .single();

    if (profileError || !profile) {
      console.log('Authenticated profile not found');
      return res.status(404).json({ error: 'User profile not found' });
    }

    if (!(await ensureActiveGlProfile(profile))) {
      return res.status(403).json({ error: 'User account is inactive' });
    }

    console.log('Authenticated profile found');

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
      accessToken: authData.session?.access_token,
      refreshToken: authData.session?.refresh_token,
      expiresAt: authData.session?.expires_at,
    });
  } catch (error) {
    console.error('Login error');
    res.status(500).json({ error: 'Internal server error' });
  }
});

/**
 * POST /api/auth/refresh
 * Refresh a Supabase Auth session without exposing service keys to the client.
 */
router.post('/refresh', authRateLimit, async (req: Request, res: Response) => {
  try {
    const { refreshToken } = req.body;

    if (!refreshToken) {
      return res.status(400).json({ error: 'Refresh token is required' });
    }

    const { data, error } = await supabase.auth.refreshSession({
      refresh_token: refreshToken,
    });

    if (error || !data.session || !data.user) {
      return res.status(401).json({ error: 'Session refresh failed' });
    }

    const freshClient = createFreshClient();
    const { data: profile, error: profileError } = await freshClient
      .from('users')
      .select(USER_PROFILE_SELECT)
      .eq('id', data.user.id)
      .single();

    if (profileError || !profile) {
      return res.status(404).json({ error: 'User profile not found' });
    }

    if (!(await ensureActiveGlProfile(profile))) {
      return res.status(403).json({ error: 'User account is inactive' });
    }

    res.json({
      user: {
        id: data.user.id,
        username: data.user.email,
        email: data.user.email,
        role: profile.role,
        firstName: profile.first_name,
        lastName: profile.last_name,
        gebietsleiter_id: profile.gebietsleiter_id,
      },
      accessToken: data.session.access_token,
      refreshToken: data.session.refresh_token,
      expiresAt: data.session.expires_at,
    });
  } catch (error) {
    console.error('Refresh error');
    res.status(500).json({ error: 'Internal server error' });
  }
});

/**
 * GET /api/auth/me
 * Get current user info (for session restore)
 */
router.get('/me', authenticateToken, async (req: AuthRequest, res: Response) => {
  console.log('👤 /me endpoint called');
  res.json({ user: req.user || null });
});

router.post('/logout', authenticateToken, async (req: AuthRequest, res: Response) => {
  try {
    const authHeader = req.headers.authorization || '';
    const token = authHeader.startsWith('Bearer ') ? authHeader.slice(7) : '';

    if (!token) {
      return res.status(401).json({ error: 'Authentication token required' });
    }

    const { error } = await supabase.auth.admin.signOut(token, 'global');
    if (error) {
      console.error('Logout error');
      return res.status(500).json({ error: 'Failed to logout' });
    }

    res.json({ success: true });
  } catch (error) {
    console.error('Logout error');
    res.status(500).json({ error: 'Failed to logout' });
  }
});

/**
 * POST /api/auth/register
 * Create user in Supabase Auth + profile in users table
 */
router.post('/register', authenticateToken, requireAdmin, async (req: Request, res: Response) => {
  try {
    const { email, password, role, firstName, lastName, gebietsleiter_id } = req.body;

    if (!email || !password || !role || !firstName || !lastName) {
      return res.status(400).json({ error: 'All fields are required' });
    }

    if (!isValidRole(role)) {
      return res.status(400).json({ error: 'Invalid role' });
    }

    if (role === 'gl' && !gebietsleiter_id) {
      return res.status(400).json({ error: 'gebietsleiter_id is required for GL users' });
    }

    // Create user in Supabase Auth
    const { data: authData, error: authError } = await supabase.auth.admin.createUser({
      email,
      password,
      email_confirm: true, // Auto-confirm
    });

    if (authError || !authData.user) {
      return res.status(400).json({ error: 'Failed to create user' });
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
      console.error('User profile creation failed');
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
    console.error('Registration error');
    res.status(500).json({ error: 'Internal server error' });
  }
});

// ============================================================================
// ADMIN MANAGEMENT: Get all admin accounts
// ============================================================================
router.get('/admins', authenticateToken, requireAdmin, async (req: Request, res: Response) => {
  try {
    const freshClient = createFreshClient();
    
    // Get all admin accounts from users table
    const { data: admins, error } = await freshClient
      .from('users')
      .select('id, first_name, last_name, created_at')
      .eq('role', 'admin')
      .order('created_at', { ascending: true });

    if (error) {
      console.error('Error fetching admins');
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
    console.error('Error in /admins');
    res.status(500).json({ error: 'Failed to fetch admin accounts' });
  }
});

// ============================================================================
// ADMIN MANAGEMENT: Create new admin account
// ============================================================================
router.post('/create-admin', authenticateToken, requireAdmin, async (req: Request, res: Response) => {
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
      return res.status(400).json({ error: 'Failed to create admin account' });
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
      console.error('Admin profile creation failed');
      // Try to clean up auth user if profile creation failed
      await supabase.auth.admin.deleteUser(authData.user.id);
      return res.status(500).json({ error: 'Failed to create admin profile' });
    }

    console.log('Created admin account');
    res.status(201).json({
      id: authData.user.id,
      email: authData.user.email
    });
  } catch (error) {
    console.error('Error creating admin');
    res.status(500).json({ error: 'Failed to create admin account' });
  }
});

// ============================================================================
// ADMIN MANAGEMENT: Delete admin account
// ============================================================================
router.delete('/admin/:id', authenticateToken, requireAdmin, async (req: AuthRequest, res: Response) => {
  try {
    const { id } = req.params;
    const requesterId = req.user?.id;

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

    // Delete from Supabase Auth, then explicitly remove the profile row so any
    // still-valid access token cannot pass route-level role checks.
    const { error: authError } = await supabase.auth.admin.deleteUser(id);

    if (authError) {
      console.error('Error deleting admin from auth');
      throw authError;
    }

    const { error: profileDeleteError } = await freshClient
      .from('users')
      .delete()
      .eq('id', id);

    if (profileDeleteError) {
      console.error('Error deleting admin profile');
      throw profileDeleteError;
    }

    console.log('Deleted admin account');
    res.json({ success: true });
  } catch (error) {
    console.error('Error deleting admin');
    res.status(500).json({ error: 'Failed to delete admin account' });
  }
});

// ============================================================================
// ADMIN MANAGEMENT: Change password
// ============================================================================
router.put('/change-password', authenticateToken, requireSelfOrAdmin(req => req.body.userId), async (req: AuthRequest, res: Response) => {
  try {
    const { userId, currentPassword, newPassword } = req.body;
    const targetUserId = req.user?.role === 'admin' ? userId : req.user?.id;

    if (!targetUserId || !currentPassword || !newPassword) {
      return res.status(400).json({ error: 'All fields are required' });
    }

    if (newPassword.length < 8) {
      return res.status(400).json({ error: 'New password must be at least 8 characters' });
    }

    // Get user email from Supabase Auth (email is stored in auth, not users table)
    const { data: authUser, error: authError } = await supabase.auth.admin.getUserById(targetUserId);

    if (authError || !authUser?.user?.email) {
      console.error('Error fetching user from auth');
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
    const { error: updateError } = await supabase.auth.admin.updateUserById(targetUserId, {
      password: newPassword
    });

    if (updateError) {
      console.error('Error updating credential');
      throw updateError;
    }

    console.log('Credential changed');
    res.json({ success: true });
  } catch (error) {
    console.error('Error changing credential');
    res.status(500).json({ error: 'Failed to change password' });
  }
});

export default router;
