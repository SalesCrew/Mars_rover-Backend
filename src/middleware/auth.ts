import { Request, Response, NextFunction } from 'express';
import { createFreshClient, supabase } from '../config/supabase';

export interface AuthUser {
  id: string;
  username: string;
  email: string;
  role: 'gl' | 'admin';
  firstName: string;
  lastName: string;
  gebietsleiter_id?: string;
}

export interface AuthRequest extends Request {
  user?: AuthUser;
}

const normalizeProfile = (authUser: { id: string; email?: string | null }, profile: any): AuthUser => ({
  id: authUser.id,
  username: authUser.email || '',
  email: authUser.email || '',
  role: profile.role,
  firstName: profile.first_name,
  lastName: profile.last_name,
  gebietsleiter_id: profile.gebietsleiter_id || undefined,
});

export const getAuthenticatedGlId = (user?: AuthUser): string | undefined =>
  user?.gebietsleiter_id || user?.id;

const isActiveGlProfile = async (glId: string): Promise<boolean> => {
  const freshClient = createFreshClient();
  const { data, error } = await freshClient
    .from('gebietsleiter')
    .select('id, is_active')
    .eq('id', glId)
    .maybeSingle();

  if (error || !data) {
    return false;
  }

  return data.is_active !== false;
};

/**
 * Middleware to verify JWT token and attach user to request
 */
export const authenticateToken = async (req: AuthRequest, res: Response, next: NextFunction) => {
  if (req.method === 'OPTIONS') {
    return next();
  }

  const authHeader = req.headers['authorization'];
  const authParts = typeof authHeader === 'string' ? authHeader.trim().split(/\s+/) : [];
  const token = authParts.length === 2 && authParts[0] === 'Bearer' ? authParts[1] : undefined;

  if (!token) {
    return res.status(401).json({ error: 'Authentication token required' });
  }

  try {
    const { data: authData, error: authError } = await supabase.auth.getUser(token);

    if (authError || !authData.user) {
      return res.status(403).json({ error: 'Invalid or expired token' });
    }

    const freshClient = createFreshClient();
    const { data: profile, error: profileError } = await freshClient
      .from('users')
      .select('id, role, first_name, last_name, gebietsleiter_id')
      .eq('id', authData.user.id)
      .single();

    if (profileError || !profile) {
      return res.status(403).json({ error: 'User profile not found' });
    }

    if (profile.role === 'gl') {
      const glId = profile.gebietsleiter_id || profile.id;
      if (!glId || !(await isActiveGlProfile(glId))) {
        return res.status(403).json({ error: 'User account is inactive' });
      }
    }

    req.user = normalizeProfile(authData.user, profile);
    return next();
  } catch (error) {
    return res.status(403).json({ error: 'Invalid or expired token' });
  }
};

/**
 * Middleware to check if user is admin
 */
export const requireAdmin = (req: AuthRequest, res: Response, next: NextFunction) => {
  if (!req.user) {
    return res.status(401).json({ error: 'Authentication required' });
  }

  if (req.user.role !== 'admin') {
    return res.status(403).json({ error: 'Admin access required' });
  }

  next();
};

/**
 * Middleware to check if user is GL
 */
export const requireGL = (req: AuthRequest, res: Response, next: NextFunction) => {
  if (!req.user) {
    return res.status(401).json({ error: 'Authentication required' });
  }

  if (req.user.role !== 'gl') {
    return res.status(403).json({ error: 'Gebietsleiter access required' });
  }

  next();
};
export const requireSelfOrAdmin = (getUserId: (req: AuthRequest) => string | undefined) => {
  return (req: AuthRequest, res: Response, next: NextFunction) => {
    if (!req.user) {
      return res.status(401).json({ error: 'Authentication required' });
    }

    if (req.user.role === 'admin') {
      return next();
    }

    const requestedUserId = getUserId(req);
    const authenticatedGlId = getAuthenticatedGlId(req.user);
    if (!requestedUserId || (requestedUserId !== req.user.id && requestedUserId !== authenticatedGlId)) {
      return res.status(403).json({ error: 'Access denied' });
    }

    return next();
  };
};
export const requireOwnedRowOrAdmin = (
  tableName: string,
  ownerColumn = 'gebietsleiter_id',
  idParam = 'id'
) => {
  return async (req: AuthRequest, res: Response, next: NextFunction) => {
    if (!req.user) {
      return res.status(401).json({ error: 'Authentication required' });
    }

    if (req.user.role === 'admin') {
      return next();
    }

    const rowId = req.params[idParam];
    if (!rowId) {
      return res.status(400).json({ error: 'Missing row id' });
    }

    try {
      const freshClient = createFreshClient();
      const { data, error } = await freshClient
        .from(tableName)
        .select(ownerColumn)
        .eq('id', rowId)
        .maybeSingle();

      if (error) {
        throw error;
      }

      if (!data) {
        return res.status(404).json({ error: 'Record not found' });
      }

      if (String((data as Record<string, any>)[ownerColumn]) !== getAuthenticatedGlId(req.user)) {
        return res.status(403).json({ error: 'Access denied' });
      }

      return next();
    } catch (error) {
      return res.status(500).json({ error: 'Authorization check failed' });
    }
  };
};
