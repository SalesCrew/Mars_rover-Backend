import dotenv from 'dotenv';
import { createClient, SupabaseClient } from '@supabase/supabase-js';
import path from 'path';

// Load environment variables from backend/.env (only for local dev)
dotenv.config({ path: path.resolve(process.cwd(), '.env') });

const supabaseUrl = process.env.SUPABASE_URL;
const supabaseServiceKey =
  process.env.SUPABASE_SERVICE_KEY || process.env.SUPABASE_SERVICE_ROLE_KEY;
const missingSupabaseEnv = [
  !supabaseUrl ? 'SUPABASE_URL' : null,
  !supabaseServiceKey ? 'SUPABASE_SERVICE_KEY or SUPABASE_SERVICE_ROLE_KEY' : null
].filter(Boolean);

if (missingSupabaseEnv.length > 0) {
  throw new Error(
    `Missing required server-only Supabase environment variables: ${missingSupabaseEnv.join(', ')}`
  );
}

const decodeJwtPayload = (token: string): Record<string, any> | null => {
  const [, payload] = token.split('.');
  if (!payload) return null;

  try {
    const normalizedPayload = payload.replace(/-/g, '+').replace(/_/g, '/');
    const paddedPayload = normalizedPayload.padEnd(
      normalizedPayload.length + ((4 - (normalizedPayload.length % 4)) % 4),
      '='
    );
    return JSON.parse(Buffer.from(paddedPayload, 'base64').toString('utf8'));
  } catch {
    return null;
  }
};

const validateServerOnlySupabaseKey = (key: string): void => {
  if (key.startsWith('sb_publishable_')) {
    throw new Error(
      'SUPABASE_SERVICE_KEY or SUPABASE_SERVICE_ROLE_KEY must be a server-only Supabase secret/service-role key, not a publishable key'
    );
  }

  if (key.startsWith('sb_secret_')) {
    return;
  }

  const jwtPayload = decodeJwtPayload(key);
  if (!jwtPayload || jwtPayload.role !== 'service_role') {
    throw new Error(
      'SUPABASE_SERVICE_KEY or SUPABASE_SERVICE_ROLE_KEY must be a server-only Supabase secret/service-role key'
    );
  }
};

const resolvedSupabaseUrl = supabaseUrl as string;
const resolvedSupabaseServiceKey = supabaseServiceKey as string;

validateServerOnlySupabaseKey(resolvedSupabaseServiceKey);

// Create Supabase client with cache-busting headers
export const supabase = createClient(resolvedSupabaseUrl, resolvedSupabaseServiceKey, {
  auth: {
    autoRefreshToken: false,
    persistSession: false
  },
  global: {
    headers: {
      'Cache-Control': 'no-cache, no-store, must-revalidate',
      'Pragma': 'no-cache'
    }
  },
  db: {
    schema: 'public'
  }
});

// Function to create a fresh client for critical queries (bypasses any potential caching)
export const createFreshClient = (): SupabaseClient => {
  return createClient(resolvedSupabaseUrl, resolvedSupabaseServiceKey, {
    auth: {
      autoRefreshToken: false,
      persistSession: false
    },
    global: {
      headers: {
        'Cache-Control': 'no-cache, no-store, must-revalidate',
        'Pragma': 'no-cache',
        'X-Request-Id': `fresh_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`
      }
    },
    db: {
      schema: 'public'
    }
  });
};
