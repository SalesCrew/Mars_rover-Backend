import dotenv from 'dotenv';
import { createClient, SupabaseClient } from '@supabase/supabase-js';
import path from 'path';

// Load environment variables from backend/.env (only for local dev)
dotenv.config({ path: path.resolve(process.cwd(), '.env') });

const supabaseUrl = process.env.SUPABASE_URL || '';
const supabaseServiceKey = process.env.SUPABASE_SERVICE_KEY || '';

if (!supabaseUrl || !supabaseServiceKey) {
  console.error('⚠️ Supabase credentials not configured!');
}

// Create Supabase client with cache-busting headers
export const supabase = createClient(supabaseUrl, supabaseServiceKey, {
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
  return createClient(supabaseUrl, supabaseServiceKey, {
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

