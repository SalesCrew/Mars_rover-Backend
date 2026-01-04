import dotenv from 'dotenv';
import { createClient } from '@supabase/supabase-js';
import path from 'path';

// Load environment variables from backend/.env (only for local dev)
dotenv.config({ path: path.resolve(process.cwd(), '.env') });

const supabaseUrl = process.env.SUPABASE_URL || '';
const supabaseServiceKey = process.env.SUPABASE_SERVICE_KEY || '';

console.log('🔧 Environment check:', {
  url: supabaseUrl ? 'Loaded ✓' : 'Missing ✗',
  key: supabaseServiceKey ? 'Loaded ✓' : 'Missing ✗',
  keyLength: supabaseServiceKey.length,
  keyType: supabaseServiceKey.includes('service_role') ? 'SERVICE KEY ✓' : (supabaseServiceKey.length > 200 ? 'LIKELY SERVICE KEY' : 'POSSIBLY ANON KEY ⚠️'),
  actualUrl: supabaseUrl
});

if (!supabaseUrl || !supabaseServiceKey) {
  console.error('⚠️ Supabase credentials not configured!');
}

export const supabase = createClient(supabaseUrl, supabaseServiceKey, {
  auth: {
    autoRefreshToken: false,
    persistSession: false
  }
});

