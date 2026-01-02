import dotenv from 'dotenv';
import { createClient } from '@supabase/supabase-js';
import path from 'path';
import { fileURLToPath } from 'url';

// Load environment variables from backend/.env
dotenv.config({ path: path.resolve(process.cwd(), '.env') });

console.log('🔧 Environment check:', {
  url: process.env.SUPABASE_URL ? 'Loaded ✓' : 'Missing ✗',
  key: process.env.SUPABASE_SERVICE_KEY ? 'Loaded ✓' : 'Missing ✗',
  actualUrl: process.env.SUPABASE_URL
});

const supabaseUrl = process.env.SUPABASE_URL || '';
const supabaseServiceKey = process.env.SUPABASE_SERVICE_KEY || '';

if (!supabaseUrl || !supabaseServiceKey) {
  console.error('⚠️ Supabase credentials not configured!');
  console.error('Please set SUPABASE_URL and SUPABASE_SERVICE_KEY in backend/.env file');
  console.error('Current values:', { 
    url: supabaseUrl ? 'Set' : 'Missing', 
    key: supabaseServiceKey ? 'Set' : 'Missing' 
  });
}

export const supabase = createClient(supabaseUrl, supabaseServiceKey);

