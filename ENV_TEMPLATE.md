# Backend Environment Variables Template

## Required Variables

Copy this to `backend/.env` and fill in your actual values:

```env
# Supabase Configuration
# Get these from: https://app.supabase.com/project/YOUR_PROJECT/settings/api
# SUPABASE_SERVICE_KEY is server-only. Never expose it in frontend env vars.
# Legacy/common deployments may use SUPABASE_SERVICE_ROLE_KEY instead.
# Use a Supabase service_role JWT or secret key, not an anon/publishable key.
SUPABASE_URL=https://your-project-ref.supabase.co
SUPABASE_SERVICE_KEY=your-service-role-key-here
# SUPABASE_SERVICE_ROLE_KEY=your-service-role-key-here

# Browser origins allowed to call the API.
# Use a comma-separated list for multiple production/preview frontends.
CORS_ORIGINS=https://mars-rover-mu.vercel.app
FRONTEND_URL=https://mars-rover-mu.vercel.app
# Localhost CORS is enabled automatically outside NODE_ENV=production.
# Keep this unset/false in production unless you explicitly need local browser access.
# Only set this to true temporarily for a controlled production debugging window.
ALLOW_LOCAL_CORS=false

# Server Port (default: 3001)
PORT=3001
```

## How to Get Supabase Keys

1. Go to your Supabase project
2. Click **Settings** (gear icon in sidebar)
3. Click **API**
4. Copy:
   - **Project URL** -> `SUPABASE_URL`
   - **service_role key** (secret) -> `SUPABASE_SERVICE_KEY` or the legacy/common alias `SUPABASE_SERVICE_ROLE_KEY`

Never commit the `.env` file to git. It is already in `.gitignore`.

The backend fails startup if `SUPABASE_URL` is missing, if neither `SUPABASE_SERVICE_KEY` nor `SUPABASE_SERVICE_ROLE_KEY` is configured, or if the configured key looks like an anon/publishable key. This is intentional because the API performs route-level authorization before using the server-side service-role client.
