# Backend Environment Variables Template

## Required Variables

Copy this to `backend/.env` and fill in your actual values:

```env
# Supabase Configuration
# Get these from: https://app.supabase.com/project/YOUR_PROJECT/settings/api
SUPABASE_URL=https://your-project-ref.supabase.co
SUPABASE_SERVICE_ROLE_KEY=your-service-role-key-here

# JWT Secret for authentication
# IMPORTANT: Change this to a strong random secret in production!
JWT_SECRET=your-super-secret-jwt-key-change-in-production

# Server Port (default: 3001)
PORT=3001
```

## How to Get Supabase Keys:

1. Go to your Supabase project
2. Click **Settings** (gear icon in sidebar)
3. Click **API**
4. Copy:
   - **Project URL** → `SUPABASE_URL`
   - **service_role key** (secret) → `SUPABASE_SERVICE_ROLE_KEY`

⚠️ **NEVER commit the `.env` file to git!** (It's already in `.gitignore`)
