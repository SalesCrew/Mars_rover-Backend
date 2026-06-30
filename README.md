# Mars Rover Backend

Backend API server for the Mars Rover Admin application.

## Setup

1. Install dependencies:
```bash
cd backend
npm install
```

2. Create a `.env` file with the following variables:
```
PORT=3001
SUPABASE_URL=https://your-project-id.supabase.co
SUPABASE_SERVICE_KEY=your-service-role-key-here
# SUPABASE_SERVICE_ROLE_KEY=your-service-role-key-here
CORS_ORIGINS=https://mars-rover-mu.vercel.app
FRONTEND_URL=https://mars-rover-mu.vercel.app
ALLOW_LOCAL_CORS=false
```

`SUPABASE_SERVICE_KEY` is server-only and must never be exposed to the frontend. The backend also accepts the legacy/common alias `SUPABASE_SERVICE_ROLE_KEY` for existing deployments. Use a Supabase service_role JWT or secret key, not an anon/publishable key. The backend fails startup when `SUPABASE_URL` is missing, when neither service-role variable is configured, or when the key looks like an anon/publishable key, because API route authorization is enforced before the server-side service-role client touches Supabase.

Localhost CORS origins are enabled automatically outside `NODE_ENV=production`. In production, keep `ALLOW_LOCAL_CORS` unset or `false` unless temporary local browser access is explicitly required.

3. Run the development server:
```bash
npm run dev
```

4. For production:
```bash
npm run build
npm start
```

## Distribution export service proxy

The endpoint `POST /api/fragebogen/fragebogen/distribution-export.xlsx` proxies workbook generation to the dedicated `Perfectstore export backend` service.

Required env vars:

```bash
PERFECTSTORE_EXPORT_BACKEND_URL=http://perfectstore-export-backend.railway.internal
PERFECTSTORE_EXPORT_TIMEOUT_MS=120000
```

## DSGVO / RLS hardening

Database hardening SQL lives in `sql/dsgvo_rls_hardening.sql`, with apply and verification notes in `sql/README_DSGVO_RLS.md`.

Do not apply it directly to production without a backup and a short maintenance window. The frontend calls the Express API, and the backend uses the Supabase service-role key, so route-level auth remains the primary live authorization control.

## API Endpoints

### Markets

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/markets` | Get all markets |
| GET | `/api/markets/:id` | Get a single market |
| POST | `/api/markets` | Create a new market |
| POST | `/api/markets/import` | Bulk import markets |
| PUT | `/api/markets/:id` | Update a market |
| DELETE | `/api/markets/:id` | Delete a market |

### Health Check

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/health` | Server health check |

## Environment Variables

| Variable | Description |
|----------|-------------|
| `PORT` | Server port (default: 3001) |
| `SUPABASE_URL` | Your Supabase project URL |
| `SUPABASE_SERVICE_KEY` | Your Supabase service role key, preferred server-only env name |
| `SUPABASE_SERVICE_ROLE_KEY` | Optional legacy/common alias for the same server-only service role key |
| `CORS_ORIGINS` | Comma-separated browser origins allowed to call the API |
| `FRONTEND_URL` | Primary frontend origin allowed to call the API |
| `ALLOW_LOCAL_CORS` | Optional local-browser CORS override for production; keep `false` by default |
