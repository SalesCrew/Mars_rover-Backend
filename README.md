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
```

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
| `SUPABASE_SERVICE_KEY` | Your Supabase service role key |



