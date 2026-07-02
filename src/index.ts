import dotenv from 'dotenv';
import path from 'path';
dotenv.config({ path: path.resolve(process.cwd(), '.env') });

import express from 'express';
import cors from 'cors';
import authRouter from './routes/auth';
import marketsRouter from './routes/markets';
import actionHistoryRouter from './routes/actionHistory';
import gebietsleiterRouter from './routes/gebietsleiter';
import productsRouter from './routes/products';
import wellenRouter from './routes/wellen';
import vorverkaufRouter from './routes/vorverkauf';
import vorverkaufWellenRouter from './routes/vorverkaufWellen';
import activitiesRouter from './routes/activities';
import bugReportsRouter from './routes/bugReports';
import exportRouter from './routes/export';
import fragebogenRouter from './routes/fragebogen';
import naraIncentiveRouter from './routes/naraIncentive';
import mapsRouter from './routes/maps';
import wochenCheckRouter from './routes/wochenCheck';
import chatRouter from './routes/chat';
import productsUpdateRouter, { startProductUpdateScheduler } from './routes/productsUpdate';
import { authenticateToken, requireAdmin } from './middleware/auth';

const app = express();
app.set('trust proxy', 1);
const PORT = parseInt(process.env.PORT || '3001', 10);
const isProduction = process.env.NODE_ENV === 'production';
const allowLocalCors = !isProduction || process.env.ALLOW_LOCAL_CORS === 'true';
const localCorsOrigins = allowLocalCors ? [
  'http://localhost:3000',
  'http://localhost:5173',
  'http://127.0.0.1:3000',
  'http://127.0.0.1:5173',
] : [];
const configuredCorsOrigins = [
  ...(process.env.CORS_ORIGINS || '').split(','),
  process.env.FRONTEND_URL || '',
  'https://mars-rover-mu.vercel.app',
  ...localCorsOrigins,
].map(origin => origin.trim().replace(/\/+$/, '')).filter(Boolean);
const allowedCorsOrigins = Array.from(new Set(configuredCorsOrigins));
const sanitizeLoggedPath = (value: string): string =>
  value
    .replace(/[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}/gi, ':uuid')
    .replace(/\/\d{6,}(?=\/|$)/g, '/:number');

// Middleware
app.disable('x-powered-by');
app.use((_, res, next) => {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('Referrer-Policy', 'no-referrer');
  res.setHeader('X-Frame-Options', 'DENY');
  res.setHeader('Cache-Control', 'no-store');
  res.setHeader('Pragma', 'no-cache');
  res.setHeader('Expires', '0');
  next();
});
app.use(cors({
  origin(origin, callback) {
    if (!origin) {
      return callback(null, true);
    }

    const normalizedOrigin = origin.replace(/\/+$/, '');
    if (allowedCorsOrigins.includes(normalizedOrigin)) {
      return callback(null, true);
    }

    return callback(new Error('Not allowed by CORS'));
  },
  allowedHeaders: ['Authorization', 'Content-Type'],
  methods: ['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS'],
  exposedHeaders: ['Content-Disposition'],
}));
app.use(express.json({ limit: '50mb' }));

// Health checks FIRST (before any other routes) - Railway checks these
app.get('/', (_req, res) => {
  res.json({ 
    status: 'ok', 
    service: 'Mars Rover Backend API',
    timestamp: new Date().toISOString() 
  });
});

app.get('/health', (_req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

app.get('/api/health', (_req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});


// Request logging for API routes
app.use((req, _res, next) => {
  const now = new Date().toISOString();
  console.log('Request: ' + now + ' | ' + req.method + ' ' + sanitizeLoggedPath(req.path));
  next();
});

// API Routes
console.log('Registering auth routes...');
app.use('/api/auth', authRouter);

// All business-data routes below use a service-role Supabase client, so the
// Express layer must verify the caller before any route can touch data.
app.use('/api', authenticateToken);

console.log('Registering markets routes...');
app.use('/api/markets', marketsRouter);
console.log('Registering action-history routes...');
app.use('/api/action-history', requireAdmin, actionHistoryRouter);
console.log('Registering gebietsleiter routes...');
app.use('/api/gebietsleiter', gebietsleiterRouter);
console.log('Registering products routes...');
app.use('/api/products', productsRouter);
console.log('Registering wellen routes...');
app.use('/api/wellen', wellenRouter);
console.log('Registering vorverkauf routes...');
app.use('/api/vorverkauf', vorverkaufRouter);
console.log('Registering vorverkauf-wellen routes...');
app.use('/api/vorverkauf-wellen', vorverkaufWellenRouter);
console.log('Registering activities routes...');
app.use('/api/activities', requireAdmin, activitiesRouter);
console.log('Registering bug-reports routes...');
app.use('/api/bug-reports', bugReportsRouter);
console.log('Registering export routes...');
app.use('/api/export', requireAdmin, exportRouter);
console.log('Registering fragebogen routes...');
app.use('/api/fragebogen', fragebogenRouter);
console.log('Registering nara-incentive routes...');
app.use('/api/nara-incentive', naraIncentiveRouter);
console.log('Registering maps routes...');
app.use('/api/maps', mapsRouter);
console.log('Registering wochen-check routes...');
app.use('/api/wochen-check', wochenCheckRouter);
console.log('Registering chat routes...');
app.use('/api/chat', chatRouter);
console.log('Registering products-update routes...');
app.use('/api/products-update', requireAdmin, productsUpdateRouter);

// Start server
const server = app.listen(PORT, '0.0.0.0', () => {
  console.log('Backend server running on port ' + PORT);
  startProductUpdateScheduler();
});

// Graceful shutdown handling
process.on('SIGTERM', () => {
  console.log('SIGTERM received, shutting down gracefully...');
  server.close(() => {
    console.log('Server closed');
    process.exit(0);
  });
});

process.on('SIGINT', () => {
  console.log('SIGINT received, shutting down gracefully...');
  server.close(() => {
    console.log('Server closed');
    process.exit(0);
  });
});
