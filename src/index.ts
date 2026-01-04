import express from 'express';
import cors from 'cors';
import authRouter from './routes/auth';
import marketsRouter from './routes/markets';
import actionHistoryRouter from './routes/actionHistory';
import gebietsleiterRouter from './routes/gebietsleiter';
import productsRouter from './routes/products';
import wellenRouter from './routes/wellen';
import vorverkaufRouter from './routes/vorverkauf';

const app = express();
const PORT = parseInt(process.env.PORT || '3001', 10);

// Middleware
app.use(cors());
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
  console.log('Request: ' + now + ' | ' + req.method + ' ' + req.url);
  next();
});

// API Routes
console.log('Registering auth routes...');
app.use('/api/auth', authRouter);
console.log('Registering markets routes...');
app.use('/api/markets', marketsRouter);
console.log('Registering action-history routes...');
app.use('/api/action-history', actionHistoryRouter);
console.log('Registering gebietsleiter routes...');
app.use('/api/gebietsleiter', gebietsleiterRouter);
console.log('Registering products routes...');
app.use('/api/products', productsRouter);
console.log('Registering wellen routes...');
app.use('/api/wellen', wellenRouter);
console.log('Registering vorverkauf routes...');
app.use('/api/vorverkauf', vorverkaufRouter);

// Start server
const server = app.listen(PORT, '0.0.0.0', () => {
  console.log('Backend server running on port ' + PORT);
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
