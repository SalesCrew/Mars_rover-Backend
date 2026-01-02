import express from 'express';
import cors from 'cors';
import authRouter from './routes/auth';
import marketsRouter from './routes/markets';
import actionHistoryRouter from './routes/actionHistory';
import gebietsleiterRouter from './routes/gebietsleiter';
import productsRouter from './routes/products';
import wellenRouter from './routes/wellen';

const app = express();
const PORT = process.env.PORT || 3001;

// Middleware
app.use(cors());
app.use(express.json({ limit: '5000000mb' })); // Increase limit for large Excel files

// Request logging for all routes
app.use((req, res, next) => {
  console.log(`🌐 ${new Date().toISOString()} | ${req.method} ${req.url}`);
  next();
});

// Routes
console.log('📌 Registering auth routes...');
app.use('/api/auth', authRouter);
console.log('📌 Registering markets routes...');
app.use('/api/markets', marketsRouter);
console.log('📌 Registering action-history routes...');
app.use('/api/action-history', actionHistoryRouter);
console.log('📌 Registering gebietsleiter routes...');
app.use('/api/gebietsleiter', gebietsleiterRouter);
console.log('📌 Registering products routes...');
app.use('/api/products', productsRouter);
console.log('📌 Registering wellen routes...');
app.use('/api/wellen', wellenRouter);

// Health check
app.get('/api/health', (req, res) => {
  console.log('💓 Health check');
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

// Start server
app.listen(PORT, () => {
  console.log(`🚀 Backend server running on http://localhost:${PORT}`);
  console.log(`🔐 Auth API available at http://localhost:${PORT}/api/auth`);
  console.log(`📊 Markets API available at http://localhost:${PORT}/api/markets`);
  console.log(`📜 Action History API available at http://localhost:${PORT}/api/action-history`);
  console.log(`👥 Gebietsleiter API available at http://localhost:${PORT}/api/gebietsleiter`);
  console.log(`📦 Products API available at http://localhost:${PORT}/api/products`);
  console.log(`🌊 Wellen API available at http://localhost:${PORT}/api/wellen`);
});

