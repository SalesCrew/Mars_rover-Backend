import assert from 'node:assert/strict';
import test from 'node:test';
import photoHistory from '../src/utils/wellenPhotoHistory.ts';

const { summarizeWellenPhotosByMarket } = photoHistory;

test('groups photos by market and keeps the latest upload date', () => {
  const photos = [
    { market_id: 'market-a', created_at: '2026-09-15T10:00:00Z' },
    { market_id: 'market-b', created_at: '2026-09-16T09:00:00Z' },
    { market_id: 'market-a', created_at: '2026-09-17T08:00:00Z' },
  ];
  const markets = [
    { id: 'market-a', name: 'Adeg A', chain: 'ADEG', address: 'Hauptstrasse 1', postal_code: '1010', city: 'Wien' },
    { id: 'market-b', name: 'Billa B', chain: 'BILLA', address: 'Ring 2', postal_code: '4020', city: 'Linz' },
  ];

  assert.deepEqual(summarizeWellenPhotosByMarket(photos, markets), [
    { marketId: 'market-a', marketName: 'Adeg A', marketChain: 'ADEG', marketAddress: 'Hauptstrasse 1', marketPostalCode: '1010', marketCity: 'Wien', photoCount: 2, lastUploadedAt: '2026-09-17T08:00:00Z' },
    { marketId: 'market-b', marketName: 'Billa B', marketChain: 'BILLA', marketAddress: 'Ring 2', marketPostalCode: '4020', marketCity: 'Linz', photoCount: 1, lastUploadedAt: '2026-09-16T09:00:00Z' },
  ]);
});

test('keeps distinct market IDs even when names match', () => {
  const photos = [
    { market_id: 'market-a', created_at: '2026-09-17T10:00:00Z' },
    { market_id: 'market-b', created_at: '2026-09-17T09:00:00Z' },
  ];
  const markets = [
    { id: 'market-a', name: 'BILLA Plus', chain: 'BILLA Plus', address: 'A 1', postal_code: '1010', city: 'Wien' },
    { id: 'market-b', name: 'BILLA Plus', chain: 'BILLA Plus', address: 'B 2', postal_code: '4020', city: 'Linz' },
  ];
  assert.equal(summarizeWellenPhotosByMarket(photos, markets).length, 2);
});

test('retains photo counts if market metadata is unavailable', () => {
  assert.deepEqual(summarizeWellenPhotosByMarket([
    { market_id: 'deleted-market', created_at: '2026-09-17T10:00:00Z' },
    { market_id: null, created_at: '2026-09-17T09:00:00Z' },
  ], []), [
    { marketId: 'deleted-market', marketName: 'Unbekannter Markt', marketChain: '', marketAddress: '', marketPostalCode: '', marketCity: '', photoCount: 1, lastUploadedAt: '2026-09-17T10:00:00Z' },
  ]);
});
