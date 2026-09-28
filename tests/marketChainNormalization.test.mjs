import assert from 'node:assert/strict';
import test from 'node:test';
import normalization from '../src/utils/marketChainNormalization.ts';

const { normalizeMarketChain, matchesMarketChainFilter, ZOOFACHHANDEL_CHAINS } = normalization;

test('normalizes all Wau,Miau separators in market write payloads', () => {
  assert.equal(normalizeMarketChain('Wau,Miau'), 'Wau,Miau');
  assert.equal(normalizeMarketChain('Wau Miau'), 'Wau,Miau');
  assert.equal(normalizeMarketChain('WAU-MIAU'), 'Wau,Miau');
  assert.equal(normalizeMarketChain('  waumiau  '), 'Wau,Miau');
  assert.equal(normalizeMarketChain('Wau & Miau'), 'Wau,Miau');
  assert.equal(normalizeMarketChain('Wau+Miau'), 'Wau,Miau');
  assert.equal(normalizeMarketChain('Wau Miau & Co'), 'Wau,Miau');
});

test('keeps Lezanimo canonical and preserves unrelated chains', () => {
  assert.equal(normalizeMarketChain('LEZANIMO'), 'Lezanimo');
  assert.equal(normalizeMarketChain('Other existing chain'), 'Other existing chain');
});

test('canonicalizes BILLA and Spar writes without inventing extra chains', () => {
  for (const chain of ['BILLA Plus', 'BILLA+', 'Billa+']) assert.equal(normalizeMarketChain(chain), 'Billa+');
  for (const chain of ['BILLA Plus Privat', 'BILLA+ Privat']) assert.equal(normalizeMarketChain(chain), 'BILLA Plus Privat');
  assert.equal(normalizeMarketChain('BILLA Privat'), 'BILLA Privat');
  for (const chain of ['Spar Gourmet', 'SPAR Privat Popovic', 'Spar Privat']) assert.equal(normalizeMarketChain(chain), 'Spar');
  assert.equal(normalizeMarketChain('Eurospar'), 'Eurospar');
  assert.equal(normalizeMarketChain(null), null);
  assert.equal(normalizeMarketChain(undefined), undefined);
});

test('distribution export chain filters include aliases and both new chains', () => {
  assert.ok(matchesMarketChainFilter('BILLA Plus', ['Billa+']));
  assert.ok(matchesMarketChainFilter('BILLA+ Privat', ['BILLA Plus Privat']));
  assert.ok(!matchesMarketChainFilter('BILLA Plus Privat', ['Billa+']));
  assert.ok(matchesMarketChainFilter('SPAR Privat Popovic', ['Spar']));
  assert.ok(matchesMarketChainFilter('Spar', ['Spar Gourmet']));
  assert.ok(matchesMarketChainFilter('Wau Miau & Co', ['Wau,Miau']));
  assert.ok(matchesMarketChainFilter(' LEZANIMO ', ['Lezanimo']));
  assert.ok(!matchesMarketChainFilter('Zoofachhandel', ['Lezanimo', 'Wau,Miau']));
  assert.ok(matchesMarketChainFilter(null, []));
});

test('includes both new chains in Zoofachhandel reporting', () => {
  assert.ok(ZOOFACHHANDEL_CHAINS.includes('Lezanimo'));
  assert.ok(ZOOFACHHANDEL_CHAINS.includes('Wau,Miau'));
});
