import assert from 'node:assert/strict';
import test from 'node:test';
import normalization from '../src/utils/marketChainNormalization.ts';

const { normalizeMarketChain, ZOOFACHHANDEL_CHAINS } = normalization;

test('normalizes all Wau,Miau separators in market write payloads', () => {
  assert.equal(normalizeMarketChain('Wau,Miau'), 'Wau,Miau');
  assert.equal(normalizeMarketChain('Wau Miau'), 'Wau,Miau');
  assert.equal(normalizeMarketChain('WAU-MIAU'), 'Wau,Miau');
  assert.equal(normalizeMarketChain('  waumiau  '), 'Wau,Miau');
  assert.equal(normalizeMarketChain('Wau & Miau'), 'Wau,Miau');
  assert.equal(normalizeMarketChain('Wau Miau & Co'), 'Wau,Miau');
});

test('keeps Lezanimo canonical and preserves unrelated chains', () => {
  assert.equal(normalizeMarketChain('LEZANIMO'), 'Lezanimo');
  assert.equal(normalizeMarketChain('Spar Gourmet'), 'Spar Gourmet');
});

test('includes both new chains in Zoofachhandel reporting', () => {
  assert.ok(ZOOFACHHANDEL_CHAINS.includes('Lezanimo'));
  assert.ok(ZOOFACHHANDEL_CHAINS.includes('Wau,Miau'));
});
