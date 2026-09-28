const CANONICAL_CHAINS: Record<string, string> = {
  adeg: 'Adeg',
  billaplus: 'Billa+',
  billaplusprivat: 'BILLA Plus Privat',
  billaprivat: 'BILLA Privat',
  eurospar: 'Eurospar',
  futterhaus: 'Futterhaus',
  hagebau: 'Hagebau',
  interspar: 'Interspar',
  spar: 'Spar',
  spargourmet: 'Spar',
  zoofachhandel: 'Zoofachhandel',
  lezanimo: 'Lezanimo',
  waumiau: 'Wau,Miau',
  waumiauco: 'Wau,Miau',
};

export const ZOOFACHHANDEL_CHAINS = [
  'Zoofachhandel',
  'Futterhaus',
  'Fressnapf',
  'Das Futterhaus',
  'Lezanimo',
  'Wau,Miau',
] as const;

export function normalizeMarketChain(value: string): string;
export function normalizeMarketChain(value: unknown): unknown;
export function normalizeMarketChain(value: unknown): unknown {
  if (typeof value !== 'string') return value;
  const trimmed = value.trim();
  const key = trimmed.toLocaleLowerCase('de-DE').replace(/^billa\s*\+/, 'billaplus').replace(/[^a-z0-9]+/g, '');
  if (key.startsWith('sparprivat')) return 'Spar';
  return CANONICAL_CHAINS[key] || trimmed;
}

export const matchesMarketChainFilter = (chain: unknown, selectedChains: readonly string[]): boolean =>
  selectedChains.length === 0 || selectedChains.some(selected => normalizeMarketChain(selected) === normalizeMarketChain(chain));
