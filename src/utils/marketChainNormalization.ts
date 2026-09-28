const CANONICAL_CHAINS: Record<string, string> = {
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

export const normalizeMarketChain = (value: unknown): unknown => {
  if (typeof value !== 'string') return value;
  const trimmed = value.trim();
  const key = trimmed.toLocaleLowerCase('de-DE').replace(/[^a-z0-9]+/g, '');
  return CANONICAL_CHAINS[key] || trimmed;
};
