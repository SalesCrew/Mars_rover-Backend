export interface WellenPhotoMarketRow {
  id: string;
  name: string | null;
  chain: string | null;
  address: string | null;
  postal_code: string | null;
  city: string | null;
}

export interface WellenPhotoMarketSummary {
  marketId: string;
  marketName: string;
  marketChain: string;
  marketAddress: string;
  marketPostalCode: string;
  marketCity: string;
  photoCount: number;
  lastUploadedAt: string;
}

export function summarizeWellenPhotosByMarket(
  photos: Array<{ market_id: string | null; created_at: string }>,
  markets: WellenPhotoMarketRow[]
): WellenPhotoMarketSummary[] {
  const marketById = new Map(markets.map(market => [market.id, market]));
  const summaries = new Map<string, WellenPhotoMarketSummary>();

  for (const photo of photos) {
    if (!photo.market_id) continue;
    const existing = summaries.get(photo.market_id);
    if (existing) {
      existing.photoCount++;
      if (photo.created_at > existing.lastUploadedAt) existing.lastUploadedAt = photo.created_at;
      continue;
    }

    const market = marketById.get(photo.market_id);
    summaries.set(photo.market_id, {
      marketId: photo.market_id,
      marketName: market?.name || 'Unbekannter Markt',
      marketChain: market?.chain || '',
      marketAddress: market?.address || '',
      marketPostalCode: market?.postal_code || '',
      marketCity: market?.city || '',
      photoCount: 1,
      lastUploadedAt: photo.created_at
    });
  }

  return [...summaries.values()].sort((a, b) =>
    b.lastUploadedAt.localeCompare(a.lastUploadedAt) || a.marketName.localeCompare(b.marketName, 'de')
  );
}
