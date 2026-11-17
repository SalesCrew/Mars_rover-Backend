export const CUSTOMER_PAGES = [
  { key: 'dashboard', label: 'Dashboard' },
  { key: 'markets', label: 'Märkte' },
  { key: 'gebietsleiter', label: 'Gebietsleiter' },
  { key: 'vorbesteller', label: 'Vorbesteller' },
  { key: 'vorverkauf', label: 'Vorverkauf' },
  { key: 'produktersatz', label: 'Produktersatz' },
  { key: 'nara-incentive', label: 'NaRa Incentive' },
  { key: 'fragebogen', label: 'Fragebogen' },
  { key: 'zeiterfassung', label: 'Zeiterfassung' },
  { key: 'fotos', label: 'Fotos' },
  { key: 'produkte', label: 'Produkte' },
] as const;
export type CustomerPage = (typeof CUSTOMER_PAGES)[number]['key'];
export type CustomerPermissions = Partial<
  Record<CustomerPage, { view: boolean; export: boolean }>
>;
export const isCustomerPage = (key: string): key is CustomerPage =>
  CUSTOMER_PAGES.some((p) => p.key === key);
export function parseCustomerPermissions(input: unknown): CustomerPermissions {
  if (!input || typeof input !== 'object' || Array.isArray(input))
    throw new Error('Ungültige Seitenrechte.');
  const result: CustomerPermissions = {};
  for (const [key, raw] of Object.entries(input)) {
    if (
      !isCustomerPage(key) ||
      !raw ||
      typeof raw !== 'object' ||
      Array.isArray(raw)
    )
      throw new Error('Ungültige Seitenrechte.');
    const value = raw as Record<string, unknown>;
    if (
      Object.keys(value).some((k) => k !== 'view' && k !== 'export') ||
      typeof value.view !== 'boolean' ||
      typeof value.export !== 'boolean' ||
      (value.export && !value.view)
    )
      throw new Error('Export benötigt Ansichtsrechte.');
    if (value.view) result[key] = { view: true, export: value.export };
  }
  return result;
}
