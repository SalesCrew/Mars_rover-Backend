export type DistributionExportDateRange = {
  startDate: string | null;
  endDate: string | null;
};

const viennaDateFormatter = new Intl.DateTimeFormat('en-CA', {
  timeZone: 'Europe/Vienna', year: 'numeric', month: '2-digit', day: '2-digit'
});

const isDateKey = (value: string): boolean => {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) return false;
  const date = new Date(`${value}T12:00:00Z`);
  return !Number.isNaN(date.getTime()) && date.toISOString().slice(0, 10) === value;
};

export const normalizeDistributionExportDateRange = (
  start: unknown, end: unknown
): DistributionExportDateRange => {
  const parse = (value: unknown): string | null => {
    if (value === undefined || value === null || value === '') return null;
    if (typeof value !== 'string' || !isDateKey(value)) {
      throw new Error('Bitte ein gültiges Datum im Format JJJJ-MM-TT angeben.');
    }
    return value;
  };
  const startDate = parse(start);
  const endDate = parse(end);
  if (startDate && endDate && startDate > endDate) {
    throw new Error('Das Beginn-Datum darf nicht nach dem Ende-Datum liegen.');
  }
  return { startDate, endDate };
};

export const distributionViennaDateKey = (date: Date): string =>
  viennaDateFormatter.format(date);

export const isInDistributionExportDateRange = (
  timestamp: string | null, range: DistributionExportDateRange
): boolean => {
  if (!timestamp) return false;
  const date = new Date(timestamp);
  if (Number.isNaN(date.getTime())) return false;
  const key = distributionViennaDateKey(date);
  return (!range.startDate || key >= range.startDate)
    && (!range.endDate || key <= range.endDate);
};

// Extended Perfect Store questionnaires stay in their reporting quarter.
// Month/week views and the export date filter still use the completion date.
export const distributionReportingQuarter = (
  completionDate: Date,
  fragebogen?: { name?: string; start_date?: string; end_date?: string }
): { quarterKey: string; quarterLabel: string } => {
  const completionKey = distributionViennaDateKey(completionDate);
  let year = Number(completionKey.slice(0, 4));
  let quarter = Math.ceil(Number(completionKey.slice(5, 7)) / 3);
  if (/perfect\s*store/i.test(fragebogen?.name || '')) {
    const start = fragebogen?.start_date || '';
    const end = fragebogen?.end_date || '';
    const duration = (Date.parse(end) - Date.parse(start)) / 86400000;
    const hasReportingPeriod = isDateKey(start) && isDateKey(end)
      && duration >= 0 && duration <= 183;
    const namedQuarter = fragebogen?.name?.match(/\bQ([1-4])\b/i);
    if (hasReportingPeriod) {
      year = Number(start.slice(0, 4));
      quarter = Math.ceil(Number(start.slice(5, 7)) / 3);
    }
    if (namedQuarter) quarter = Number(namedQuarter[1]);
  }
  return { quarterKey: `${year}-Q${quarter}`, quarterLabel: `Q${quarter} ${year}` };
};
