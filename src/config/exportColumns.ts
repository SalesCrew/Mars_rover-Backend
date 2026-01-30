// Export column definitions for all datasets
// Defines available columns, their labels, and data types for Excel formatting

export interface ColumnDefinition {
  id: string;
  label: string;
  type: 'string' | 'number' | 'currency' | 'datetime' | 'date' | 'boolean';
  width?: number; // Excel column width
  default?: boolean; // Include in default export
}

export interface DatasetDefinition {
  id: string;
  label: string;
  table: string;
  columns: ColumnDefinition[];
  requiresJoin?: boolean; // If true, needs data from multiple tables
}

export const EXPORT_DATASETS: Record<string, DatasetDefinition> = {
  wellen_submissions: {
    id: 'wellen_submissions',
    label: 'Wellen Vorbestellungen',
    table: 'wellen_submissions',
    requiresJoin: true,
    columns: [
      { id: 'created_at', label: 'Datum', type: 'datetime', width: 18, default: true },
      { id: 'welle_name', label: 'Welle', type: 'string', width: 25, default: true },
      { id: 'gl_name', label: 'Gebietsleiter', type: 'string', width: 20, default: true },
      { id: 'gl_email', label: 'GL Email', type: 'string', width: 25, default: false },
      { id: 'market_name', label: 'Markt', type: 'string', width: 30, default: true },
      { id: 'market_chain', label: 'Kette', type: 'string', width: 15, default: true },
      { id: 'market_address', label: 'Adresse', type: 'string', width: 30, default: true },
      { id: 'market_postal_code', label: 'PLZ', type: 'string', width: 10, default: true },
      { id: 'market_city', label: 'Stadt', type: 'string', width: 15, default: true },
      { id: 'market_id', label: 'Markt ID', type: 'string', width: 15, default: false },
      { id: 'item_type', label: 'Typ', type: 'string', width: 15, default: true },
      { id: 'item_name', label: 'Artikel', type: 'string', width: 35, default: true },
      { id: 'container_name', label: 'Palette/Schütte', type: 'string', width: 25, default: true },
      { id: 'quantity', label: 'Menge', type: 'number', width: 12, default: true },
      { id: 'value_per_unit', label: 'Wert/Einheit', type: 'currency', width: 14, default: true },
      { id: 'total_value', label: 'Gesamtwert', type: 'currency', width: 14, default: true },
      { id: 'photo_url', label: 'Foto URL', type: 'string', width: 40, default: false },
      { id: 'delivery_photo_url', label: 'Lieferung Foto URL', type: 'string', width: 40, default: false },
      { id: 'submission_id', label: 'Submission ID', type: 'string', width: 36, default: false }
    ]
  },

  markets: {
    id: 'markets',
    label: 'Marktbesuche',
    table: 'markets',
    requiresJoin: false,
    columns: [
      { id: 'id', label: 'ID', type: 'string', width: 15, default: true },
      { id: 'internal_id', label: 'Interne ID', type: 'string', width: 15, default: true },
      { id: 'name', label: 'Name', type: 'string', width: 30, default: true },
      { id: 'chain', label: 'Kette', type: 'string', width: 15, default: true },
      { id: 'address', label: 'Adresse', type: 'string', width: 30, default: true },
      { id: 'city', label: 'Stadt', type: 'string', width: 15, default: true },
      { id: 'postal_code', label: 'PLZ', type: 'string', width: 10, default: true },
      { id: 'gebietsleiter_name', label: 'Gebietsleiter', type: 'string', width: 20, default: true },
      { id: 'gebietsleiter_email', label: 'GL Email', type: 'string', width: 25, default: false },
      { id: 'gebietsleiter_id', label: 'GL ID', type: 'string', width: 36, default: false },
      { id: 'frequency', label: 'Frequenz', type: 'number', width: 12, default: true },
      { id: 'current_visits', label: 'Aktuelle Besuche', type: 'number', width: 15, default: true },
      { id: 'last_visit_date', label: 'Letzter Besuch', type: 'date', width: 15, default: true },
      { id: 'is_active', label: 'Status', type: 'boolean', width: 12, default: true },
      { id: 'phone', label: 'Telefon', type: 'string', width: 18, default: false },
      { id: 'email', label: 'Email', type: 'string', width: 25, default: false },
      { id: 'channel', label: 'Kanal', type: 'string', width: 15, default: false },
      { id: 'banner', label: 'Banner', type: 'string', width: 15, default: false },
      { id: 'subgroup', label: 'Subgroup', type: 'string', width: 20, default: false },
      { id: 'created_at', label: 'Erstellt am', type: 'datetime', width: 18, default: false }
    ]
  },

  vorverkauf_entries: {
    id: 'vorverkauf_entries',
    label: 'Vorverkauf Einträge',
    table: 'vorverkauf_entries',
    requiresJoin: true,
    columns: [
      { id: 'created_at', label: 'Datum', type: 'datetime', width: 18, default: true },
      { id: 'id', label: 'Entry ID', type: 'string', width: 36, default: false },
      { id: 'gl_name', label: 'Gebietsleiter', type: 'string', width: 20, default: true },
      { id: 'gl_email', label: 'GL Email', type: 'string', width: 25, default: false },
      { id: 'market_name', label: 'Markt', type: 'string', width: 30, default: true },
      { id: 'market_chain', label: 'Kette', type: 'string', width: 15, default: true },
      { id: 'market_address', label: 'Adresse', type: 'string', width: 30, default: true },
      { id: 'market_postal_code', label: 'PLZ', type: 'string', width: 10, default: true },
      { id: 'market_city', label: 'Stadt', type: 'string', width: 15, default: true },
      { id: 'reason', label: 'Grund', type: 'string', width: 20, default: true },
      { id: 'status', label: 'Status', type: 'string', width: 12, default: true },
      { id: 'notes', label: 'Notizen', type: 'string', width: 40, default: true },
      { id: 'products_summary', label: 'Produkte', type: 'string', width: 50, default: true },
      { id: 'products_json', label: 'Produkte (JSON)', type: 'string', width: 60, default: false }
    ]
  },

  action_history: {
    id: 'action_history',
    label: 'Aktionsverlauf',
    table: 'action_history',
    requiresJoin: false,
    columns: [
      { id: 'timestamp', label: 'Zeitstempel', type: 'datetime', width: 18, default: true },
      { id: 'action_type', label: 'Aktionstyp', type: 'string', width: 15, default: true },
      { id: 'market_id', label: 'Markt ID', type: 'string', width: 15, default: false },
      { id: 'market_chain', label: 'Kette', type: 'string', width: 15, default: true },
      { id: 'market_address', label: 'Adresse', type: 'string', width: 30, default: true },
      { id: 'market_postal_code', label: 'PLZ', type: 'string', width: 10, default: true },
      { id: 'market_city', label: 'Stadt', type: 'string', width: 15, default: true },
      { id: 'target_gl', label: 'Ziel GL', type: 'string', width: 20, default: true },
      { id: 'previous_gl', label: 'Vorheriger GL', type: 'string', width: 20, default: true },
      { id: 'performed_by', label: 'Durchgeführt von', type: 'string', width: 20, default: false },
      { id: 'notes', label: 'Notizen', type: 'string', width: 40, default: false }
    ]
  },

  gebietsleiter: {
    id: 'gebietsleiter',
    label: 'Gebietsleiter',
    table: 'gebietsleiter',
    requiresJoin: true,
    columns: [
      { id: 'id', label: 'ID', type: 'string', width: 36, default: false },
      { id: 'name', label: 'Name', type: 'string', width: 25, default: true },
      { id: 'email', label: 'Email', type: 'string', width: 30, default: true },
      { id: 'phone', label: 'Telefon', type: 'string', width: 18, default: true },
      { id: 'address', label: 'Adresse', type: 'string', width: 30, default: true },
      { id: 'city', label: 'Stadt', type: 'string', width: 15, default: true },
      { id: 'postal_code', label: 'PLZ', type: 'string', width: 10, default: true },
      { id: 'is_active', label: 'Status', type: 'boolean', width: 12, default: true },
      { id: 'total_visits', label: 'Gesamtbesuche', type: 'number', width: 15, default: true },
      { id: 'display_count', label: 'Displays', type: 'number', width: 12, default: true },
      { id: 'kartonware_count', label: 'Kartonware', type: 'number', width: 12, default: true },
      { id: 'paletten_value', label: 'Paletten Wert', type: 'currency', width: 16, default: true },
      { id: 'schuetten_value', label: 'Schütten Wert', type: 'currency', width: 16, default: true },
      { id: 'created_at', label: 'Erstellt am', type: 'datetime', width: 18, default: false },
      { id: 'profile_picture_url', label: 'Profilbild URL', type: 'string', width: 40, default: false }
    ]
  }
};

// Get default columns for a dataset
export const getDefaultColumns = (datasetId: string): string[] => {
  const dataset = EXPORT_DATASETS[datasetId];
  if (!dataset) return [];
  return dataset.columns.filter(col => col.default).map(col => col.id);
};

// Get all column IDs for a dataset
export const getAllColumns = (datasetId: string): string[] => {
  const dataset = EXPORT_DATASETS[datasetId];
  if (!dataset) return [];
  return dataset.columns.map(col => col.id);
};

// Get column definition
export const getColumnDef = (datasetId: string, columnId: string): ColumnDefinition | undefined => {
  const dataset = EXPORT_DATASETS[datasetId];
  if (!dataset) return undefined;
  return dataset.columns.find(col => col.id === columnId);
};
