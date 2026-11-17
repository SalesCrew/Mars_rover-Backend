import { Response, NextFunction } from 'express';
import type { AuthRequest } from './auth';
import type { CustomerPage } from '../utils/customerAccess';

type Rule = {
  method: 'GET' | 'POST';
  path: RegExp;
  pages: CustomerPage[];
  action: 'view' | 'export';
};
const rules: Rule[] = [];
const allow = (
  path: RegExp,
  pages: CustomerPage[],
  action: 'view' | 'export' = 'view',
  method: 'GET' | 'POST' = 'GET'
) => rules.push({ path, pages, action, method });
// Explicit read operations used by the existing admin pages. Unknown routes,
// including new routes added later, remain closed to customers by default.
allow(/^\/action-history\/?$/, ['markets']);
allow(/^\/markets\/?$/, ['markets', 'gebietsleiter', 'fragebogen']);
allow(/^\/markets\/admin-comments$/, ['markets']);
allow(/^\/markets\/[^/]+(?:\/(?:admin-comment|history|visit-crm))?$/, [
  'markets',
]);
allow(/^\/gebietsleiter\/?$/, [
  'dashboard',
  'markets',
  'gebietsleiter',
  'vorbesteller',
  'produktersatz',
  'nara-incentive',
  'fragebogen',
  'zeiterfassung',
]);
allow(/^\/gebietsleiter\/[^/]+(?:\/(?:dashboard-stats|profile-stats))?$/, [
  'gebietsleiter',
]);
allow(/^\/products(?:\/[^/]+)?\/?$/, ['produkte']);
allow(/^\/activities\/?$/, ['dashboard']);
allow(/^\/wellen\/dashboard\/(?:waves|chain-averages)$/, ['dashboard']);
allow(/^\/wellen\/?$/, ['dashboard', 'vorbesteller']);
allow(/^\/wellen\/gl\/[^/]+\/chain-performance$/, ['gebietsleiter']);
allow(/^\/wellen\/[^/]+\/(?:all-progress|markets-status)$/, [
  'dashboard',
  'vorbesteller',
]);
allow(
  /^\/wellen\/[^/]+\/(?:progress|gl-photo-markets|gl-submissions|submitted-markets)\/[^/]+$/,
  ['dashboard', 'vorbesteller']
);
allow(/^\/wellen\/photos(?:\/facets|\/[^/]+\/original-url)?\/?$/, ['fotos']);
allow(/^\/wellen\/photos\/export\.zip$/, ['fotos'], 'export');
allow(
  /^\/wellen\/export\/(?:submissions|progress)$/,
  ['dashboard', 'vorbesteller'],
  'export'
);
allow(/^\/wellen\/[0-9a-f-]{36}$/, ['dashboard', 'vorbesteller']);
allow(/^\/vorverkauf\/?$/, ['vorverkauf', 'produktersatz']);
allow(/^\/vorverkauf\/stats\/summary$/, ['vorverkauf', 'produktersatz']);
allow(/^\/vorverkauf\/[^/]+$/, ['vorverkauf', 'produktersatz']);
allow(/^\/nara-incentive\/?$/, ['nara-incentive']);
allow(/^\/fragebogen\/photos\/?$/, ['fotos', 'fragebogen']);
allow(/^\/fragebogen\/photos\/export\.zip$/, ['fotos', 'fragebogen'], 'export');
allow(
  /^\/fragebogen\/(?:questions|modules)(?:\/[^/]+|\/stats\/[^/]+|\/[^/]+\/(?:module-count|usage))?$/,
  ['fragebogen']
);
allow(
  /^\/fragebogen\/fragebogen(?:\/stats\/[^/]+|\/status\/gl\/[^/]+|\/[^/]+(?:\/market-status)?)?$/,
  ['fragebogen']
);
allow(
  /^\/fragebogen\/responses\/(?:fragebogen\/[^/]+|completed-map|gl-history\/[^/]+|stats\/fragebogen\/[^/]+|[^/]+)$/,
  ['fragebogen']
);
allow(
  /^\/fragebogen\/responses\/photo-url$/,
  ['fragebogen', 'fotos'],
  'view',
  'POST'
);
allow(
  /^\/fragebogen\/fragebogen\/[^/]+\/export\.xlsx$/,
  ['fragebogen'],
  'export'
);
allow(
  /^\/fragebogen\/fragebogen\/distribution-export\.xlsx$/,
  ['fragebogen'],
  'export',
  'POST'
);
allow(
  /^\/fragebogen\/(?:zeiterfassung\/admin|zusatz-zeiterfassung-all|day-tracking-all|zusatz-zeiterfassung\/[^/]+|zeiterfassung\/gl\/[^/]+(?:\/date\/[^/]+)?|day-tracking\/[^/]+\/[^/]+\/(?:visits|summary))$/,
  ['zeiterfassung']
);

const exportDatasets: Record<string, CustomerPage[]> = {
  wellen_submissions: ['dashboard', 'vorbesteller'],
  markets: ['markets'],
  vorverkauf_entries: ['dashboard', 'vorverkauf', 'produktersatz'],
  action_history: ['markets'],
  gebietsleiter: ['gebietsleiter'],
};
export function authorizeCustomerPage(
  req: AuthRequest,
  res: Response,
  next: NextFunction
) {
  if (req.user?.role !== 'customer') return next();
  const permissions = req.user.permissions;
  const permitted = (pages: CustomerPage[], action: 'view' | 'export') =>
    pages.some(
      (page) =>
        permissions?.[page]?.view === true &&
        (action === 'view' || permissions[page]?.export === true)
    );
  const path = req.path.replace(/\/$/, '') || '/';
  // This POST only generates an Excel file; it never mutates business data.
  if (path === '/export/custom' && req.method === 'POST') {
    const datasets: unknown = req.body?.datasets;
    if (
      !Array.isArray(datasets) ||
      datasets.length === 0 ||
      !datasets.every(
        (ds) =>
          typeof ds === 'string' &&
          exportDatasets[ds] &&
          permitted(exportDatasets[ds], 'export')
      ) ||
      (req.body?.options?.singleWaveExport &&
        !permitted(['dashboard', 'vorbesteller'], 'export'))
    ) {
      return res.status(403).json({ error: 'Export nicht freigegeben.' });
    }
    req.customerReadAuthorized = true;
    return next();
  }
  if (
    path === '/export/dataset-stats' &&
    req.method === 'GET' &&
    permitted(['dashboard'], 'export')
  ) {
    req.customerReadAuthorized = true;
    return next();
  }
  // First match wins: a denied export can never fall through to an ID read rule.
  const rule = rules.find(
    (rule) => rule.method === req.method && rule.path.test(path)
  );
  if (!rule || !permitted(rule.pages, rule.action))
    return res
      .status(403)
      .json({ error: 'Diese Funktion ist nicht freigegeben.' });
  req.customerReadAuthorized = true;
  res.setHeader('Cache-Control', 'no-store');
  return next();
}
