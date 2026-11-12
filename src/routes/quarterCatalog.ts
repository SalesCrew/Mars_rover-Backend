import { Router } from 'express';
import { createFreshClient } from '../config/supabase';
import { AuthRequest, requireAdmin } from '../middleware/auth';
import { loadQuarterCatalog, viennaQuarter } from '../utils/quarterPricing';

export const createQuarterCatalogRouter = (getClient: () => any = createFreshClient) => {
  const router = Router();
  router.use((req: AuthRequest, res, next) => req.user?.id ? next() : res.status(401).json({ error: 'Anmeldung erforderlich.' }));
  router.get('/catalog', async (req, res) => {
    const current = viennaQuarter();
    const year = Number(req.query.year || current.year), quarter = Number(req.query.quarter || current.quarter);
    if (!Number.isInteger(year) || year < 2000 || year > 2100 || ![1,2,3,4].includes(quarter)) return res.status(400).json({ error: 'Ungültiges Quartal.' });
    try { res.setHeader('Cache-Control', 'no-store'); return res.json({ year, quarter, items: await loadQuarterCatalog(getClient(), year, quarter) }); }
    catch { return res.status(503).json({ error: 'Quartalskatalog konnte nicht geladen werden.' }); }
  });
  router.post('/catalog/import', requireAdmin, async (req: AuthRequest, res) => {
    const { year, quarter, filename, sha256, rows } = req.body || {};
    if (!Number.isInteger(year) || year < 2000 || year > 2100 || ![1,2,3,4].includes(quarter)
      || typeof filename !== 'string' || !filename.trim() || filename.length > 500
      || typeof sha256 !== 'string' || !/^[a-f0-9]{64}$/.test(sha256)
      || !Array.isArray(rows) || rows.length < 1 || rows.length > 5000
      || rows.some(row => !row || typeof row !== 'object' || Array.isArray(row)
        || !['food','pets'].includes(row.department) || !['display','kartonware','einzelprodukt'].includes(row.item_type)
        || typeof row.artikel_nr !== 'string' || !row.artikel_nr.trim() || row.artikel_nr.length > 80
        || typeof row.name !== 'string' || !row.name.trim() || row.name.length > 500
        || !Number.isInteger(row.units_per_ve) || row.units_per_ve < 1 || row.units_per_ve > 100000
        || typeof row.price_per_order !== 'number' || !Number.isFinite(row.price_per_order) || row.price_per_order < 0 || row.price_per_order >= 100000000
        || typeof row.source_cell !== 'string' || !row.source_cell.trim())) return res.status(400).json({ error: 'Bitte eine gültige Preisliste auswählen.' });
    try {
      const { data, error } = await getClient().rpc('import_quarter_prices', { p_year: year, p_quarter: quarter, p_filename: filename, p_sha256: sha256, p_rows: rows, p_actor: req.user!.id });
      if (error) return res.status(error.code === '22023' ? 422 : 503).json({ error: error.code === '22023' ? error.message : 'Preisliste konnte nicht gespeichert werden.' });
      return res.json(data);
    } catch { return res.status(503).json({ error: 'Preisliste konnte nicht gespeichert werden.' }); }
  });
  const reconcile = (apply: boolean) => async (req: AuthRequest, res: any) => {
    if (apply && !/^[a-f0-9]{64}$/.test(req.body.token || '')) return res.status(400).json({ error: 'Bitte die Preisvorschau erneut öffnen.' });
    try {
      const { data, error } = await getClient().rpc('reconcile_wave_quarter_prices', { p_apply: apply, p_token: apply ? req.body.token : null, p_actor: apply ? req.user!.id : null });
      if (error) return res.status(error.code === '40001' ? 409 : 503).json({ error: error.code === '40001' ? 'Vorschau veraltet. Bitte erneut prüfen.' : 'Quartalspreisabgleich konnte nicht abgeschlossen werden.' });
      res.setHeader('Cache-Control', 'no-store'); return res.json(data);
    } catch { return res.status(503).json({ error: 'Quartalspreisabgleich konnte nicht abgeschlossen werden.' }); }
  };
  router.get('/catalog/reconciliation', requireAdmin, reconcile(false));
  router.post('/catalog/reconciliation', requireAdmin, reconcile(true));
  return router;
};
