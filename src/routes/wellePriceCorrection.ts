import { Router, Response } from 'express';
import { AuthRequest, requireAdmin } from '../middleware/auth';
import { createFreshClient } from '../config/supabase';

const uuidPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

// Dependency injection keeps route tests isolated from the production database.
export function createWellePriceCorrectionRouter(getClient = createFreshClient) {
  const router = Router();
  const handle = (apply: boolean) => async (req: AuthRequest, res: Response) => {
    res.setHeader('Cache-Control', 'no-store');
    if (!uuidPattern.test(req.params.id) || (apply && (typeof req.body?.token !== 'string' || !/^[0-9a-f]{64}$/.test(req.body.token)))) {
      return res.status(400).json({ code: 'MR-WELLE-PRICE-REQUEST-001', error: 'Ungültige Welle oder Preisvorschau.' });
    }
    try {
      const { data, error } = await getClient().rpc('correct_welle_submission_prices', {
        p_welle_id: req.params.id,
        p_apply: apply,
        p_expected_token: apply ? req.body.token : null,
        p_actor_id: apply ? req.user!.id : null,
      });
      if (error || !data) {
        console.error('Welle price correction failed:', error);
        return res.status(503).json({
          code: apply ? 'MR-WELLE-PRICE-APPLY-001' : 'MR-WELLE-PRICE-PREVIEW-001',
          error: apply
            ? 'Die Übernahme konnte nicht bestätigt werden. Bitte den Preisvergleich erneut laden, bevor du es nochmals versuchst.'
            : 'Die Welle ist gespeichert, aber die Buchungspreise konnten nicht geprüft werden.',
        });
      }
      if (data.code) {
        const stale = data.code === 'WELLE_PRICE_PREVIEW_STALE';
        return res.status(stale ? 409 : 404).json({
          code: stale ? 'MR-WELLE-PRICE-STALE-001' : 'MR-WELLE-PRICE-NOT-FOUND-001',
          error: stale
            ? 'Preise oder Buchungen wurden inzwischen geändert. Bitte die neue Vorschau prüfen und erneut bestätigen.'
            : 'Die Welle ist nicht mehr verfügbar.',
        });
      }
      return res.json(data);
    } catch (error) {
      console.error('Welle price correction request failed:', error);
      return res.status(503).json({ code: apply ? 'MR-WELLE-PRICE-APPLY-001' : 'MR-WELLE-PRICE-REQUEST-002', error: apply ? 'Übernahme nicht bestätigt. Bitte den Preisvergleich erneut laden.' : 'Preisvergleich nicht erreichbar. Bitte erneut prüfen.' });
    }
  };
  router.get('/:id/submission-prices', requireAdmin, handle(false));
  router.post('/:id/submission-prices', requireAdmin, handle(true));
  return router;
}
