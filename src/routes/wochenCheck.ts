import { Router, Request, Response } from 'express';
import { createFreshClient } from '../config/supabase';
import { AuthRequest, getAuthenticatedGlId, requireSelfOrAdmin } from '../middleware/auth';
import { sendInternalError } from '../utils/httpErrors';

const router = Router();

router.get('/:glId', requireSelfOrAdmin(req => req.params.glId), async (req: AuthRequest, res: Response) => {
  try {
    const { glId } = req.params;
    const { week_start_date } = req.query;
    const effectiveGlId = req.user?.role === 'admin' ? glId : getAuthenticatedGlId(req.user);

    if (!week_start_date) {
      return res.status(400).json({ error: 'week_start_date query param required' });
    }
    if (!effectiveGlId) {
      return res.status(400).json({ error: 'gebietsleiter_id required' });
    }

    const freshClient = createFreshClient();

    const { data, error } = await freshClient
      .from('zeiterfassung_wochen_checks')
      .select('id, confirmed_at')
      .eq('gebietsleiter_id', effectiveGlId)
      .eq('week_start_date', week_start_date as string)
      .maybeSingle();

    if (error) throw error;

    return res.json({ confirmed: !!data, record: data });
  } catch (err: any) {
    console.error('Error checking wochen-check:');
    return sendInternalError(res);
  }
});

router.post('/', requireSelfOrAdmin(req => req.body.gebietsleiter_id), async (req: AuthRequest, res: Response) => {
  try {
    const { gebietsleiter_id, week_start_date } = req.body;
    const effectiveGlId = req.user?.role === 'admin' ? gebietsleiter_id : getAuthenticatedGlId(req.user);

    if (!effectiveGlId || !week_start_date) {
      return res.status(400).json({ error: 'gebietsleiter_id and week_start_date required' });
    }

    const freshClient = createFreshClient();

    const { data, error } = await freshClient
      .from('zeiterfassung_wochen_checks')
      .upsert(
        { gebietsleiter_id: effectiveGlId, week_start_date },
        { onConflict: 'gebietsleiter_id,week_start_date' }
      )
      .select('id, gebietsleiter_id, week_start_date, confirmed_at')
      .single();

    if (error) throw error;

    return res.json(data);
  } catch (err: any) {
    console.error('Error saving wochen-check:');
    return sendInternalError(res);
  }
});

export default router;
