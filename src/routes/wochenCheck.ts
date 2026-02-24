import { Router, Request, Response } from 'express';
import { createFreshClient } from '../config/supabase';

const router = Router();

router.get('/:glId', async (req: Request, res: Response) => {
  try {
    const { glId } = req.params;
    const { week_start_date } = req.query;

    if (!week_start_date) {
      return res.status(400).json({ error: 'week_start_date query param required' });
    }

    const freshClient = createFreshClient();

    const { data, error } = await freshClient
      .from('zeiterfassung_wochen_checks')
      .select('id, confirmed_at')
      .eq('gebietsleiter_id', glId)
      .eq('week_start_date', week_start_date as string)
      .maybeSingle();

    if (error) throw error;

    return res.json({ confirmed: !!data, record: data });
  } catch (err: any) {
    console.error('Error checking wochen-check:', err);
    return res.status(500).json({ error: err.message });
  }
});

router.post('/', async (req: Request, res: Response) => {
  try {
    const { gebietsleiter_id, week_start_date } = req.body;

    if (!gebietsleiter_id || !week_start_date) {
      return res.status(400).json({ error: 'gebietsleiter_id and week_start_date required' });
    }

    const freshClient = createFreshClient();

    const { data, error } = await freshClient
      .from('zeiterfassung_wochen_checks')
      .upsert(
        { gebietsleiter_id, week_start_date },
        { onConflict: 'gebietsleiter_id,week_start_date' }
      )
      .select()
      .single();

    if (error) throw error;

    return res.json(data);
  } catch (err: any) {
    console.error('Error saving wochen-check:', err);
    return res.status(500).json({ error: err.message });
  }
});

export default router;
