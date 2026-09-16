import { Router, Request, Response } from 'express';
import { createFreshClient } from '../config/supabase';
import { requireAdmin } from '../middleware/auth';
import { sendInternalError } from '../utils/httpErrors';

const validMarketId = (value: unknown): value is string =>
  typeof value === 'string' && value.length > 0 && value.length <= 50;

export const createMarketAdminCommentsRouter = (
  clientFactory: () => ReturnType<typeof createFreshClient> = createFreshClient
) => {
  const router = Router();

  router.get('/admin-comments', requireAdmin, async (_req: Request, res: Response) => {
    try {
      const client = clientFactory();
      const comments: Array<{ market_id: string; comment: string }> = [];
      const pageSize = 1000;
      for (let offset = 0; ; offset += pageSize) {
        const { data, error } = await client
          .from('market_admin_comments')
          .select('market_id, comment')
          .order('market_id', { ascending: true })
          .range(offset, offset + pageSize - 1);
        if (error) throw error;
        comments.push(...(data ?? []));
        if (!data || data.length < pageSize) break;
      }
      res.setHeader('Cache-Control', 'no-store');
      return res.json(comments);
    } catch (error) {
      console.error('Error loading market admin comments');
      return sendInternalError(res);
    }
  });

  router.get('/:id/admin-comment', requireAdmin, async (req: Request, res: Response) => {
    const { id } = req.params;
    if (!validMarketId(id)) return res.status(400).json({ error: 'Invalid market ID' });

    try {
      const client = clientFactory();
      const { data: market, error: marketError } = await client
        .from('markets').select('id').eq('id', id).maybeSingle();
      if (marketError) throw marketError;
      if (!market) return res.status(404).json({ error: 'Market not found' });

      const { data: note, error: noteError } = await client
        .from('market_admin_comments').select('comment, updated_at').eq('market_id', id).maybeSingle();
      if (noteError) throw noteError;

      res.setHeader('Cache-Control', 'no-store');
      return res.json({ comment: note?.comment ?? '', updatedAt: note?.updated_at ?? null });
    } catch (error) {
      console.error('Error loading market admin comment');
      return sendInternalError(res);
    }
  });

  router.put('/:id/admin-comment', requireAdmin, async (req: Request, res: Response) => {
    const { id } = req.params;
    const comment = req.body?.comment;
    if (!validMarketId(id)) return res.status(400).json({ error: 'Invalid market ID' });
    if (typeof comment !== 'string' || comment.length > 5000) {
      return res.status(400).json({ error: 'Comment must be text with at most 5000 characters' });
    }

    try {
      const client = clientFactory();
      const { data: market, error: marketError } = await client
        .from('markets').select('id').eq('id', id).maybeSingle();
      if (marketError) throw marketError;
      if (!market) return res.status(404).json({ error: 'Market not found' });

      const { data: note, error: noteError } = await client
        .from('market_admin_comments')
        .upsert({ market_id: id, comment: comment.trim(), updated_at: new Date().toISOString() }, { onConflict: 'market_id' })
        .select('comment, updated_at')
        .single();
      if (noteError) throw noteError;

      res.setHeader('Cache-Control', 'no-store');
      return res.json({ comment: note.comment, updatedAt: note.updated_at });
    } catch (error) {
      console.error('Error saving market admin comment');
      return sendInternalError(res);
    }
  });

  return router;
};

export default createMarketAdminCommentsRouter();
