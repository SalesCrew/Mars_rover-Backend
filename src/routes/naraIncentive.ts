import { Router, Request, Response } from 'express';
import { createFreshClient } from '../config/supabase';
import { AuthRequest, getAuthenticatedGlId, requireAdmin } from '../middleware/auth';
import { sendInternalError } from '../utils/httpErrors';

const router = Router();

router.post('/', async (req: AuthRequest, res: Response) => {
  try {
    const { gebietsleiter_id, market_id, items } = req.body;
    const effectiveGlId = req.user?.role === 'admin' ? gebietsleiter_id : getAuthenticatedGlId(req.user);

    if (!effectiveGlId || !market_id || !items || !Array.isArray(items) || items.length === 0) {
      return res.status(400).json({ error: 'gebietsleiter_id, market_id, and items are required' });
    }

    console.log(`Creating NARA-Incentive submission with ${items.length} items`);

    const freshClient = createFreshClient();

    const { data: submission, error: submissionError } = await freshClient
      .from('nara_incentive_submissions')
      .insert({ gebietsleiter_id: effectiveGlId, market_id })
      .select('id, gebietsleiter_id, market_id, created_at')
      .single();

    if (submissionError) {
      console.error('Error creating submission:');
      throw submissionError;
    }

    const itemRows = items.map((item: { product_id: string; quantity: number }) => ({
      submission_id: submission.id,
      product_id: item.product_id,
      quantity: item.quantity
    }));

    const { error: itemsError } = await freshClient
      .from('nara_incentive_items')
      .insert(itemRows);

    if (itemsError) {
      console.error('Error creating items:');
      throw itemsError;
    }

    console.log(`NARA-Incentive submission created with ${items.length} items`);
    res.status(201).json({ id: submission.id, itemsCount: items.length });
  } catch (error: any) {
    console.error('Error creating NARA-Incentive submission:');
    sendInternalError(res);
  }
});

router.get('/', async (req: AuthRequest, res: Response) => {
  try {
    const { glId } = req.query;
    const freshClient = createFreshClient();

    let query = freshClient
      .from('nara_incentive_submissions')
      .select(`
        id,
        gebietsleiter_id,
        market_id,
        created_at,
        gebietsleiter ( name ),
        markets ( name, chain, address, postal_code, city ),
        nara_incentive_items (
          id,
          product_id,
          quantity,
          products ( name, weight, price, department, product_type )
        )
      `)
      .order('created_at', { ascending: false });

    if (req.user?.role !== 'admin') {
      query = query.eq('gebietsleiter_id', getAuthenticatedGlId(req.user));
    } else if (glId) {
      query = query.eq('gebietsleiter_id', glId as string);
    }

    const { data, error } = await query;

    if (error) {
      console.error('Error fetching NARA-Incentive submissions:');
      throw error;
    }

    const submissions = (data || []).map((row: any) => {
      const items = (row.nara_incentive_items || []).map((item: any) => {
        const price = item.products?.price || 0;
        return {
          id: item.id,
          productId: item.product_id,
          productName: item.products?.name || 'Unbekannt',
          productWeight: item.products?.weight || '',
          productPrice: price,
          quantity: item.quantity,
          lineTotal: price * item.quantity
        };
      });

      return {
        id: row.id,
        glId: row.gebietsleiter_id,
        glName: row.gebietsleiter?.name || 'Unbekannt',
        marketId: row.market_id,
        marketName: row.markets?.name || 'Unbekannt',
        marketChain: row.markets?.chain || '',
        marketAddress: row.markets?.address || '',
        marketPostalCode: row.markets?.postal_code || '',
        marketCity: row.markets?.city || '',
        totalValue: items.reduce((sum: number, i: any) => sum + i.lineTotal, 0),
        createdAt: row.created_at,
        items
      };
    });

    res.json(submissions);
  } catch (error: any) {
    console.error('Error fetching NARA-Incentive submissions:');
    sendInternalError(res);
  }
});

router.delete('/:id', requireAdmin, async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    const freshClient = createFreshClient();

    const { error } = await freshClient
      .from('nara_incentive_submissions')
      .delete()
      .eq('id', id);

    if (error) {
      console.error('Error deleting NARA-Incentive submission:');
      throw error;
    }

    console.log('NARA-Incentive submission deleted');
    res.json({ success: true });
  } catch (error: any) {
    console.error('Error deleting NARA-Incentive submission:');
    sendInternalError(res);
  }
});

export default router;
