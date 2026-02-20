import { Router, Request, Response } from 'express';
import { createFreshClient } from '../config/supabase';

const router = Router();

router.post('/', async (req: Request, res: Response) => {
  try {
    const { gebietsleiter_id, market_id, items } = req.body;

    if (!gebietsleiter_id || !market_id || !items || !Array.isArray(items) || items.length === 0) {
      return res.status(400).json({ error: 'gebietsleiter_id, market_id, and items are required' });
    }

    console.log(`📦 Creating NARA-Incentive submission for GL ${gebietsleiter_id}, market ${market_id}, ${items.length} items...`);

    const freshClient = createFreshClient();

    const { data: submission, error: submissionError } = await freshClient
      .from('nara_incentive_submissions')
      .insert({ gebietsleiter_id, market_id })
      .select()
      .single();

    if (submissionError) {
      console.error('Error creating submission:', submissionError);
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
      console.error('Error creating items:', itemsError);
      throw itemsError;
    }

    console.log(`✅ NARA-Incentive submission created: ${submission.id} with ${items.length} items`);
    res.status(201).json({ id: submission.id, itemsCount: items.length });
  } catch (error: any) {
    console.error('Error creating NARA-Incentive submission:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

router.get('/', async (req: Request, res: Response) => {
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
        markets ( name, chain, address, city ),
        nara_incentive_items (
          id,
          product_id,
          quantity,
          products ( name, weight, price, department, product_type )
        )
      `)
      .order('created_at', { ascending: false });

    if (glId) {
      query = query.eq('gebietsleiter_id', glId as string);
    }

    const { data, error } = await query;

    if (error) {
      console.error('Error fetching NARA-Incentive submissions:', error);
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
        marketCity: row.markets?.city || '',
        totalValue: items.reduce((sum: number, i: any) => sum + i.lineTotal, 0),
        createdAt: row.created_at,
        items
      };
    });

    res.json(submissions);
  } catch (error: any) {
    console.error('Error fetching NARA-Incentive submissions:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

export default router;
