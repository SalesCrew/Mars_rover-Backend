import { Router, Request, Response } from 'express';
import { supabase } from '../config/supabase';

const router = Router();

/**
 * GET /api/markets
 * Get all markets
 */
router.get('/', async (req: Request, res: Response) => {
  try {
    console.log('📋 Fetching all markets...');
    
    const { data, error } = await supabase
      .from('markets')
      .select('*')
      .order('name', { ascending: true });

    if (error) {
      console.error('Supabase error:', error);
      throw error;
    }

    console.log(`✅ Fetched ${data?.length || 0} markets`);
    res.json(data || []);
  } catch (error: any) {
    console.error('Error fetching markets:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

/**
 * GET /api/markets/:id
 * Get a single market by ID
 */
router.get('/:id', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    console.log(`📋 Fetching market ${id}...`);

    const { data, error } = await supabase
      .from('markets')
      .select('*')
      .eq('id', id)
      .single();

    if (error) {
      console.error('Supabase error:', error);
      throw error;
    }

    if (!data) {
      return res.status(404).json({ error: 'Market not found' });
    }

    console.log(`✅ Fetched market ${id}`);
    res.json(data);
  } catch (error: any) {
    console.error('Error fetching market:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

/**
 * POST /api/markets
 * Create a new market
 */
router.post('/', async (req: Request, res: Response) => {
  try {
    console.log('➕ Creating new market...');
    
    const { data, error } = await supabase
      .from('markets')
      .insert(req.body)
      .select()
      .single();

    if (error) {
      console.error('Supabase error:', error);
      throw error;
    }

    console.log(`✅ Created market ${data?.id}`);
    res.status(201).json(data);
  } catch (error: any) {
    console.error('Error creating market:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

/**
 * POST /api/markets/import
 * Bulk import markets
 */
router.post('/import', async (req: Request, res: Response) => {
  try {
    const markets = req.body;

    if (!Array.isArray(markets) || markets.length === 0) {
      return res.status(400).json({ error: 'Invalid request: markets array required' });
    }

    console.log(`📥 Importing ${markets.length} markets...`);

    const { data, error } = await supabase
      .from('markets')
      .upsert(markets, { 
        onConflict: 'id',
        ignoreDuplicates: false 
      })
      .select();

    if (error) {
      console.error('Supabase error:', error);
      throw error;
    }

    console.log(`✅ Successfully imported ${data?.length || 0} markets`);
    res.json({
      success: data?.length || 0,
      failed: markets.length - (data?.length || 0),
    });
  } catch (error: any) {
    console.error('Error importing markets:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

/**
 * PUT /api/markets/:id
 * Update a market
 */
router.put('/:id', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    console.log(`✏️ Updating market ${id}...`);

    const { data, error } = await supabase
      .from('markets')
      .update(req.body)
      .eq('id', id)
      .select()
      .single();

    if (error) {
      console.error('Supabase error:', error);
      throw error;
    }

    console.log(`✅ Updated market ${id}`);
    res.json(data);
  } catch (error: any) {
    console.error('Error updating market:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

/**
 * DELETE /api/markets/:id
 * Delete a market
 */
router.delete('/:id', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    console.log(`🗑️ Deleting market ${id}...`);

    const { error } = await supabase
      .from('markets')
      .delete()
      .eq('id', id);

    if (error) {
      console.error('Supabase error:', error);
      throw error;
    }

    console.log(`✅ Deleted market ${id}`);
    res.status(204).send();
  } catch (error: any) {
    console.error('Error deleting market:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

export default router;

