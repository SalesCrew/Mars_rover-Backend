import express, { Request, Response } from 'express';
import { supabase, createFreshClient } from '../config/supabase';
import { sendInternalError } from '../utils/httpErrors';

const router = express.Router();
const ACTION_HISTORY_SELECT = 'id, action_type, market_id, market_chain, market_address, market_postal_code, market_city, target_gl, previous_gl, performed_by, notes, timestamp';

// Get all action history (with optional filtering)
router.get('/', async (req: Request, res: Response) => {
  try {
    const { target_gl, limit = '100', offset = '0' } = req.query;
    
    const freshClient = createFreshClient();
    
    let query = freshClient
      .from('action_history')
      .select(ACTION_HISTORY_SELECT)
      .order('timestamp', { ascending: false })
      .range(parseInt(offset as string), parseInt(offset as string) + parseInt(limit as string) - 1);
    
    // Filter by GL if provided
    if (target_gl) {
      query = query.eq('target_gl', target_gl);
    }
    
    const { data, error } = await query;
    
    if (error) {
      console.error('Error fetching action history:');
      throw error;
    }
    
    console.log(`✅ Fetched ${data?.length || 0} action history entries`);
    res.json(data || []);
  } catch (error: any) {
    console.error('Error fetching action history:');
    sendInternalError(res);
  }
});

// Create a new action history entry
router.post('/', async (req: Request, res: Response) => {
  try {
    const {
      action_type,
      market_id,
      market_chain,
      market_address,
      market_postal_code,
      market_city,
      target_gl,
      previous_gl,
      performed_by,
      notes
    } = req.body;
    
    // Validate required fields
    if (!action_type || !market_chain || !market_address || !target_gl) {
      return res.status(400).json({ 
        error: 'Missing required fields: action_type, market_chain, market_address, target_gl' 
      });
    }
    
    // Validate action_type
    if (!['assign', 'swap', 'remove'].includes(action_type)) {
      return res.status(400).json({ 
        error: 'Invalid action_type. Must be: assign, swap, or remove' 
      });
    }
    
    const freshClient = createFreshClient();
    
    const { data, error } = await freshClient
      .from('action_history')
      .insert([{
        action_type,
        market_id,
        market_chain,
        market_address,
        market_postal_code,
        market_city,
        target_gl,
        previous_gl,
        performed_by,
        notes
      }])
      .select(ACTION_HISTORY_SELECT)
      .single();
    
    if (error) {
      console.error('Error creating action history:');
      throw error;
    }
    
    console.log(`Created action history entry: ${action_type}`);
    res.status(201).json(data);
  } catch (error: any) {
    console.error('Error creating action history:');
    sendInternalError(res);
  }
});

// Delete action history entry (for cleanup/testing)
router.delete('/:id', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    
    const freshClient = createFreshClient();
    
    const { error } = await freshClient
      .from('action_history')
      .delete()
      .eq('id', id);
    
    if (error) {
      console.error('Error deleting action history:');
      throw error;
    }
    
    console.log('Deleted action history entry');
    res.status(204).send();
  } catch (error: any) {
    console.error('Error deleting action history:');
    sendInternalError(res);
  }
});

export default router;

