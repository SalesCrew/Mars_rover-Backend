import { Router, Request, Response } from 'express';
import { supabase } from '../config/supabase';

const router = Router();

// Helper to determine status based on dates
const determineStatus = (startDate: string, endDate: string): 'upcoming' | 'active' | 'past' => {
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  const start = new Date(startDate);
  const end = new Date(endDate);
  
  if (today > end) return 'past';
  if (today >= start && today <= end) return 'active';
  return 'upcoming';
};

// ============================================================================
// GET ALL VORVERKAUF WELLEN (Admin)
// ============================================================================
router.get('/', async (_req: Request, res: Response) => {
  try {
    // First update statuses based on dates (ignore errors if function doesn't exist)
    try {
      await supabase.rpc('update_vorverkauf_welle_status');
    } catch {
      // Function might not exist yet, ignore
    }

    const { data: wellen, error } = await supabase
      .from('vorverkauf_wellen')
      .select('*')
      .order('start_date', { ascending: false });

    if (error) throw error;

    // Get market counts for each welle
    const wellenWithCounts = await Promise.all((wellen || []).map(async (welle) => {
      const { data: markets } = await supabase
        .from('vorverkauf_wellen_markets')
        .select('market_id')
        .eq('welle_id', welle.id);

      return {
        id: welle.id,
        name: welle.name,
        image: welle.image_url,
        startDate: welle.start_date,
        endDate: welle.end_date,
        status: determineStatus(welle.start_date, welle.end_date),
        assignedMarketIds: (markets || []).map(m => m.market_id),
        createdAt: welle.created_at,
        updatedAt: welle.updated_at
      };
    }));

    res.json(wellenWithCounts);
  } catch (error) {
    console.error('Error fetching vorverkauf wellen:', error);
    res.status(500).json({ error: 'Failed to fetch vorverkauf wellen' });
  }
});

// ============================================================================
// GET VORVERKAUF WELLEN FOR GL (active/upcoming for their markets)
// NOTE: This route MUST be before /:id to avoid "gl" being matched as an ID
// ============================================================================
router.get('/gl/:glId', async (req: Request, res: Response) => {
  try {
    const { glId } = req.params;

    console.log('Fetching vorverkauf wellen for GL:', glId);

    // Get ALL active/upcoming wellen first (since markets may not be assigned to GL yet)
    const { data: wellen, error: wellenError } = await supabase
      .from('vorverkauf_wellen')
      .select('*')
      .order('start_date', { ascending: true });

    if (wellenError) throw wellenError;

    console.log('Found wellen:', wellen?.length || 0);

    // Format and filter by status based on current date
    const wellenWithStatus = (wellen || []).map(welle => ({
      id: welle.id,
      name: welle.name,
      image: welle.image_url,
      startDate: welle.start_date,
      endDate: welle.end_date,
      status: determineStatus(welle.start_date, welle.end_date)
    })).filter(w => w.status === 'active' || w.status === 'upcoming');

    console.log('Active/upcoming wellen:', wellenWithStatus.length);

    res.json(wellenWithStatus);
  } catch (error) {
    console.error('Error fetching GL vorverkauf wellen:', error);
    res.status(500).json({ error: 'Failed to fetch vorverkauf wellen for GL' });
  }
});

// ============================================================================
// GET SUBMISSIONS FOR A WAVE (Admin)
// NOTE: This route MUST be before /:id to avoid "submissions" being matched as an ID
// ============================================================================
router.get('/submissions/:waveId', async (req: Request, res: Response) => {
  try {
    const { waveId } = req.params;

    const { data: submissions, error: submissionsError } = await supabase
      .from('vorverkauf_submissions')
      .select(`
        *,
        gebietsleiter:gebietsleiter_id (id, name),
        market:market_id (id, name, city, chain)
      `)
      .eq('vorverkauf_welle_id', waveId)
      .order('created_at', { ascending: false });

    if (submissionsError) {
      console.error('Error fetching submissions:', submissionsError);
      throw submissionsError;
    }

    // Get products for each submission
    const submissionsWithProducts = await Promise.all((submissions || []).map(async (sub) => {
      const { data: products } = await supabase
        .from('vorverkauf_submission_products')
        .select(`
          *,
          product:product_id (id, name, department, product_type, weight, price)
        `)
        .eq('submission_id', sub.id);

      // Parse GL name into first_name and last_name for frontend compatibility
      const glName = sub.gebietsleiter?.name || '';
      const nameParts = glName.split(' ');
      const firstName = nameParts[0] || '';
      const lastName = nameParts.slice(1).join(' ') || '';

      return {
        id: sub.id,
        welleId: sub.vorverkauf_welle_id,
        gebietsleiter: sub.gebietsleiter ? {
          id: sub.gebietsleiter.id,
          first_name: firstName,
          last_name: lastName,
          name: glName
        } : null,
        market: sub.market,
        products: (products || []).map(p => ({
          id: p.id,
          product: p.product,
          quantity: p.quantity,
          reason: p.reason
        })),
        notes: sub.notes,
        createdAt: sub.created_at
      };
    }));

    res.json(submissionsWithProducts);
  } catch (error) {
    console.error('Error fetching submissions:', error);
    res.status(500).json({ error: 'Failed to fetch submissions' });
  }
});

// ============================================================================
// GET SUBMISSION STATS FOR A WAVE (Admin)
// NOTE: This route MUST be before /:id to avoid "stats" being matched as an ID
// ============================================================================
router.get('/stats/:waveId', async (req: Request, res: Response) => {
  try {
    const { waveId } = req.params;

    // Get submission count
    const { data: submissions, error: submissionsError } = await supabase
      .from('vorverkauf_submissions')
      .select('id, gebietsleiter_id, market_id')
      .eq('vorverkauf_welle_id', waveId);

    if (submissionsError) throw submissionsError;

    const submissionIds = (submissions || []).map(s => s.id);

    // Get product counts by reason
    let reasonCounts = { OOS: 0, Listungslücke: 0, Platzierung: 0 };
    let totalProducts = 0;

    if (submissionIds.length > 0) {
      const { data: products } = await supabase
        .from('vorverkauf_submission_products')
        .select('quantity, reason')
        .in('submission_id', submissionIds);

      (products || []).forEach(p => {
        totalProducts += p.quantity;
        if (p.reason in reasonCounts) {
          reasonCounts[p.reason as keyof typeof reasonCounts] += p.quantity;
        }
      });
    }

    res.json({
      totalSubmissions: submissions?.length || 0,
      uniqueGLs: new Set((submissions || []).map(s => s.gebietsleiter_id)).size,
      uniqueMarkets: new Set((submissions || []).map(s => s.market_id)).size,
      totalProducts,
      byReason: reasonCounts
    });
  } catch (error) {
    console.error('Error fetching stats:', error);
    res.status(500).json({ error: 'Failed to fetch stats' });
  }
});

// ============================================================================
// GET SINGLE VORVERKAUF WELLE
// ============================================================================
router.get('/:id', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;

    const { data: welle, error } = await supabase
      .from('vorverkauf_wellen')
      .select('*')
      .eq('id', id)
      .single();

    if (error) throw error;
    if (!welle) {
      return res.status(404).json({ error: 'Vorverkauf welle not found' });
    }

    // Get assigned markets
    const { data: markets } = await supabase
      .from('vorverkauf_wellen_markets')
      .select('market_id')
      .eq('welle_id', id);

    res.json({
      id: welle.id,
      name: welle.name,
      image: welle.image_url,
      startDate: welle.start_date,
      endDate: welle.end_date,
      status: determineStatus(welle.start_date, welle.end_date),
      assignedMarketIds: (markets || []).map(m => m.market_id),
      createdAt: welle.created_at,
      updatedAt: welle.updated_at
    });
  } catch (error) {
    console.error('Error fetching vorverkauf welle:', error);
    res.status(500).json({ error: 'Failed to fetch vorverkauf welle' });
  }
});

// ============================================================================
// CREATE VORVERKAUF WELLE (Admin)
// ============================================================================
router.post('/', async (req: Request, res: Response) => {
  try {
    const { name, image, startDate, endDate, assignedMarketIds } = req.body;

    if (!name || !startDate || !endDate) {
      return res.status(400).json({ error: 'Name, startDate, and endDate are required' });
    }

    const status = determineStatus(startDate, endDate);

    // Insert welle
    const { data: welle, error: welleError } = await supabase
      .from('vorverkauf_wellen')
      .insert({
        name,
        image_url: image || null,
        start_date: startDate,
        end_date: endDate,
        status
      })
      .select()
      .single();

    if (welleError) throw welleError;

    // Insert market assignments if provided
    if (assignedMarketIds && assignedMarketIds.length > 0) {
      const marketInserts = assignedMarketIds.map((marketId: string) => ({
        welle_id: welle.id,
        market_id: marketId
      }));

      const { error: marketsError } = await supabase
        .from('vorverkauf_wellen_markets')
        .insert(marketInserts);

      if (marketsError) {
        console.error('Error assigning markets:', marketsError);
        // Continue anyway, welle is created
      }
    }

    res.status(201).json({
      id: welle.id,
      name: welle.name,
      image: welle.image_url,
      startDate: welle.start_date,
      endDate: welle.end_date,
      status,
      assignedMarketIds: assignedMarketIds || [],
      createdAt: welle.created_at,
      updatedAt: welle.updated_at
    });
  } catch (error) {
    console.error('Error creating vorverkauf welle:', error);
    res.status(500).json({ error: 'Failed to create vorverkauf welle' });
  }
});

// ============================================================================
// UPDATE VORVERKAUF WELLE (Admin)
// ============================================================================
router.put('/:id', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    const { name, image, startDate, endDate, assignedMarketIds } = req.body;

    const status = determineStatus(startDate, endDate);

    // Update welle
    const { data: welle, error: welleError } = await supabase
      .from('vorverkauf_wellen')
      .update({
        name,
        image_url: image || null,
        start_date: startDate,
        end_date: endDate,
        status
      })
      .eq('id', id)
      .select()
      .single();

    if (welleError) throw welleError;

    // Update market assignments
    if (assignedMarketIds !== undefined) {
      // Delete existing assignments
      await supabase
        .from('vorverkauf_wellen_markets')
        .delete()
        .eq('welle_id', id);

      // Insert new assignments
      if (assignedMarketIds.length > 0) {
        const marketInserts = assignedMarketIds.map((marketId: string) => ({
          welle_id: id,
          market_id: marketId
        }));

        await supabase
          .from('vorverkauf_wellen_markets')
          .insert(marketInserts);
      }
    }

    res.json({
      id: welle.id,
      name: welle.name,
      image: welle.image_url,
      startDate: welle.start_date,
      endDate: welle.end_date,
      status,
      assignedMarketIds: assignedMarketIds || [],
      createdAt: welle.created_at,
      updatedAt: welle.updated_at
    });
  } catch (error) {
    console.error('Error updating vorverkauf welle:', error);
    res.status(500).json({ error: 'Failed to update vorverkauf welle' });
  }
});

// ============================================================================
// DELETE VORVERKAUF WELLE (Admin)
// ============================================================================
router.delete('/:id', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;

    const { error } = await supabase
      .from('vorverkauf_wellen')
      .delete()
      .eq('id', id);

    if (error) throw error;

    res.json({ success: true });
  } catch (error) {
    console.error('Error deleting vorverkauf welle:', error);
    res.status(500).json({ error: 'Failed to delete vorverkauf welle' });
  }
});

// ============================================================================
// GET MARKETS ASSIGNED TO A VORVERKAUF WELLE
// Returns all markets assigned to the wave (admin selected these specifically)
// ============================================================================
router.get('/:id/markets', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;

    console.log('Fetching markets for vorverkauf welle:', id);

    // Get markets assigned to this welle
    const { data: welleMarkets, error: welleMarketsError } = await supabase
      .from('vorverkauf_wellen_markets')
      .select('market_id')
      .eq('welle_id', id);

    if (welleMarketsError) {
      console.error('Error fetching welle markets junction:', welleMarketsError);
      throw welleMarketsError;
    }

    console.log('Wave market assignments:', welleMarkets?.length || 0);

    const marketIds = (welleMarkets || []).map(wm => wm.market_id);

    if (marketIds.length === 0) {
      console.log('No markets assigned to this wave');
      return res.json([]);
    }

    // Get all market details for assigned markets
    const { data: markets, error: marketsError } = await supabase
      .from('markets')
      .select('*')
      .in('id', marketIds);

    if (marketsError) {
      console.error('Error fetching market details:', marketsError);
      throw marketsError;
    }

    console.log('Found markets:', markets?.length || 0);

    // Transform to frontend format
    const formattedMarkets = (markets || []).map(m => ({
      id: m.id,
      name: m.name,
      address: m.address || m.street || '',
      city: m.city || '',
      postalCode: m.postal_code || '',
      chain: m.chain || '',
      gebietsleiter: m.gebietsleiter_id || m.gebietsleiter_name || '',
      frequency: m.frequency || 0,
      currentVisits: m.current_visits || 0,
      lastVisitDate: m.last_visit_date,
      isCompleted: false
    }));

    res.json(formattedMarkets);
  } catch (error) {
    console.error('Error fetching welle markets:', error);
    res.status(500).json({ error: 'Failed to fetch welle markets' });
  }
});

// ============================================================================
// SUBMIT VORVERKAUF ENTRY (GL)
// ============================================================================
router.post('/submit', async (req: Request, res: Response) => {
  try {
    const { welleId, gebietsleiter_id, market_id, products, notes } = req.body;

    if (!welleId || !gebietsleiter_id || !market_id || !products || products.length === 0) {
      return res.status(400).json({ 
        error: 'welleId, gebietsleiter_id, market_id, and products are required' 
      });
    }

    // Create submission entry
    const { data: submission, error: submissionError } = await supabase
      .from('vorverkauf_submissions')
      .insert({
        vorverkauf_welle_id: welleId,
        gebietsleiter_id,
        market_id,
        notes: notes || null
      })
      .select()
      .single();

    if (submissionError) throw submissionError;

    // Insert products
    const productInserts = products.map((p: { productId: string; quantity: number; reason: string }) => ({
      submission_id: submission.id,
      product_id: p.productId,
      quantity: p.quantity,
      reason: p.reason
    }));

    const { error: productsError } = await supabase
      .from('vorverkauf_submission_products')
      .insert(productInserts);

    if (productsError) throw productsError;

    // Update market visit count
    const today = new Date().toISOString().split('T')[0];
    const { data: market } = await supabase
      .from('markets')
      .select('last_visit_date, current_visits')
      .eq('id', market_id)
      .single();

    if (market && market.last_visit_date !== today) {
      await supabase
        .from('markets')
        .update({
          current_visits: (market.current_visits || 0) + 1,
          last_visit_date: today
        })
        .eq('id', market_id);
    }

    res.status(201).json({
      id: submission.id,
      welleId: submission.vorverkauf_welle_id,
      gebietsleiterId: submission.gebietsleiter_id,
      marketId: submission.market_id,
      productCount: products.length,
      createdAt: submission.created_at
    });
  } catch (error) {
    console.error('Error submitting vorverkauf:', error);
    res.status(500).json({ error: 'Failed to submit vorverkauf' });
  }
});

export default router;
