import { Router, Request, Response } from 'express';
import { supabase } from '../config/supabase';

const router = Router();

// UPLOAD screenshot (base64)
router.post('/upload', async (req: Request, res: Response) => {
  try {
    const { base64Data, fileName, mimeType, userId } = req.body;

    if (!base64Data) {
      return res.status(400).json({ error: 'No file data provided' });
    }

    // Convert base64 to buffer
    const base64Content = base64Data.replace(/^data:[^;]+;base64,/, '');
    const buffer = Buffer.from(base64Content, 'base64');

    const fileExt = fileName?.split('.').pop() || 'png';
    const storagePath = `bug-reports/${userId || 'unknown'}_${Date.now()}.${fileExt}`;

    const { error: uploadError } = await supabase.storage
      .from('bug-screenshots')
      .upload(storagePath, buffer, {
        contentType: mimeType || 'image/png',
        upsert: false
      });

    if (uploadError) {
      console.error('Error uploading to storage:', uploadError);
      return res.status(500).json({ error: 'Failed to upload file' });
    }

    const { data: urlData } = supabase.storage
      .from('bug-screenshots')
      .getPublicUrl(storagePath);

    res.json({ url: urlData.publicUrl, path: storagePath });
  } catch (error) {
    console.error('Error in POST /bug-reports/upload:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// CREATE bug report
router.post('/', async (req: Request, res: Response) => {
  try {
    const { 
      gebietsleiter_id, 
      gebietsleiter_name,
      description, 
      screenshot_url,
      page_url,
      user_agent
    } = req.body;

    if (!description || description.trim().length === 0) {
      return res.status(400).json({ error: 'Description is required' });
    }

    const { data, error } = await supabase
      .from('bug_reports')
      .insert({
        gebietsleiter_id,
        gebietsleiter_name,
        description: description.trim(),
        screenshot_url,
        page_url,
        user_agent,
        status: 'new'
      })
      .select()
      .single();

    if (error) {
      console.error('Error creating bug report:', error);
      return res.status(500).json({ error: 'Failed to create bug report' });
    }

    res.status(201).json(data);
  } catch (error) {
    console.error('Error in POST /bug-reports:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET all bug reports (for admin review - direct DB access for now)
router.get('/', async (req: Request, res: Response) => {
  try {
    const { status } = req.query;

    let query = supabase
      .from('bug_reports')
      .select('*')
      .order('created_at', { ascending: false });

    if (status && typeof status === 'string') {
      query = query.eq('status', status);
    }

    const { data, error } = await query;

    if (error) {
      console.error('Error fetching bug reports:', error);
      return res.status(500).json({ error: 'Failed to fetch bug reports' });
    }

    res.json(data);
  } catch (error) {
    console.error('Error in GET /bug-reports:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// UPDATE bug report status
router.patch('/:id', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    const { status } = req.body;

    const validStatuses = ['new', 'reviewed', 'fixed', 'wont_fix'];
    if (!validStatuses.includes(status)) {
      return res.status(400).json({ error: 'Invalid status' });
    }

    const { data, error } = await supabase
      .from('bug_reports')
      .update({ status })
      .eq('id', id)
      .select()
      .single();

    if (error) {
      console.error('Error updating bug report:', error);
      return res.status(500).json({ error: 'Failed to update bug report' });
    }

    res.json(data);
  } catch (error) {
    console.error('Error in PATCH /bug-reports/:id:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

export default router;
