import { Router, Request, Response } from 'express';
import { randomUUID } from 'crypto';
import { supabase } from '../config/supabase';
import { AuthRequest, getAuthenticatedGlId, requireAdmin } from '../middleware/auth';
import { sendInternalError } from '../utils/httpErrors';

const router = Router();
const BUG_SCREENSHOTS_BUCKET = 'bug-screenshots';
const BUG_SCREENSHOT_SIGNED_URL_SECONDS = 60 * 60;
const BUG_REPORT_SELECT = 'id, gebietsleiter_id, gebietsleiter_name, description, screenshot_url, page_url, user_agent, status, created_at, updated_at';
const BUG_SCREENSHOT_MIME_TO_EXTENSION: Record<string, string> = {
  'image/jpeg': 'jpg',
  'image/png': 'png',
  'image/webp': 'webp'
};
const BUG_SCREENSHOT_MAX_BYTES = 5 * 1024 * 1024;

const extractBugScreenshotPath = (value?: string | null): string | null => {
  if (!value) return null;
  const marker = `/${BUG_SCREENSHOTS_BUCKET}/`;
  const markerIndex = value.indexOf(marker);
  if (markerIndex >= 0) {
    return decodeURIComponent(value.slice(markerIndex + marker.length).split('?')[0]);
  }
  return value;
};

const attachSignedScreenshotUrl = async (report: any) => {
  const screenshotPath = extractBugScreenshotPath(report?.screenshot_url);
  if (!screenshotPath) {
    return report;
  }

  const { data, error } = await supabase.storage
    .from(BUG_SCREENSHOTS_BUCKET)
    .createSignedUrl(screenshotPath, BUG_SCREENSHOT_SIGNED_URL_SECONDS);

  if (error || !data?.signedUrl) {
    console.error('Error signing bug screenshot URL');
    return { ...report, screenshot_url: null };
  }

  return {
    ...report,
    screenshot_url: data.signedUrl,
    screenshot_path: screenshotPath
  };
};

// UPLOAD screenshot (base64)
router.post('/upload', async (req: AuthRequest, res: Response) => {
  try {
    const { base64Data, mimeType } = req.body;

    if (!base64Data) {
      return res.status(400).json({ error: 'No file data provided' });
    }

    // Convert base64 to buffer
    const base64Content = base64Data.replace(/^data:[^;]+;base64,/, '');
    const buffer = Buffer.from(base64Content, 'base64');
    if (!buffer.length || buffer.length > BUG_SCREENSHOT_MAX_BYTES) {
      return res.status(400).json({ error: 'Screenshot is empty or too large' });
    }

    const contentType = typeof mimeType === 'string' ? mimeType.toLowerCase().trim() : 'image/png';
    const fileExt = BUG_SCREENSHOT_MIME_TO_EXTENSION[contentType];
    if (!fileExt) {
      return res.status(400).json({ error: 'Unsupported screenshot type' });
    }

    const storagePath = `bug-reports/${new Date().toISOString().slice(0, 10)}/${randomUUID()}.${fileExt}`;

    const { error: uploadError } = await supabase.storage
      .from(BUG_SCREENSHOTS_BUCKET)
      .upload(storagePath, buffer, {
        contentType,
        upsert: false
      });

    if (uploadError) {
      console.error('Error uploading bug screenshot');
      return sendInternalError(res, 'Failed to upload file');
    }

    res.json({ url: storagePath, path: storagePath });
  } catch (error) {
    console.error('Error in POST /bug-reports/upload');
    sendInternalError(res);
  }
});

// CREATE bug report
router.post('/', async (req: AuthRequest, res: Response) => {
  try {
    const { 
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
        gebietsleiter_id: getAuthenticatedGlId(req.user),
        gebietsleiter_name: req.user ? `${req.user.firstName} ${req.user.lastName}`.trim() : null,
        description: description.trim(),
        screenshot_url: extractBugScreenshotPath(screenshot_url),
        page_url,
        user_agent,
        status: 'new'
      })
      .select(BUG_REPORT_SELECT)
      .single();

    if (error) {
      console.error('Error creating bug report');
      return sendInternalError(res, 'Failed to create bug report');
    }

    res.status(201).json(data);
  } catch (error) {
    console.error('Error in POST /bug-reports');
    sendInternalError(res);
  }
});

// GET all bug reports (for admin review - direct DB access for now)
router.get('/', requireAdmin, async (req: Request, res: Response) => {
  try {
    const { status } = req.query;

    let query = supabase
      .from('bug_reports')
      .select(BUG_REPORT_SELECT)
      .order('created_at', { ascending: false });

    if (status && typeof status === 'string') {
      query = query.eq('status', status);
    }

    const { data, error } = await query;

    if (error) {
      console.error('Error fetching bug reports');
      return sendInternalError(res, 'Failed to fetch bug reports');
    }

    const signedReports = await Promise.all((data || []).map(attachSignedScreenshotUrl));
    res.json(signedReports);
  } catch (error) {
    console.error('Error in GET /bug-reports');
    sendInternalError(res);
  }
});

// UPDATE bug report status
router.patch('/:id', requireAdmin, async (req: Request, res: Response) => {
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
      .select(BUG_REPORT_SELECT)
      .single();

    if (error) {
      console.error('Error updating bug report');
      return sendInternalError(res, 'Failed to update bug report');
    }

    res.json(await attachSignedScreenshotUrl(data));
  } catch (error) {
    console.error('Error in PATCH /bug-reports/:id');
    sendInternalError(res);
  }
});

export default router;
