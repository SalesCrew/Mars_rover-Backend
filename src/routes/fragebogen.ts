import express, { Router, Request, Response } from 'express';
import ExcelJS from 'exceljs';
import { spawn } from 'child_process';
import { promises as fs } from 'fs';
import os from 'os';
import path from 'path';
import { createFreshClient } from '../config/supabase';

const router: Router = express.Router();

// Request logging
router.use((req, res, next) => {
  console.log(`📋 Fragebogen Route: ${req.method} ${req.path}`);
  next();
});

// Keep Fragebogen status transitions deterministic in Vienna local date.
const getViennaDateString = (): string => {
  const formatter = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Europe/Vienna',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit'
  });
  return formatter.format(new Date()); // YYYY-MM-DD
};

const refreshFragebogenStatuses = async (freshClient: ReturnType<typeof createFreshClient>): Promise<void> => {
  const viennaDate = getViennaDateString();

  const { error: inactiveError } = await freshClient
    .from('fb_fragebogen')
    .update({ status: 'inactive' })
    .eq('archived', false)
    .neq('status', 'inactive')
    .lt('end_date', viennaDate);
  if (inactiveError) throw inactiveError;

  const { error: activeError } = await freshClient
    .from('fb_fragebogen')
    .update({ status: 'active' })
    .eq('archived', false)
    .eq('status', 'scheduled')
    .lte('start_date', viennaDate)
    .gte('end_date', viennaDate);
  if (activeError) throw activeError;
};

const runDistributionPythonExporter = async (payload: unknown): Promise<Buffer> => {
  const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'fragebogen-distribution-'));
  const inputPath = path.join(tempDir, 'input.json');
  const outputPath = path.join(tempDir, 'export.xlsx');
  const scriptPath = path.resolve(process.cwd(), 'src/exporters/fragebogen_distribution_export.py');
  const pythonCandidates = Array.from(new Set([
    process.env.PYTHON_BIN,
    'py',
    'python3',
    'python',
    'python3.13',
    'python3.12',
    'python3.11',
    'python3.10',
    '/usr/bin/python3',
    '/usr/local/bin/python3',
    '/opt/venv/bin/python',
    '/nix/var/nix/profiles/default/bin/python3'
  ].filter(Boolean))) as string[];
  const shellQuote = (value: string): string => `'${value.replace(/'/g, `'\\''`)}'`;

  const executeExporter = async (pythonBin: string): Promise<void> => {
    await new Promise<void>((resolve, reject) => {
      const child = spawn(pythonBin, [scriptPath, inputPath, outputPath], {
        cwd: process.cwd(),
        windowsHide: true
      });

      let stderr = '';
      let stdout = '';
      let timedOut = false;

      const timeoutMs = Number(process.env.FRAGEBOGEN_EXPORT_PY_TIMEOUT_MS || 90_000);
      const timeoutHandle = setTimeout(() => {
        timedOut = true;
        child.kill('SIGTERM');
      }, timeoutMs);

      child.stdout.on('data', (chunk) => {
        stdout += String(chunk);
      });

      child.stderr.on('data', (chunk) => {
        stderr += String(chunk);
      });

      child.on('error', (error: NodeJS.ErrnoException) => {
        clearTimeout(timeoutHandle);
        if (error.code === 'ENOENT') {
          reject(new Error(`Python runtime "${pythonBin}" wurde nicht gefunden.`));
          return;
        }
        reject(error);
      });

      child.on('close', (code) => {
        clearTimeout(timeoutHandle);
        if (timedOut) {
          reject(new Error(`Python Export Timeout (${timeoutMs}ms).`));
          return;
        }
        if (code !== 0) {
          reject(new Error((stderr || stdout || `Python exporter failed with code ${code}`).trim()));
          return;
        }
        resolve();
      });
    });
  };

  const executeExporterViaShell = async (): Promise<void> => {
    await new Promise<void>((resolve, reject) => {
      const timeoutMs = Number(process.env.FRAGEBOGEN_EXPORT_PY_TIMEOUT_MS || 90_000);
      const command = [
        'python3',
        'python',
        'python3.13',
        'python3.12',
        'python3.11',
        '/usr/bin/python3',
        '/usr/local/bin/python3',
        '/opt/venv/bin/python',
        '/nix/var/nix/profiles/default/bin/python3'
      ].map((bin) => `${bin} ${shellQuote(scriptPath)} ${shellQuote(inputPath)} ${shellQuote(outputPath)}`).join(' || ');
      const child = spawn('sh', ['-lc', command], {
        cwd: process.cwd(),
        windowsHide: true
      });

      let stderr = '';
      let stdout = '';
      let timedOut = false;
      const timeoutHandle = setTimeout(() => {
        timedOut = true;
        child.kill('SIGTERM');
      }, timeoutMs);

      child.stdout.on('data', (chunk) => {
        stdout += String(chunk);
      });

      child.stderr.on('data', (chunk) => {
        stderr += String(chunk);
      });

      child.on('error', (error) => {
        clearTimeout(timeoutHandle);
        reject(error);
      });

      child.on('close', (code) => {
        clearTimeout(timeoutHandle);
        if (timedOut) {
          reject(new Error(`Python Export Timeout (${timeoutMs}ms).`));
          return;
        }
        if (code !== 0) {
          reject(new Error((stderr || stdout || `Python shell exporter failed with code ${code}`).trim()));
          return;
        }
        resolve();
      });
    });
  };

  try {
    await fs.writeFile(inputPath, JSON.stringify(payload), 'utf-8');

    let lastError: Error | null = null;
    for (const pythonBin of pythonCandidates) {
      try {
        await executeExporter(pythonBin);
        const fileBuffer = await fs.readFile(outputPath);
        return fileBuffer;
      } catch (error: any) {
        lastError = error;
      }
    }

    try {
      await executeExporterViaShell();
      const fileBuffer = await fs.readFile(outputPath);
      return fileBuffer;
    } catch (error: any) {
      lastError = error;
    }

    if (lastError) {
      throw new Error(
        `Python Export konnte nicht gestartet werden. Versucht: ${pythonCandidates.join(', ')}, sh -lc python3/python. Letzter Fehler: ${lastError.message}`
      );
    }
    throw new Error('Kein Python Runtime-Kandidat konfiguriert.');
  } finally {
    await fs.rm(tempDir, { recursive: true, force: true });
  }
};

// ============================================================================
// QUESTIONS API - /api/fragebogen/questions
// ============================================================================

/**
 * POST /api/fragebogen/questions/upload-image
 * Upload an image for a question to Supabase storage
 */
router.post('/questions/upload-image', async (req: Request, res: Response) => {
  try {
    const { image, filename } = req.body;
    if (!image) return res.status(400).json({ error: 'No image provided' });

    let base64Data = image;
    let contentType = 'image/jpeg';
    if (image.startsWith('data:')) {
      const matches = image.match(/^data:([^;]+);base64,(.+)$/);
      if (matches) { contentType = matches[1]; base64Data = matches[2]; }
    }

    const buffer = Buffer.from(base64Data, 'base64');
    const ext = contentType.split('/')[1] || 'jpg';
    const finalFilename = filename || `${Date.now()}-${Math.random().toString(36).substring(2, 8)}.${ext}`;
    const filePath = `questions/${finalFilename}`;

    const storageClient = createFreshClient();
    const { data, error } = await storageClient.storage
      .from('question-images')
      .upload(filePath, buffer, { contentType, upsert: true });

    if (error) { console.error('❌ Question image upload error:', error); return res.status(500).json({ error: error.message }); }

    const { data: urlData } = storageClient.storage.from('question-images').getPublicUrl(data.path);
    console.log('✅ Question image uploaded:', urlData.publicUrl);
    res.json({ success: true, url: urlData.publicUrl });
  } catch (error: any) {
    console.error('Error uploading question image:', error);
    res.status(500).json({ error: error.message || 'Failed to upload image' });
  }
});

/**
 * GET /api/fragebogen/questions
 * List all questions with optional filtering
 */
router.get('/questions', async (req: Request, res: Response) => {
  try {
    const freshClient = createFreshClient();
    const { type, is_template, archived, search } = req.query;
    
    let query = freshClient
      .from('fb_questions')
      .select('*')
      .order('created_at', { ascending: false });
    
    // Apply filters
    if (type) {
      query = query.eq('type', type);
    }
    
    if (is_template !== undefined) {
      query = query.eq('is_template', is_template === 'true');
    }
    
    if (archived !== undefined) {
      query = query.eq('archived', archived === 'true');
    } else {
      // Default: don't show archived
      query = query.eq('archived', false);
    }
    
    if (search) {
      query = query.ilike('question_text', `%${search}%`);
    }
    
    const { data, error } = await query;
    
    if (error) throw error;
    
    res.json(data);
  } catch (error: any) {
    console.error('Error fetching questions:', error);
    res.status(500).json({ error: error.message || 'Failed to fetch questions' });
  }
});

/**
 * GET /api/fragebogen/questions/:id
 * Get a single question by ID
 */
router.get('/questions/:id', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    const freshClient = createFreshClient();
    
    const { data, error } = await freshClient
      .from('fb_questions')
      .select('*')
      .eq('id', id)
      .single();
    
    if (error) throw error;
    
    if (!data) {
      return res.status(404).json({ error: 'Question not found' });
    }
    
    res.json(data);
  } catch (error: any) {
    console.error('Error fetching question:', error);
    res.status(500).json({ error: error.message || 'Failed to fetch question' });
  }
});

/**
 * POST /api/fragebogen/questions
 * Create a new question. Ensures options and matrix entries have stable IDs.
 */
router.post('/questions', async (req: Request, res: Response) => {
  try {
    const freshClient = createFreshClient();
    const {
      type,
      question_text,
      instruction,
      is_template,
      options,
      likert_scale,
      matrix_config,
      numeric_constraints,
      slider_config,
      created_by
    } = req.body;
    
    if (!type || !question_text) {
      return res.status(400).json({ error: 'type and question_text are required' });
    }
    
    if ((type === 'single_choice' || type === 'multiple_choice') && (!options || !Array.isArray(options))) {
      return res.status(400).json({ error: 'options array is required for choice questions' });
    }
    if (type === 'likert' && !likert_scale) {
      return res.status(400).json({ error: 'likert_scale is required for likert questions' });
    }
    if (type === 'matrix' && !matrix_config) {
      return res.status(400).json({ error: 'matrix_config is required for matrix questions' });
    }

    // Ensure options carry stable IDs — accept both string[] (legacy) and {id,label}[]
    let normalisedOptions: any[] | null = null;
    if (options && Array.isArray(options)) {
      normalisedOptions = options.map((opt: any, idx: number) => {
        if (typeof opt === 'string') {
          return { id: `opt_${idx}_${Date.now().toString(36)}`, label: opt };
        }
        if (!opt.id) {
          return { ...opt, id: `opt_${idx}_${Date.now().toString(36)}` };
        }
        return opt;
      });
    }

    // Ensure matrix rows and columns carry stable IDs
    let normalisedMatrix: any = matrix_config || null;
    if (normalisedMatrix) {
      normalisedMatrix = {
        rows: (normalisedMatrix.rows || []).map((r: any, idx: number) => {
          if (typeof r === 'string') return { id: `row_${idx}_${Date.now().toString(36)}`, label: r };
          if (!r.id) return { ...r, id: `row_${idx}_${Date.now().toString(36)}` };
          return r;
        }),
        columns: (normalisedMatrix.columns || []).map((c: any, idx: number) => {
          if (typeof c === 'string') return { id: `col_${idx}_${Date.now().toString(36)}`, label: c };
          if (!c.id) return { ...c, id: `col_${idx}_${Date.now().toString(36)}` };
          return c;
        }),
      };
    }
    
    const { data, error } = await freshClient
      .from('fb_questions')
      .insert({
        type,
        question_text,
        instruction: instruction || null,
        is_template: is_template || false,
        options: normalisedOptions,
        likert_scale: likert_scale || null,
        matrix_config: normalisedMatrix,
        numeric_constraints: numeric_constraints || null,
        slider_config: slider_config || null,
        images: req.body.images || [],
        created_by: created_by || null
      })
      .select()
      .single();
    
    if (error) throw error;
    
    console.log(`✅ Created question: ${data.id}`);
    res.status(201).json(data);
  } catch (error: any) {
    console.error('Error creating question:', error);
    res.status(500).json({ error: error.message || 'Failed to create question' });
  }
});

/**
 * PUT /api/fragebogen/questions/:id
 * Update an existing question. Preserves existing option/matrix IDs; assigns new
 * stable IDs to any item that arrives without one.
 */
router.put('/questions/:id', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    const freshClient = createFreshClient();
    const updates = { ...req.body };
    
    delete updates.id;
    delete updates.created_at;
    delete updates.created_by;

    // Normalise options if present
    if (updates.options && Array.isArray(updates.options)) {
      updates.options = updates.options.map((opt: any, idx: number) => {
        if (typeof opt === 'string') return { id: `opt_${idx}_${Date.now().toString(36)}`, label: opt };
        if (!opt.id) return { ...opt, id: `opt_${idx}_${Date.now().toString(36)}` };
        return opt;
      });
    }

    // Normalise matrix_config if present
    if (updates.matrix_config) {
      updates.matrix_config = {
        rows: (updates.matrix_config.rows || []).map((r: any, idx: number) => {
          if (typeof r === 'string') return { id: `row_${idx}_${Date.now().toString(36)}`, label: r };
          if (!r.id) return { ...r, id: `row_${idx}_${Date.now().toString(36)}` };
          return r;
        }),
        columns: (updates.matrix_config.columns || []).map((c: any, idx: number) => {
          if (typeof c === 'string') return { id: `col_${idx}_${Date.now().toString(36)}`, label: c };
          if (!c.id) return { ...c, id: `col_${idx}_${Date.now().toString(36)}` };
          return c;
        }),
      };
    }
    
    const { data, error } = await freshClient
      .from('fb_questions')
      .update(updates)
      .eq('id', id)
      .select()
      .single();
    
    if (error) throw error;
    
    if (!data) {
      return res.status(404).json({ error: 'Question not found' });
    }
    
    console.log(`✅ Updated question: ${id}`);
    res.json(data);
  } catch (error: any) {
    console.error('Error updating question:', error);
    res.status(500).json({ error: error.message || 'Failed to update question' });
  }
});

/**
 * DELETE /api/fragebogen/questions/:id
 * Soft delete (archive) a question
 */
router.delete('/questions/:id', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    const freshClient = createFreshClient();
    
    const { data, error } = await freshClient
      .from('fb_questions')
      .update({ archived: true })
      .eq('id', id)
      .select()
      .single();
    
    if (error) throw error;
    
    if (!data) {
      return res.status(404).json({ error: 'Question not found' });
    }
    
    console.log(`✅ Archived question: ${id}`);
    res.json({ success: true, data });
  } catch (error: any) {
    console.error('Error archiving question:', error);
    res.status(500).json({ error: error.message || 'Failed to archive question' });
  }
});

/**
 * GET /api/fragebogen/questions/stats/:id
 * Get usage statistics for a question
 */
router.get('/questions/stats/:id', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    const freshClient = createFreshClient();
    
    // Get question with usage stats from view
    const { data, error } = await freshClient
      .from('fb_questions_usage')
      .select('*')
      .eq('id', id)
      .single();
    
    if (error) throw error;
    
    if (!data) {
      return res.status(404).json({ error: 'Question not found' });
    }
    
    res.json(data);
  } catch (error: any) {
    console.error('Error fetching question stats:', error);
    res.status(500).json({ error: error.message || 'Failed to fetch question stats' });
  }
});

/**
 * GET /api/fragebogen/questions/:id/module-count
 * Get the number of modules that use this question
 * Used for copy-on-write logic - if a question is used by multiple modules,
 * editing it should create a new question instead of modifying the shared one
 */
router.get('/questions/:id/module-count', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    const freshClient = createFreshClient();
    
    // Count how many modules use this question
    const { count, error } = await freshClient
      .from('fb_module_questions')
      .select('*', { count: 'exact', head: true })
      .eq('question_id', id);
    
    if (error) throw error;
    
    res.json({ questionId: id, moduleCount: count || 0 });
  } catch (error: any) {
    console.error('Error fetching question module count:', error);
    res.status(500).json({ error: error.message || 'Failed to fetch question module count' });
  }
});

// ============================================================================
// MODULES API - /api/fragebogen/modules
// ============================================================================

/**
 * GET /api/fragebogen/modules
 * List all modules
 */
router.get('/modules', async (req: Request, res: Response) => {
  try {
    const freshClient = createFreshClient();
    const { archived, search } = req.query;
    
    let query = freshClient
      .from('fb_modules_overview')
      .select('*')
      .order('created_at', { ascending: false });
    
    if (archived !== undefined) {
      query = query.eq('archived', archived === 'true');
    } else {
      query = query.eq('archived', false);
    }
    
    if (search) {
      query = query.ilike('name', `%${search}%`);
    }
    
    const { data, error } = await query;
    
    if (error) throw error;
    
    res.json(data);
  } catch (error: any) {
    console.error('Error fetching modules:', error);
    res.status(500).json({ error: error.message || 'Failed to fetch modules' });
  }
});

/**
 * GET /api/fragebogen/modules/:id
 * Get a module with its questions and rules
 */
router.get('/modules/:id', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    const freshClient = createFreshClient();
    
    // Get module
    const { data: module, error: moduleError } = await freshClient
      .from('fb_modules')
      .select('*')
      .eq('id', id)
      .single();
    
    if (moduleError) throw moduleError;
    
    if (!module) {
      return res.status(404).json({ error: 'Module not found' });
    }
    
    // Get questions with their details
    const { data: moduleQuestions, error: questionsError } = await freshClient
      .from('fb_module_questions')
      .select(`
        id,
        order_index,
        required,
        local_id,
        question:fb_questions (*)
      `)
      .eq('module_id', id)
      .order('order_index', { ascending: true });
    
    if (questionsError) throw questionsError;
    
    // Get rules
    const { data: rules, error: rulesError } = await freshClient
      .from('fb_module_rules')
      .select('*')
      .eq('module_id', id);
    
    if (rulesError) throw rulesError;
    
    res.json({
      ...module,
      questions: moduleQuestions || [],
      rules: rules || []
    });
  } catch (error: any) {
    console.error('Error fetching module:', error);
    res.status(500).json({ error: error.message || 'Failed to fetch module' });
  }
});

/**
 * POST /api/fragebogen/modules
 * Create a new module with questions and rules
 */
router.post('/modules', async (req: Request, res: Response) => {
  try {
    const freshClient = createFreshClient();
    const { name, description, questions, rules, created_by } = req.body;
    
    if (!name) {
      return res.status(400).json({ error: 'name is required' });
    }
    
    // Create module
    const { data: module, error: moduleError } = await freshClient
      .from('fb_modules')
      .insert({
        name,
        description: description || null,
        created_by: created_by || null
      })
      .select()
      .single();
    
    if (moduleError) throw moduleError;
    
    // Add questions if provided
    if (questions && Array.isArray(questions) && questions.length > 0) {
      const moduleQuestionsToInsert = questions.map((q: any, index: number) => ({
        module_id: module.id,
        question_id: q.question_id,
        order_index: q.order_index ?? index,
        required: q.required ?? true,
        local_id: q.local_id || `q${index + 1}`
      }));
      
      const { error: insertQuestionsError } = await freshClient
        .from('fb_module_questions')
        .insert(moduleQuestionsToInsert);
      
      if (insertQuestionsError) throw insertQuestionsError;
    }
    
    // Add rules if provided
    if (rules && Array.isArray(rules) && rules.length > 0) {
      const rulesToInsert = rules.map((r: any) => ({
        module_id: module.id,
        trigger_local_id: r.trigger_local_id,
        trigger_answer: r.trigger_answer,
        operator: r.operator || 'equals',
        trigger_answer_max: r.trigger_answer_max || null,
        action: r.action,
        target_local_ids: r.target_local_ids
      }));
      
      const { error: insertRulesError } = await freshClient
        .from('fb_module_rules')
        .insert(rulesToInsert);
      
      if (insertRulesError) throw insertRulesError;
    }
    
    console.log(`✅ Created module: ${module.id}`);
    res.status(201).json(module);
  } catch (error: any) {
    console.error('Error creating module:', error);
    res.status(500).json({ error: error.message || 'Failed to create module' });
  }
});

/**
 * PUT /api/fragebogen/modules/:id
 * Update a module, its questions, and rules
 */
router.put('/modules/:id', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    const freshClient = createFreshClient();
    const { name, description, questions, rules } = req.body;
    
    // Update module basic info
    const moduleUpdates: any = {};
    if (name !== undefined) moduleUpdates.name = name;
    if (description !== undefined) moduleUpdates.description = description;
    
    if (Object.keys(moduleUpdates).length > 0) {
      const { error: moduleError } = await freshClient
        .from('fb_modules')
        .update(moduleUpdates)
        .eq('id', id);
      
      if (moduleError) throw moduleError;
    }
    
    // Update questions if provided
    if (questions && Array.isArray(questions)) {
      // Delete existing questions
      const { error: deleteQuestionsError } = await freshClient
        .from('fb_module_questions')
        .delete()
        .eq('module_id', id);
      
      if (deleteQuestionsError) throw deleteQuestionsError;
      
      // Insert new questions
      if (questions.length > 0) {
        const moduleQuestionsToInsert = questions.map((q: any, index: number) => ({
          module_id: id,
          question_id: q.question_id,
          order_index: q.order_index ?? index,
          required: q.required ?? true,
          local_id: q.local_id || `q${index + 1}`
        }));
        
        const { error: insertQuestionsError } = await freshClient
          .from('fb_module_questions')
          .insert(moduleQuestionsToInsert);
        
        if (insertQuestionsError) throw insertQuestionsError;
      }
    }
    
    // Update rules if provided
    if (rules && Array.isArray(rules)) {
      // Delete existing rules
      const { error: deleteRulesError } = await freshClient
        .from('fb_module_rules')
        .delete()
        .eq('module_id', id);
      
      if (deleteRulesError) throw deleteRulesError;
      
      // Insert new rules
      if (rules.length > 0) {
        const rulesToInsert = rules.map((r: any) => ({
          module_id: id,
          trigger_local_id: r.trigger_local_id,
          trigger_answer: r.trigger_answer,
          operator: r.operator || 'equals',
          trigger_answer_max: r.trigger_answer_max || null,
          action: r.action,
          target_local_ids: r.target_local_ids
        }));
        
        const { error: insertRulesError } = await freshClient
          .from('fb_module_rules')
          .insert(rulesToInsert);
        
        if (insertRulesError) throw insertRulesError;
      }
    }
    
    // Fetch updated module
    const { data: updatedModule, error: fetchError } = await freshClient
      .from('fb_modules')
      .select('*')
      .eq('id', id)
      .single();
    
    if (fetchError) throw fetchError;
    
    console.log(`✅ Updated module: ${id}`);
    res.json(updatedModule);
  } catch (error: any) {
    console.error('Error updating module:', error);
    res.status(500).json({ error: error.message || 'Failed to update module' });
  }
});

/**
 * POST /api/fragebogen/modules/:id/duplicate
 * Duplicate a module
 */
router.post('/modules/:id/duplicate', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    const freshClient = createFreshClient();
    const { new_name } = req.body;
    
    // Get original module
    const { data: original, error: fetchError } = await freshClient
      .from('fb_modules')
      .select('*')
      .eq('id', id)
      .single();
    
    if (fetchError) throw fetchError;
    
    if (!original) {
      return res.status(404).json({ error: 'Module not found' });
    }
    
    // Create new module
    const { data: newModule, error: createError } = await freshClient
      .from('fb_modules')
      .insert({
        name: new_name || `Kopie von ${original.name}`,
        description: original.description,
        created_by: original.created_by
      })
      .select()
      .single();
    
    if (createError) throw createError;
    
    // Get original questions
    const { data: originalQuestions, error: questionsError } = await freshClient
      .from('fb_module_questions')
      .select('*')
      .eq('module_id', id);
    
    if (questionsError) throw questionsError;
    
    // Copy questions
    if (originalQuestions && originalQuestions.length > 0) {
      const newQuestions = originalQuestions.map(q => ({
        module_id: newModule.id,
        question_id: q.question_id,
        order_index: q.order_index,
        required: q.required,
        local_id: q.local_id
      }));
      
      const { error: insertQuestionsError } = await freshClient
        .from('fb_module_questions')
        .insert(newQuestions);
      
      if (insertQuestionsError) throw insertQuestionsError;
    }
    
    // Get original rules
    const { data: originalRules, error: rulesError } = await freshClient
      .from('fb_module_rules')
      .select('*')
      .eq('module_id', id);
    
    if (rulesError) throw rulesError;
    
    // Copy rules
    if (originalRules && originalRules.length > 0) {
      const newRules = originalRules.map(r => ({
        module_id: newModule.id,
        trigger_local_id: r.trigger_local_id,
        trigger_answer: r.trigger_answer,
        operator: r.operator,
        trigger_answer_max: r.trigger_answer_max,
        action: r.action,
        target_local_ids: r.target_local_ids
      }));
      
      const { error: insertRulesError } = await freshClient
        .from('fb_module_rules')
        .insert(newRules);
      
      if (insertRulesError) throw insertRulesError;
    }
    
    console.log(`✅ Duplicated module ${id} -> ${newModule.id}`);
    res.status(201).json(newModule);
  } catch (error: any) {
    console.error('Error duplicating module:', error);
    res.status(500).json({ error: error.message || 'Failed to duplicate module' });
  }
});

/**
 * PUT /api/fragebogen/modules/:id/archive
 * Archive or unarchive a module
 */
router.put('/modules/:id/archive', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    const freshClient = createFreshClient();
    const { archived } = req.body;
    
    const { data, error } = await freshClient
      .from('fb_modules')
      .update({ archived: archived ?? true })
      .eq('id', id)
      .select()
      .single();
    
    if (error) throw error;
    
    if (!data) {
      return res.status(404).json({ error: 'Module not found' });
    }
    
    console.log(`✅ ${archived ? 'Archived' : 'Unarchived'} module: ${id}`);
    res.json(data);
  } catch (error: any) {
    console.error('Error archiving module:', error);
    res.status(500).json({ error: error.message || 'Failed to archive module' });
  }
});

/**
 * DELETE /api/fragebogen/modules/:id
 * Soft delete (archive) a module
 */
router.delete('/modules/:id', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    const freshClient = createFreshClient();
    
    const { data, error } = await freshClient
      .from('fb_modules')
      .update({ archived: true })
      .eq('id', id)
      .select()
      .single();
    
    if (error) throw error;
    
    if (!data) {
      return res.status(404).json({ error: 'Module not found' });
    }
    
    console.log(`✅ Deleted (archived) module: ${id}`);
    res.json({ success: true, data });
  } catch (error: any) {
    console.error('Error deleting module:', error);
    res.status(500).json({ error: error.message || 'Failed to delete module' });
  }
});

/**
 * GET /api/fragebogen/modules/:id/usage
 * Get detailed usage information for a module (which fragebögen use it)
 */
router.get('/modules/:id/usage', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    const freshClient = createFreshClient();
    
    // Get all fragebogen that use this module
    const { data: fragebogenModules, error: fmError } = await freshClient
      .from('fb_fragebogen_modules')
      .select('fragebogen_id')
      .eq('module_id', id);
    
    if (fmError) throw fmError;
    
    if (!fragebogenModules || fragebogenModules.length === 0) {
      return res.json({ activeFragebogen: [], inactiveFragebogen: [], totalUsage: 0 });
    }
    
    const fragebogenIds = fragebogenModules.map(fm => fm.fragebogen_id);
    
    // Get fragebogen details
    const { data: fragebogenList, error: fError } = await freshClient
      .from('fb_fragebogen')
      .select('id, name, status, archived')
      .in('id', fragebogenIds);
    
    if (fError) throw fError;
    
    // Separate into active and inactive
    const activeFragebogen = (fragebogenList || []).filter(f => f.status === 'active' && !f.archived);
    const inactiveFragebogen = (fragebogenList || []).filter(f => f.status !== 'active' || f.archived);
    
    res.json({
      activeFragebogen,
      inactiveFragebogen,
      totalUsage: fragebogenList?.length || 0
    });
  } catch (error: any) {
    console.error('Error fetching module usage:', error);
    res.status(500).json({ error: error.message || 'Failed to fetch module usage' });
  }
});

/**
 * DELETE /api/fragebogen/modules/:id/permanent
 * Permanently delete a module and optionally its questions.
 * Blocked if any question in this module has existing answers.
 */
router.delete('/modules/:id/permanent', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    const { deleteQuestions } = req.query;
    const freshClient = createFreshClient();
    
    // Get all question IDs associated with this module
    const { data: moduleQuestions, error: mqError } = await freshClient
      .from('fb_module_questions')
      .select('question_id')
      .eq('module_id', id);
    
    if (mqError) throw mqError;
    
    const questionIds = (moduleQuestions || []).map((mq: any) => mq.question_id);

    // Guard: block deletion if any question in this module has saved answers
    if (questionIds.length > 0) {
      const { count: answerCount, error: acError } = await freshClient
        .from('fb_response_answers')
        .select('*', { count: 'exact', head: true })
        .in('question_id', questionIds);
      if (acError) throw acError;
      if ((answerCount ?? 0) > 0) {
        return res.status(409).json({
          error: 'Cannot permanently delete this module because it contains questions with saved answers. Archive it instead.'
        });
      }
    }
    
    // Delete module rules
    const { error: rulesError } = await freshClient
      .from('fb_module_rules')
      .delete()
      .eq('module_id', id);
    
    if (rulesError) throw rulesError;
    
    // Delete module-question associations
    const { error: mqDeleteError } = await freshClient
      .from('fb_module_questions')
      .delete()
      .eq('module_id', id);
    
    if (mqDeleteError) throw mqDeleteError;
    
    // Remove module from any fragebogen
    const { error: fmError } = await freshClient
      .from('fb_fragebogen_modules')
      .delete()
      .eq('module_id', id);
    
    if (fmError) throw fmError;
    
    // Delete the module itself
    const { error: moduleError } = await freshClient
      .from('fb_modules')
      .delete()
      .eq('id', id);
    
    if (moduleError) throw moduleError;
    
    // If deleteQuestions is true, delete the questions too
    // But only if they are not used by any other module
    if (deleteQuestions === 'true' && questionIds.length > 0) {
      for (const questionId of questionIds) {
        // Check if question is used by other modules
        const { count, error: countError } = await freshClient
          .from('fb_module_questions')
          .select('*', { count: 'exact', head: true })
          .eq('question_id', questionId);
        
        if (countError) throw countError;
        
        // Only delete if not used by any other module
        if (count === 0) {
          await freshClient
            .from('fb_questions')
            .delete()
            .eq('id', questionId);
        }
      }
    }
    
    console.log(`✅ Permanently deleted module: ${id}${deleteQuestions === 'true' ? ' (with questions)' : ''}`);
    res.json({ success: true });
  } catch (error: any) {
    console.error('Error permanently deleting module:', error);
    res.status(500).json({ error: error.message || 'Failed to permanently delete module' });
  }
});

/**
 * GET /api/fragebogen/modules/stats/:id
 * Get usage statistics for a module
 */
router.get('/modules/stats/:id', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    const freshClient = createFreshClient();
    
    const { data, error } = await freshClient
      .from('fb_modules_overview')
      .select('*')
      .eq('id', id)
      .single();
    
    if (error) throw error;
    
    if (!data) {
      return res.status(404).json({ error: 'Module not found' });
    }
    
    res.json(data);
  } catch (error: any) {
    console.error('Error fetching module stats:', error);
    res.status(500).json({ error: error.message || 'Failed to fetch module stats' });
  }
});

// ============================================================================
// FRAGEBOGEN API - /api/fragebogen/fragebogen
// ============================================================================

/**
 * GET /api/fragebogen/fragebogen
 * List all fragebogen with their module_ids and market_ids
 */
router.get('/fragebogen', async (req: Request, res: Response) => {
  try {
    const freshClient = createFreshClient();
    const { status, archived, search } = req.query;
    await refreshFragebogenStatuses(freshClient);
    
    // Get basic fragebogen data from overview
    let query = freshClient
      .from('fb_fragebogen_overview')
      .select('*')
      .order('created_at', { ascending: false });
    
    if (status) {
      query = query.eq('status', status);
    }
    
    if (archived !== undefined) {
      query = query.eq('archived', archived === 'true');
    }
    // Note: If archived is not specified, return ALL fragebogen (both archived and non-archived)
    
    if (search) {
      query = query.ilike('name', `%${search}%`);
    }
    
    const { data: fragebogenList, error: fragebogenError } = await query;
    
    if (fragebogenError) throw fragebogenError;
    
    if (!fragebogenList || fragebogenList.length === 0) {
      return res.json([]);
    }
    
    // Get all fragebogen IDs
    const fragebogenIds = fragebogenList.map(f => f.id);
    
    // Fetch modules for all fragebogen in one query
    const { data: allModules, error: modulesError } = await freshClient
      .from('fb_fragebogen_modules')
      .select('fragebogen_id, module_id, order_index')
      .in('fragebogen_id', fragebogenIds)
      .order('order_index', { ascending: true });
    
    if (modulesError) throw modulesError;
    
    // Fetch markets for all fragebogen in one query
    const { data: allMarkets, error: marketsError } = await freshClient
      .from('fb_fragebogen_markets')
      .select('fragebogen_id, market_id')
      .in('fragebogen_id', fragebogenIds);
    
    if (marketsError) throw marketsError;
    
    // Group modules and markets by fragebogen_id
    const modulesByFragebogen: Record<string, string[]> = {};
    const marketsByFragebogen: Record<string, string[]> = {};
    
    (allModules || []).forEach(m => {
      if (!modulesByFragebogen[m.fragebogen_id]) {
        modulesByFragebogen[m.fragebogen_id] = [];
      }
      modulesByFragebogen[m.fragebogen_id].push(m.module_id);
    });
    
    (allMarkets || []).forEach(m => {
      if (!marketsByFragebogen[m.fragebogen_id]) {
        marketsByFragebogen[m.fragebogen_id] = [];
      }
      marketsByFragebogen[m.fragebogen_id].push(m.market_id);
    });
    
    // Combine the data
    const enrichedFragebogen = fragebogenList.map(f => ({
      ...f,
      module_ids: modulesByFragebogen[f.id] || [],
      market_ids: marketsByFragebogen[f.id] || []
    }));
    
    res.json(enrichedFragebogen);
  } catch (error: any) {
    console.error('Error fetching fragebogen:', error);
    res.status(500).json({ error: error.message || 'Failed to fetch fragebogen' });
  }
});

/**
 * GET /api/fragebogen/fragebogen/:id
 * Get a fragebogen with its modules and questions
 */
router.get('/fragebogen/:id', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    const freshClient = createFreshClient();
    await refreshFragebogenStatuses(freshClient);
    
    // Get fragebogen
    const { data: fragebogen, error: fragebogenError } = await freshClient
      .from('fb_fragebogen')
      .select('*')
      .eq('id', id)
      .single();
    
    if (fragebogenError) throw fragebogenError;
    
    if (!fragebogen) {
      return res.status(404).json({ error: 'Fragebogen not found' });
    }
    
    // Get modules with their order
    const { data: fragebogenModules, error: modulesError } = await freshClient
      .from('fb_fragebogen_modules')
      .select(`
        id,
        order_index,
        module:fb_modules (
          id,
          name,
          description
        )
      `)
      .eq('fragebogen_id', id)
      .order('order_index', { ascending: true });
    
    if (modulesError) throw modulesError;
    
    // Get markets
    const { data: fragebogenMarkets, error: marketsError } = await freshClient
      .from('fb_fragebogen_markets')
      .select('market_id')
      .eq('fragebogen_id', id);
    
    if (marketsError) throw marketsError;
    
    // For each module, get questions and rules
    const modulesWithDetails = await Promise.all(
      (fragebogenModules || []).map(async (fm: any) => {
        const moduleId = fm.module?.id;
        if (!moduleId) return fm;
        
        // Get questions
        const { data: questions } = await freshClient
          .from('fb_module_questions')
          .select(`
            id,
            order_index,
            required,
            local_id,
            question:fb_questions (*)
          `)
          .eq('module_id', moduleId)
          .order('order_index', { ascending: true });
        
        // Get rules
        const { data: rules } = await freshClient
          .from('fb_module_rules')
          .select('*')
          .eq('module_id', moduleId);
        
        // Normalise question.images to always be a safe array
        const normalisedQuestions = (questions || []).map((q: any) => ({
          ...q,
          question: q.question
            ? { ...q.question, images: Array.isArray(q.question.images) ? q.question.images : [] }
            : q.question
        }));

        return {
          ...fm,
          module: {
            ...fm.module,
            questions: normalisedQuestions,
            rules: rules || []
          }
        };
      })
    );
    
    res.json({
      ...fragebogen,
      modules: modulesWithDetails,
      market_ids: (fragebogenMarkets || []).map((fm: any) => fm.market_id)
    });
  } catch (error: any) {
    console.error('Error fetching fragebogen:', error);
    res.status(500).json({ error: error.message || 'Failed to fetch fragebogen' });
  }
});

/**
 * POST /api/fragebogen/fragebogen
 * Create a new fragebogen
 */
router.post('/fragebogen', async (req: Request, res: Response) => {
  try {
    const freshClient = createFreshClient();
    const { 
      name, 
      description, 
      start_date, 
      end_date, 
      module_ids, 
      market_ids,
      created_by 
    } = req.body;
    
    if (!name || !start_date || !end_date) {
      return res.status(400).json({ error: 'name, start_date, and end_date are required' });
    }
    
    // Determine initial status
    const now = new Date();
    const startDate = new Date(start_date);
    const endDate = new Date(end_date);
    
    let status = 'scheduled';
    if (startDate <= now && endDate >= now) {
      status = 'active';
    } else if (endDate < now) {
      status = 'inactive';
    }
    
    // Create fragebogen
    const { data: fragebogen, error: fragebogenError } = await freshClient
      .from('fb_fragebogen')
      .insert({
        name,
        description: description || null,
        start_date,
        end_date,
        status,
        created_by: created_by || null
      })
      .select()
      .single();
    
    if (fragebogenError) throw fragebogenError;
    
    // Add modules if provided
    if (module_ids && Array.isArray(module_ids) && module_ids.length > 0) {
      const modulesToInsert = module_ids.map((moduleId: string, index: number) => ({
        fragebogen_id: fragebogen.id,
        module_id: moduleId,
        order_index: index
      }));
      
      const { error: modulesError } = await freshClient
        .from('fb_fragebogen_modules')
        .insert(modulesToInsert);
      
      if (modulesError) throw modulesError;
    }
    
    // Add markets if provided
    if (market_ids && Array.isArray(market_ids) && market_ids.length > 0) {
      const marketsToInsert = market_ids.map((marketId: string) => ({
        fragebogen_id: fragebogen.id,
        market_id: marketId
      }));
      
      const { error: marketsError } = await freshClient
        .from('fb_fragebogen_markets')
        .insert(marketsToInsert);
      
      if (marketsError) throw marketsError;
    }
    
    console.log(`✅ Created fragebogen: ${fragebogen.id}`);
    res.status(201).json(fragebogen);
  } catch (error: any) {
    console.error('Error creating fragebogen:', error);
    res.status(500).json({ error: error.message || 'Failed to create fragebogen' });
  }
});

/**
 * PUT /api/fragebogen/fragebogen/:id
 * Update a fragebogen
 */
router.put('/fragebogen/:id', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    const freshClient = createFreshClient();
    const { name, description, start_date, end_date, status, module_ids, market_ids } = req.body;
    
    // Update fragebogen basic info
    const updates: any = {};
    if (name !== undefined) updates.name = name;
    if (description !== undefined) updates.description = description;
    if (start_date !== undefined) updates.start_date = start_date;
    if (end_date !== undefined) updates.end_date = end_date;
    if (status !== undefined) updates.status = status;
    
    if (Object.keys(updates).length > 0) {
      const { error: updateError } = await freshClient
        .from('fb_fragebogen')
        .update(updates)
        .eq('id', id);
      
      if (updateError) throw updateError;
    }
    
    // Update modules if provided
    if (module_ids && Array.isArray(module_ids)) {
      // Delete existing modules
      const { error: deleteModulesError } = await freshClient
        .from('fb_fragebogen_modules')
        .delete()
        .eq('fragebogen_id', id);
      
      if (deleteModulesError) throw deleteModulesError;
      
      // Insert new modules
      if (module_ids.length > 0) {
        const modulesToInsert = module_ids.map((moduleId: string, index: number) => ({
          fragebogen_id: id,
          module_id: moduleId,
          order_index: index
        }));
        
        const { error: insertModulesError } = await freshClient
          .from('fb_fragebogen_modules')
          .insert(modulesToInsert);
        
        if (insertModulesError) throw insertModulesError;
      }
    }
    
    // Update markets if provided
    if (market_ids && Array.isArray(market_ids)) {
      // Delete existing markets
      const { error: deleteMarketsError } = await freshClient
        .from('fb_fragebogen_markets')
        .delete()
        .eq('fragebogen_id', id);
      
      if (deleteMarketsError) throw deleteMarketsError;
      
      // Insert new markets
      if (market_ids.length > 0) {
        const marketsToInsert = market_ids.map((marketId: string) => ({
          fragebogen_id: id,
          market_id: marketId
        }));
        
        const { error: insertMarketsError } = await freshClient
          .from('fb_fragebogen_markets')
          .insert(marketsToInsert);
        
        if (insertMarketsError) throw insertMarketsError;
      }
    }
    
    // Fetch updated fragebogen
    const { data: updatedFragebogen, error: fetchError } = await freshClient
      .from('fb_fragebogen')
      .select('*')
      .eq('id', id)
      .single();
    
    if (fetchError) throw fetchError;
    
    console.log(`✅ Updated fragebogen: ${id}`);
    res.json(updatedFragebogen);
  } catch (error: any) {
    console.error('Error updating fragebogen:', error);
    res.status(500).json({ error: error.message || 'Failed to update fragebogen' });
  }
});

/**
 * PUT /api/fragebogen/fragebogen/:id/archive
 * Archive or unarchive a fragebogen
 */
router.put('/fragebogen/:id/archive', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    const freshClient = createFreshClient();
    const { archived } = req.body;
    
    const updates: any = { archived: archived ?? true };
    
    // If archiving, also set status to inactive
    if (archived) {
      updates.status = 'inactive';
    }
    
    const { data, error } = await freshClient
      .from('fb_fragebogen')
      .update(updates)
      .eq('id', id)
      .select()
      .single();
    
    if (error) throw error;
    
    if (!data) {
      return res.status(404).json({ error: 'Fragebogen not found' });
    }
    
    console.log(`✅ ${archived ? 'Archived' : 'Unarchived'} fragebogen: ${id}`);
    res.json(data);
  } catch (error: any) {
    console.error('Error archiving fragebogen:', error);
    res.status(500).json({ error: error.message || 'Failed to archive fragebogen' });
  }
});

/**
 * DELETE /api/fragebogen/fragebogen/:id
 * Soft delete (archive) a fragebogen
 */
router.delete('/fragebogen/:id', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    const freshClient = createFreshClient();
    
    const { data, error } = await freshClient
      .from('fb_fragebogen')
      .update({ archived: true, status: 'inactive' })
      .eq('id', id)
      .select()
      .single();
    
    if (error) throw error;
    
    if (!data) {
      return res.status(404).json({ error: 'Fragebogen not found' });
    }
    
    console.log(`✅ Deleted (archived) fragebogen: ${id}`);
    res.json({ success: true, data });
  } catch (error: any) {
    console.error('Error deleting fragebogen:', error);
    res.status(500).json({ error: error.message || 'Failed to delete fragebogen' });
  }
});

/**
 * DELETE /api/fragebogen/fragebogen/:id/permanent
 * Permanently delete a fragebogen (keeps modules and questions intact).
 * Blocked if any completed responses exist.
 */
router.delete('/fragebogen/:id/permanent', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    const freshClient = createFreshClient();

    // Guard: block if completed responses exist
    const { count: completedCount, error: ccError } = await freshClient
      .from('fb_responses')
      .select('*', { count: 'exact', head: true })
      .eq('fragebogen_id', id)
      .eq('status', 'completed');
    if (ccError) throw ccError;
    if ((completedCount ?? 0) > 0) {
      return res.status(409).json({
        error: 'Cannot permanently delete this Fragebogen because it has completed responses. Archive it instead.'
      });
    }
    
    // Delete fragebogen-module associations
    const { error: fmError } = await freshClient
      .from('fb_fragebogen_modules')
      .delete()
      .eq('fragebogen_id', id);
    
    if (fmError) throw fmError;
    
    // Delete fragebogen-market associations
    const { error: marketError } = await freshClient
      .from('fb_fragebogen_markets')
      .delete()
      .eq('fragebogen_id', id);
    
    if (marketError) throw marketError;
    
    // Delete any responses associated with this fragebogen
    // First get response IDs
    const { data: responses, error: respQueryError } = await freshClient
      .from('fb_responses')
      .select('id')
      .eq('fragebogen_id', id);
    
    if (respQueryError) throw respQueryError;
    
    if (responses && responses.length > 0) {
      const responseIds = responses.map(r => r.id);
      
      // Delete response answers
      const { error: ansError } = await freshClient
        .from('fb_response_answers')
        .delete()
        .in('response_id', responseIds);
      
      if (ansError) throw ansError;
      
      // Delete responses
      const { error: respError } = await freshClient
        .from('fb_responses')
        .delete()
        .eq('fragebogen_id', id);
      
      if (respError) throw respError;
    }
    
    // Delete the fragebogen itself
    const { error: fragebogenError } = await freshClient
      .from('fb_fragebogen')
      .delete()
      .eq('id', id);
    
    if (fragebogenError) throw fragebogenError;
    
    console.log(`✅ Permanently deleted fragebogen: ${id}`);
    res.json({ success: true });
  } catch (error: any) {
    console.error('Error permanently deleting fragebogen:', error);
    res.status(500).json({ error: error.message || 'Failed to permanently delete fragebogen' });
  }
});

/**
 * GET /api/fragebogen/fragebogen/stats/:id
 * Get response statistics for a fragebogen
 */
router.get('/fragebogen/stats/:id', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    const freshClient = createFreshClient();
    await refreshFragebogenStatuses(freshClient);
    
    const { data, error } = await freshClient
      .from('fb_fragebogen_overview')
      .select('*')
      .eq('id', id)
      .single();
    
    if (error) throw error;
    
    if (!data) {
      return res.status(404).json({ error: 'Fragebogen not found' });
    }
    
    res.json(data);
  } catch (error: any) {
    console.error('Error fetching fragebogen stats:', error);
    res.status(500).json({ error: error.message || 'Failed to fetch fragebogen stats' });
  }
});

// ============================================================================
// RESPONSES API - /api/fragebogen/responses
// ============================================================================

/**
 * GET /api/fragebogen/responses/fragebogen/:fragebogenId
 * Get all responses for a fragebogen
 */
router.get('/responses/fragebogen/:fragebogenId', async (req: Request, res: Response) => {
  try {
    const { fragebogenId } = req.params;
    const freshClient = createFreshClient();
    const { status } = req.query;
    
    let query = freshClient
      .from('fb_responses')
      .select(`
        *,
        user:users (id, first_name, last_name),
        market:markets (id, name, chain)
      `)
      .eq('fragebogen_id', fragebogenId)
      .order('started_at', { ascending: false });
    
    if (status) {
      query = query.eq('status', status);
    }
    
    const { data, error } = await query;
    
    if (error) throw error;
    
    res.json(data);
  } catch (error: any) {
    console.error('Error fetching responses:', error);
    res.status(500).json({ error: error.message || 'Failed to fetch responses' });
  }
});

/**
 * GET /api/fragebogen/responses/:id
 * Get a single response with all answers and resolved label context.
 */
router.get('/responses/:id', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    const freshClient = createFreshClient();
    
    const { data: response, error: responseError } = await freshClient
      .from('fb_responses')
      .select(`
        *,
        user:users (id, first_name, last_name),
        market:markets (id, name, chain)
      `)
      .eq('id', id)
      .single();
    
    if (responseError) throw responseError;
    if (!response) {
      return res.status(404).json({ error: 'Response not found' });
    }
    
    const { data: answers, error: answersError } = await freshClient
      .from('fb_response_answers')
      .select(`
        *,
        question:fb_questions (id, type, question_text, options, matrix_config, likert_scale)
      `)
      .eq('response_id', id)
      .order('answered_at', { ascending: true });
    
    if (answersError) throw answersError;

    // Enrich each answer with a human-readable display value resolved from current definitions
    const enrichedAnswers = (answers || []).map((a: any) => {
      let displayValue: any = null;
      const q = a.question;
      if (!q) return { ...a, display_value: null };

      switch (q.type) {
        case 'yesno':
          displayValue = a.answer_boolean === true ? 'Ja' : a.answer_boolean === false ? 'Nein' : null;
          break;
        case 'single_choice': {
          const opt = (q.options || []).find((o: any) => o.id === a.answer_text);
          displayValue = opt ? opt.label : a.answer_text;
          break;
        }
        case 'multiple_choice': {
          const ids: string[] = a.answer_json || [];
          displayValue = ids.map((id: string) => {
            const opt = (q.options || []).find((o: any) => o.id === id);
            return opt ? opt.label : id;
          });
          break;
        }
        case 'matrix': {
          const map: Record<string, string> = a.answer_json || {};
          displayValue = Object.entries(map).map(([rowId, colId]) => {
            const row = (q.matrix_config?.rows || []).find((r: any) => r.id === rowId);
            const col = (q.matrix_config?.columns || []).find((c: any) => c.id === colId);
            return `${row?.label ?? rowId}: ${col?.label ?? colId}`;
          });
          break;
        }
        case 'likert':
        case 'open_numeric':
        case 'slider':
          displayValue = a.answer_numeric;
          break;
        case 'open_text':
        case 'barcode_scanner':
          displayValue = a.answer_text;
          break;
        case 'photo_upload':
          displayValue = a.answer_file_url;
          break;
        default:
          displayValue = a.answer_text ?? a.answer_numeric ?? a.answer_json;
      }

      return { ...a, display_value: displayValue };
    });
    
    res.json({ ...response, answers: enrichedAnswers });
  } catch (error: any) {
    console.error('Error fetching response:', error);
    res.status(500).json({ error: error.message || 'Failed to fetch response' });
  }
});

/**
 * POST /api/fragebogen/responses
 * Start a new response run for a GL at a market.
 * Multiple runs per (fragebogen, GL, market) are supported.
 */
router.post('/responses', async (req: Request, res: Response) => {
  try {
    const freshClient = createFreshClient();
    const { fragebogen_id, gebietsleiter_id, market_id, zeiterfassung_submission_id } = req.body;
    
    if (!fragebogen_id || !gebietsleiter_id || !market_id) {
      return res.status(400).json({ 
        error: 'fragebogen_id, gebietsleiter_id, and market_id are required' 
      });
    }
    
    // Always create a new run — multiple historical runs are intentional
    const { data, error } = await freshClient
      .from('fb_responses')
      .insert({
        fragebogen_id,
        gebietsleiter_id,
        market_id,
        zeiterfassung_submission_id: zeiterfassung_submission_id || null,
        status: 'in_progress'
      })
      .select()
      .single();
    
    if (error) throw error;
    
    console.log(`✅ Started response run: ${data.id}`);
    res.status(201).json(data);
  } catch (error: any) {
    console.error('Error creating response:', error);
    res.status(500).json({ error: error.message || 'Failed to create response' });
  }
});

/**
 * PUT /api/fragebogen/responses/:id
 * Save/update answers for a response run. Validates each answer against the
 * authoritative question definition in the database before persisting.
 */
router.put('/responses/:id', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    const freshClient = createFreshClient();
    const { answers } = req.body;
    
    if (!answers || !Array.isArray(answers)) {
      return res.status(400).json({ error: 'answers array is required' });
    }

    // --- Guard: response must exist and must not be completed ---
    const { data: responseRow, error: responseFetchError } = await freshClient
      .from('fb_responses')
      .select('id, status')
      .eq('id', id)
      .maybeSingle();

    if (responseFetchError) throw responseFetchError;
    if (!responseRow) {
      return res.status(404).json({ error: 'Response not found' });
    }
    if (responseRow.status === 'completed') {
      return res.status(409).json({ error: 'Response is already completed and cannot be modified' });
    }

    const now = new Date().toISOString();

    for (const answer of answers) {
      const { question_id, module_id } = answer;

      if (!question_id || !module_id) {
        return res.status(400).json({ error: 'Each answer must have question_id and module_id' });
      }

      // --- Validate question belongs to module ---
      const { data: moduleQuestion, error: mqError } = await freshClient
        .from('fb_module_questions')
        .select('question_id')
        .eq('module_id', module_id)
        .eq('question_id', question_id)
        .maybeSingle();

      if (mqError) throw mqError;
      if (!moduleQuestion) {
        return res.status(400).json({
          error: `Question ${question_id} is not part of module ${module_id}`
        });
      }

      // --- Fetch authoritative question definition ---
      const { data: questionDef, error: qError } = await freshClient
        .from('fb_questions')
        .select('id, type, options, matrix_config, likert_scale, numeric_constraints, slider_config')
        .eq('id', question_id)
        .single();

      if (qError || !questionDef) {
        return res.status(400).json({ error: `Question ${question_id} not found` });
      }

      // Use DB question_type — do not trust client-provided question_type
      const dbType: string = questionDef.type;

      // --- Per-type validation ---
      let kind: string;
      let text: any = null;
      let numeric: any = null;
      let boolVal: any = null;
      let json: any = null;
      let file: any = null;

      if (dbType === 'yesno') {
        if (typeof answer.answer_boolean !== 'boolean') {
          return res.status(400).json({ error: `Question ${question_id} (yesno) requires a boolean answer_boolean` });
        }
        kind = 'boolean';
        boolVal = answer.answer_boolean;

      } else if (dbType === 'likert') {
        const numVal = answer.answer_numeric;
        if (typeof numVal !== 'number' || isNaN(numVal)) {
          return res.status(400).json({ error: `Question ${question_id} (likert) requires a numeric answer_numeric` });
        }
        const scale = questionDef.likert_scale as { min?: number; max?: number } | null;
        if (scale?.min !== undefined && numVal < scale.min) {
          return res.status(400).json({ error: `Question ${question_id}: value ${numVal} below likert min ${scale.min}` });
        }
        if (scale?.max !== undefined && numVal > scale.max) {
          return res.status(400).json({ error: `Question ${question_id}: value ${numVal} above likert max ${scale.max}` });
        }
        kind = 'numeric';
        numeric = numVal;

      } else if (dbType === 'open_numeric') {
        const numVal = answer.answer_numeric;
        if (typeof numVal !== 'number' || isNaN(numVal)) {
          return res.status(400).json({ error: `Question ${question_id} (open_numeric) requires a numeric answer_numeric` });
        }
        const nc = questionDef.numeric_constraints as { min?: number; max?: number; decimals?: number } | null;
        if (nc?.min !== undefined && numVal < nc.min) {
          return res.status(400).json({ error: `Question ${question_id}: value ${numVal} below min ${nc.min}` });
        }
        if (nc?.max !== undefined && numVal > nc.max) {
          return res.status(400).json({ error: `Question ${question_id}: value ${numVal} above max ${nc.max}` });
        }
        if (nc?.decimals !== undefined && Number.isInteger(nc.decimals) && nc.decimals >= 0) {
          const scale = Math.pow(10, nc.decimals);
          const rounded = Math.round(numVal * scale) / scale;
          if (Math.abs(rounded - numVal) > 1e-9) {
            return res.status(400).json({ error: `Question ${question_id}: value ${numVal} exceeds allowed decimal places (${nc.decimals})` });
          }
        }
        kind = 'numeric';
        numeric = numVal;

      } else if (dbType === 'slider') {
        const numVal = answer.answer_numeric;
        if (typeof numVal !== 'number' || isNaN(numVal)) {
          return res.status(400).json({ error: `Question ${question_id} (slider) requires a numeric answer_numeric` });
        }
        const sc = questionDef.slider_config as { min?: number; max?: number; step?: number } | null;
        if (sc?.min !== undefined && numVal < sc.min) {
          return res.status(400).json({ error: `Question ${question_id}: slider value ${numVal} below min ${sc.min}` });
        }
        if (sc?.max !== undefined && numVal > sc.max) {
          return res.status(400).json({ error: `Question ${question_id}: slider value ${numVal} above max ${sc.max}` });
        }
        if (sc?.step !== undefined && sc.step > 0 && sc.min !== undefined) {
          const stepsFromMin = (numVal - sc.min) / sc.step;
          if (Math.abs(stepsFromMin - Math.round(stepsFromMin)) > 1e-9) {
            return res.status(400).json({ error: `Question ${question_id}: slider value ${numVal} is not aligned to step ${sc.step}` });
          }
        }
        kind = 'numeric';
        numeric = numVal;

      } else if (dbType === 'single_choice') {
        const val = answer.answer_text;
        if (!val || typeof val !== 'string') {
          return res.status(400).json({ error: `Question ${question_id} (single_choice) requires a string answer_text` });
        }
        const options = (questionDef.options ?? []) as { id: string; label: string }[];
        if (options.length > 0 && !options.some(o => o.id === val)) {
          return res.status(400).json({ error: `Question ${question_id}: option id '${val}' is not valid` });
        }
        kind = 'text';
        text = val;

      } else if (dbType === 'multiple_choice') {
        const vals = answer.answer_json;
        if (!Array.isArray(vals)) {
          return res.status(400).json({ error: `Question ${question_id} (multiple_choice) requires an array answer_json` });
        }
        if (vals.some((v: unknown) => typeof v !== 'string')) {
          return res.status(400).json({ error: `Question ${question_id} (multiple_choice) requires string[] option ids` });
        }
        const options = (questionDef.options ?? []) as { id: string; label: string }[];
        if (options.length > 0) {
          const validIds = new Set(options.map(o => o.id));
          const invalid = vals.filter((v: string) => !validIds.has(v));
          if (invalid.length > 0) {
            return res.status(400).json({ error: `Question ${question_id}: invalid option ids: ${invalid.join(', ')}` });
          }
        }
        kind = 'json';
        json = vals;

      } else if (dbType === 'matrix') {
        const val = answer.answer_json;
        if (!val || typeof val !== 'object' || Array.isArray(val)) {
          return res.status(400).json({ error: `Question ${question_id} (matrix) requires an object answer_json` });
        }
        const mc = questionDef.matrix_config as { rows?: { id: string }[]; columns?: { id: string }[] } | null;
        if (mc) {
          const validRows = new Set((mc.rows ?? []).map((r: { id: string }) => r.id));
          const validCols = new Set((mc.columns ?? []).map((c: { id: string }) => c.id));
          for (const [rowId, colId] of Object.entries(val)) {
            if (validRows.size > 0 && !validRows.has(rowId)) {
              return res.status(400).json({ error: `Question ${question_id}: invalid matrix row id '${rowId}'` });
            }
            if (validCols.size > 0 && !validCols.has(colId as string)) {
              return res.status(400).json({ error: `Question ${question_id}: invalid matrix column id '${colId}'` });
            }
          }
        }
        kind = 'json';
        json = val;

      } else if (dbType === 'photo_upload') {
        const val = answer.answer_file_url;
        if (!val || typeof val !== 'string') {
          return res.status(400).json({ error: `Question ${question_id} (photo_upload) requires a string answer_file_url` });
        }
        kind = 'file';
        file = val;

      } else {
        // open_text, barcode_scanner, fallback
        const val = answer.answer_text;
        if (val === undefined || val === null || val === '') {
          return res.status(400).json({ error: `Question ${question_id} (${dbType}) requires a non-empty answer_text` });
        }
        kind = 'text';
        text = String(val);
      }

      // --- Upsert the validated answer ---
      const { data: existing } = await freshClient
        .from('fb_response_answers')
        .select('id')
        .eq('response_id', id)
        .eq('question_id', question_id)
        .eq('module_id', module_id)
        .maybeSingle();
      
      if (existing) {
        const { error: updateError } = await freshClient
          .from('fb_response_answers')
          .update({
            question_type: dbType,
            answer_kind: kind,
            answer_text: text,
            answer_numeric: numeric,
            answer_boolean: boolVal,
            answer_json: json,
            answer_file_url: file,
            answered_at: now,
            updated_at: now
          })
          .eq('id', existing.id);
        if (updateError) throw updateError;
      } else {
        const { error: insertError } = await freshClient
          .from('fb_response_answers')
          .insert({
            response_id: id,
            question_id,
            module_id,
            question_type: dbType,
            answer_kind: kind,
            answer_text: text,
            answer_numeric: numeric,
            answer_boolean: boolVal,
            answer_json: json,
            answer_file_url: file,
            answered_at: now,
            created_at: now,
            updated_at: now
          });
        if (insertError) throw insertError;
      }
    }
    
    const { data: response } = await freshClient
      .from('fb_responses')
      .select('*')
      .eq('id', id)
      .single();
    
    console.log(`✅ Saved ${answers.length} answer(s) for response: ${id}`);
    res.json(response);
  } catch (error: any) {
    console.error('Error updating response:', error);
    res.status(500).json({ error: error.message || 'Failed to update response' });
  }
});

/**
 * PUT /api/fragebogen/responses/:id/complete
 * Mark a response as completed
 */
router.put('/responses/:id/complete', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    const freshClient = createFreshClient();
    
    const { data, error } = await freshClient
      .from('fb_responses')
      .update({
        status: 'completed',
        completed_at: new Date().toISOString()
      })
      .eq('id', id)
      .select()
      .single();
    
    if (error) throw error;
    
    if (!data) {
      return res.status(404).json({ error: 'Response not found' });
    }
    
    console.log(`✅ Completed response: ${id}`);
    res.json(data);
  } catch (error: any) {
    console.error('Error completing response:', error);
    res.status(500).json({ error: error.message || 'Failed to complete response' });
  }
});

/**
 * GET /api/fragebogen/responses/stats/fragebogen/:fragebogenId
 * Get detailed statistics for a fragebogen's responses
 */
router.get('/responses/stats/fragebogen/:fragebogenId', async (req: Request, res: Response) => {
  try {
    const { fragebogenId } = req.params;
    const freshClient = createFreshClient();
    
    // Get basic stats
    const { data: fragebogenStats, error: statsError } = await freshClient
      .from('fb_fragebogen_overview')
      .select('*')
      .eq('id', fragebogenId)
      .single();
    
    if (statsError) throw statsError;
    
    // Get responses by market
    const { data: responsesByMarket, error: marketError } = await freshClient
      .from('fb_responses')
      .select(`
        market_id,
        market:markets (name, chain),
        status
      `)
      .eq('fragebogen_id', fragebogenId);
    
    if (marketError) throw marketError;
    
    // Aggregate by market
    const marketStats = (responsesByMarket || []).reduce((acc: any, r: any) => {
      const key = r.market_id;
      if (!acc[key]) {
        acc[key] = {
          market_id: r.market_id,
          market_name: r.market?.name,
          chain: r.market?.chain,
          total: 0,
          completed: 0
        };
      }
      acc[key].total++;
      if (r.status === 'completed') acc[key].completed++;
      return acc;
    }, {});
    
    res.json({
      ...fragebogenStats,
      markets: Object.values(marketStats)
    });
  } catch (error: any) {
    console.error('Error fetching response stats:', error);
    res.status(500).json({ error: error.message || 'Failed to fetch response stats' });
  }
});

// ============================================================================
// ZEITERFASSUNG API - /api/fragebogen/zeiterfassung
// ============================================================================

/**
 * POST /api/fragebogen/zeiterfassung
 * Submit zeiterfassung (time tracking) data for a market visit
 */
router.post('/zeiterfassung', async (req: Request, res: Response) => {
  try {
    const freshClient = createFreshClient();
    const {
      response_id,
      fragebogen_id,
      gebietsleiter_id,
      market_id,
      fahrzeit_von,
      fahrzeit_bis,
      besuchszeit_von,
      besuchszeit_bis,
      distanz_km,
      kommentar,
      food_prozent
    } = req.body;
    
    // Validate required fields
    if (!gebietsleiter_id || !market_id) {
      return res.status(400).json({ 
        error: 'gebietsleiter_id and market_id are required' 
      });
    }
    
    // Calculate time differences if both times are provided
    let fahrzeit_diff = null;
    if (fahrzeit_von && fahrzeit_bis) {
      // Parse times and calculate difference in interval format
      const von = fahrzeit_von.split(':');
      const bis = fahrzeit_bis.split(':');
      const vonMinutes = parseInt(von[0]) * 60 + parseInt(von[1]);
      const bisMinutes = parseInt(bis[0]) * 60 + parseInt(bis[1]);
      let diffMinutes = bisMinutes - vonMinutes;
      if (diffMinutes < 0) diffMinutes += 24 * 60; // Handle overnight
      
      const hours = Math.floor(diffMinutes / 60);
      const minutes = diffMinutes % 60;
      fahrzeit_diff = `${hours}:${minutes.toString().padStart(2, '0')}:00`;
    }
    
    let besuchszeit_diff = null;
    if (besuchszeit_von && besuchszeit_bis) {
      const von = besuchszeit_von.split(':');
      const bis = besuchszeit_bis.split(':');
      const vonMinutes = parseInt(von[0]) * 60 + parseInt(von[1]);
      const bisMinutes = parseInt(bis[0]) * 60 + parseInt(bis[1]);
      let diffMinutes = bisMinutes - vonMinutes;
      if (diffMinutes < 0) diffMinutes += 24 * 60;
      
      const hours = Math.floor(diffMinutes / 60);
      const minutes = diffMinutes % 60;
      besuchszeit_diff = `${hours}:${minutes.toString().padStart(2, '0')}:00`;
    }
    
    // Insert zeiterfassung data
    const { data, error } = await freshClient
      .from('fb_zeiterfassung_submissions')
      .insert({
        response_id: response_id || null,
        fragebogen_id: fragebogen_id || null,
        gebietsleiter_id,
        market_id,
        fahrzeit_von: fahrzeit_von || null,
        fahrzeit_bis: fahrzeit_bis || null,
        fahrzeit_diff,
        besuchszeit_von: besuchszeit_von || null,
        besuchszeit_bis: besuchszeit_bis || null,
        besuchszeit_diff,
        distanz_km: distanz_km ? parseFloat(distanz_km) : null,
        kommentar: kommentar || null,
        food_prozent: food_prozent !== undefined ? parseInt(food_prozent) : null
      })
      .select()
      .single();
    
    if (error) throw error;
    
    console.log(`✅ Saved zeiterfassung: ${data.id}`);
    res.status(201).json(data);
  } catch (error: any) {
    console.error('Error saving zeiterfassung:', error);
    res.status(500).json({ error: error.message || 'Failed to save zeiterfassung' });
  }
});

/**
 * PATCH /api/fragebogen/zeiterfassung/:id
 * Update an existing zeiterfassung submission (partial update)
 */
router.patch('/zeiterfassung/:id', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    const freshClient = createFreshClient();
    const {
      besuchszeit_von,
      besuchszeit_bis,
      fahrzeit_von,
      fahrzeit_bis,
      distanz_km,
      kommentar,
      food_prozent
    } = req.body;

    const updateData: Record<string, any> = {};

    if (besuchszeit_von !== undefined) updateData.besuchszeit_von = besuchszeit_von || null;
    if (besuchszeit_bis !== undefined) updateData.besuchszeit_bis = besuchszeit_bis || null;
    if (fahrzeit_von !== undefined) updateData.fahrzeit_von = fahrzeit_von || null;
    if (fahrzeit_bis !== undefined) updateData.fahrzeit_bis = fahrzeit_bis || null;
    if (distanz_km !== undefined) updateData.distanz_km = distanz_km ? parseFloat(distanz_km) : null;
    if (kommentar !== undefined) updateData.kommentar = kommentar || null;
    if (food_prozent !== undefined) updateData.food_prozent = food_prozent !== null ? parseInt(food_prozent) : null;

    // Recalculate diffs: need current row to merge with incoming partial data
    const { data: existing } = await freshClient
      .from('fb_zeiterfassung_submissions')
      .select('besuchszeit_von, besuchszeit_bis, fahrzeit_von, fahrzeit_bis')
      .eq('id', id)
      .single();

    if (!existing) {
      return res.status(404).json({ error: 'Submission not found' });
    }

    const finalBVon = besuchszeit_von !== undefined ? besuchszeit_von : existing.besuchszeit_von;
    const finalBBis = besuchszeit_bis !== undefined ? besuchszeit_bis : existing.besuchszeit_bis;
    const finalFVon = fahrzeit_von !== undefined ? fahrzeit_von : existing.fahrzeit_von;
    const finalFBis = fahrzeit_bis !== undefined ? fahrzeit_bis : existing.fahrzeit_bis;

    if (finalBVon && finalBBis) {
      const von = String(finalBVon).split(':');
      const bis = String(finalBBis).split(':');
      const vonMin = parseInt(von[0]) * 60 + parseInt(von[1]);
      const bisMin = parseInt(bis[0]) * 60 + parseInt(bis[1]);
      let diff = bisMin - vonMin;
      if (diff < 0) diff += 24 * 60;
      updateData.besuchszeit_diff = `${Math.floor(diff / 60)}:${(diff % 60).toString().padStart(2, '0')}:00`;
    }

    if (finalFVon && finalFBis) {
      const von = String(finalFVon).split(':');
      const bis = String(finalFBis).split(':');
      const vonMin = parseInt(von[0]) * 60 + parseInt(von[1]);
      const bisMin = parseInt(bis[0]) * 60 + parseInt(bis[1]);
      let diff = bisMin - vonMin;
      if (diff < 0) diff += 24 * 60;
      updateData.fahrzeit_diff = `${Math.floor(diff / 60)}:${(diff % 60).toString().padStart(2, '0')}:00`;
    }

    if (Object.keys(updateData).length === 0) {
      return res.status(400).json({ error: 'No fields to update' });
    }

    const { data, error } = await freshClient
      .from('fb_zeiterfassung_submissions')
      .update(updateData)
      .eq('id', id)
      .select()
      .single();

    if (error) throw error;

    console.log(`✅ Updated zeiterfassung: ${id}`);
    res.json(data);
  } catch (error: any) {
    console.error('Error updating zeiterfassung:', error);
    res.status(500).json({ error: error.message || 'Failed to update zeiterfassung' });
  }
});

/**
 * DELETE /api/fragebogen/zeiterfassung/:id
 * Delete a zeiterfassung submission
 */
router.delete('/zeiterfassung/:id', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    const freshClient = createFreshClient();
    
    const { error } = await freshClient
      .from('fb_zeiterfassung_submissions')
      .delete()
      .eq('id', id);
    
    if (error) throw error;
    
    console.log(`✅ Deleted zeiterfassung submission: ${id}`);
    res.json({ message: 'Deleted successfully', id });
  } catch (error: any) {
    console.error('Error deleting zeiterfassung:', error);
    res.status(500).json({ error: error.message || 'Failed to delete zeiterfassung' });
  }
});

/**
 * GET /api/fragebogen/zeiterfassung/gl/:glId
 * Get zeiterfassung submissions for a GL
 */
router.get('/zeiterfassung/gl/:glId', async (req: Request, res: Response) => {
  try {
    const { glId } = req.params;
    const freshClient = createFreshClient();
    const { limit, offset } = req.query;
    
    let query = freshClient
      .from('fb_zeiterfassung_submissions')
      .select(`
        *,
        market:markets (id, name, chain, address, postal_code, city),
        fragebogen:fb_fragebogen (id, name)
      `)
      .eq('gebietsleiter_id', glId)
      .order('created_at', { ascending: false });
    
    if (limit) {
      query = query.limit(parseInt(limit as string));
    }
    
    if (offset) {
      query = query.range(
        parseInt(offset as string),
        parseInt(offset as string) + parseInt(limit as string || '10') - 1
      );
    }
    
    const { data, error } = await query;
    
    if (error) throw error;
    
    res.json(data);
  } catch (error: any) {
    console.error('Error fetching zeiterfassung:', error);
    res.status(500).json({ error: error.message || 'Failed to fetch zeiterfassung' });
  }
});

/**
 * GET /api/fragebogen/zeiterfassung/admin
 * Get all zeiterfassung submissions grouped by date for admin view
 */
router.get('/zeiterfassung/admin', async (req: Request, res: Response) => {
  try {
    const freshClient = createFreshClient();
    const { start_date, end_date, gl_id } = req.query;
    
    let query = freshClient
      .from('fb_zeiterfassung_submissions')
      .select(`
        *,
        gebietsleiter:users!gebietsleiter_id (id, first_name, last_name),
        market:markets (id, name, chain, address, postal_code, city)
      `)
      .order('created_at', { ascending: false });
    
    // Filter by date range if provided
    if (start_date) {
      query = query.gte('created_at', start_date);
    }
    if (end_date) {
      query = query.lte('created_at', end_date);
    }
    
    // Filter by GL if provided
    if (gl_id) {
      query = query.eq('gebietsleiter_id', gl_id);
    }
    
    const { data, error } = await query;
    
    if (error) throw error;
    
    res.json(data);
  } catch (error: any) {
    console.error('Error fetching admin zeiterfassung:', error);
    res.status(500).json({ error: error.message || 'Failed to fetch zeiterfassung' });
  }
});

/**
 * GET /api/fragebogen/zeiterfassung/gl/:glId/date/:date
 * Get detailed zeiterfassung for a GL on a specific date with all related submissions
 */
router.get('/zeiterfassung/gl/:glId/date/:date', async (req: Request, res: Response) => {
  try {
    const { glId, date } = req.params;
    const freshClient = createFreshClient();
    
    // Fetch day tracking record for this date to calculate Fahrzeit
    const { data: dayTracking } = await freshClient
      .from('fb_day_tracking')
      .select('*')
      .eq('gebietsleiter_id', glId)
      .eq('tracking_date', date)
      .single();
    
    // Fetch zeiterfassung entries for this GL on this date
    const { data: zeitEntries, error: zeitError } = await freshClient
      .from('fb_zeiterfassung_submissions')
      .select(`
        *,
        market:markets (id, name, chain, address, postal_code, city)
      `)
      .eq('gebietsleiter_id', glId)
      .gte('created_at', `${date}T00:00:00`)
      .lt('created_at', `${date}T23:59:59`)
      .order('created_at', { ascending: true });
    
    if (zeitError) throw zeitError;
    
    if (!zeitEntries || zeitEntries.length === 0) {
      return res.json([]);
    }
    
    // Calculate Fahrzeit on-the-fly for entries that don't have it
    if (dayTracking && dayTracking.day_start_time) {
      for (let i = 0; i < zeitEntries.length; i++) {
        const entry = zeitEntries[i];
        // Only calculate if not already calculated
        if (!entry.calculated_fahrzeit && !entry.fahrzeit_diff) {
          const visitStartTime = entry.market_start_time || entry.besuchszeit_von;
          
          if (i === 0 && !dayTracking.skipped_first_fahrzeit && visitStartTime) {
            // First visit: Fahrzeit from day start to visit start
            const { interval } = calculateTimeDiff(dayTracking.day_start_time, visitStartTime);
            entry.calculated_fahrzeit = interval;
          } else if (i > 0 && visitStartTime) {
            // Subsequent visits: Fahrzeit from previous end to current start
            const prevEntry = zeitEntries[i - 1];
            const prevEndTime = prevEntry.market_end_time || prevEntry.besuchszeit_bis;
            if (prevEndTime) {
              const { interval } = calculateTimeDiff(prevEndTime, visitStartTime);
              entry.calculated_fahrzeit = interval;
            }
          }
        }
      }
    }
    
    // Get all market IDs from zeiterfassung entries
    const marketIds = [...new Set(zeitEntries.map(e => e.market_id))];
    
    // Fetch wellen_submissions (Vorbesteller) for all markets on this date, including wave goal_type
    const { data: wellenSubs, error: wellenError } = await freshClient
      .from('wellen_submissions')
      .select('*, wellen:welle_id ( goal_type )')
      .eq('gebietsleiter_id', glId)
      .in('market_id', marketIds)
      .gte('created_at', `${date}T00:00:00`)
      .lt('created_at', `${date}T23:59:59`);
    
    if (wellenError) throw wellenError;
    
    // Fetch vorverkauf_entries for all markets on this date
    const { data: vorverkaufEntries, error: vorverkaufError } = await freshClient
      .from('vorverkauf_entries')
      .select('*')
      .eq('gebietsleiter_id', glId)
      .in('market_id', marketIds)
      .gte('created_at', `${date}T00:00:00`)
      .lt('created_at', `${date}T23:59:59`);
    
    if (vorverkaufError) throw vorverkaufError;
    
    // Enrich each zeiterfassung entry with submission data
    const enrichedEntries = zeitEntries.map(zeitEntry => {
      const marketId = zeitEntry.market_id;
      
      // Filter submissions for this market
      const marketWellenSubs = (wellenSubs || []).filter(s => s.market_id === marketId);
      const marketVorverkauf = (vorverkaufEntries || []).filter(e => 
        e.market_id === marketId && e.reason !== 'Produkttausch'
      );
      const marketProduktausch = (vorverkaufEntries || []).filter(e => 
        e.market_id === marketId && e.reason === 'Produkttausch'
      );
      
      // Separate value-based vs percentage-based submissions
      const valueSubs = marketWellenSubs.filter((s: any) => s.wellen?.goal_type === 'value');
      const nonValueSubs = marketWellenSubs.filter((s: any) => s.wellen?.goal_type !== 'value');
      
      const vorbestellerCount = marketWellenSubs.length;
      const vorbestellerValue = valueSubs.reduce((sum: number, s: any) => {
        const qty = s.quantity || 0;
        const value = s.value_per_unit || 0;
        return sum + (qty * value);
      }, 0);
      
      return {
        ...zeitEntry,
        submissions: {
          vorbesteller: {
            count: vorbestellerCount,
            valueCount: valueSubs.length,
            nonValueCount: nonValueSubs.length,
            totalValue: vorbestellerValue,
            items: marketWellenSubs
          },
          vorverkauf: {
            count: marketVorverkauf.length,
            items: marketVorverkauf
          },
          produkttausch: {
            count: marketProduktausch.length,
            items: marketProduktausch
          }
        }
      };
    });
    
    res.json(enrichedEntries);
  } catch (error: any) {
    console.error('Error fetching detailed zeiterfassung:', error);
    res.status(500).json({ error: error.message || 'Failed to fetch detailed zeiterfassung' });
  }
});

// ============================================================================
// DAY TRACKING ENDPOINTS
// ============================================================================
// ZUSATZ ZEITERFASSUNG ENDPOINTS
// ============================================================================

/**
 * POST /api/fragebogen/zusatz-zeiterfassung
 * Create one or more zusatz zeiterfassung entries
 */
router.post('/zusatz-zeiterfassung', async (req: Request, res: Response) => {
  try {
    const freshClient = createFreshClient();
    const { gebietsleiter_id, entries } = req.body;
    
    if (!gebietsleiter_id || !entries || !Array.isArray(entries) || entries.length === 0) {
      return res.status(400).json({ error: 'gebietsleiter_id and entries array are required' });
    }
    
    const today = new Date().toISOString().split('T')[0];
    
    // Map entries to database format
    const dbEntries = entries.map((entry: any) => {
      // Calculate duration
      const [vonH, vonM] = entry.von.split(':').map(Number);
      const [bisH, bisM] = entry.bis.split(':').map(Number);
      let diffMinutes = (bisH * 60 + bisM) - (vonH * 60 + vonM);
      if (diffMinutes < 0) diffMinutes += 24 * 60;
      const diffHours = Math.floor(diffMinutes / 60);
      const diffMins = diffMinutes % 60;
      
      const dbEntry: any = {
        gebietsleiter_id,
        entry_date: entry.entryDate || today,
        reason: entry.reason,
        reason_label: entry.reasonLabel,
        zeit_von: entry.von,
        zeit_bis: entry.bis,
        zeit_diff: `${diffHours}:${diffMins.toString().padStart(2, '0')}:00`,
        kommentar: entry.kommentar || null,
        is_work_time_deduction: entry.reason === 'unterbrechung'
      };
      
      if (entry.market_id) {
        dbEntry.market_id = entry.market_id;
      }

      if (entry.schulungOrt) {
        dbEntry.schulung_ort = entry.schulungOrt;
      }
      
      return dbEntry;
    });
    
    const { data, error } = await freshClient
      .from('fb_zusatz_zeiterfassung')
      .insert(dbEntries)
      .select();
    
    if (error) throw error;
    
    // For sonderaufgabe/marktbesuch entries with market_id, also create a zeiterfassung submission (market visit)
    for (const entry of entries) {
      if ((entry.reason === 'sonderaufgabe' || entry.reason === 'marktbesuch') && entry.market_id) {
        const entryDateStr = entry.entryDate || today;

        // Calculate besuchszeit_diff
        const [vonH, vonM] = entry.von.split(':').map(Number);
        const [bisH, bisM] = entry.bis.split(':').map(Number);
        let diffMinutes = (bisH * 60 + bisM) - (vonH * 60 + vonM);
        if (diffMinutes < 0) diffMinutes += 24 * 60;
        const diffHours = Math.floor(diffMinutes / 60);
        const diffMins = diffMinutes % 60;

        const kommentarLabel = entry.reason === 'marktbesuch'
          ? (entry.kommentar ? `Marktbesuch (nachgetragen): ${entry.kommentar}` : 'Marktbesuch (nachgetragen)')
          : (entry.kommentar ? `Sonderaufgabe: ${entry.kommentar}` : 'Sonderaufgabe');
        
        // Insert zeiterfassung submission so it counts as a market visit
        const { error: zeitError } = await freshClient
          .from('fb_zeiterfassung_submissions')
          .insert({
            gebietsleiter_id,
            market_id: entry.market_id,
            besuchszeit_von: entry.von,
            besuchszeit_bis: entry.bis,
            besuchszeit_diff: `${diffHours}:${diffMins.toString().padStart(2, '0')}:00`,
            kommentar: kommentarLabel,
            market_start_time: entry.von,
            market_end_time: entry.bis,
            created_at: `${entryDateStr}T${entry.von}:00`
          });
        
        if (zeitError) {
          console.warn(`⚠️ Could not create zeiterfassung submission for ${entry.reason}:`, zeitError.message);
        } else {
          console.log(`📍 Created zeiterfassung submission for ${entry.reason} at market ${entry.market_id}`);
        }
        
        // Increment market visit count
        const { data: market } = await freshClient
          .from('markets')
          .select('last_visit_date, current_visits')
          .eq('id', entry.market_id)
          .single();
        
        if (market && market.last_visit_date !== entryDateStr) {
          await freshClient
            .from('markets')
            .update({
              current_visits: (market.current_visits || 0) + 1,
              last_visit_date: entryDateStr
            })
            .eq('id', entry.market_id);
          console.log(`📍 Incremented visit count for market ${entry.market_id}`);
        }

        await freshClient
          .from('market_visits')
          .upsert({
            market_id: entry.market_id,
            gebietsleiter_id,
            visit_date: entryDateStr,
            source: 'zusatz'
          }, { onConflict: 'market_id,visit_date', ignoreDuplicates: true });
      }
    }
    
    console.log(`✅ Created ${data.length} zusatz zeiterfassung entries for GL ${gebietsleiter_id}`);
    res.json(data);
  } catch (error: any) {
    console.error('Error creating zusatz zeiterfassung:', error);
    res.status(500).json({ error: error.message || 'Failed to create zusatz zeiterfassung' });
  }
});

/**
 * GET /api/fragebogen/zusatz-zeiterfassung/:glId
 * Get all zusatz zeiterfassung entries for a GL
 */
router.get('/zusatz-zeiterfassung/:glId', async (req: Request, res: Response) => {
  try {
    const freshClient = createFreshClient();
    const { glId } = req.params;
    const { date, start_date, end_date } = req.query;
    
    let query = freshClient
      .from('fb_zusatz_zeiterfassung')
      .select('*')
      .eq('gebietsleiter_id', glId)
      .order('created_at', { ascending: false });
    
    if (date) {
      query = query.eq('entry_date', date);
    } else if (start_date && end_date) {
      query = query.gte('entry_date', start_date).lte('entry_date', end_date);
    }
    
    const { data, error } = await query;
    
    if (error) throw error;
    
    const entries = data || [];
    const marketIds = [...new Set(entries.filter(e => e.market_id).map(e => e.market_id))];
    let marketsMap: Record<string, any> = {};
    if (marketIds.length > 0) {
      const { data: markets } = await freshClient.from('markets').select('id, name, chain').in('id', marketIds);
      (markets || []).forEach((m: any) => { marketsMap[m.id] = m; });
    }
    
    const enriched = entries.map(e => ({
      ...e,
      market: e.market_id ? marketsMap[e.market_id] || null : null
    }));
    
    res.json(enriched);
  } catch (error: any) {
    console.error('Error getting zusatz zeiterfassung:', error);
    res.status(500).json({ error: error.message || 'Failed to get zusatz zeiterfassung' });
  }
});

/**
 * GET /api/fragebogen/zusatz-zeiterfassung/all
 * Get all zusatz zeiterfassung entries (for admin)
 */
router.get('/zusatz-zeiterfassung-all', async (req: Request, res: Response) => {
  try {
    const freshClient = createFreshClient();
    const { start_date, end_date } = req.query;
    
    let query = freshClient
      .from('fb_zusatz_zeiterfassung')
      .select('*')
      .order('created_at', { ascending: false });
    
    if (start_date) {
      query = query.gte('entry_date', start_date);
    }
    if (end_date) {
      query = query.lte('entry_date', end_date);
    }
    
    const { data, error } = await query;
    
    if (error) throw error;
    
    const entries = data || [];
    const marketIds = [...new Set(entries.filter(e => e.market_id).map(e => e.market_id))];
    let marketsMap: Record<string, any> = {};
    if (marketIds.length > 0) {
      const { data: markets } = await freshClient.from('markets').select('id, name, chain').in('id', marketIds);
      (markets || []).forEach((m: any) => { marketsMap[m.id] = m; });
    }
    
    const enriched = entries.map(e => ({
      ...e,
      market: e.market_id ? marketsMap[e.market_id] || null : null
    }));
    
    console.log(`✅ Fetched ${enriched.length} zusatz zeiterfassung entries`);
    res.json(enriched);
  } catch (error: any) {
    console.error('Error getting all zusatz zeiterfassung:', error);
    res.status(500).json({ error: error.message || 'Failed to get zusatz zeiterfassung' });
  }
});

/**
 * PATCH /api/fragebogen/zusatz-zeiterfassung/:id
 * Update an existing zusatz zeiterfassung entry (partial update)
 */
router.patch('/zusatz-zeiterfassung/:id', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    const freshClient = createFreshClient();
    const { zeit_von, zeit_bis, kommentar, schulung_ort } = req.body;

    const updateData: Record<string, any> = {};
    if (zeit_von !== undefined) updateData.zeit_von = zeit_von || null;
    if (zeit_bis !== undefined) updateData.zeit_bis = zeit_bis || null;
    if (kommentar !== undefined) updateData.kommentar = kommentar;
    if (schulung_ort !== undefined) updateData.schulung_ort = schulung_ort || null;

    if (Object.keys(updateData).length === 0) {
      return res.status(400).json({ error: 'No fields to update' });
    }

    const { data: existing } = await freshClient
      .from('fb_zusatz_zeiterfassung')
      .select('zeit_von, zeit_bis')
      .eq('id', id)
      .single();

    if (!existing) {
      return res.status(404).json({ error: 'Entry not found' });
    }

    const finalVon = zeit_von !== undefined ? zeit_von : existing.zeit_von;
    const finalBis = zeit_bis !== undefined ? zeit_bis : existing.zeit_bis;

    if (finalVon && finalBis) {
      const von = String(finalVon).split(':');
      const bis = String(finalBis).split(':');
      const vonMin = parseInt(von[0]) * 60 + parseInt(von[1]);
      const bisMin = parseInt(bis[0]) * 60 + parseInt(bis[1]);
      let diff = bisMin - vonMin;
      if (diff < 0) diff += 24 * 60;
      updateData.zeit_diff = `${Math.floor(diff / 60)}:${(diff % 60).toString().padStart(2, '0')}:00`;
    }

    const { data, error } = await freshClient
      .from('fb_zusatz_zeiterfassung')
      .update(updateData)
      .eq('id', id)
      .select()
      .single();

    if (error) throw error;

    console.log(`✅ Updated zusatz zeiterfassung: ${id}`);
    res.json(data);
  } catch (error: any) {
    console.error('Error updating zusatz zeiterfassung:', error);
    res.status(500).json({ error: error.message || 'Failed to update zusatz zeiterfassung' });
  }
});

/**
 * DELETE /api/fragebogen/zusatz-zeiterfassung/:id
 * Delete a zusatz zeiterfassung entry
 */
router.delete('/zusatz-zeiterfassung/:id', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    const freshClient = createFreshClient();

    const { error } = await freshClient
      .from('fb_zusatz_zeiterfassung')
      .delete()
      .eq('id', id);

    if (error) throw error;

    console.log(`✅ Deleted zusatz zeiterfassung: ${id}`);
    res.json({ message: 'Deleted successfully', id });
  } catch (error: any) {
    console.error('Error deleting zusatz zeiterfassung:', error);
    res.status(500).json({ error: error.message || 'Failed to delete zusatz zeiterfassung' });
  }
});

// ============================================================================
// DAY TRACKING ENDPOINTS
// ============================================================================

// Helper: Calculate time difference in minutes
const calculateTimeDiff = (startTime: string, endTime: string): { interval: string; minutes: number } => {
  const [startH, startM] = startTime.split(':').map(Number);
  const [endH, endM] = endTime.split(':').map(Number);
  
  let startMinutes = startH * 60 + startM;
  let endMinutes = endH * 60 + endM;
  
  // Handle overnight (add 24 hours if negative)
  if (endMinutes < startMinutes) {
    endMinutes += 24 * 60;
  }
  
  const diffMinutes = endMinutes - startMinutes;
  const hours = Math.floor(diffMinutes / 60);
  const minutes = diffMinutes % 60;
  
  return {
    interval: `${hours}:${minutes.toString().padStart(2, '0')}:00`,
    minutes: diffMinutes
  };
};

// Helper: Get current time as HH:MM string
const getCurrentTimeString = (): string => {
  const now = new Date();
  return `${now.getHours().toString().padStart(2, '0')}:${now.getMinutes().toString().padStart(2, '0')}`;
};

// GET ALL DAY TRACKING (for admin)
router.get('/day-tracking-all', async (req: Request, res: Response) => {
  try {
    const freshClient = createFreshClient();
    const { data, error } = await freshClient
      .from('fb_day_tracking')
      .select('gebietsleiter_id, tracking_date, day_start_time, day_end_time, skipped_first_fahrzeit, km_stand_start, km_stand_end')
      .order('tracking_date', { ascending: false });

    if (error) throw error;
    res.json(data || []);
  } catch (error: any) {
    console.error('Error getting all day tracking:', error);
    res.status(500).json({ error: error.message || 'Failed to get day tracking' });
  }
});

// START DAY - Create or update day tracking record
router.post('/day-tracking/start', async (req: Request, res: Response) => {
  try {
    const freshClient = createFreshClient();
    const { gebietsleiter_id, skip_fahrzeit, start_time, km_stand_start } = req.body;
    
    if (!gebietsleiter_id) {
      return res.status(400).json({ error: 'gebietsleiter_id is required' });
    }
    
    const today = new Date().toISOString().split('T')[0];
    const dayStartTime = start_time || getCurrentTimeString();
    
    // Check if a record already exists for today
    const { data: existing } = await freshClient
      .from('fb_day_tracking')
      .select('*')
      .eq('gebietsleiter_id', gebietsleiter_id)
      .eq('tracking_date', today)
      .single();
    
    if (existing && existing.status === 'active') {
      return res.status(400).json({ error: 'Day tracking already started for today' });
    }
    
    const upsertData: Record<string, any> = {
      gebietsleiter_id,
      tracking_date: today,
      day_start_time: dayStartTime,
      skipped_first_fahrzeit: skip_fahrzeit || false,
      status: 'active',
      markets_visited: 0
    };
    if (km_stand_start !== undefined && km_stand_start !== null && km_stand_start !== '') {
      upsertData.km_stand_start = parseFloat(km_stand_start);
    }
    
    // Create or update day tracking record
    const { data, error } = await freshClient
      .from('fb_day_tracking')
      .upsert(upsertData, {
        onConflict: 'gebietsleiter_id,tracking_date'
      })
      .select()
      .single();
    
    if (error) throw error;
    
    console.log(`✅ Day tracking started for GL ${gebietsleiter_id} at ${dayStartTime}`);
    res.json(data);
  } catch (error: any) {
    console.error('Error starting day tracking:', error);
    res.status(500).json({ error: error.message || 'Failed to start day tracking' });
  }
});

// UPDATE DAY TRACKING TIMES - Edit day_start_time or day_end_time
router.patch('/day-tracking/update-times', async (req: Request, res: Response) => {
  try {
    const freshClient = createFreshClient();
    const { gebietsleiter_id, date, day_start_time, day_end_time } = req.body;
    
    if (!gebietsleiter_id || !date) {
      return res.status(400).json({ error: 'gebietsleiter_id and date are required' });
    }
    
    // Build update object with only provided fields
    const updateData: Record<string, any> = {};
    if (day_start_time !== undefined) updateData.day_start_time = day_start_time;
    if (day_end_time !== undefined) updateData.day_end_time = day_end_time;
    
    if (Object.keys(updateData).length === 0) {
      return res.status(400).json({ error: 'No fields to update' });
    }
    
    const { data, error } = await freshClient
      .from('fb_day_tracking')
      .update(updateData)
      .eq('gebietsleiter_id', gebietsleiter_id)
      .eq('tracking_date', date)
      .select()
      .single();
    
    if (error) throw error;
    
    console.log(`✅ Day tracking times updated for GL ${gebietsleiter_id} on ${date}:`, updateData);
    res.json(data);
  } catch (error: any) {
    console.error('Error updating day tracking times:', error);
    res.status(500).json({ error: error.message || 'Failed to update day tracking times' });
  }
});

// UPDATE KM STAND START - Update km_stand_start on an active day record
router.patch('/day-tracking/update-km-start', async (req: Request, res: Response) => {
  try {
    const freshClient = createFreshClient();
    const { gebietsleiter_id, km_stand_start } = req.body;

    if (!gebietsleiter_id || km_stand_start === undefined || km_stand_start === '') {
      return res.status(400).json({ error: 'gebietsleiter_id and km_stand_start are required' });
    }

    const today = new Date().toISOString().split('T')[0];

    const { data, error } = await freshClient
      .from('fb_day_tracking')
      .update({ km_stand_start: parseFloat(String(km_stand_start).replace(',', '.')) })
      .eq('gebietsleiter_id', gebietsleiter_id)
      .eq('tracking_date', today)
      .select()
      .single();

    if (error) throw error;

    console.log(`✅ km_stand_start updated for GL ${gebietsleiter_id} on ${today}:`, km_stand_start);
    res.json(data);
  } catch (error: any) {
    console.error('Error updating km_stand_start:', error);
    res.status(500).json({ error: error.message || 'Failed to update km_stand_start' });
  }
});

// UPDATE KM STAND BY DATE - Update km_stand_start or km_stand_end for any specific date
router.patch('/day-tracking/update-km', async (req: Request, res: Response) => {
  try {
    const freshClient = createFreshClient();
    const { gebietsleiter_id, date, km_stand_start, km_stand_end } = req.body;

    if (!gebietsleiter_id || !date) {
      return res.status(400).json({ error: 'gebietsleiter_id and date are required' });
    }

    const updateData: Record<string, any> = {};
    if (km_stand_start !== undefined && km_stand_start !== null && km_stand_start !== '') {
      updateData.km_stand_start = parseFloat(String(km_stand_start).replace(',', '.'));
    }
    if (km_stand_end !== undefined && km_stand_end !== null && km_stand_end !== '') {
      updateData.km_stand_end = parseFloat(String(km_stand_end).replace(',', '.'));
    }

    if (Object.keys(updateData).length === 0) {
      return res.status(400).json({ error: 'No KM fields to update' });
    }

    const { data, error } = await freshClient
      .from('fb_day_tracking')
      .update(updateData)
      .eq('gebietsleiter_id', gebietsleiter_id)
      .eq('tracking_date', date)
      .select()
      .single();

    if (error) throw error;

    console.log(`✅ KM Stand updated for GL ${gebietsleiter_id} on ${date}:`, updateData);
    res.json(data);
  } catch (error: any) {
    console.error('Error updating KM Stand:', error);
    res.status(500).json({ error: error.message || 'Failed to update KM Stand' });
  }
});

// END DAY - Complete day tracking and calculate totals
router.post('/day-tracking/end', async (req: Request, res: Response) => {
  try {
    const freshClient = createFreshClient();
    const { gebietsleiter_id, end_time, force_close, km_stand_end } = req.body;
    
    if (!gebietsleiter_id || !end_time) {
      return res.status(400).json({ error: 'gebietsleiter_id and end_time are required' });
    }
    
    const today = new Date().toISOString().split('T')[0];
    
    // Get current day tracking record
    const { data: dayTracking, error: fetchError } = await freshClient
      .from('fb_day_tracking')
      .select('*')
      .eq('gebietsleiter_id', gebietsleiter_id)
      .eq('tracking_date', today)
      .eq('status', 'active')
      .single();
    
    if (fetchError || !dayTracking) {
      return res.status(404).json({ error: 'No active day tracking found for today' });
    }
    
    // Get all market visits for today to calculate totals
    const { data: visits } = await freshClient
      .from('fb_zeiterfassung_submissions')
      .select('*')
      .eq('gebietsleiter_id', gebietsleiter_id)
      .gte('created_at', `${today}T00:00:00`)
      .lt('created_at', `${today}T23:59:59`)
      .order('created_at', { ascending: true });
    
    // Get all Unterbrechung entries for today
    const { data: unterbrechungen } = await freshClient
      .from('fb_zusatz_zeiterfassung')
      .select('*')
      .eq('gebietsleiter_id', gebietsleiter_id)
      .eq('entry_date', today)
      .eq('reason', 'unterbrechung');

    // Load all zusatz entries for today to detect homeoffice-as-last-action
    const { data: allZusatzToday } = await freshClient
      .from('fb_zusatz_zeiterfassung')
      .select('reason, zeit_bis')
      .eq('gebietsleiter_id', gebietsleiter_id)
      .eq('entry_date', today);
    
    // Calculate totals
    let totalFahrzeitMinutes = 0;
    let totalBesuchszeitMinutes = 0;
    let totalUnterbrechungMinutes = 0;
    
    // Calculate Fahrzeit for each visit
    // Use besuchszeit_von/bis as they contain the actual visit times
    for (let i = 0; i < (visits?.length || 0); i++) {
      const visit = visits![i];
      const visitStartTime = visit.market_start_time || visit.besuchszeit_von;
      const visitEndTime = visit.market_end_time || visit.besuchszeit_bis;
      
      // Calculate Fahrzeit
      if (i === 0 && !dayTracking.skipped_first_fahrzeit && visitStartTime) {
        // First visit: Fahrzeit from day start to market start
        const { minutes } = calculateTimeDiff(dayTracking.day_start_time, visitStartTime);
        totalFahrzeitMinutes += minutes;
        
        // Update the visit with calculated fahrzeit
        await freshClient
          .from('fb_zeiterfassung_submissions')
          .update({ calculated_fahrzeit: `${Math.floor(minutes / 60)}:${(minutes % 60).toString().padStart(2, '0')}:00` })
          .eq('id', visit.id);
      } else if (i > 0 && visitStartTime) {
        // Subsequent visits: Fahrzeit from previous end to current start
        const prevVisitEndTime = visits![i - 1].market_end_time || visits![i - 1].besuchszeit_bis;
        if (prevVisitEndTime) {
          const { minutes } = calculateTimeDiff(prevVisitEndTime, visitStartTime);
          totalFahrzeitMinutes += minutes;
          
          await freshClient
            .from('fb_zeiterfassung_submissions')
            .update({ calculated_fahrzeit: `${Math.floor(minutes / 60)}:${(minutes % 60).toString().padStart(2, '0')}:00` })
            .eq('id', visit.id);
        }
      }
      
      // Add Besuchszeit
      if (visit.besuchszeit_diff) {
        const parts = visit.besuchszeit_diff.split(':');
        totalBesuchszeitMinutes += parseInt(parts[0]) * 60 + parseInt(parts[1]);
      }
    }

    // --- Homeoffice last-action check ---
    // Build a list of all actions (market + zusatz) by their end time to find the final one
    const allActions: { endTime: string; isHomeoffice: boolean }[] = [];
    for (const v of (visits || [])) {
      const endTime = v.market_end_time || v.besuchszeit_bis;
      if (endTime) allActions.push({ endTime: endTime.substring(0, 5), isHomeoffice: false });
    }
    for (const z of (allZusatzToday || [])) {
      if (z.zeit_bis) allActions.push({ endTime: z.zeit_bis.substring(0, 5), isHomeoffice: (z.reason || '').toLowerCase() === 'homeoffice' });
    }
    allActions.sort((a, b) => a.endTime.localeCompare(b.endTime));
    const lastAction = allActions.length > 0 ? allActions[allActions.length - 1] : null;
    const lastIsHomeoffice = lastAction?.isHomeoffice === true;
    const effectiveEndTime = lastIsHomeoffice ? lastAction!.endTime : end_time;

    // Calculate Heimfahrt (last visit end to day end) — skipped when last action is homeoffice
    if (!lastIsHomeoffice && visits && visits.length > 0) {
      const lastVisit = visits[visits.length - 1];
      const lastVisitEndTime = lastVisit.market_end_time || lastVisit.besuchszeit_bis;
      if (lastVisitEndTime) {
        const { minutes } = calculateTimeDiff(lastVisitEndTime, end_time);
        totalFahrzeitMinutes += minutes;
      }
    }
    
    // Calculate total Unterbrechung time
    for (const u of (unterbrechungen || [])) {
      if (u.zeit_diff) {
        const parts = u.zeit_diff.split(':');
        totalUnterbrechungMinutes += parseInt(parts[0]) * 60 + parseInt(parts[1]);
      }
    }
    
    // Calculate total Arbeitszeit:
    // When last action is homeoffice, cap at homeoffice end (no phantom Heimfahrt time)
    const { minutes: totalDayMinutes } = calculateTimeDiff(dayTracking.day_start_time, effectiveEndTime);
    const totalArbeitszeitMinutes = Math.max(0, totalDayMinutes - totalUnterbrechungMinutes);
    
    // Format intervals
    const formatInterval = (mins: number) => {
      const h = Math.floor(mins / 60);
      const m = mins % 60;
      return `${h}:${m.toString().padStart(2, '0')}:00`;
    };
    
    // Update day tracking record
    const { data, error } = await freshClient
      .from('fb_day_tracking')
      .update({
        day_end_time: end_time,
        total_fahrzeit: formatInterval(totalFahrzeitMinutes),
        total_besuchszeit: formatInterval(totalBesuchszeitMinutes),
        total_unterbrechung: formatInterval(totalUnterbrechungMinutes),
        total_arbeitszeit: formatInterval(totalArbeitszeitMinutes),
        markets_visited: visits?.length || 0,
        status: force_close ? 'force_closed' : 'completed',
        ...(km_stand_end !== undefined && km_stand_end !== null && km_stand_end !== '' ? { km_stand_end: parseFloat(km_stand_end) } : {})
      })
      .eq('id', dayTracking.id)
      .select()
      .single();
    
    if (error) throw error;
    
    console.log(`✅ Day tracking ended for GL ${gebietsleiter_id} at ${end_time}`);
    res.json(data);
  } catch (error: any) {
    console.error('Error ending day tracking:', error);
    res.status(500).json({ error: error.message || 'Failed to end day tracking' });
  }
});

// GET DAY TRACKING STATUS
router.get('/day-tracking/status/:glId', async (req: Request, res: Response) => {
  try {
    const freshClient = createFreshClient();
    const { glId } = req.params;
    const date = (req.query.date as string) || new Date().toISOString().split('T')[0];
    
    const { data, error } = await freshClient
      .from('fb_day_tracking')
      .select('*')
      .eq('gebietsleiter_id', glId)
      .eq('tracking_date', date)
      .single();
    
    if (error && error.code !== 'PGRST116') { // PGRST116 = no rows returned
      throw error;
    }
    
    if (!data) {
      return res.status(404).json({ error: 'No day tracking found' });
    }
    
    res.json(data);
  } catch (error: any) {
    console.error('Error getting day tracking status:', error);
    res.status(500).json({ error: error.message || 'Failed to get day tracking status' });
  }
});

// GET MARKET VISITS FOR DAY
router.get('/day-tracking/:glId/:date/visits', async (req: Request, res: Response) => {
  try {
    const freshClient = createFreshClient();
    const { glId, date } = req.params;
    
    const { data, error } = await freshClient
      .from('fb_zeiterfassung_submissions')
      .select(`
        id,
        market_id,
        market_start_time,
        market_end_time,
        besuchszeit_von,
        besuchszeit_bis,
        besuchszeit_diff,
        calculated_fahrzeit,
        visit_order,
        created_at,
        market:markets(id, name)
      `)
      .eq('gebietsleiter_id', glId)
      .gte('created_at', `${date}T00:00:00`)
      .lt('created_at', `${date}T23:59:59`)
      .order('created_at', { ascending: true });
    
    if (error) throw error;
    
    const visits = (data || []).map(v => ({
      ...v,
      market_name: (v.market as any)?.name || 'Unknown'
    }));
    
    res.json(visits);
  } catch (error: any) {
    console.error('Error getting market visits:', error);
    res.status(500).json({ error: error.message || 'Failed to get market visits' });
  }
});

// GET DAY SUMMARY
router.get('/day-tracking/:glId/:date/summary', async (req: Request, res: Response) => {
  try {
    const freshClient = createFreshClient();
    const { glId, date } = req.params;
    
    // Get day tracking record
    const { data: dayTracking } = await freshClient
      .from('fb_day_tracking')
      .select('*')
      .eq('gebietsleiter_id', glId)
      .eq('tracking_date', date)
      .single();
    
    // Get market visits
    const { data: visits } = await freshClient
      .from('fb_zeiterfassung_submissions')
      .select(`
        id,
        market_id,
        market_start_time,
        market_end_time,
        besuchszeit_von,
        besuchszeit_bis,
        besuchszeit_diff,
        calculated_fahrzeit,
        visit_order,
        created_at,
        market:markets(id, name)
      `)
      .eq('gebietsleiter_id', glId)
      .gte('created_at', `${date}T00:00:00`)
      .lt('created_at', `${date}T23:59:59`)
      .order('created_at', { ascending: true });
    
    const summary = {
      dayTracking,
      marketVisits: (visits || []).map(v => ({
        ...v,
        market_name: (v.market as any)?.name || 'Unknown'
      })),
      totalFahrzeit: dayTracking?.total_fahrzeit || '0:00:00',
      totalBesuchszeit: dayTracking?.total_besuchszeit || '0:00:00',
      totalUnterbrechung: dayTracking?.total_unterbrechung || '0:00:00',
      totalArbeitszeit: dayTracking?.total_arbeitszeit || '0:00:00',
      marketsVisited: dayTracking?.markets_visited || visits?.length || 0
    };
    
    res.json(summary);
  } catch (error: any) {
    console.error('Error getting day summary:', error);
    res.status(500).json({ error: error.message || 'Failed to get day summary' });
  }
});

// RECORD MARKET START - Called when GL starts a market visit
router.post('/day-tracking/market-start', async (req: Request, res: Response) => {
  try {
    const freshClient = createFreshClient();
    const { gebietsleiter_id, market_id, start_time } = req.body;
    
    if (!gebietsleiter_id || !market_id) {
      return res.status(400).json({ error: 'gebietsleiter_id and market_id are required' });
    }
    
    const today = new Date().toISOString().split('T')[0];
    const marketStartTime = start_time || getCurrentTimeString();
    
    // Get day tracking to check if day is started
    const { data: dayTracking } = await freshClient
      .from('fb_day_tracking')
      .select('*')
      .eq('gebietsleiter_id', gebietsleiter_id)
      .eq('tracking_date', today)
      .eq('status', 'active')
      .single();
    
    if (!dayTracking) {
      return res.status(400).json({ error: 'Day tracking not started. Please start your day first.' });
    }
    
    // Get previous visits to calculate visit order
    const { data: previousVisits } = await freshClient
      .from('fb_zeiterfassung_submissions')
      .select('id, market_end_time, besuchszeit_bis, visit_order')
      .eq('gebietsleiter_id', gebietsleiter_id)
      .gte('created_at', `${today}T00:00:00`)
      .lt('created_at', `${today}T23:59:59`)
      .order('created_at', { ascending: true });
    
    const visitOrder = (previousVisits?.length || 0) + 1;
    
    // Calculate Fahrzeit
    let calculatedFahrzeit: string | null = null;
    
    if (visitOrder === 1 && !dayTracking.skipped_first_fahrzeit) {
      // First visit: Fahrzeit from day start to market start
      const { interval } = calculateTimeDiff(dayTracking.day_start_time, marketStartTime);
      calculatedFahrzeit = interval;
    } else if (visitOrder > 1 && previousVisits && previousVisits.length > 0) {
      // Subsequent visits: Fahrzeit from previous end to current start
      const lastVisit = previousVisits[previousVisits.length - 1];
      const lastVisitEndTime = lastVisit.market_end_time || lastVisit.besuchszeit_bis;
      if (lastVisitEndTime) {
        const { interval } = calculateTimeDiff(lastVisitEndTime, marketStartTime);
        calculatedFahrzeit = interval;
      }
    }
    
    res.json({
      visit_order: visitOrder,
      calculated_fahrzeit: calculatedFahrzeit,
      market_start_time: marketStartTime
    });
  } catch (error: any) {
    console.error('Error recording market start:', error);
    res.status(500).json({ error: error.message || 'Failed to record market start' });
  }
});


// ============================================================================
// FRAGEBOGEN EXCEL EXPORT
// GET /api/fragebogen/fragebogen/:id/export.xlsx
// Generates a multi-sheet Excel workbook:
//   - One sheet per GL with market-block layout
//   - One "Auswertung" sheet with question-level averages
// ============================================================================

// ---- Helper: resolve a typed answer to a human-readable string ----
function resolveAnswerDisplay(answer: any, question: any): string {
  if (!answer) return '—';

  const type: string = question?.type || answer.question_type || '';
  const options: { id: string; label: string }[] = question?.options ?? [];
  const mc = question?.matrix_config ?? null;
  const likertScale = question?.likert_scale ?? null;
  const sliderCfg = question?.slider_config ?? null;

  switch (type) {
    case 'yesno':
      if (answer.answer_boolean === true) return 'Ja';
      if (answer.answer_boolean === false) return 'Nein';
      return '—';

    case 'single_choice': {
      const val = answer.answer_text;
      if (!val) return '—';
      const opt = options.find(o => o.id === val);
      return opt ? opt.label : `[Unbekannt: ${val}]`;
    }

    case 'multiple_choice': {
      const vals: string[] = answer.answer_json ?? [];
      if (!Array.isArray(vals) || vals.length === 0) return '—';
      return vals.map(v => {
        const opt = options.find(o => o.id === v);
        return opt ? opt.label : `[Unbekannt: ${v}]`;
      }).join(', ');
    }

    case 'likert': {
      const n = answer.answer_numeric;
      if (n === null || n === undefined) return '—';
      const parts = [`${n}`];
      if (likertScale?.min !== undefined && likertScale?.max !== undefined) {
        parts.push(`(${likertScale.min}–${likertScale.max})`);
      }
      if (n === likertScale?.min && likertScale?.minLabel) parts.push(`— ${likertScale.minLabel}`);
      if (n === likertScale?.max && likertScale?.maxLabel) parts.push(`— ${likertScale.maxLabel}`);
      return parts.join(' ');
    }

    case 'open_numeric': {
      const n = answer.answer_numeric;
      if (n === null || n === undefined) return '—';
      return String(n);
    }

    case 'slider': {
      const n = answer.answer_numeric;
      if (n === null || n === undefined) return '—';
      const unit = sliderCfg?.unit ?? '';
      return unit ? `${n} ${unit}` : `${n}`;
    }

    case 'open_text':
      return answer.answer_text?.trim() || '—';

    case 'barcode_scanner':
      return answer.answer_text || '—';

    case 'photo_upload':
      return answer.answer_file_url ? answer.answer_file_url : '—';

    case 'matrix': {
      const val = answer.answer_json;
      if (!val || typeof val !== 'object' || Array.isArray(val)) return '—';
      const rows: { id: string; label: string }[] = mc?.rows ?? [];
      const cols: { id: string; label: string }[] = mc?.columns ?? [];
      return Object.entries(val).map(([rowId, colId]) => {
        const rowLabel = rows.find(r => r.id === rowId)?.label ?? `[Zeile: ${rowId}]`;
        const colLabel = cols.find(c => c.id === colId as string)?.label ?? `[Spalte: ${colId}]`;
        return `${rowLabel} → ${colLabel}`;
      }).join('\n') || '—';
    }

    default:
      return answer.answer_text ?? answer.answer_numeric ?? (answer.answer_boolean != null ? String(answer.answer_boolean) : '—');
  }
}

// ---- Helper: question type German label ----
function typeLabel(type: string): string {
  const map: Record<string, string> = {
    single_choice: 'Einfachauswahl',
    multiple_choice: 'Mehrfachauswahl',
    yesno: 'Ja/Nein',
    likert: 'Likert-Skala',
    open_numeric: 'Numerisch',
    slider: 'Slider',
    open_text: 'Freitext',
    barcode_scanner: 'Barcode',
    photo_upload: 'Foto-Upload',
    matrix: 'Matrix',
  };
  return map[type] ?? type;
}

// ---- Style helpers ----
function applyHeaderStyle(row: ExcelJS.Row, bgArgb = 'FF1E3A5F') {
  row.eachCell(cell => {
    cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: bgArgb } };
    cell.font = { bold: true, color: { argb: 'FFFFFFFF' }, size: 11 };
    cell.alignment = { vertical: 'middle', wrapText: true };
    cell.border = {
      bottom: { style: 'medium', color: { argb: 'FFB0B8C4' } }
    };
  });
}

function applySectionHeader(row: ExcelJS.Row) {
  row.eachCell(cell => {
    cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFD6E4F7' } };
    cell.font = { bold: true, size: 10, color: { argb: 'FF1E3A5F' } };
    cell.alignment = { vertical: 'middle' };
    cell.border = { bottom: { style: 'thin', color: { argb: 'FFA0B4CA' } } };
  });
}

function applySubHeader(row: ExcelJS.Row) {
  row.eachCell(cell => {
    cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFEDF4FF' } };
    cell.font = { bold: true, size: 9, italic: true, color: { argb: 'FF2C5F8A' } };
    cell.alignment = { vertical: 'middle' };
  });
}

function applyDataRow(row: ExcelJS.Row, zebra: boolean) {
  row.eachCell(cell => {
    cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: zebra ? 'FFF7FAFF' : 'FFFFFFFF' } };
    cell.font = { size: 10 };
    cell.alignment = { vertical: 'top', wrapText: true };
    cell.border = { bottom: { style: 'hair', color: { argb: 'FFD0D7E2' } } };
  });
}

function applyAverageHeader(row: ExcelJS.Row) {
  row.eachCell(cell => {
    cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF0F2C4A' } };
    cell.font = { bold: true, color: { argb: 'FFFFFFFF' }, size: 11 };
    cell.alignment = { vertical: 'middle', horizontal: 'center', wrapText: true };
  });
}

function applyGroupRow(row: ExcelJS.Row) {
  row.eachCell(cell => {
    cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFE8F0FB' } };
    cell.font = { bold: true, size: 10, color: { argb: 'FF1E3A5F' } };
    cell.alignment = { vertical: 'middle' };
    cell.border = { top: { style: 'thin', color: { argb: 'FFA0B4CA' } } };
  });
}

router.get('/fragebogen/:id/export.xlsx', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    const freshClient = createFreshClient();

    // 1) Load Fragebogen definition
    const { data: fragebogen, error: fbError } = await freshClient
      .from('fb_fragebogen')
      .select('id, name, start_date, end_date')
      .eq('id', id)
      .single();
    if (fbError || !fragebogen) return res.status(404).json({ error: 'Fragebogen not found' });

    // 2) Load all responses for this Fragebogen
    const { data: responses, error: rError } = await freshClient
      .from('fb_responses')
      .select(`
        id, status, started_at, completed_at, gebietsleiter_id, market_id,
        user:users!gebietsleiter_id (id, first_name, last_name),
        market:markets!market_id (id, name, chain, address, postal_code, city)
      `)
      .eq('fragebogen_id', id)
      .order('started_at', { ascending: true });
    if (rError) throw rError;
    if (!responses || responses.length === 0) {
      return res.status(404).json({ error: 'No responses found for this Fragebogen' });
    }

    const responseIds = responses.map((r: any) => r.id);

    // 3) Load all answers with question metadata
    const { data: allAnswers, error: aError } = await freshClient
      .from('fb_response_answers')
      .select(`
        id, response_id, question_id, module_id, question_type,
        answer_kind, answer_text, answer_numeric, answer_boolean,
        answer_json, answer_file_url, answered_at,
        question:fb_questions!question_id (
          id, type, question_text, options, matrix_config, likert_scale,
          numeric_constraints, slider_config
        )
      `)
      .in('response_id', responseIds)
      .order('answered_at', { ascending: true });
    if (aError) throw aError;

    // 4) Load Fragebogen module/question order for consistent column ordering
    const { data: fbModules } = await freshClient
      .from('fb_fragebogen_modules')
      .select(`
        order_index,
        module:fb_modules!module_id (
          id, name,
          questions:fb_module_questions (
            order_index, question_id,
            question:fb_questions!question_id (id, type, question_text, options, matrix_config, likert_scale, slider_config)
          )
        )
      `)
      .eq('fragebogen_id', id)
      .order('order_index', { ascending: true });

    // Build ordered question list
    type OrderedQuestion = {
      id: string; type: string; question_text: string; module_name: string;
      options: any[]; matrix_config: any; likert_scale: any; slider_config: any;
    };
    const orderedQuestions: OrderedQuestion[] = [];
    const questionIds: string[] = [];
    for (const fm of (fbModules ?? [])) {
      const mod = (fm as any).module;
      const moduleQuestions = [...((mod?.questions ?? []) as any[])]
        .sort((a: any, b: any) => a.order_index - b.order_index);
      for (const mq of moduleQuestions) {
        const q = mq.question;
        if (!q || questionIds.includes(q.id)) continue;
        questionIds.push(q.id);
        orderedQuestions.push({
          id: q.id, type: q.type, question_text: q.question_text,
          module_name: mod?.name ?? '',
          options: q.options ?? [], matrix_config: q.matrix_config ?? null,
          likert_scale: q.likert_scale ?? null, slider_config: q.slider_config ?? null,
        });
      }
    }
    // Fallback: questions that appeared in answers but not in module order
    for (const ans of (allAnswers ?? [])) {
      const q = (ans as any).question;
      if (!q || questionIds.includes(q.id)) continue;
      questionIds.push(q.id);
      orderedQuestions.push({
        id: q.id, type: q.type, question_text: q.question_text, module_name: '',
        options: q.options ?? [], matrix_config: q.matrix_config ?? null,
        likert_scale: q.likert_scale ?? null, slider_config: q.slider_config ?? null,
      });
    }

    // Index answers by response_id -> question_id -> answer
    const answerIndex: Record<string, Record<string, any>> = {};
    for (const ans of (allAnswers ?? [])) {
      const a = ans as any;
      if (!answerIndex[a.response_id]) answerIndex[a.response_id] = {};
      answerIndex[a.response_id][a.question_id] = a;
    }

    // Group responses by GL
    type AnyResp = any;
    const byGL: Record<string, { gl: any; responses: AnyResp[] }> = {};
    for (const resp of responses) {
      const r = resp as any;
      const glId = r.gebietsleiter_id;
      if (!byGL[glId]) byGL[glId] = { gl: r.user, responses: [] };
      byGL[glId].responses.push(r);
    }

    // ---- Build Workbook ----
    const workbook = new ExcelJS.Workbook();
    workbook.creator = 'Mars Rover Admin';
    workbook.created = new Date();

    const COLS = {
      Q_NUM: 6, Q_TYPE: 16, Q_TEXT: 40, ANSWER: 50, STATUS: 12
    };

    // ---- Averages Sheet (first) ----
    const avgWs = workbook.addWorksheet('Auswertung');
    avgWs.columns = [
      { key: 'c1', width: 6 },
      { key: 'c2', width: 18 },
      { key: 'c3', width: 42 },
      { key: 'c4', width: 30 },
      { key: 'c5', width: 14 },
      { key: 'c6', width: 14 },
      { key: 'c7', width: 14 },
      { key: 'c8', width: 14 },
    ];
    // ---- GL Sheets (after Auswertung is created first) ----
    for (const glId of Object.keys(byGL)) {
      const { gl, responses: glResponses } = byGL[glId];
      const glName = gl ? `${gl.first_name} ${gl.last_name}` : `GL ${glId.slice(0, 8)}`;
      // Excel sheet names max 31 chars, no special chars
      const sheetName = glName.replace(/[\\\/\*\?\[\]:]/g, '').slice(0, 31);
      const ws = workbook.addWorksheet(sheetName);

      // Column definitions
      ws.columns = [
        { key: 'col1', width: 5 },   // #
        { key: 'col2', width: COLS.Q_TYPE },  // Typ
        { key: 'col3', width: COLS.Q_TEXT },  // Frage
        { key: 'col4', width: COLS.ANSWER },  // Antwort
        { key: 'col5', width: 16 },  // Status / Info
      ];

      // Sheet title
      const titleRow = ws.addRow([`Fragebogen: ${fragebogen.name}   —   GL: ${glName}`]);
      titleRow.height = 28;
      ws.mergeCells(titleRow.number, 1, titleRow.number, 5);
      titleRow.getCell(1).fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF0F2C4A' } };
      titleRow.getCell(1).font = { bold: true, size: 13, color: { argb: 'FFFFFFFF' } };
      titleRow.getCell(1).alignment = { vertical: 'middle', horizontal: 'left' };

      ws.addRow([]); // spacer

      // Group responses by market
      const byMarket: Record<string, AnyResp[]> = {};
      const marketOrder: string[] = [];
      for (const resp of glResponses) {
        const r = resp as any;
        if (!byMarket[r.market_id]) { byMarket[r.market_id] = []; marketOrder.push(r.market_id); }
        byMarket[r.market_id].push(r);
      }

      let questionCounter = 0;

      for (const marketId of marketOrder) {
        const marketResponses = byMarket[marketId];
        const firstResp = marketResponses[0] as any;
        const market = firstResp.market as any;
        const marketLabel = market
          ? `${market.chain}  ${market.name}  —  ${market.address ?? ''}, ${market.postal_code ?? ''} ${market.city ?? ''}`
          : `Markt ${marketId.slice(0, 8)}`;

        // Market header
        const mRow = ws.addRow([``, marketLabel, '', '', '']);
        ws.mergeCells(mRow.number, 1, mRow.number, 5);
        mRow.getCell(1).value = marketLabel;
        mRow.height = 22;
        applySectionHeader(mRow);

        for (let subIdx = 0; subIdx < marketResponses.length; subIdx++) {
          const resp = marketResponses[subIdx] as any;
          const submissionDate = resp.started_at
            ? new Date(resp.started_at).toLocaleString('de-AT', { day: '2-digit', month: '2-digit', year: 'numeric', hour: '2-digit', minute: '2-digit' })
            : '—';
          const statusLabel = resp.status === 'completed' ? 'Abgeschlossen' : 'In Bearbeitung';
          const subLabel = marketResponses.length > 1
            ? `Einreichung ${subIdx + 1} von ${marketResponses.length}  |  ${submissionDate}  |  ${statusLabel}`
            : `Einreichung  |  ${submissionDate}  |  ${statusLabel}`;

          const subRow = ws.addRow(['', subLabel, '', '', '']);
          ws.mergeCells(subRow.number, 1, subRow.number, 5);
          subRow.getCell(1).value = subLabel;
          subRow.height = 18;
          applySubHeader(subRow);

          // Column header for questions block
          const colHeaderRow = ws.addRow(['#', 'Fragetyp', 'Frage', 'Antwort', 'Modul']);
          colHeaderRow.height = 20;
          applyHeaderStyle(colHeaderRow, 'FF2C5F8A');

          const answers = answerIndex[resp.id] ?? {};

          if (orderedQuestions.length === 0) {
            const noQRow = ws.addRow(['', '', 'Keine Fragen vorhanden', '—', '']);
            applyDataRow(noQRow, false);
          } else {
            let localQ = 0;
            for (const oq of orderedQuestions) {
              localQ++;
              questionCounter++;
              const ans = answers[oq.id];
              const displayVal = ans ? resolveAnswerDisplay(ans, oq) : '—';
              const dataRow = ws.addRow([localQ, typeLabel(oq.type), oq.question_text, displayVal, oq.module_name]);
              dataRow.getCell(4).alignment = { wrapText: true, vertical: 'top' };
              dataRow.getCell(3).alignment = { wrapText: true, vertical: 'top' };
              dataRow.height = displayVal.includes('\n') ? Math.min(20 * (displayVal.split('\n').length), 80) : 18;
              applyDataRow(dataRow, localQ % 2 === 0);
            }
          }

          ws.addRow([]); // spacer between submissions
        }

        ws.addRow([]); // spacer between markets
      }

      void questionCounter; // used for tracking only
    }

    // ---- Now fill Auswertung sheet content ----

    // Title
    const avgTitle = avgWs.addRow([`Auswertung: ${fragebogen.name}`]);
    avgTitle.height = 28;
    avgWs.mergeCells(avgTitle.number, 1, avgTitle.number, 8);
    avgTitle.getCell(1).fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF0F2C4A' } };
    avgTitle.getCell(1).font = { bold: true, size: 13, color: { argb: 'FFFFFFFF' } };
    avgTitle.getCell(1).alignment = { vertical: 'middle' };

    // Meta row
    const totalSubmissions = responses.length;
    const completedCount = responses.filter((r: any) => r.status === 'completed').length;
    const metaRow = avgWs.addRow([
      '',
      `Einreichungen gesamt: ${totalSubmissions}`,
      `Davon abgeschlossen: ${completedCount}`,
      `Exportdatum: ${new Date().toLocaleDateString('de-AT')}`,
      '', '', '', ''
    ]);
    avgWs.mergeCells(metaRow.number, 2, metaRow.number, 4);
    metaRow.getCell(2).font = { italic: true, size: 9, color: { argb: 'FF555555' } };
    avgWs.addRow([]);

    // Column header
    const avgColHeader = avgWs.addRow(['#', 'Fragetyp', 'Frage', 'Antwort / Wert', 'Anzahl', 'Anteil', 'Min', 'Max']);
    avgColHeader.height = 22;
    applyAverageHeader(avgColHeader);

    // Compute aggregates per question
    let qIdx = 0;
    let groupHeaderPrinted = '';

    for (const oq of orderedQuestions) {
      qIdx++;

      // Print module group header if changed
      if (oq.module_name && oq.module_name !== groupHeaderPrinted) {
        groupHeaderPrinted = oq.module_name;
        const grpRow = avgWs.addRow(['', '', `Modul: ${oq.module_name}`, '', '', '', '', '']);
        avgWs.mergeCells(grpRow.number, 1, grpRow.number, 8);
        grpRow.getCell(1).value = `Modul: ${oq.module_name}`;
        applyGroupRow(grpRow);
      }

      // Gather all answers for this question across all responses
      const questionAnswers = (allAnswers ?? []).filter((a: any) => a.question_id === oq.id);
      const answered = questionAnswers.length;
      const pct = (n: number) => totalSubmissions > 0 ? `${Math.round((n / totalSubmissions) * 100)}%` : '—';

      const numericTypes = ['likert', 'open_numeric', 'slider'];
      const choiceTypes = ['single_choice', 'multiple_choice'];
      const isNumeric = numericTypes.includes(oq.type);
      const isChoice = choiceTypes.includes(oq.type);

      if (isNumeric) {
        const nums = questionAnswers
          .map((a: any) => a.answer_numeric)
          .filter((n: any) => typeof n === 'number' && !isNaN(n));
        const mean = nums.length > 0 ? nums.reduce((s: number, n: number) => s + n, 0) / nums.length : null;
        const min = nums.length > 0 ? Math.min(...nums) : null;
        const max = nums.length > 0 ? Math.max(...nums) : null;

        const dRow = avgWs.addRow([
          qIdx, typeLabel(oq.type), oq.question_text,
          mean !== null ? `Ø ${mean.toFixed(2)}` : '—',
          answered, pct(answered),
          min !== null ? min : '—',
          max !== null ? max : '—'
        ]);
        applyDataRow(dRow, qIdx % 2 === 0);

      } else if (oq.type === 'yesno') {
        const jaCount = questionAnswers.filter((a: any) => a.answer_boolean === true).length;
        const neinCount = questionAnswers.filter((a: any) => a.answer_boolean === false).length;

        const r1 = avgWs.addRow([qIdx, typeLabel(oq.type), oq.question_text, 'Ja', jaCount, pct(jaCount), '', '']);
        applyDataRow(r1, qIdx % 2 === 0);
        const r2 = avgWs.addRow(['', '', '', 'Nein', neinCount, pct(neinCount), '', '']);
        applyDataRow(r2, false);

      } else if (isChoice) {
        const options = oq.options ?? [];
        // Count per option
        const optCounts: Record<string, number> = {};
        for (const opt of options) optCounts[opt.id] = 0;

        for (const ans of questionAnswers) {
          const a = ans as any;
          if (oq.type === 'single_choice' && a.answer_text) {
            optCounts[a.answer_text] = (optCounts[a.answer_text] ?? 0) + 1;
          } else if (oq.type === 'multiple_choice' && Array.isArray(a.answer_json)) {
            for (const optId of a.answer_json) {
              optCounts[optId] = (optCounts[optId] ?? 0) + 1;
            }
          }
        }

        const denom = oq.type === 'single_choice' ? totalSubmissions : answered;
        let first = true;
        for (const opt of options) {
          const cnt = optCounts[opt.id] ?? 0;
          const optPct = denom > 0 ? `${Math.round((cnt / denom) * 100)}%` : '—';
          const dRow = avgWs.addRow([
            first ? qIdx : '',
            first ? typeLabel(oq.type) : '',
            first ? oq.question_text : '',
            opt.label, cnt, optPct, '', ''
          ]);
          applyDataRow(dRow, qIdx % 2 === 0);
          first = false;
        }
        if (options.length === 0) {
          const dRow = avgWs.addRow([qIdx, typeLabel(oq.type), oq.question_text, '—', answered, pct(answered), '', '']);
          applyDataRow(dRow, qIdx % 2 === 0);
        }

      } else if (oq.type === 'matrix') {
        const rows: { id: string; label: string }[] = oq.matrix_config?.rows ?? [];
        const cols: { id: string; label: string }[] = oq.matrix_config?.columns ?? [];
        // Count [rowId][colId]
        const matCounts: Record<string, Record<string, number>> = {};
        for (const row of rows) {
          matCounts[row.id] = {};
          for (const col of cols) matCounts[row.id][col.id] = 0;
        }
        for (const ans of questionAnswers) {
          const val = (ans as any).answer_json;
          if (!val || typeof val !== 'object') continue;
          for (const [rowId, colId] of Object.entries(val)) {
            if (matCounts[rowId]) {
              matCounts[rowId][colId as string] = (matCounts[rowId][colId as string] ?? 0) + 1;
            }
          }
        }
        let first = true;
        for (const row of rows) {
          for (const col of cols) {
            const cnt = matCounts[row.id]?.[col.id] ?? 0;
            const optPct = answered > 0 ? `${Math.round((cnt / answered) * 100)}%` : '—';
            const dRow = avgWs.addRow([
              first ? qIdx : '',
              first ? typeLabel(oq.type) : '',
              first ? oq.question_text : '',
              `${row.label} → ${col.label}`,
              cnt, optPct, '', ''
            ]);
            applyDataRow(dRow, qIdx % 2 === 0);
            first = false;
          }
        }
        if (rows.length === 0) {
          const dRow = avgWs.addRow([qIdx, typeLabel(oq.type), oq.question_text, '—', answered, pct(answered), '', '']);
          applyDataRow(dRow, qIdx % 2 === 0);
        }

      } else {
        // open_text, barcode_scanner, photo_upload
        const textSamples = questionAnswers
          .map((a: any) => a.answer_text ?? a.answer_file_url ?? '')
          .filter(Boolean)
          .slice(0, 3)
          .join(' | ');
        const dRow = avgWs.addRow([
          qIdx, typeLabel(oq.type), oq.question_text,
          textSamples || '—',
          answered, pct(answered), '', ''
        ]);
        dRow.getCell(4).alignment = { wrapText: true, vertical: 'top' };
        applyDataRow(dRow, qIdx % 2 === 0);
      }
    }

    // Stream response
    const safeName = fragebogen.name.replace(/[^a-zA-Z0-9_\-]/g, '_').slice(0, 40);
    const dateStr = new Date().toISOString().slice(0, 10);
    const fileName = `fragebogen_${safeName}_${dateStr}.xlsx`;

    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', `attachment; filename="${fileName}"`);

    console.log(`📊 Exporting Fragebogen "${fragebogen.name}" — ${responses.length} responses, ${orderedQuestions.length} questions`);
    await workbook.xlsx.write(res);
    res.end();
  } catch (error: any) {
    console.error('Error generating Fragebogen export:', error);
    if (!res.headersSent) {
      res.status(500).json({ error: error.message || 'Failed to generate export' });
    }
  }
});

// ============================================================================
// FRAGEBOGEN DISTRIBUTION EXPORT (Monatlich, Ja/Nein)
// POST /api/fragebogen/fragebogen/distribution-export.xlsx
// ============================================================================
router.post('/fragebogen/distribution-export.xlsx', async (req: Request, res: Response) => {
  try {
    const fragebogenIds: string[] = Array.from(new Set((req.body?.fragebogen_ids || []).filter(Boolean)));
    const questionIds: string[] = Array.from(new Set((req.body?.question_ids || []).filter(Boolean)));
    const selectedChains: string[] = Array.from(
      new Set((req.body?.chains || []).map((c: string) => String(c).trim()).filter(Boolean))
    );

    if (fragebogenIds.length === 0) {
      return res.status(400).json({ error: 'Mindestens ein Fragebogen muss ausgewählt werden.' });
    }
    if (questionIds.length === 0) {
      return res.status(400).json({ error: 'Mindestens ein Ja/Nein-Item muss ausgewählt werden.' });
    }

    const freshClient = createFreshClient();

    const { data: fragebogenRows, error: fbError } = await freshClient
      .from('fb_fragebogen')
      .select('id,name')
      .in('id', fragebogenIds);
    if (fbError) throw fbError;
    if (!fragebogenRows || fragebogenRows.length === 0) {
      return res.status(400).json({ error: 'Keine gültigen Fragebögen gefunden.' });
    }

    const { data: questionRows, error: qError } = await freshClient
      .from('fb_questions')
      .select('id,question_text,type')
      .in('id', questionIds);
    if (qError) throw qError;
    if (!questionRows || questionRows.length !== questionIds.length) {
      return res.status(400).json({ error: 'Mindestens eine ausgewählte Frage wurde nicht gefunden.' });
    }

    const invalidQuestion = questionRows.find((q: any) => q.type !== 'yesno');
    if (invalidQuestion) {
      return res.status(400).json({ error: `Nur Ja/Nein-Fragen erlaubt: ${invalidQuestion.question_text}` });
    }

    const { data: responses, error: rError } = await freshClient
      .from('fb_responses')
      .select('id,fragebogen_id,market_id,gebietsleiter_id,status,completed_at')
      .in('fragebogen_id', fragebogenIds)
      .eq('status', 'completed');
    if (rError) throw rError;

    const completedResponses = (responses || []).filter((r: any) => r.market_id && r.completed_at);
    const responseIds = completedResponses.map((r: any) => r.id);
    const marketIds = Array.from(new Set(completedResponses.map((r: any) => r.market_id).filter(Boolean)));
    const glIds = Array.from(new Set(completedResponses.map((r: any) => r.gebietsleiter_id).filter(Boolean)));

    const { data: marketRows, error: mError } = marketIds.length > 0
      ? await freshClient.from('markets').select('id,name,chain').in('id', marketIds)
      : { data: [], error: null as any };
    if (mError) throw mError;

    const { data: glRows, error: glError } = glIds.length > 0
      ? await freshClient.from('users').select('id,first_name,last_name').in('id', glIds)
      : { data: [], error: null as any };
    if (glError) throw glError;

    const marketById = new Map((marketRows || []).map((m: any) => [m.id, m]));
    const glById = new Map((glRows || []).map((u: any) => [u.id, `${u.first_name || ''} ${u.last_name || ''}`.trim() || 'Unbekannt']));
    const fragebogenById = new Map((fragebogenRows || []).map((f: any) => [f.id, f]));
    const questionById = new Map((questionRows || []).map((q: any) => [q.id, q]));

    const allowedResponseIds = completedResponses
      .filter((r: any) => {
        if (selectedChains.length === 0) return true;
        const chain = String(marketById.get(r.market_id)?.chain || '').trim();
        return selectedChains.includes(chain);
      })
      .map((r: any) => r.id);

    const answers: any[] = [];
    const BATCH = 500;
    for (let i = 0; i < allowedResponseIds.length; i += BATCH) {
      const chunkIds = allowedResponseIds.slice(i, i + BATCH);
      if (chunkIds.length === 0) continue;
      const { data: answerRows, error: aError } = await freshClient
        .from('fb_response_answers')
        .select('response_id,question_id,answer_boolean,answered_at')
        .in('response_id', chunkIds)
        .in('question_id', questionIds);
      if (aError) throw aError;
      answers.push(...(answerRows || []).filter((a: any) => a.answer_boolean !== null));
    }

    const responseById = new Map(
      completedResponses
        .filter((r: any) => allowedResponseIds.includes(r.id))
        .map((r: any) => [r.id, r])
    );

    const rows = answers
      .map((answer: any) => {
        const resp = responseById.get(answer.response_id);
        if (!resp) return null;

        const market = marketById.get(resp.market_id);
        const chain = String(market?.chain || '').trim();
        const completedAt = String(resp.completed_at || '');
        const date = new Date(completedAt);
        if (Number.isNaN(date.getTime())) return null;
        const monthKey = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;
        const monthLabel = `${String(date.getMonth() + 1).padStart(2, '0')}.${date.getFullYear()}`;

        return {
          monthKey,
          monthLabel,
          fragebogenName: fragebogenById.get(resp.fragebogen_id)?.name || resp.fragebogen_id,
          questionId: answer.question_id,
          questionLabel: questionById.get(answer.question_id)?.question_text || answer.question_id,
          answerBoolean: Boolean(answer.answer_boolean),
          answerLabel: answer.answer_boolean ? 'Ja' : 'Nein',
          marketName: market?.name || resp.market_id,
          chain,
          glName: glById.get(resp.gebietsleiter_id) || 'Unbekannt',
          responseId: answer.response_id
        };
      })
      .filter(Boolean) as Array<{
        monthKey: string;
        monthLabel: string;
        fragebogenName: string;
        questionId: string;
        questionLabel: string;
        answerBoolean: boolean;
        answerLabel: string;
        marketName: string;
        chain: string;
        glName: string;
        responseId: string;
      }>;

    const exportPayload = {
      generatedAt: new Date().toISOString(),
      fragebogen: (fragebogenRows || []).map((fb: any) => ({
        id: fb.id,
        name: fb.name
      })),
      selectedChains,
      selectedQuestionIds: questionIds,
      selectedQuestions: (questionRows || []).map((q: any) => ({
        id: q.id,
        label: q.question_text || q.id
      })),
      rows
    };

    const workbookBuffer = await runDistributionPythonExporter(exportPayload);
    const fileName = `fragebogen_distribution_${new Date().toISOString().slice(0, 10)}.xlsx`;
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', `attachment; filename="${fileName}"`);
    res.status(200).send(workbookBuffer);
  } catch (error: any) {
    console.error('Error generating distribution export:', error);
    if (!res.headersSent) {
      res.status(500).json({ error: error.message || 'Distribution-Export fehlgeschlagen' });
    }
  }
});

export default router;
