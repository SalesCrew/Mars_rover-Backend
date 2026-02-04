import express, { Router, Request, Response } from 'express';
import { createFreshClient } from '../config/supabase';

const router: Router = express.Router();

// Request logging
router.use((req, res, next) => {
  console.log(`📋 Fragebogen Route: ${req.method} ${req.path}`);
  next();
});

// ============================================================================
// QUESTIONS API - /api/fragebogen/questions
// ============================================================================

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
 * Create a new question
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
    
    // Validate required fields
    if (!type || !question_text) {
      return res.status(400).json({ error: 'type and question_text are required' });
    }
    
    // Validate type-specific fields
    if ((type === 'single_choice' || type === 'multiple_choice') && (!options || !Array.isArray(options))) {
      return res.status(400).json({ error: 'options array is required for choice questions' });
    }
    
    if (type === 'likert' && !likert_scale) {
      return res.status(400).json({ error: 'likert_scale is required for likert questions' });
    }
    
    if (type === 'matrix' && !matrix_config) {
      return res.status(400).json({ error: 'matrix_config is required for matrix questions' });
    }
    
    const { data, error } = await freshClient
      .from('fb_questions')
      .insert({
        type,
        question_text,
        instruction: instruction || null,
        is_template: is_template || false,
        options: options || null,
        likert_scale: likert_scale || null,
        matrix_config: matrix_config || null,
        numeric_constraints: numeric_constraints || null,
        slider_config: slider_config || null,
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
 * Update an existing question
 */
router.put('/questions/:id', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    const freshClient = createFreshClient();
    const updates = req.body;
    
    // Remove fields that shouldn't be updated directly
    delete updates.id;
    delete updates.created_at;
    delete updates.created_by;
    
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
 * Permanently delete a module and optionally its questions
 */
router.delete('/modules/:id/permanent', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    const { deleteQuestions } = req.query;
    const freshClient = createFreshClient();
    
    // First get all question IDs associated with this module
    const { data: moduleQuestions, error: mqError } = await freshClient
      .from('fb_module_questions')
      .select('question_id')
      .eq('module_id', id);
    
    if (mqError) throw mqError;
    
    const questionIds = (moduleQuestions || []).map(mq => mq.question_id);
    
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
        
        return {
          ...fm,
          module: {
            ...fm.module,
            questions: questions || [],
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
 * Permanently delete a fragebogen (keeps modules and questions intact)
 */
router.delete('/fragebogen/:id/permanent', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    const freshClient = createFreshClient();
    
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
 * Get a single response with all answers
 */
router.get('/responses/:id', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    const freshClient = createFreshClient();
    
    // Get response
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
    
    // Get answers
    const { data: answers, error: answersError } = await freshClient
      .from('fb_response_answers')
      .select(`
        *,
        question:fb_questions (id, type, question_text)
      `)
      .eq('response_id', id)
      .order('answered_at', { ascending: true });
    
    if (answersError) throw answersError;
    
    res.json({
      ...response,
      answers: answers || []
    });
  } catch (error: any) {
    console.error('Error fetching response:', error);
    res.status(500).json({ error: error.message || 'Failed to fetch response' });
  }
});

/**
 * POST /api/fragebogen/responses
 * Start a new response (GL)
 */
router.post('/responses', async (req: Request, res: Response) => {
  try {
    const freshClient = createFreshClient();
    const { fragebogen_id, gebietsleiter_id, market_id } = req.body;
    
    if (!fragebogen_id || !gebietsleiter_id || !market_id) {
      return res.status(400).json({ 
        error: 'fragebogen_id, gebietsleiter_id, and market_id are required' 
      });
    }
    
    // Check if response already exists
    const { data: existing } = await freshClient
      .from('fb_responses')
      .select('id, status')
      .eq('fragebogen_id', fragebogen_id)
      .eq('gebietsleiter_id', gebietsleiter_id)
      .eq('market_id', market_id)
      .single();
    
    if (existing) {
      return res.json(existing); // Return existing response
    }
    
    // Create new response
    const { data, error } = await freshClient
      .from('fb_responses')
      .insert({
        fragebogen_id,
        gebietsleiter_id,
        market_id,
        status: 'in_progress'
      })
      .select()
      .single();
    
    if (error) throw error;
    
    console.log(`✅ Started response: ${data.id}`);
    res.status(201).json(data);
  } catch (error: any) {
    console.error('Error creating response:', error);
    res.status(500).json({ error: error.message || 'Failed to create response' });
  }
});

/**
 * PUT /api/fragebogen/responses/:id
 * Update a response (add/update answers)
 */
router.put('/responses/:id', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    const freshClient = createFreshClient();
    const { answers } = req.body;
    
    if (!answers || !Array.isArray(answers)) {
      return res.status(400).json({ error: 'answers array is required' });
    }
    
    // Upsert answers
    for (const answer of answers) {
      const { question_id, module_id, answer_text, answer_numeric, answer_json, answer_file_url } = answer;
      
      // Check if answer exists
      const { data: existing } = await freshClient
        .from('fb_response_answers')
        .select('id')
        .eq('response_id', id)
        .eq('question_id', question_id)
        .eq('module_id', module_id)
        .single();
      
      if (existing) {
        // Update existing answer
        const { error: updateError } = await freshClient
          .from('fb_response_answers')
          .update({
            answer_text,
            answer_numeric,
            answer_json,
            answer_file_url,
            answered_at: new Date().toISOString()
          })
          .eq('id', existing.id);
        
        if (updateError) throw updateError;
      } else {
        // Insert new answer
        const { error: insertError } = await freshClient
          .from('fb_response_answers')
          .insert({
            response_id: id,
            question_id,
            module_id,
            answer_text,
            answer_numeric,
            answer_json,
            answer_file_url
          });
        
        if (insertError) throw insertError;
      }
    }
    
    // Get updated response
    const { data: response } = await freshClient
      .from('fb_responses')
      .select('*')
      .eq('id', id)
      .single();
    
    console.log(`✅ Updated response: ${id}`);
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
        market:markets (id, name, chain),
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
    
    // Fetch wellen_submissions (Vorbesteller) for all markets on this date
    const { data: wellenSubs, error: wellenError } = await freshClient
      .from('wellen_submissions')
      .select('*')
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
      
      // Calculate vorbesteller totals
      const vorbestellerCount = marketWellenSubs.length;
      const vorbestellerValue = marketWellenSubs.reduce((sum, s) => {
        const qty = s.quantity || 0;
        const value = s.value_per_unit || 0;
        return sum + (qty * value);
      }, 0);
      
      return {
        ...zeitEntry,
        submissions: {
          vorbesteller: {
            count: vorbestellerCount,
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
      
      return {
        gebietsleiter_id,
        entry_date: today,
        reason: entry.reason,
        reason_label: entry.reasonLabel,
        zeit_von: entry.von,
        zeit_bis: entry.bis,
        zeit_diff: `${diffHours}:${diffMins.toString().padStart(2, '0')}:00`,
        kommentar: entry.kommentar || null,
        is_work_time_deduction: entry.reason === 'unterbrechung'
      };
    });
    
    const { data, error } = await freshClient
      .from('fb_zusatz_zeiterfassung')
      .insert(dbEntries)
      .select();
    
    if (error) throw error;
    
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
    
    res.json(data || []);
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
    
    console.log(`✅ Fetched ${(data || []).length} zusatz zeiterfassung entries`);
    res.json(data || []);
  } catch (error: any) {
    console.error('Error getting all zusatz zeiterfassung:', error);
    res.status(500).json({ error: error.message || 'Failed to get zusatz zeiterfassung' });
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

// START DAY - Create or update day tracking record
router.post('/day-tracking/start', async (req: Request, res: Response) => {
  try {
    const freshClient = createFreshClient();
    const { gebietsleiter_id, skip_fahrzeit, start_time } = req.body;
    
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
    
    // Create or update day tracking record
    const { data, error } = await freshClient
      .from('fb_day_tracking')
      .upsert({
        gebietsleiter_id,
        tracking_date: today,
        day_start_time: dayStartTime,
        skipped_first_fahrzeit: skip_fahrzeit || false,
        status: 'active',
        markets_visited: 0
      }, {
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

// END DAY - Complete day tracking and calculate totals
router.post('/day-tracking/end', async (req: Request, res: Response) => {
  try {
    const freshClient = createFreshClient();
    const { gebietsleiter_id, end_time, force_close } = req.body;
    
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
    
    // Calculate Heimfahrt (last visit end to day end)
    if (visits && visits.length > 0) {
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
    
    // Calculate total Arbeitszeit (day end - day start - unterbrechung)
    const { minutes: totalDayMinutes } = calculateTimeDiff(dayTracking.day_start_time, end_time);
    const totalArbeitszeitMinutes = totalDayMinutes - totalUnterbrechungMinutes;
    
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
        status: force_close ? 'force_closed' : 'completed'
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
    const { gebietsleiter_id, market_id } = req.body;
    
    if (!gebietsleiter_id || !market_id) {
      return res.status(400).json({ error: 'gebietsleiter_id and market_id are required' });
    }
    
    const today = new Date().toISOString().split('T')[0];
    const marketStartTime = getCurrentTimeString();
    
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

export default router;
