import express from 'express';
import { randomUUID } from 'crypto';
import { createFreshClient } from '../config/supabase.js';
import { sendInternalError } from '../utils/httpErrors';

const router = express.Router();

const PRODUCTS_UPDATE_SELECT = 'id, batch_id, name, department, product_type, weight, content, pallet_size, price, sku, artikel_nr, palette_products, is_active';
const BATCH_SELECT = 'id, status, scheduled_for, created_at, updated_at, applied_at, applied_inserted_count, applied_soft_deleted_count, error_message';
const EDITABLE_BATCH_STATUSES = ['draft', 'scheduled'];
const SCHEDULER_INTERVAL_MS = Math.max(
  15_000,
  parseInt(process.env.PRODUCT_UPDATE_SCHEDULER_INTERVAL_MS || '60000', 10)
);

let schedulerStarted = false;
let schedulerRunning = false;
let schedulerTimer: NodeJS.Timeout | undefined;

const transformFromDB = (row: any) => ({
  id: row.id,
  name: row.name,
  department: row.department,
  productType: row.product_type,
  weight: row.weight,
  content: row.content || undefined,
  palletSize: row.pallet_size || undefined,
  price: parseFloat(row.price),
  sku: row.sku || undefined,
  artikelNr: row.artikel_nr || undefined,
  paletteProducts: row.palette_products || undefined,
  isActive: row.is_active !== false,
});

const transformToDB = (p: any, batchId: string) => ({
  id: randomUUID(),
  batch_id: batchId,
  name: p.name,
  department: p.department,
  product_type: p.productType,
  weight: p.weight,
  content: p.content || null,
  pallet_size: p.palletSize || null,
  price: p.price,
  sku: p.sku || null,
  artikel_nr: p.artikelNr || null,
  palette_products: p.paletteProducts || null,
  is_active: p.isActive !== false,
});

const transformBatchFromDB = (row: any, productCount = 0) => ({
  id: row.id,
  status: row.status,
  scheduledFor: row.scheduled_for || null,
  createdAt: row.created_at,
  updatedAt: row.updated_at,
  appliedAt: row.applied_at || null,
  inserted: row.applied_inserted_count ?? null,
  softDeleted: row.applied_soft_deleted_count ?? null,
  error: row.error_message || null,
  productCount,
});

const normalizeErrorMessage = (error: any): string => {
  const raw = error?.message || error?.details || error?.hint || String(error || 'Unknown error');
  return raw.slice(0, 500);
};

const countProductsForBatch = async (client: any, batchId: string): Promise<number> => {
  const { count, error } = await client
    .from('products_update')
    .select('id', { count: 'exact', head: true })
    .eq('batch_id', batchId);

  if (error) throw error;
  return count || 0;
};

const clearScheduleIfBatchEmpty = async (client: any, batchId: string) => {
  const remainingCount = await countProductsForBatch(client, batchId);
  if (remainingCount === 0) {
    await client
      .from('product_update_batches')
      .update({ status: 'draft', scheduled_for: null, error_message: null })
      .eq('id', batchId);
  }
  return remainingCount;
};

const getEditableBatch = async (client: any, createIfMissing = false): Promise<any | null> => {
  const { data, error } = await client
    .from('product_update_batches')
    .select(BATCH_SELECT)
    .in('status', EDITABLE_BATCH_STATUSES)
    .order('created_at', { ascending: false })
    .limit(1)
    .maybeSingle();

  if (error) throw error;
  if (data) return data;

  if (!createIfMissing) return null;

  const { data: created, error: createError } = await client
    .from('product_update_batches')
    .insert({ status: 'draft' })
    .select(BATCH_SELECT)
    .single();

  if (createError) throw createError;
  return created;
};

const loadEditableBatchSummary = async (client: any) => {
  const batch = await getEditableBatch(client, false);
  if (!batch) return null;
  const productCount = await countProductsForBatch(client, batch.id);
  return transformBatchFromDB(batch, productCount);
};

const activateBatch = async (client: any, batchId: string) => {
  const { data, error } = await client.rpc('activate_product_update_batch', {
    p_batch_id: batchId,
  });

  if (error) throw error;

  const result = Array.isArray(data) ? data[0] : data;
  return {
    inserted: result?.inserted_count || 0,
    softDeleted: result?.soft_deleted_count || 0,
  };
};

const markBatchFailed = async (client: any, batchId: string, error: any) => {
  await client
    .from('product_update_batches')
    .update({
      status: 'failed',
      error_message: normalizeErrorMessage(error),
    })
    .eq('id', batchId);
};

export const runDueProductUpdateBatches = async () => {
  if (schedulerRunning) return;
  schedulerRunning = true;

  const client = createFreshClient();
  try {
    const { data: dueBatches, error } = await client
      .from('product_update_batches')
      .select('id, scheduled_for')
      .eq('status', 'scheduled')
      .lte('scheduled_for', new Date().toISOString())
      .order('scheduled_for', { ascending: true })
      .limit(5);

    if (error) throw error;

    for (const batch of dueBatches || []) {
      try {
        const result = await activateBatch(client, batch.id);
        console.log(
          `Product update batch ${batch.id} applied: ${result.inserted} inserted, ${result.softDeleted} archived`
        );
      } catch (batchError) {
        console.error('Product update batch failed:');
        await markBatchFailed(client, batch.id, batchError);
      }
    }
  } catch (error) {
    console.error('Product update scheduler failed:');
  } finally {
    schedulerRunning = false;
  }
};

export const startProductUpdateScheduler = () => {
  if (schedulerStarted) return;
  schedulerStarted = true;

  runDueProductUpdateBatches().catch(() => undefined);
  schedulerTimer = setInterval(() => {
    runDueProductUpdateBatches().catch(() => undefined);
  }, SCHEDULER_INTERVAL_MS);
  schedulerTimer.unref?.();
};

// GET /api/products-update - fetch all staged products for the editable batch
router.get('/', async (_req, res) => {
  try {
    const client = createFreshClient();
    const batch = await getEditableBatch(client, false);
    if (!batch) return res.json([]);

    const { data, error } = await client
      .from('products_update')
      .select(PRODUCTS_UPDATE_SELECT)
      .eq('batch_id', batch.id)
      .order('name', { ascending: true });

    if (error) throw error;
    res.json((data || []).map(transformFromDB));
  } catch (err: any) {
    console.error('GET /products-update failed:');
    sendInternalError(res);
  }
});

// GET /api/products-update/batch - fetch editable batch state
router.get('/batch', async (_req, res) => {
  try {
    const client = createFreshClient();
    res.json(await loadEditableBatchSummary(client));
  } catch (err: any) {
    console.error('GET /products-update/batch failed:');
    sendInternalError(res);
  }
});

// POST /api/products-update - replace the editable staging list with new products
router.post('/', async (req, res) => {
  try {
    const products: any[] = req.body;
    if (!Array.isArray(products) || products.length === 0) {
      return res.status(400).json({ error: 'products array is required' });
    }

    const client = createFreshClient();
    const batch = await getEditableBatch(client, true);

    const { error: delError } = await client
      .from('products_update')
      .delete()
      .eq('batch_id', batch.id);
    if (delError) throw delError;

    const rows = products.map((product) => transformToDB(product, batch.id));
    const { data, error } = await client
      .from('products_update')
      .insert(rows)
      .select(PRODUCTS_UPDATE_SELECT);

    if (error) throw error;

    console.log(`products_update batch ${batch.id}: replaced with ${rows.length} products`);
    res.json((data || []).map(transformFromDB));
  } catch (err: any) {
    console.error('POST /products-update failed:');
    sendInternalError(res);
  }
});

// POST /api/products-update/append - add products to the editable staging list
router.post('/append', async (req, res) => {
  try {
    const products: any[] = req.body;
    if (!Array.isArray(products) || products.length === 0) {
      return res.status(400).json({ error: 'products array is required' });
    }

    const client = createFreshClient();
    const batch = await getEditableBatch(client, true);
    const rows = products.map((product) => transformToDB(product, batch.id));

    const { data, error } = await client
      .from('products_update')
      .insert(rows)
      .select(PRODUCTS_UPDATE_SELECT);

    if (error) throw error;

    console.log(`products_update batch ${batch.id}: appended ${rows.length} products`);
    res.json((data || []).map(transformFromDB));
  } catch (err: any) {
    console.error('POST /products-update/append failed:');
    sendInternalError(res);
  }
});

// DELETE /api/products-update - clear the editable staging list and remove its schedule
router.delete('/', async (_req, res) => {
  try {
    const client = createFreshClient();
    const batch = await getEditableBatch(client, false);
    if (!batch) return res.json({ message: 'Staging table cleared' });

    const { error } = await client
      .from('products_update')
      .delete()
      .eq('batch_id', batch.id);
    if (error) throw error;

    await client
      .from('product_update_batches')
      .update({ status: 'draft', scheduled_for: null, error_message: null })
      .eq('id', batch.id);

    console.log(`products_update batch ${batch.id}: cleared`);
    res.json({ message: 'Staging table cleared' });
  } catch (err: any) {
    console.error('DELETE /products-update failed:');
    sendInternalError(res);
  }
});

// PATCH /api/products-update/:id - update a single staged product field
router.patch('/:id', async (req, res) => {
  try {
    const client = createFreshClient();
    const batch = await getEditableBatch(client, false);
    if (!batch) return res.status(404).json({ error: 'No editable product batch found' });

    const updates: Record<string, any> = {};

    if (req.body.department !== undefined) updates.department = req.body.department;
    if (req.body.product_type !== undefined) updates.product_type = req.body.product_type;
    if (req.body.name !== undefined) updates.name = req.body.name;
    if (req.body.price !== undefined) updates.price = req.body.price;

    if (Object.keys(updates).length === 0) {
      return res.status(400).json({ error: 'No fields to update' });
    }

    const { data, error } = await client
      .from('products_update')
      .update(updates)
      .eq('id', req.params.id)
      .eq('batch_id', batch.id)
      .select(PRODUCTS_UPDATE_SELECT)
      .single();

    if (error) throw error;
    res.json(transformFromDB(data));
  } catch (err: any) {
    console.error('PATCH /products-update/:id failed:');
    sendInternalError(res);
  }
});

// DELETE /api/products-update/schedule - remove the schedule from the editable batch
router.delete('/schedule', async (_req, res) => {
  try {
    const client = createFreshClient();
    const batch = await getEditableBatch(client, false);
    if (!batch) return res.json(null);

    const productCount = await countProductsForBatch(client, batch.id);
    const { data, error } = await client
      .from('product_update_batches')
      .update({ status: 'draft', scheduled_for: null, error_message: null })
      .eq('id', batch.id)
      .select(BATCH_SELECT)
      .single();

    if (error) throw error;
    res.json(transformBatchFromDB(data, productCount));
  } catch (err: any) {
    console.error('DELETE /products-update/schedule failed:');
    sendInternalError(res);
  }
});

// POST /api/products-update/remove-by-names - delete staged products matching given names
router.post('/remove-by-names', async (req, res) => {
  try {
    const { names } = req.body as { names: string[] };
    if (!Array.isArray(names) || names.length === 0) {
      return res.status(400).json({ error: 'names array is required' });
    }

    const client = createFreshClient();
    const batch = await getEditableBatch(client, false);
    if (!batch) return res.json({ removed: 0 });

    const { error } = await client
      .from('products_update')
      .delete()
      .eq('batch_id', batch.id)
      .in('name', names);

    if (error) throw error;

    await clearScheduleIfBatchEmpty(client, batch.id);
    console.log(`products_update batch ${batch.id}: removed ${names.length} products by name`);
    res.json({ removed: names.length });
  } catch (err: any) {
    console.error('POST /products-update/remove-by-names failed:');
    sendInternalError(res);
  }
});

// POST /api/products-update/schedule - schedule the editable batch once
router.post('/schedule', async (req, res) => {
  try {
    const scheduledFor = req.body?.scheduledFor;
    const scheduledDate = new Date(scheduledFor);

    if (!scheduledFor || Number.isNaN(scheduledDate.getTime())) {
      return res.status(400).json({ error: 'scheduledFor must be a valid date' });
    }

    if (scheduledDate.getTime() <= Date.now()) {
      return res.status(400).json({ error: 'scheduledFor must be in the future' });
    }

    const client = createFreshClient();
    const batch = await getEditableBatch(client, false);
    if (!batch) return res.status(400).json({ error: 'Keine Produkte in der Staging-Liste' });

    const productCount = await countProductsForBatch(client, batch.id);
    if (productCount === 0) {
      return res.status(400).json({ error: 'Keine Produkte in der Staging-Liste' });
    }

    const { data, error } = await client
      .from('product_update_batches')
      .update({
        status: 'scheduled',
        scheduled_for: scheduledDate.toISOString(),
        error_message: null,
      })
      .eq('id', batch.id)
      .select(BATCH_SELECT)
      .single();

    if (error) throw error;

    res.json(transformBatchFromDB(data, productCount));
  } catch (err: any) {
    console.error('POST /products-update/schedule failed:');
    sendInternalError(res);
  }
});

// DELETE /api/products-update/:id - remove a single staged product
router.delete('/:id', async (req, res) => {
  try {
    const client = createFreshClient();
    const batch = await getEditableBatch(client, false);
    if (!batch) return res.status(404).json({ error: 'No editable product batch found' });

    const { error } = await client
      .from('products_update')
      .delete()
      .eq('id', req.params.id)
      .eq('batch_id', batch.id);

    if (error) throw error;

    await clearScheduleIfBatchEmpty(client, batch.id);
    res.json({ message: 'Product removed from staging' });
  } catch (err: any) {
    console.error('DELETE /products-update/:id failed:');
    sendInternalError(res);
  }
});

// POST /api/products-update/apply - activate immediately via DB transaction
router.post('/apply', async (_req, res) => {
  try {
    const client = createFreshClient();
    const batch = await getEditableBatch(client, false);
    if (!batch) {
      return res.status(400).json({ error: 'Keine Produkte in der Staging-Liste' });
    }

    const productCount = await countProductsForBatch(client, batch.id);
    if (productCount === 0) {
      return res.status(400).json({ error: 'Keine Produkte in der Staging-Liste' });
    }

    const result = await activateBatch(client, batch.id);
    console.log(
      `Product swap complete for batch ${batch.id}: ${result.inserted} inserted, ${result.softDeleted} archived`
    );
    res.json({ ...result, batchId: batch.id });
  } catch (err: any) {
    console.error('POST /products-update/apply failed:');
    sendInternalError(res);
  }
});

export default router;
