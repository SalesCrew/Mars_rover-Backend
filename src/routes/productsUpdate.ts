import express from 'express';
import { randomUUID } from 'crypto';
import { createFreshClient } from '../config/supabase.js';

const router = express.Router();

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

const transformToDB = (p: any) => ({
  id: p.id,
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

// GET /api/products-update — fetch all staged update products
router.get('/', async (_req, res) => {
  try {
    const client = createFreshClient();
    const { data, error } = await client
      .from('products_update')
      .select('*')
      .order('name', { ascending: true });

    if (error) throw error;
    res.json((data || []).map(transformFromDB));
  } catch (err: any) {
    console.error('❌ GET /products-update:', err);
    res.status(500).json({ error: err.message || 'Internal server error' });
  }
});

// POST /api/products-update — replace the entire staging table with new products
router.post('/', async (req, res) => {
  try {
    const products: any[] = req.body;
    if (!Array.isArray(products) || products.length === 0) {
      return res.status(400).json({ error: 'products array is required' });
    }

    const client = createFreshClient();

    // Clear existing staging data first, then insert fresh
    const { error: delError } = await client.from('products_update').delete().neq('id', '');
    if (delError) throw delError;

    const rows = products.map(transformToDB);
    const { data, error } = await client
      .from('products_update')
      .insert(rows)
      .select();

    if (error) throw error;

    console.log(`✅ products_update: replaced with ${rows.length} products`);
    res.json((data || []).map(transformFromDB));
  } catch (err: any) {
    console.error('❌ POST /products-update:', err);
    res.status(500).json({ error: err.message || 'Internal server error' });
  }
});

// POST /api/products-update/append — add products without clearing existing ones
router.post('/append', async (req, res) => {
  try {
    const products: any[] = req.body;
    if (!Array.isArray(products) || products.length === 0) {
      return res.status(400).json({ error: 'products array is required' });
    }

    const client = createFreshClient();
    const rows = products.map(transformToDB);
    const { data, error } = await client
      .from('products_update')
      .insert(rows)
      .select();

    if (error) throw error;

    console.log(`✅ products_update: appended ${rows.length} products`);
    res.json((data || []).map(transformFromDB));
  } catch (err: any) {
    console.error('❌ POST /products-update/append:', err);
    res.status(500).json({ error: err.message || 'Internal server error' });
  }
});

// DELETE /api/products-update — clear all staged products
router.delete('/', async (_req, res) => {
  try {
    const client = createFreshClient();
    const { error } = await client.from('products_update').delete().neq('id', '');
    if (error) throw error;

    console.log('✅ products_update: cleared');
    res.json({ message: 'Staging table cleared' });
  } catch (err: any) {
    console.error('❌ DELETE /products-update:', err);
    res.status(500).json({ error: err.message || 'Internal server error' });
  }
});

// PATCH /api/products-update/:id — update a single staged product field
router.patch('/:id', async (req, res) => {
  try {
    const client = createFreshClient();
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
      .select()
      .single();

    if (error) throw error;
    res.json(transformFromDB(data));
  } catch (err: any) {
    console.error('❌ PATCH /products-update/:id:', err);
    res.status(500).json({ error: err.message || 'Internal server error' });
  }
});

// DELETE /api/products-update/:id — remove a single staged product
router.delete('/:id', async (req, res) => {
  try {
    const client = createFreshClient();
    const { error } = await client
      .from('products_update')
      .delete()
      .eq('id', req.params.id);

    if (error) throw error;
    res.json({ message: 'Product removed from staging' });
  } catch (err: any) {
    console.error('❌ DELETE /products-update/:id:', err);
    res.status(500).json({ error: err.message || 'Internal server error' });
  }
});

// POST /api/products-update/remove-by-names — delete staged products matching given names
router.post('/remove-by-names', async (req, res) => {
  try {
    const { names } = req.body as { names: string[] };
    if (!Array.isArray(names) || names.length === 0) {
      return res.status(400).json({ error: 'names array is required' });
    }

    const client = createFreshClient();
    const { error } = await client
      .from('products_update')
      .delete()
      .in('name', names);

    if (error) throw error;

    console.log(`✅ products_update: removed ${names.length} products by name`);
    res.json({ removed: names.length });
  } catch (err: any) {
    console.error('❌ POST /products-update/remove-by-names:', err);
    res.status(500).json({ error: err.message || 'Internal server error' });
  }
});

// POST /api/products-update/apply — full clean swap:
// 1. INSERT all staged products as FRESH rows (new UUIDs, no ID conflicts possible)
// 2. Soft-delete ALL previous products (those not in the newly inserted set)
// 3. Clear staging table
// Safe order: insert new FIRST (fail = old products untouched), then archive old.
router.post('/apply', async (req, res) => {
  try {
    const client = createFreshClient();

    // 1. Fetch all staged products
    const { data: staged, error: fetchError } = await client
      .from('products_update')
      .select('*');

    if (fetchError) throw fetchError;
    if (!staged || staged.length === 0) {
      return res.status(400).json({ error: 'Keine Produkte in der Staging-Liste' });
    }

    // 2. Build fresh rows with brand-new UUIDs.
    //    We never reuse staging IDs so there is zero chance of a PK conflict with
    //    any existing (active or already-archived) product row.
    const rows = staged.map((p: any) => ({
      id: randomUUID(),           // always a fresh row — no matching with old products
      name: p.name,
      department: p.department,
      product_type: p.product_type,
      weight: p.weight,
      content: p.content || null,
      pallet_size: p.pallet_size || null,
      price: p.price,
      sku: p.sku || null,
      artikel_nr: p.artikel_nr || null,
      palette_products: p.palette_products || null,
      is_active: p.is_active !== false,
      is_deleted: false,
    }));

    // INSERT — if this fails we throw and the old product list is completely untouched.
    const { error: insertError } = await client
      .from('products')
      .insert(rows);

    if (insertError) throw insertError;

    const newIds = rows.map((r: any) => r.id);

    // 3. Soft-delete ALL previous products.
    //    Because newIds are brand-new UUIDs, the NOT IN clause excludes only the rows
    //    we just inserted and soft-deletes every other row — exactly a full archive
    //    of the old product list, no exceptions.
    const { data: softDeleted, error: archiveError } = await client
      .from('products')
      .update({ is_deleted: true })
      .eq('is_deleted', false)
      .not('id', 'in', `(${newIds.map((id: string) => `"${id}"`).join(',')})`)
      .select('id');

    if (archiveError) {
      console.error('⚠️ Soft-delete of old products failed (new products already live):', archiveError);
      // Don't throw — new list is already active, old ones still visible but not catastrophic
    }

    const softDeletedCount = softDeleted?.length ?? 0;

    // 4. Clear staging table
    const { error: clearError } = await client
      .from('products_update')
      .delete()
      .neq('id', '');

    if (clearError) {
      console.error('⚠️ Could not clear staging table (swap already complete):', clearError);
    }

    console.log(`✅ Product swap complete: ${rows.length} inserted fresh, ${softDeletedCount} archived`);
    res.json({ inserted: rows.length, softDeleted: softDeletedCount });
  } catch (err: any) {
    console.error('❌ POST /products-update/apply:', err);
    res.status(500).json({ error: err.message || 'Internal server error' });
  }
});

export default router;
