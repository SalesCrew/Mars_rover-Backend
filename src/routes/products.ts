import express from 'express';
import { createFreshClient } from '../config/supabase.js';
import { AuthRequest, requireAdmin } from '../middleware/auth';
import { sendInternalError } from '../utils/httpErrors';

const router = express.Router();
const PRODUCT_SELECT_COLUMNS = 'id, name, department, product_type, weight, content, pallet_size, price, sku, artikel_nr, palette_products, is_active, is_deleted';

// Transform from DB format to API format
const transformProductFromDB = (dbProduct: any) => ({
  id: dbProduct.id,
  name: dbProduct.name,
  department: dbProduct.department,
  productType: dbProduct.product_type,
  weight: dbProduct.weight,
  content: dbProduct.content || undefined,
  palletSize: dbProduct.pallet_size || undefined,
  price: parseFloat(dbProduct.price),
  sku: dbProduct.sku || undefined,
  artikelNr: dbProduct.artikel_nr || undefined,
  paletteProducts: dbProduct.palette_products || undefined,
  isActive: dbProduct.is_active !== false,
  isArchived: dbProduct.is_deleted === true,
});

// Transform from API format to DB format
const transformProductToDB = (product: any) => ({
  id: product.id,
  name: product.name,
  department: product.department,
  product_type: product.productType,
  weight: product.weight,
  content: product.content || null,
  pallet_size: product.palletSize || null,
  price: product.price,
  sku: product.sku || null,
  artikel_nr: product.artikelNr || null,
  palette_products: product.paletteProducts || null,
  is_active: product.isActive !== false,
});

// GET /api/products - Get all active (non-deleted) products
// Optional query param: ?includeArchived=true — returns all products including archived ones
router.get('/', async (req: AuthRequest, res) => {
  try {
    const freshClient = createFreshClient();
    const includeArchived = req.user?.role === 'admin' && req.query.includeArchived === 'true';

    let query = freshClient
      .from('products')
      .select(PRODUCT_SELECT_COLUMNS)
      .order('name', { ascending: true });

    if (!includeArchived) {
      query = query.eq('is_deleted', false);
    }

    const { data, error } = await query;

    if (error) {
      console.error('Error fetching products:');
      return sendInternalError(res);
    }

    const products = (data || []).map(transformProductFromDB);
    res.json(products);
  } catch (error) {
    console.error('Error in GET /products:');
    sendInternalError(res);
  }
});

// GET /api/products/:id - Get single product
router.get('/:id', async (req: AuthRequest, res) => {
  try {
    const freshClient = createFreshClient();

    let query = freshClient
      .from('products')
      .select(PRODUCT_SELECT_COLUMNS)
      .eq('id', req.params.id);

    if (req.user?.role !== 'admin') {
      query = query.eq('is_deleted', false);
    }

    const { data, error } = await query.single();

    if (error) {
      console.error('Error fetching product:');
      return res.status(404).json({ error: 'Product not found' });
    }

    res.json(transformProductFromDB(data));
  } catch (error) {
    console.error('Error in GET /products/:id:');
    sendInternalError(res);
  }
});

// POST /api/products - Create products (bulk insert)
router.post('/', requireAdmin, async (req, res) => {
  try {
    const freshClient = createFreshClient();

    const products = Array.isArray(req.body) ? req.body : [req.body];
    const dbProducts = products.map(transformProductToDB);

    const { data, error } = await freshClient
      .from('products')
      .insert(dbProducts)
      .select(PRODUCT_SELECT_COLUMNS);

    if (error) {
      console.error('Error creating products:');
      return sendInternalError(res);
    }

    const createdProducts = (data || []).map(transformProductFromDB);
    res.status(201).json(createdProducts);
  } catch (error) {
    console.error('Error in POST /products:');
    sendInternalError(res);
  }
});

// PUT /api/products/:id - Update product
router.put('/:id', requireAdmin, async (req, res) => {
  try {
    const freshClient = createFreshClient();

    const updates: any = {};

    if (req.body.name !== undefined) updates.name = req.body.name;
    if (req.body.department !== undefined) updates.department = req.body.department;
    if (req.body.productType !== undefined) updates.product_type = req.body.productType;
    if (req.body.weight !== undefined) updates.weight = req.body.weight;
    if (req.body.content !== undefined) updates.content = req.body.content || null;
    if (req.body.palletSize !== undefined) updates.pallet_size = req.body.palletSize || null;
    if (req.body.price !== undefined) updates.price = req.body.price;
    if (req.body.sku !== undefined) updates.sku = req.body.sku || null;
    if (req.body.artikelNr !== undefined) updates.artikel_nr = req.body.artikelNr || null;
    if (req.body.paletteProducts !== undefined) updates.palette_products = req.body.paletteProducts || null;

    const { data, error } = await freshClient
      .from('products')
      .update(updates)
      .eq('id', req.params.id)
      .select(PRODUCT_SELECT_COLUMNS)
      .single();

    if (error) {
      console.error('Error updating product:');
      return sendInternalError(res);
    }

    res.json(transformProductFromDB(data));
  } catch (error) {
    console.error('Error in PUT /products/:id:');
    sendInternalError(res);
  }
});

// PATCH /api/products/:id/archive - Soft-delete a product (sets is_deleted=true, row is never removed)
// Hard deletion is intentionally not supported to preserve historical submission data.
router.patch('/:id/archive', requireAdmin, async (req, res) => {
  try {
    const freshClient = createFreshClient();

    const { data, error } = await freshClient
      .from('products')
      .update({ is_deleted: true })
      .eq('id', req.params.id)
      .select(PRODUCT_SELECT_COLUMNS)
      .single();

    if (error) {
      console.error('Error archiving product:');
      return sendInternalError(res);
    }

    console.log('Product archived (soft-deleted)');
    res.json(transformProductFromDB(data));
  } catch (error) {
    console.error('Error in PATCH /products/:id/archive:');
    sendInternalError(res);
  }
});

export default router;
