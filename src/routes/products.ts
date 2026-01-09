import express from 'express';
import { supabase, createFreshClient } from '../config/supabase.js';

const router = express.Router();

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
  paletteProducts: dbProduct.palette_products || undefined,
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
  palette_products: product.paletteProducts || null,
});

// GET /api/products - Get all products
router.get('/', async (req, res) => {
  try {
    const freshClient = createFreshClient();
    
    const { data, error } = await freshClient
      .from('products')
      .select('*')
      .order('name', { ascending: true });

    if (error) {
      console.error('Error fetching products:', error);
      return res.status(500).json({ error: 'Failed to fetch products' });
    }

    const products = (data || []).map(transformProductFromDB);
    res.json(products);
  } catch (error) {
    console.error('Error in GET /products:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/products/:id - Get single product
router.get('/:id', async (req, res) => {
  try {
    const freshClient = createFreshClient();
    
    const { data, error } = await freshClient
      .from('products')
      .select('*')
      .eq('id', req.params.id)
      .single();

    if (error) {
      console.error('Error fetching product:', error);
      return res.status(404).json({ error: 'Product not found' });
    }

    res.json(transformProductFromDB(data));
  } catch (error) {
    console.error('Error in GET /products/:id:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// POST /api/products - Create products (bulk insert)
router.post('/', async (req, res) => {
  try {
    const freshClient = createFreshClient();
    
    const products = Array.isArray(req.body) ? req.body : [req.body];
    const dbProducts = products.map(transformProductToDB);

    const { data, error } = await freshClient
      .from('products')
      .insert(dbProducts)
      .select();

    if (error) {
      console.error('Error creating products:', error);
      return res.status(500).json({ error: 'Failed to create products', details: error });
    }

    const createdProducts = (data || []).map(transformProductFromDB);
    res.status(201).json(createdProducts);
  } catch (error) {
    console.error('Error in POST /products:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// PUT /api/products/:id - Update product
router.put('/:id', async (req, res) => {
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
    if (req.body.paletteProducts !== undefined) updates.palette_products = req.body.paletteProducts || null;

    const { data, error } = await freshClient
      .from('products')
      .update(updates)
      .eq('id', req.params.id)
      .select()
      .single();

    if (error) {
      console.error('Error updating product:', error);
      return res.status(500).json({ error: 'Failed to update product' });
    }

    res.json(transformProductFromDB(data));
  } catch (error) {
    console.error('Error in PUT /products/:id:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// DELETE /api/products/:id - Delete product
router.delete('/:id', async (req, res) => {
  try {
    const freshClient = createFreshClient();
    
    const { error } = await freshClient
      .from('products')
      .delete()
      .eq('id', req.params.id);

    if (error) {
      console.error('Error deleting product:', error);
      return res.status(500).json({ error: 'Failed to delete product' });
    }

    res.status(204).send();
  } catch (error) {
    console.error('Error in DELETE /products/:id:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

export default router;
