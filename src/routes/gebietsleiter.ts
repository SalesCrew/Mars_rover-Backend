import { Router, Request, Response } from 'express';
import { supabase, createFreshClient } from '../config/supabase';
import { aggregateSubmissions } from './wellen';
import { AuthRequest, requireAdmin, requireSelfOrAdmin } from '../middleware/auth';
import { sendInternalError } from '../utils/httpErrors';

const router = Router();
const GL_PROFILE_PICTURES_BUCKET = 'gl-profile-pictures';
const LEGACY_PROFILE_PICTURES_BUCKET = 'wellen-images';
const GL_PROFILE_PICTURE_SIGNED_URL_SECONDS = 60 * 60;
const GL_PROFILE_PICTURE_ALLOWED_MIME_TYPES = ['image/jpeg', 'image/png', 'image/webp'];
const GL_PROFILE_PICTURE_MAX_BYTES = 5 * 1024 * 1024;

const logGebietsleiterDbError = (context: string, error: any) => {
  console.error(`${context}: ${error?.code || 'database_error'}`);
};

const extractStoragePathFromValue = (value: string | null | undefined, bucket: string): string | null => {
  if (!value) return null;
  const trimmed = String(value).trim();
  if (!trimmed) return null;

  const objectSegments = [
    `/storage/v1/object/public/${bucket}/`,
    `/storage/v1/object/sign/${bucket}/`
  ];

  for (const segment of objectSegments) {
    const index = trimmed.indexOf(segment);
    if (index >= 0) {
      const pathWithQuery = trimmed.slice(index + segment.length);
      const [pathOnly] = pathWithQuery.split('?');
      return decodeURIComponent(pathOnly);
    }
  }

  if (!trimmed.startsWith('http://') && !trimmed.startsWith('https://') && !trimmed.startsWith('blob:')) {
    return trimmed;
  }

  return null;
};

const signProfilePictureUrl = async (
  client: ReturnType<typeof createFreshClient>,
  value: string | null | undefined
): Promise<string | null> => {
  if (!value) return null;

  const privatePath = extractStoragePathFromValue(value, GL_PROFILE_PICTURES_BUCKET);
  if (privatePath) {
    const { data, error } = await client.storage
      .from(GL_PROFILE_PICTURES_BUCKET)
      .createSignedUrl(privatePath, GL_PROFILE_PICTURE_SIGNED_URL_SECONDS);

    if (!error && data?.signedUrl) {
      return data.signedUrl;
    }
  }

  const legacyPath = extractStoragePathFromValue(value, LEGACY_PROFILE_PICTURES_BUCKET);
  if (legacyPath?.startsWith('profile-pictures/')) {
    const { data, error } = await client.storage
      .from(LEGACY_PROFILE_PICTURES_BUCKET)
      .createSignedUrl(legacyPath, GL_PROFILE_PICTURE_SIGNED_URL_SECONDS);

    if (!error && data?.signedUrl) {
      return data.signedUrl;
    }
  }

  return value;
};

const signProfilePictureRows = async <T extends { profile_picture_url?: string | null }>(
  client: ReturnType<typeof createFreshClient>,
  rows: T[]
): Promise<T[]> => Promise.all(rows.map(async (row) => ({
  ...row,
  profile_picture_url: await signProfilePictureUrl(client, row.profile_picture_url)
})));

const removeProfilePictureObject = async (
  client: ReturnType<typeof createFreshClient>,
  value: string | null | undefined
): Promise<void> => {
  const privatePath = extractStoragePathFromValue(value, GL_PROFILE_PICTURES_BUCKET);
  if (privatePath) {
    const { error } = await client.storage.from(GL_PROFILE_PICTURES_BUCKET).remove([privatePath]);
    if (error) console.error('Error removing private profile picture object');
    return;
  }

  const legacyPath = extractStoragePathFromValue(value, LEGACY_PROFILE_PICTURES_BUCKET);
  if (legacyPath?.startsWith('profile-pictures/')) {
    const { error } = await client.storage.from(LEGACY_PROFILE_PICTURES_BUCKET).remove([legacyPath]);
    if (error) console.error('Error removing legacy profile picture object');
  }
};

const isMissingBucketError = (error: any): boolean => {
  const message = storageErrorMessage(error);
  return message.includes('bucket not found') || message.includes('bucket_not_found') || message.includes('404');
};

const storageErrorMessage = (error: unknown): string => {
  const value = error as { message?: string; statusCode?: string | number; status?: string | number };
  return `${value?.message || ''} ${value?.statusCode || ''} ${value?.status || ''}`.toLowerCase();
};

const ensureProfilePicturesBucket = async (client: ReturnType<typeof createFreshClient>): Promise<void> => {
  const { error } = await client.storage.createBucket(GL_PROFILE_PICTURES_BUCKET, {
    public: false,
    allowedMimeTypes: GL_PROFILE_PICTURE_ALLOWED_MIME_TYPES,
    fileSizeLimit: GL_PROFILE_PICTURE_MAX_BYTES
  });

  if (error) {
    const message = storageErrorMessage(error);
    if (!message.includes('already exists') && !message.includes('already_exist') && !message.includes('409')) {
      throw error;
    }

    await client.storage.updateBucket(GL_PROFILE_PICTURES_BUCKET, {
      public: false,
      allowedMimeTypes: GL_PROFILE_PICTURE_ALLOWED_MIME_TYPES,
      fileSizeLimit: GL_PROFILE_PICTURE_MAX_BYTES
    });
  }
};

const DASHBOARD_SUBMISSIONS_PAGE_SIZE = 1000;
const DASHBOARD_ID_CHUNK_SIZE = 50;

type DashboardSubmissionRow = {
  id: string;
  welle_id: string;
  gebietsleiter_id: string;
  item_type: string;
  item_id: string;
  quantity: number | null;
  value_per_unit: number | null;
};

type AggregatedDashboardSubmission = ReturnType<typeof aggregateSubmissions>[number];

const toNumber = (value: any): number => {
  const parsed = typeof value === 'number' ? value : parseFloat(value);
  return Number.isFinite(parsed) ? parsed : 0;
};

const chunkArray = <T,>(items: T[], chunkSize: number): T[][] => {
  if (chunkSize <= 0) return [items];
  const chunks: T[][] = [];
  for (let i = 0; i < items.length; i += chunkSize) {
    chunks.push(items.slice(i, i + chunkSize));
  }
  return chunks;
};

async function fetchDashboardSubmissions(
  client: ReturnType<typeof createFreshClient>,
  valueWelleIds: string[],
  yearStart: string,
  gebietsleiterId?: string
): Promise<DashboardSubmissionRow[]> {
  const rows: DashboardSubmissionRow[] = [];
  const valueWelleIdSet = new Set(valueWelleIds.filter(Boolean));
  if (valueWelleIdSet.size === 0) return rows;

  for (let from = 0; ; from += DASHBOARD_SUBMISSIONS_PAGE_SIZE) {
    let query = client
      .from('wellen_submissions')
      .select('id, welle_id, gebietsleiter_id, item_type, item_id, quantity, value_per_unit')
      .gte('created_at', yearStart)
      .order('id', { ascending: true })
      .range(from, from + DASHBOARD_SUBMISSIONS_PAGE_SIZE - 1);

    if (gebietsleiterId) {
      query = query.eq('gebietsleiter_id', gebietsleiterId);
    }

    const { data, error } = await query;
    if (error) throw error;

    const page = (data || []) as DashboardSubmissionRow[];
    rows.push(...page.filter(row => valueWelleIdSet.has(row.welle_id)));
    if (page.length < DASHBOARD_SUBMISSIONS_PAGE_SIZE) break;
  }

  return rows;
}

async function fetchValueWelleIds(client: ReturnType<typeof createFreshClient>): Promise<string[]> {
  const ids: string[] = [];

  for (let from = 0; ; from += DASHBOARD_SUBMISSIONS_PAGE_SIZE) {
    const { data, error } = await client
      .from('wellen')
      .select('id')
      .eq('goal_type', 'value')
      .order('id', { ascending: true })
      .range(from, from + DASHBOARD_SUBMISSIONS_PAGE_SIZE - 1);

    if (error) throw error;

    const page = (data || []) as Array<{ id: string }>;
    ids.push(...page.map(w => w.id).filter(Boolean));
    if (page.length < DASHBOARD_SUBMISSIONS_PAGE_SIZE) break;
  }

  return ids;
}

async function fetchValueMap(
  client: ReturnType<typeof createFreshClient>,
  table: string,
  ids: string[],
  valueColumn: string
): Promise<Map<string, number>> {
  const valueMap = new Map<string, number>();
  const uniqueIds = [...new Set(ids.filter(Boolean))];
  if (uniqueIds.length === 0) return valueMap;

  for (const idChunk of chunkArray(uniqueIds, DASHBOARD_ID_CHUNK_SIZE)) {
    const { data, error } = await client
      .from(table)
      .select(`id, ${valueColumn}`)
      .in('id', idChunk);

    if (error) throw error;

    for (const row of ((data || []) as Array<Record<string, any>>)) {
      valueMap.set(row.id, toNumber(row[valueColumn]));
    }
  }

  return valueMap;
}

async function calculateDashboardRevenue(
  client: ReturnType<typeof createFreshClient>,
  progress: AggregatedDashboardSubmission[]
): Promise<number> {
  if (progress.length === 0) return 0;

  const idsByType = new Map<string, string[]>();
  for (const item of progress) {
    if (!item.item_id) continue;
    const ids = idsByType.get(item.item_type) || [];
    ids.push(item.item_id);
    idsByType.set(item.item_type, ids);
  }

  const [
    displayValues,
    kartonwareValues,
    waveEinzelproduktValues,
    masterProductValues,
    paletteProductValues,
    schuetteProductValues
  ] = await Promise.all([
    fetchValueMap(client, 'wellen_displays', idsByType.get('display') || [], 'item_value'),
    fetchValueMap(client, 'wellen_kartonware', idsByType.get('kartonware') || [], 'item_value'),
    fetchValueMap(client, 'wellen_einzelprodukte', idsByType.get('einzelprodukt') || [], 'item_value'),
    fetchValueMap(client, 'products', idsByType.get('einzelprodukt') || [], 'price'),
    fetchValueMap(client, 'wellen_paletten_products', idsByType.get('palette') || [], 'value_per_ve'),
    fetchValueMap(client, 'wellen_schuetten_products', idsByType.get('schuette') || [], 'value_per_ve')
  ]);

  const valueForSubmission = (item: AggregatedDashboardSubmission): number => {
    if (item.item_type === 'display') {
      return displayValues.get(item.item_id) ?? toNumber(item.value_per_unit);
    }
    if (item.item_type === 'kartonware') {
      return kartonwareValues.get(item.item_id) ?? toNumber(item.value_per_unit);
    }
    if (item.item_type === 'einzelprodukt') {
      return waveEinzelproduktValues.get(item.item_id)
        ?? masterProductValues.get(item.item_id)
        ?? toNumber(item.value_per_unit);
    }
    if (item.item_type === 'palette') {
      return toNumber(item.value_per_unit) || paletteProductValues.get(item.item_id) || 0;
    }
    if (item.item_type === 'schuette') {
      return toNumber(item.value_per_unit) || schuetteProductValues.get(item.item_id) || 0;
    }

    // Future value-bearing submission types should still count when they store a unit value.
    return toNumber(item.value_per_unit);
  };

  return progress.reduce((sum, item) => {
    return sum + valueForSubmission(item) * (item.current_number || 0);
  }, 0);
}

/**
 * GET /api/gebietsleiter
 * Get all active gebietsleiter
 */
router.get('/', requireAdmin, async (req: Request, res: Response) => {
  try {
    console.log('Fetching all gebietsleiter...');
    
    const freshClient = createFreshClient();
    
    const { data, error } = await freshClient
      .from('gebietsleiter')
      .select('id, name, address, postal_code, city, phone, email, profile_picture_url, is_active, is_test, created_at, updated_at')
      .neq('is_active', false) // Filter out inactive/deleted GLs
      .order('created_at', { ascending: false });

    if (error) {
      logGebietsleiterDbError('Error fetching gebietsleiter rows', error);
      throw error;
    }

    console.log(`✅ Fetched ${data?.length || 0} gebietsleiter`);
    const signedRows = await signProfilePictureRows(freshClient, data || []);
    res.json(signedRows);
  } catch (error: any) {
    console.error('Error fetching gebietsleiter');
    sendInternalError(res);
  }
});

/**
 * GET /api/gebietsleiter/:id
 * Get a single gebietsleiter by ID
 */
router.get('/:id', requireSelfOrAdmin(req => req.params.id), async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    console.log('Fetching gebietsleiter profile');
    
    const freshClient = createFreshClient();

    const { data, error } = await freshClient
      .from('gebietsleiter')
      .select('id, name, address, postal_code, city, phone, email, profile_picture_url, is_test, created_at, updated_at')
      .eq('id', id)
      .single();

    if (error) {
      logGebietsleiterDbError('Error fetching gebietsleiter row', error);
      throw error;
    }

    if (!data) {
      return res.status(404).json({ error: 'Gebietsleiter not found' });
    }

    console.log('Fetched gebietsleiter profile');
    const [signedRow] = await signProfilePictureRows(freshClient, [data]);
    res.json(signedRow);
  } catch (error: any) {
    console.error('Error fetching gebietsleiter');
    sendInternalError(res);
  }
});

/**
 * POST /api/gebietsleiter/upload-profile-picture
 * Upload a profile picture to Supabase Storage
 */
router.post('/upload-profile-picture', requireAdmin, async (req: Request, res: Response) => {
  try {
    const { base64, contentType, fileName } = req.body;
    if (!base64) return res.status(400).json({ error: 'Missing base64 image data' });

    const buffer = Buffer.from(base64, 'base64');
    const normalizedContentType = contentType || 'image/jpeg';
    if (!GL_PROFILE_PICTURE_ALLOWED_MIME_TYPES.includes(normalizedContentType)) {
      return res.status(400).json({ error: 'Unsupported profile picture type' });
    }

    if (buffer.length > GL_PROFILE_PICTURE_MAX_BYTES) {
      return res.status(400).json({ error: 'Profile picture is too large' });
    }

    const ext = (fileName?.split('.').pop() || normalizedContentType.split('/')[1] || 'jpg').toLowerCase();
    const storagePath = `profile-pictures/${Date.now()}-${Math.random().toString(36).slice(2, 8)}.${ext}`;

    const freshClient = createFreshClient();
    let { error: uploadError } = await freshClient.storage
      .from(GL_PROFILE_PICTURES_BUCKET)
      .upload(storagePath, buffer, { contentType: normalizedContentType, upsert: true });

    if (uploadError && isMissingBucketError(uploadError)) {
      await ensureProfilePicturesBucket(freshClient);
      ({ error: uploadError } = await freshClient.storage
        .from(GL_PROFILE_PICTURES_BUCKET)
        .upload(storagePath, buffer, { contentType: normalizedContentType, upsert: true }));
    }

    if (uploadError) {
      console.error('Upload error');
      return res.status(500).json({ error: 'Failed to upload image' });
    }

    const signedUrl = await signProfilePictureUrl(freshClient, storagePath);
    res.json({ path: storagePath, url: signedUrl });
  } catch (error: any) {
    console.error('Error uploading profile picture');
    sendInternalError(res);
  }
});

/**
 * POST /api/gebietsleiter
 * Create a new gebietsleiter with Supabase Auth account
 */
router.post('/', requireAdmin, async (req: Request, res: Response) => {
  try {
    console.log('Creating new gebietsleiter with Supabase Auth...');
    
    const { name, address, postalCode, city, phone, email, password, profilePictureUrl, isTest } = req.body;

    // Validate required fields
    if (!name || !address || !postalCode || !city || !phone || !email || !password) {
      return res.status(400).json({ error: 'Missing required fields' });
    }

    // Split name into first and last name
    const nameParts = name.trim().split(' ');
    const firstName = nameParts[0] || name;
    const lastName = nameParts.slice(1).join(' ') || '';

    // Step 1: Create user in Supabase Auth
    console.log('Creating Supabase Auth user...');
    const { data: authData, error: authError } = await supabase.auth.admin.createUser({
      email,
      password,
      email_confirm: true, // Auto-confirm email
    });

    if (authError || !authData.user) {
      console.error('Gebietsleiter auth user creation failed');
      
      // Check for duplicate email in Auth
      if (authError?.message?.includes('already')) {
        return res.status(409).json({ error: 'Email already exists in authentication system' });
      }
      
      return res.status(400).json({ error: 'Failed to create auth user' });
    }

    const authUserId = authData.user.id;
    console.log('Created Supabase Auth user');
    
    const freshClient = createFreshClient();

    // Step 2: Create entry in users table with role 'gl'
    console.log('Creating users table entry...');
    const { error: userError } = await freshClient
      .from('users')
      .insert({
        id: authUserId,
        role: 'gl',
        first_name: firstName,
        last_name: lastName,
        gebietsleiter_id: authUserId, // Use auth ID as gebietsleiter_id
      });

    if (userError) {
      console.error('Users table error');
      // Try to clean up the auth user if users table insert fails
      await supabase.auth.admin.deleteUser(authUserId);
      throw userError;
    }

    console.log('✅ Created users table entry');

    // Step 3: Insert into gebietsleiter table
    console.log('Creating gebietsleiter table entry...');
    const { data, error } = await freshClient
      .from('gebietsleiter')
      .insert({
        id: authUserId, // Use same ID as auth user for consistency
        name,
        address,
        postal_code: postalCode,
        city,
        phone,
        email,
        password_hash: 'SUPABASE_AUTH', // Marker that password is in Supabase Auth
        profile_picture_url: profilePictureUrl || null,
        is_test: isTest === true
      })
      .select('id, name, address, postal_code, city, phone, email, profile_picture_url, is_test, created_at, updated_at')
      .single();

    if (error) {
      console.error('Gebietsleiter table error');
      
      // Clean up on failure
      await freshClient.from('users').delete().eq('id', authUserId);
      await supabase.auth.admin.deleteUser(authUserId);
      
      // Check for unique constraint violation (duplicate email)
      if (error.code === '23505') {
        return res.status(409).json({ error: 'Email already exists' });
      }
      
      throw error;
    }

    console.log('Created gebietsleiter with full Supabase Auth integration');
    const [signedRow] = await signProfilePictureRows(freshClient, [data]);
    res.status(201).json(signedRow);
  } catch (error: any) {
    console.error('Error creating gebietsleiter');
    sendInternalError(res);
  }
});

/**
 * PUT /api/gebietsleiter/:id
 * Update a gebietsleiter
 */
router.put('/:id', requireAdmin, async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    console.log('Updating gebietsleiter profile');
    
    const freshClient = createFreshClient();

    const { name, address, postalCode, city, phone, email, password, profilePictureUrl, isTest } = req.body;

    const updateData: any = {};
    
    if (name) updateData.name = name;
    if (address) updateData.address = address;
    if (postalCode) updateData.postal_code = postalCode;
    if (city) updateData.city = city;
    if (phone) updateData.phone = phone;
    if (email) updateData.email = email;
    if (profilePictureUrl !== undefined) updateData.profile_picture_url = profilePictureUrl;
    if (isTest !== undefined) updateData.is_test = isTest === true;
    
    if (password) {
      const { error: passwordError } = await supabase.auth.admin.updateUserById(id, {
        password,
      });

      if (passwordError) {
        console.error('Error updating gebietsleiter auth credential');
        return res.status(500).json({ error: 'Failed to update password' });
      }
    }

    const { data, error } = await freshClient
      .from('gebietsleiter')
      .update(updateData)
      .eq('id', id)
      .select('id, name, address, postal_code, city, phone, email, profile_picture_url, is_test, created_at, updated_at')
      .single();

    if (error) {
      logGebietsleiterDbError('Error updating gebietsleiter row', error);
      
      // Check for unique constraint violation (duplicate email)
      if (error.code === '23505') {
        return res.status(409).json({ error: 'Email already exists' });
      }
      
      throw error;
    }

    console.log('Updated gebietsleiter profile');
    const [signedRow] = await signProfilePictureRows(freshClient, [data]);
    res.json(signedRow);
  } catch (error: any) {
    console.error('Error updating gebietsleiter');
    sendInternalError(res);
  }
});

/**
 * DELETE /api/gebietsleiter/:id
 * Deactivate a gebietsleiter - deletes auth user but keeps data for progress tracking
 */
router.delete('/:id', requireAdmin, async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    console.log('Deactivating gebietsleiter profile');
    
    const freshClient = createFreshClient();

    // 1. Delete the Supabase Auth user so they can't login anymore
    // The GL id is the same as the auth user id
    const { error: authError } = await supabase.auth.admin.deleteUser(id);
    
    if (authError) {
      console.error('Error deleting auth user');
      // Continue even if auth deletion fails - the user might already be deleted
      // or might not have an auth account
    } else {
      console.log('Deleted auth user for gebietsleiter profile');
    }

    const { data: existingGl, error: existingGlError } = await freshClient
      .from('gebietsleiter')
      .select('profile_picture_url')
      .eq('id', id)
      .maybeSingle();

    if (existingGlError) {
      logGebietsleiterDbError('Error loading gebietsleiter before deactivation', existingGlError);
      throw existingGlError;
    }

    await removeProfilePictureObject(freshClient, existingGl?.profile_picture_url);

    // 2. Mark the GL as inactive and pseudonymize personal profile fields.
    // This preserves historical foreign keys/progress tracking without retaining contact data.
    const { error: updateError } = await freshClient
      .from('gebietsleiter')
      .update({ 
        is_active: false,
        name: `Deleted GL ${id.substring(0, 8)}`,
        address: '',
        postal_code: '',
        city: '',
        phone: '',
        email: `deleted_${Date.now()}_${id.substring(0, 8)}@deleted.local`,
        profile_picture_url: null,
        password_hash: 'DEACTIVATED'
      })
      .eq('id', id);

    if (updateError) {
      logGebietsleiterDbError('Error marking GL as inactive', updateError);
      throw updateError;
    }

    console.log('Deactivated gebietsleiter profile - data preserved for progress tracking');
    res.status(204).send();
  } catch (error: any) {
    console.error('Error deactivating gebietsleiter');
    sendInternalError(res);
  }
});

/**
 * POST /api/gebietsleiter/:id/change-password
 * Change password for a gebietsleiter using Supabase Auth
 */
router.post('/:id/change-password', requireSelfOrAdmin(req => req.params.id), async (req: AuthRequest, res: Response) => {
  try {
    const { id } = req.params;
    const { currentPassword, newPassword } = req.body;
    const targetUserId = req.user?.role === 'admin' ? id : req.user?.id;
    
    console.log('Credential change request for gebietsleiter');
    
    if (!currentPassword || !newPassword) {
      return res.status(400).json({ error: 'Current password and new password are required' });
    }

    if (newPassword.length < 8) {
      return res.status(400).json({ error: 'New password must be at least 8 characters long' });
    }

    if (!targetUserId) {
      return res.status(400).json({ error: 'Gebietsleiter not found' });
    }

    const { data: authUser, error: authError } = await supabase.auth.admin.getUserById(targetUserId);
    if (authError || !authUser?.user?.email) {
      console.error('Auth user lookup error');
      return res.status(404).json({ error: 'Gebietsleiter not found' });
    }

    // Verify current password by attempting to sign in
    const { error: signInError } = await supabase.auth.signInWithPassword({
      email: authUser.user.email,
      password: currentPassword
    });

    if (signInError) {
      console.error('Current credential verification failed');
      return res.status(401).json({ error: 'Aktuelles Passwort ist falsch' });
    }

    // Update the password using admin API
    const { error: updateError } = await supabase.auth.admin.updateUserById(targetUserId, {
      password: newPassword
    });

    if (updateError) {
      console.error('Credential update error');
      return res.status(500).json({ error: 'Failed to update password' });
    }

    console.log('Credential changed successfully for gebietsleiter account');
    res.json({ success: true, message: 'Password changed successfully' });
  } catch (error: any) {
    console.error('Error changing credential');
    sendInternalError(res);
  }
});

/**
 * GET /api/gebietsleiter/:id/dashboard-stats
 * Get dashboard statistics for a specific GL
 */
router.get('/:id/dashboard-stats', requireSelfOrAdmin(req => req.params.id), async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    console.log('Fetching dashboard stats for GL profile');
    
    const freshClient = createFreshClient();

    const currentYear = new Date().getFullYear();
    const yearStart = new Date(currentYear, 0, 1).toISOString();

    // 1. Get value-based wellen IDs only
    const valueWelleIds = await fetchValueWelleIds(freshClient);

    // 2. Get GL's total vorbesteller value YTD (only from value-based waves)
    const rawGlSubs = await fetchDashboardSubmissions(freshClient, valueWelleIds, yearStart, id);
    const glProgress = aggregateSubmissions(rawGlSubs);
    const glYearTotal = await calculateDashboardRevenue(freshClient, glProgress);

    // 3. Get agency average (all real GLs' total from value-based waves only - exclude test GLs)
    const { data: allGLs } = await freshClient.from('gebietsleiter').select('id').neq('is_active', false).neq('is_test', true);
    const glCount = allGLs?.length || 1;
    const realGLIds = new Set((allGLs || []).map((g: any) => g.id));

    const rawAllSubs = await fetchDashboardSubmissions(freshClient, valueWelleIds, yearStart);
    const allProgress = aggregateSubmissions(rawAllSubs);
    const realProgress = allProgress.filter(p => realGLIds.has(p.gebietsleiter_id));
    const agencyTotal = await calculateDashboardRevenue(freshClient, realProgress);

    const agencyAverage = glCount > 0 ? agencyTotal / glCount : 0;
    const percentageChange = agencyAverage > 0 ? ((glYearTotal - agencyAverage) / agencyAverage) * 100 : 0;

    // 3. Get Vorverkauf count (from vorverkauf_submissions table)
    const { count: vorverkaufCount } = await freshClient
      .from('vorverkauf_submissions')
      .select('id', { count: 'exact', head: true })
      .eq('gebietsleiter_id', id);

    // 4. Get Vorbestellung count (unique submissions based on distinct timestamps)
    // Each unique timestamp represents one vorbestellung action (multiple items submitted together have same timestamp)
    const { data: vorbestellerSubmissions } = await freshClient
      .from('wellen_submissions')
      .select('created_at')
      .eq('gebietsleiter_id', id);

    // Count unique timestamps - each unique timestamp = 1 vorbestellung
    const uniqueVorbestellungen = new Set<string>();
    vorbestellerSubmissions?.forEach(s => {
      // Use full timestamp (rounded to minute) to group items submitted together
      const timestamp = new Date(s.created_at).toISOString().slice(0, 16);
      uniqueVorbestellungen.add(timestamp);
    });
    const vorbestellungCount = uniqueVorbestellungen.size;

    // 5. Get total markets assigned to this GL (via gebietsleiter_id field in markets table)
    const { count: totalAssignedMarkets } = await freshClient
      .from('markets')
      .select('id', { count: 'exact', head: true })
      .eq('gebietsleiter_id', id)
      .eq('is_active', true);

    // 6. Get count of markets with current_visits > 0 (markets that have been visited at least once)
    const { count: marketsVisited } = await freshClient
      .from('markets')
      .select('id', { count: 'exact', head: true })
      .eq('gebietsleiter_id', id)
      .eq('is_active', true)
      .gt('current_visits', 0);

    console.log(`Dashboard stats calculated: vorverkauf=${vorverkaufCount}, vorbestellung=${vorbestellungCount}, markets=${marketsVisited}/${totalAssignedMarkets}`);

    res.json({
      yearTotal: Math.round(glYearTotal),
      percentageChange: Math.round(percentageChange * 10) / 10,
      vorverkaufCount: vorverkaufCount || 0,
      vorbestellungCount: vorbestellungCount,
      marketsVisited,
      totalMarkets: totalAssignedMarkets || 0
    });
  } catch (error: any) {
    console.error('Error fetching dashboard stats');
    sendInternalError(res);
  }
});

// Helper functions for KW+Day matching (same logic as PreorderNotification)
const getCurrentKWNumber = (): number => {
  const now = new Date();
  const startOfYear = new Date(now.getFullYear(), 0, 1);
  const days = Math.floor((now.getTime() - startOfYear.getTime()) / (24 * 60 * 60 * 1000));
  return Math.ceil((days + startOfYear.getDay() + 1) / 7);
};

const getCurrentDayAbbr = (): string => {
  const days = ['SO', 'MO', 'DI', 'MI', 'DO', 'FR', 'SA'];
  return days[new Date().getDay()];
};

const extractKWNumber = (kwString: string): number => {
  const match = kwString.match(/(\d+)/);
  return match ? parseInt(match[1], 10) : -1;
};

/**
 * GET /api/gebietsleiter/:id/suggested-markets
 * Smart market suggestions based on priority scoring:
 * - Vorbesteller (KW+Day match): +200 pts
 * - Frequency overdue: +100 pts
 * - Vorverkauf last 3 days: +80 pts
 * - Frequency soon: +50 pts
 * - Vorverkauf last week: +40 pts
 * - Vorverkauf active: +20 pts
 */
router.get('/:id/suggested-markets', requireSelfOrAdmin(req => req.params.id), async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    console.log('Fetching smart suggested markets for GL profile');
    
    const freshClient = createFreshClient();

    const now = new Date();
    const currentKW = getCurrentKWNumber();
    const currentDay = getCurrentDayAbbr();

    // 1. Get ALL markets assigned to this GL (via gebietsleiter_id field)
    const { data: glMarkets } = await freshClient
      .from('markets')
      .select('id, name, address, city, postal_code, chain, frequency, last_visit_date, current_visits')
      .eq('gebietsleiter_id', id)
      .eq('is_active', true);

    if (!glMarkets || glMarkets.length === 0) {
      console.log('No markets assigned to GL profile');
      return res.json([]);
    }

    const marketIds = glMarkets.map(m => m.id);

    // 2. Get active Vorbesteller waves with KW days
    const { data: vorbestellerWaves } = await freshClient
      .from('wellen')
      .select('id, name')
      .eq('status', 'active');

    // Get KW days for active waves
    let vorbestellerKwDays: { welle_id: string; kw: string; days: string[] }[] = [];
    if (vorbestellerWaves && vorbestellerWaves.length > 0) {
      const waveIds = vorbestellerWaves.map(w => w.id);
      const { data: kwDaysData } = await freshClient
        .from('wellen_kw_days')
        .select('welle_id, kw, days')
        .in('welle_id', waveIds);
      if (kwDaysData) {
        vorbestellerKwDays = kwDaysData;
      }
    }

    // Get markets linked to active Vorbesteller waves
    let vorbestellerMarketIds: Set<string> = new Set();
    if (vorbestellerWaves && vorbestellerWaves.length > 0) {
      const waveIds = vorbestellerWaves.map(w => w.id);
      const { data: wellenMarkets } = await freshClient
        .from('wellen_markets')
        .select('market_id, welle_id')
        .in('welle_id', waveIds)
        .in('market_id', marketIds);
      
      if (wellenMarkets) {
        wellenMarkets.forEach(wm => vorbestellerMarketIds.add(wm.market_id));
      }
    }

    // Check if today matches any Vorbesteller KW+Day
    const isVorbestellerToday = vorbestellerKwDays.some(kwDay => {
      const kwNum = extractKWNumber(kwDay.kw);
      const matchesKW = kwNum === currentKW;
      const matchesDay = kwDay.days.some(day => day.toUpperCase() === currentDay);
      return matchesKW && matchesDay;
    });

    // 3. Get active Vorverkauf waves
    const { data: vorverkaufWaves } = await freshClient
      .from('vorverkauf_wellen')
      .select('id, name, start_date, end_date')
      .eq('status', 'active');

    // Get markets linked to active Vorverkauf waves
    let vorverkaufMarketData: Map<string, { endDate: Date; daysUntilEnd: number }> = new Map();
    if (vorverkaufWaves && vorverkaufWaves.length > 0) {
      const waveIds = vorverkaufWaves.map(w => w.id);
      const { data: vvMarkets } = await freshClient
        .from('vorverkauf_wellen_markets')
        .select('market_id, welle_id')
        .in('welle_id', waveIds)
        .in('market_id', marketIds);
      
      if (vvMarkets) {
        vvMarkets.forEach(vvm => {
          const wave = vorverkaufWaves.find(w => w.id === vvm.welle_id);
          if (wave) {
            const endDate = new Date(wave.end_date);
            const daysUntilEnd = Math.floor((endDate.getTime() - now.getTime()) / (1000 * 60 * 60 * 24));
            // Keep the one with the soonest end date
            const existing = vorverkaufMarketData.get(vvm.market_id);
            if (!existing || daysUntilEnd < existing.daysUntilEnd) {
              vorverkaufMarketData.set(vvm.market_id, { endDate, daysUntilEnd });
            }
          }
        });
      }
    }

    // 4. Calculate priority score and reason for each market
    const suggestions = glMarkets.map(market => {
      let priorityScore = 0;
      const reasons: string[] = [];

      const frequency = market.frequency || 12;
      const expectedIntervalDays = 365 / frequency;
      
      // Calculate days since last visit
      let daysSinceVisit = 999; // Never visited = very overdue
      if (market.last_visit_date) {
        const lastVisit = new Date(market.last_visit_date);
        daysSinceVisit = Math.floor((now.getTime() - lastVisit.getTime()) / (1000 * 60 * 60 * 24));
      }

      const weeksAgo = Math.floor(daysSinceVisit / 7);

      // --- VORBESTELLER SCORE (+200) ---
      if (vorbestellerMarketIds.has(market.id) && isVorbestellerToday) {
        priorityScore += 200;
        reasons.push('HEUTE: Vorbesteller');
      }

      // --- FREQUENCY OVERDUE SCORE (+100) ---
      const isOverdue = daysSinceVisit > expectedIntervalDays;
      if (isOverdue) {
        priorityScore += 100;
        reasons.push('Frequenz überfällig');
      }

      // --- VORVERKAUF CLOSING SCORE (+80/+40/+20) ---
      const vvData = vorverkaufMarketData.get(market.id);
      if (vvData) {
        if (vvData.daysUntilEnd <= 3 && vvData.daysUntilEnd >= 0) {
          priorityScore += 80;
          reasons.push('Vorverkauf: letzte 3 Tage');
        } else if (vvData.daysUntilEnd <= 7) {
          priorityScore += 40;
          reasons.push('Vorverkauf: letzte Woche');
        } else {
          priorityScore += 20;
          reasons.push('Vorverkauf aktiv');
        }
      }

      // --- FREQUENCY SOON SCORE (+50) ---
      const isSoon = !isOverdue && daysSinceVisit > (expectedIntervalDays - 7);
      if (isSoon) {
        priorityScore += 50;
        reasons.push('Bald Frequenz fällig');
      }

      // Determine primary reason (highest priority one)
      let priorityReason = reasons.length > 0 ? reasons[0] : 'Regelmäßiger Besuch';
      
      // Combine reasons if multiple high-priority ones
      if (reasons.includes('HEUTE: Vorbesteller') && reasons.includes('Frequenz überfällig')) {
        priorityReason = 'HEUTE: Vorbesteller + Frequenz';
      } else if (reasons.includes('HEUTE: Vorbesteller') && reasons.some(r => r.includes('Vorverkauf'))) {
        priorityReason = 'HEUTE: Vorbesteller + Vorverkauf';
      }

      const currentVisits = market.current_visits || 0;
      
      return {
        marketId: market.id,
        name: `${market.chain} ${market.name}`.trim(),
        address: `${market.address}, ${market.postal_code} ${market.city}`,
        lastVisitWeeks: weeksAgo,
        visits: { current: Math.min(currentVisits, frequency), required: frequency },
        status: (currentVisits >= frequency * 0.5 ? 'on-track' : 'at-risk') as 'on-track' | 'at-risk',
        priorityScore,
        priorityReason
      };
    });

    // Sort by priority score (highest first), then by weeks since last visit
    suggestions.sort((a, b) => {
      if (b.priorityScore !== a.priorityScore) {
        return b.priorityScore - a.priorityScore;
      }
      return b.lastVisitWeeks - a.lastVisitWeeks;
    });

    console.log(`Found ${suggestions.length} suggested markets, returning top 15`);
    res.json(suggestions.slice(0, 15));
  } catch (error: any) {
    console.error('Error fetching suggested markets');
    sendInternalError(res);
  }
});

/**
 * GET /api/gebietsleiter/:id/profile-stats
 * Get profile statistics for a GL
 */
router.get('/:id/profile-stats', requireSelfOrAdmin(req => req.params.id), async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    console.log('Fetching profile stats for GL profile');
    
    const freshClient = createFreshClient();

    const now = new Date();
    const currentMonth = now.getMonth();
    const currentYear = now.getFullYear();
    
    // Get current month start/end
    const currentMonthStart = new Date(currentYear, currentMonth, 1).toISOString();
    const currentMonthEnd = new Date(currentYear, currentMonth + 1, 0, 23, 59, 59).toISOString();
    
    // Get previous month start/end
    const prevMonthStart = new Date(currentYear, currentMonth - 1, 1).toISOString();
    const prevMonthEnd = new Date(currentYear, currentMonth, 0, 23, 59, 59).toISOString();

    // 1. Get GL's total markets
    const { count: totalMarkets } = await freshClient
      .from('markets')
      .select('id', { count: 'exact', head: true })
      .eq('gebietsleiter_id', id)
      .eq('is_active', true);

    // 2. Get unique markets visited this month (primarily from fb_zeiterfassung_submissions, also wellen, vorverkauf)
    const [zeiterfassungSubs, wellenSubs, vorverkaufSubs, produkttauschEntries] = await Promise.all([
      freshClient.from('fb_zeiterfassung_submissions').select('market_id, created_at').eq('gebietsleiter_id', id).gte('created_at', currentMonthStart).lte('created_at', currentMonthEnd),
      freshClient.from('wellen_submissions').select('market_id, created_at').eq('gebietsleiter_id', id).gte('created_at', currentMonthStart).lte('created_at', currentMonthEnd),
      freshClient.from('vorverkauf_submissions').select('market_id, created_at').eq('gebietsleiter_id', id).gte('created_at', currentMonthStart).lte('created_at', currentMonthEnd),
      freshClient.from('vorverkauf_entries').select('market_id, created_at').eq('gebietsleiter_id', id).gte('created_at', currentMonthStart).lte('created_at', currentMonthEnd)
    ]);

    const currentMonthMarkets = new Set([
      ...(zeiterfassungSubs.data || []).map(s => s.market_id),
      ...(wellenSubs.data || []).map(s => s.market_id),
      ...(vorverkaufSubs.data || []).map(s => s.market_id),
      ...(produkttauschEntries.data || []).map(e => e.market_id)
    ]);
    const monthlyVisits = currentMonthMarkets.size;

    // 3. Get previous month visits for comparison
    const [zeiterfassungSubsPrev, wellenSubsPrev, vorverkaufSubsPrev, produkttauschEntriesPrev] = await Promise.all([
      freshClient.from('fb_zeiterfassung_submissions').select('market_id, created_at').eq('gebietsleiter_id', id).gte('created_at', prevMonthStart).lte('created_at', prevMonthEnd),
      freshClient.from('wellen_submissions').select('market_id, created_at').eq('gebietsleiter_id', id).gte('created_at', prevMonthStart).lte('created_at', prevMonthEnd),
      freshClient.from('vorverkauf_submissions').select('market_id, created_at').eq('gebietsleiter_id', id).gte('created_at', prevMonthStart).lte('created_at', prevMonthEnd),
      freshClient.from('vorverkauf_entries').select('market_id, created_at').eq('gebietsleiter_id', id).gte('created_at', prevMonthStart).lte('created_at', prevMonthEnd)
    ]);

    const prevMonthMarkets = new Set([
      ...(zeiterfassungSubsPrev.data || []).map(s => s.market_id),
      ...(wellenSubsPrev.data || []).map(s => s.market_id),
      ...(vorverkaufSubsPrev.data || []).map(s => s.market_id),
      ...(produkttauschEntriesPrev.data || []).map(e => e.market_id)
    ]);
    const prevMonthVisits = prevMonthMarkets.size;
    const monthChangePercent = prevMonthVisits > 0 ? Math.round(((monthlyVisits - prevMonthVisits) / prevMonthVisits) * 100) : 0;

    // 4. Get Vorbesteller success rate (markets with vorbesteller / total markets)
    const { data: vorbestellerSubs } = await freshClient
      .from('wellen_submissions')
      .select('welle_id, item_type, quantity')
      .eq('gebietsleiter_id', id)
      .gt('quantity', 0);

    const wellenWithProgress = new Set((vorbestellerSubs || []).map(p => p.welle_id));
    
    // Get total markets in wellen for this GL
    let totalWellenMarkets = 0;
    let marketsWithVorbesteller = 0;
    
    if (wellenWithProgress.size > 0) {
      const { data: wellenMarkets } = await freshClient
        .from('wellen_markets')
        .select('market_id, welle_id')
        .in('welle_id', Array.from(wellenWithProgress));
      
      // Filter to only markets that belong to this GL
      const { data: glMarkets } = await freshClient
        .from('markets')
        .select('id')
        .eq('gebietsleiter_id', id);
      
      const glMarketIds = new Set((glMarkets || []).map(m => m.id));
      const relevantWellenMarkets = (wellenMarkets || []).filter(wm => glMarketIds.has(wm.market_id));
      totalWellenMarkets = new Set(relevantWellenMarkets.map(wm => wm.market_id)).size;
      
      // Markets that have vorbesteller progress
      const { data: progressMarkets } = await freshClient
        .from('wellen_submissions')
        .select('market_id')
        .eq('gebietsleiter_id', id);
      
      marketsWithVorbesteller = new Set((progressMarkets || []).map(p => p.market_id)).size;
    }

    const sellInSuccessRate = totalWellenMarkets > 0 ? Math.round((marketsWithVorbesteller / totalWellenMarkets) * 100) : 0;

    // 5. Get previous month sell-in rate for comparison
    const { data: prevVorbestellerProgress } = await freshClient
      .from('wellen_submissions')
      .select('market_id')
      .eq('gebietsleiter_id', id)
      .gte('created_at', prevMonthStart)
      .lte('created_at', prevMonthEnd);

    const prevMonthVorbestellerMarkets = new Set((prevVorbestellerProgress || []).map(p => p.market_id)).size;
    const prevSellInRate = totalWellenMarkets > 0 ? Math.round((prevMonthVorbestellerMarkets / totalWellenMarkets) * 100) : 0;
    const sellInChangePercent = prevSellInRate > 0 ? Math.round(sellInSuccessRate - prevSellInRate) : 0;

    // 6. Get most visited market - use current_visits from markets table (same as admin side)
    const { data: glMarketsData } = await freshClient
      .from('markets')
      .select('id, name, chain, current_visits, last_visit_date')
      .eq('gebietsleiter_id', id)
      .eq('is_active', true)
      .order('current_visits', { ascending: false })
      .limit(1);
    
    let mostVisitedMarket = { name: 'Keine Daten', chain: '', visitCount: 0 };
    
    if (glMarketsData && glMarketsData.length > 0 && glMarketsData[0].current_visits > 0) {
      const topMarket = glMarketsData[0];
      mostVisitedMarket = {
        name: topMarket.name || 'Unbekannt',
        chain: topMarket.chain || '',
        visitCount: topMarket.current_visits || 0
      };
    }

    // 7. Get this month's Vorverkäufe, Vorbesteller, and Produkttausch counts
    const vorverkaufeCount = (vorverkaufSubs.data || []).length;
    const vorbestellerCount = (wellenSubs.data || []).length;
    const produkttauschCount = (produkttauschEntries.data || []).length;

    // 8. Get top 3 visited markets - use current_visits from markets table (same as admin side)
    const { data: top3MarketsData } = await freshClient
      .from('markets')
      .select('id, name, chain, address, city, postal_code, current_visits, last_visit_date')
      .eq('gebietsleiter_id', id)
      .eq('is_active', true)
      .order('current_visits', { ascending: false })
      .limit(3);
    
    const topMarkets = (top3MarketsData || []).map(market => ({
      id: market.id,
      name: market.name || 'Unbekannt',
      chain: market.chain || '',
      address: market.address || '',
      visitCount: market.current_visits || 0,
      lastVisit: market.last_visit_date || ''
    }));

    res.json({
      monthlyVisits,
      totalMarkets: totalMarkets || 0,
      monthChangePercent,
      sellInSuccessRate,
      sellInChangePercent,
      mostVisitedMarket,
      vorverkaufeCount,
      vorbestellerCount,
      produkttauschCount,
      topMarkets
    });
  } catch (error: any) {
    console.error('Error fetching profile stats');
    sendInternalError(res);
  }
});

// ============================================================================
// ONBOARDING ENDPOINTS
// ============================================================================

// Check if GL has read a specific onboarding feature
router.get('/:id/onboarding/:featureKey', requireSelfOrAdmin(req => req.params.id), async (req: Request, res: Response) => {
  try {
    const { id, featureKey } = req.params;
    
    console.log(`Checking GL onboarding status for feature: ${featureKey}`);
    
    const freshClient = createFreshClient();
    
    const { data, error } = await freshClient
      .from('gl_onboarding_reads')
      .select('id')
      .eq('gl_id', id)
      .eq('feature_key', featureKey)
      .maybeSingle();
    
    if (error) throw error;
    
    res.json({ hasRead: !!data });
  } catch (error: any) {
    console.error('Error checking onboarding status');
    sendInternalError(res);
  }
});

// Mark an onboarding feature as read
router.post('/:id/onboarding/:featureKey', requireSelfOrAdmin(req => req.params.id), async (req: Request, res: Response) => {
  try {
    const { id, featureKey } = req.params;
    
    console.log(`Marking GL onboarding as read for feature: ${featureKey}`);
    
    const freshClient = createFreshClient();
    
    const { error } = await freshClient
      .from('gl_onboarding_reads')
      .upsert({
        gl_id: id,
        feature_key: featureKey,
        read_at: new Date().toISOString()
      }, {
        onConflict: 'gl_id,feature_key'
      });
    
    if (error) throw error;
    
    res.json({ success: true });
  } catch (error: any) {
    console.error('Error marking onboarding as read');
    sendInternalError(res);
  }
});

export default router;


