-- Fix product-list activation so partial imports only replace their own segment.
-- Example: a Food Standard batch archives old Food Standard rows, but leaves
-- active Tiernahrung Standard rows untouched.

begin;

set local lock_timeout = '5s';
set local statement_timeout = '2min';

create or replace function public.activate_product_update_batch(p_batch_id uuid)
returns table(inserted_count integer, soft_deleted_count integer)
language plpgsql
as $$
declare
  batch_status text;
  new_ids text[];
begin
  select status
  into batch_status
  from public.product_update_batches
  where id = p_batch_id
  for update;

  if batch_status is null then
    raise exception 'Product update batch % not found', p_batch_id;
  end if;

  if batch_status = 'applied' then
    return query
      select
        coalesce(applied_inserted_count, 0),
        coalesce(applied_soft_deleted_count, 0)
      from public.product_update_batches
      where id = p_batch_id;
    return;
  end if;

  if batch_status not in ('draft', 'scheduled', 'processing') then
    raise exception 'Product update batch % cannot be activated from status %', p_batch_id, batch_status;
  end if;

  if not exists (select 1 from public.products_update where batch_id = p_batch_id) then
    raise exception 'Product update batch % has no staged products', p_batch_id;
  end if;

  update public.product_update_batches
  set status = 'processing',
      error_message = null
  where id = p_batch_id;

  with inserted as (
    insert into public.products (
      id,
      name,
      department,
      product_type,
      weight,
      content,
      pallet_size,
      price,
      sku,
      artikel_nr,
      palette_products,
      is_active,
      is_deleted
    )
    select
      gen_random_uuid()::text,
      name,
      department,
      product_type,
      weight,
      content,
      pallet_size,
      price,
      sku,
      artikel_nr,
      palette_products,
      is_active,
      false
    from public.products_update
    where batch_id = p_batch_id
    order by created_at, id
    returning id
  )
  select count(*)::integer, coalesce(array_agg(id), array[]::text[])
  into inserted_count, new_ids
  from inserted;

  update public.products active_product
  set is_deleted = true
  where coalesce(active_product.is_deleted, false) = false
    and not (active_product.id = any(new_ids))
    and exists (
      select 1
      from public.products_update staged
      where staged.batch_id = p_batch_id
        and staged.department = active_product.department
        and staged.product_type = active_product.product_type
    );

  get diagnostics soft_deleted_count = row_count;

  delete from public.products_update
  where batch_id = p_batch_id;

  update public.product_update_batches
  set status = 'applied',
      applied_at = now(),
      applied_inserted_count = inserted_count,
      applied_soft_deleted_count = soft_deleted_count
  where id = p_batch_id;

  return next;
end;
$$;

revoke all on function public.activate_product_update_batch(uuid) from public, anon, authenticated;
grant execute on function public.activate_product_update_batch(uuid) to service_role;

commit;

