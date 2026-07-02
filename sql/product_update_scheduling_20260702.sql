-- Scheduled product-list switch support.
-- Historical live products are preserved: activation inserts a fresh active list and
-- soft-deletes old active rows with is_deleted=true inside one database transaction.

begin;

set local lock_timeout = '5s';
set local statement_timeout = '2min';

create table if not exists public.product_update_batches (
  id uuid primary key default gen_random_uuid(),
  status text not null default 'draft'
    check (status in ('draft', 'scheduled', 'processing', 'applied', 'cancelled', 'failed')),
  scheduled_for timestamptz,
  created_by text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  applied_at timestamptz,
  applied_inserted_count integer,
  applied_soft_deleted_count integer,
  error_message text
);

alter table public.products_update
  add column if not exists batch_id uuid references public.product_update_batches(id) on delete cascade;

create index if not exists idx_product_update_batches_status_schedule
  on public.product_update_batches(status, scheduled_for);

create index if not exists idx_products_update_batch_id
  on public.products_update(batch_id);

do $$
declare
  legacy_batch_id uuid;
begin
  if exists (select 1 from public.products_update where batch_id is null) then
    insert into public.product_update_batches(status)
    values ('draft')
    returning id into legacy_batch_id;

    update public.products_update
    set batch_id = legacy_batch_id
    where batch_id is null;
  end if;
end $$;

create or replace function public.update_product_update_batches_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

drop trigger if exists product_update_batches_updated_at_trigger on public.product_update_batches;
create trigger product_update_batches_updated_at_trigger
before update on public.product_update_batches
for each row
execute function public.update_product_update_batches_updated_at();

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
  select count(*)::integer, array_agg(id)
  into inserted_count, new_ids
  from inserted;

  update public.products
  set is_deleted = true
  where is_deleted = false
    and not (id = any(new_ids));

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

alter table public.product_update_batches enable row level security;

revoke all on table public.product_update_batches from anon, authenticated;
grant select, insert, update, delete on table public.product_update_batches to service_role;
revoke all on function public.activate_product_update_batch(uuid) from public, anon, authenticated;
grant execute on function public.activate_product_update_batch(uuid) to service_role;
revoke all on function public.update_product_update_batches_updated_at() from public, anon, authenticated;
grant execute on function public.update_product_update_batches_updated_at() to service_role;

commit;
