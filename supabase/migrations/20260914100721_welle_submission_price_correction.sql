-- No existing bookings are changed by installing this migration.
create table public.wellen_submission_price_corrections (
  id uuid primary key default gen_random_uuid(),
  welle_id uuid not null,
  actor_id uuid not null,
  created_at timestamptz not null default now(),
  preview_token text not null,
  changes jsonb not null
);
alter table public.wellen_submission_price_corrections enable row level security;
revoke all on public.wellen_submission_price_corrections from public, anon, authenticated;
grant select, insert on public.wellen_submission_price_corrections to service_role;

create or replace function public.correct_welle_submission_prices(
  p_welle_id uuid,
  p_apply boolean default false,
  p_expected_token text default null,
  p_actor_id uuid default null
) returns jsonb
language plpgsql
security invoker
set search_path = ''
as $$
declare
  v_name text;
  v_snapshot jsonb;
  v_changes jsonb;
  v_groups jsonb;
  v_token text;
  v_count integer;
  v_updated integer;
  v_skipped integer;
  v_missing_old integer;
  v_old numeric;
  v_new numeric;
begin
  if p_apply and (p_expected_token is null or p_actor_id is null) then
    return jsonb_build_object('code', 'WELLE_PRICE_CONFIRMATION_REQUIRED');
  end if;

  if p_apply then
    -- Lock only this wave and its definitions/bookings. FK inserts wait on the wave lock.
    select w.name into v_name from public.wellen w
      where w.id = p_welle_id and not coalesce(w.is_deleted, false) for update;
  else
    select w.name into v_name from public.wellen w
      where w.id = p_welle_id and not coalesce(w.is_deleted, false);
  end if;
  if not found then
    return jsonb_build_object('code', 'WELLE_PRICE_NOT_FOUND');
  end if;

  if p_apply then
    perform id from public.wellen_displays where welle_id = p_welle_id order by id for share;
    perform id from public.wellen_kartonware where welle_id = p_welle_id order by id for share;
    perform id from public.wellen_einzelprodukte where welle_id = p_welle_id order by id for share;
    perform id from public.wellen_paletten where welle_id = p_welle_id order by id for share;
    perform p.id from public.wellen_paletten_products p join public.wellen_paletten g on g.id = p.palette_id
      where g.welle_id = p_welle_id order by p.id for share of p;
    perform id from public.wellen_schuetten where welle_id = p_welle_id order by id for share;
    perform p.id from public.wellen_schuetten_products p join public.wellen_schuetten g on g.id = p.schuette_id
      where g.welle_id = p_welle_id order by p.id for share of p;
    perform id from public.wellen_submissions where welle_id = p_welle_id order by id for update;
  end if;

  -- Match by type + product ID + wave ownership, never by name or master-product ID.
  with prices as (
    select id, 'display'::text as item_type, name, item_value as price
      from public.wellen_displays where welle_id = p_welle_id
    union all
    select id, 'kartonware', name, item_value
      from public.wellen_kartonware where welle_id = p_welle_id
    union all
    select id, 'einzelprodukt', name, item_value
      from public.wellen_einzelprodukte where welle_id = p_welle_id
    union all
    select p.id, 'palette', g.name || ' / ' || p.name, p.value_per_ve
      from public.wellen_paletten_products p join public.wellen_paletten g on g.id = p.palette_id
      where g.welle_id = p_welle_id
    union all
    select p.id, 'schuette', g.name || ' / ' || p.name, p.value_per_ve
      from public.wellen_schuetten_products p join public.wellen_schuetten g on g.id = p.schuette_id
      where g.welle_id = p_welle_id
  )
  select coalesce(jsonb_agg(jsonb_build_object(
    'id', s.id, 'itemId', s.item_id, 'itemType', s.item_type, 'name', p.name,
    'quantity', s.quantity, 'oldPrice', s.value_per_unit, 'newPrice', p.price,
    'eligible', p.id is not null and p.price is not null and p.price >= 0
      and p.price::text not in ('NaN', 'Infinity', '-Infinity'),
    'skipped', p.id is null or (s.value_per_unit is not null and (p.price is null
      or p.price < 0 or p.price::text in ('NaN', 'Infinity', '-Infinity')))
  ) order by s.id), '[]'::jsonb) into v_snapshot
  from public.wellen_submissions s left join prices p on p.id = s.item_id and p.item_type = s.item_type
  where s.welle_id = p_welle_id;

  v_token := encode(sha256(convert_to(p_welle_id::text || v_snapshot::text, 'UTF8')), 'hex');
  if p_apply and p_expected_token is distinct from v_token then
    return jsonb_build_object('code', 'WELLE_PRICE_PREVIEW_STALE');
  end if;

  select coalesce(jsonb_agg(e), '[]'::jsonb) into v_changes
  from jsonb_array_elements(v_snapshot) e
  where (e->>'eligible')::boolean and e->'oldPrice' is distinct from e->'newPrice';
  v_count := jsonb_array_length(v_changes);
  select count(*) into v_skipped from jsonb_array_elements(v_snapshot) e where (e->>'skipped')::boolean;
  select count(*) filter (where e->>'oldPrice' is null),
    coalesce(sum((e->>'quantity')::numeric * (e->>'oldPrice')::numeric), 0),
    coalesce(sum((e->>'quantity')::numeric * (e->>'newPrice')::numeric), 0)
    into v_missing_old, v_old, v_new from jsonb_array_elements(v_changes) e;
  select coalesce(jsonb_agg(g order by g.name, g."itemType", g."itemId", g."oldPrice"), '[]'::jsonb)
    into v_groups from (
      select e->>'itemId' as "itemId", e->>'itemType' as "itemType", e->>'name' as name,
        e->>'oldPrice' as "oldPrice", e->>'newPrice' as "newPrice", count(*) as count,
        sum((e->>'quantity')::numeric) as quantity
      from jsonb_array_elements(v_changes) e group by 1, 2, 3, 4, 5
    ) g;

  if p_apply and v_count > 0 then
    update public.wellen_submissions s set value_per_unit = (e->>'newPrice')::numeric
      from jsonb_array_elements(v_changes) e
      where s.welle_id = p_welle_id and s.id = (e->>'id')::uuid
        and s.item_id = (e->>'itemId')::uuid and s.item_type = e->>'itemType'
        and s.value_per_unit is not distinct from (e->>'oldPrice')::numeric;
    get diagnostics v_updated = row_count;
    if v_updated <> v_count then
      raise exception 'WELLE_PRICE_UPDATE_COUNT_MISMATCH';
    end if;
    insert into public.wellen_submission_price_corrections(welle_id, actor_id, preview_token, changes)
      values (p_welle_id, p_actor_id, v_token, v_changes);
  end if;

  return jsonb_build_object('welleId', p_welle_id, 'welleName', v_name, 'token', v_token,
    'count', v_count, 'skippedCount', v_skipped, 'missingOldPriceCount', v_missing_old,
    'oldTotal', v_old::text, 'newTotal', v_new::text, 'groups', v_groups,
    'applied', p_apply, 'updatedCount', coalesce(v_updated, 0));
end;
$$;
revoke all on function public.correct_welle_submission_prices(uuid, boolean, text, uuid) from public, anon, authenticated;
grant execute on function public.correct_welle_submission_prices(uuid, boolean, text, uuid) to service_role;
