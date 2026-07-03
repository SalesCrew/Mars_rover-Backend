-- One-time repair for the Tiernahrung Standard batch that was correctly
-- inserted at 2026-07-02 12:00:19 UTC and then incorrectly archived by the
-- later Food-only product switch at 2026-07-02 13:06:17 UTC.

begin;

set local lock_timeout = '5s';
set local statement_timeout = '2min';

do $$
declare
  expected_count constant integer := 273;
  target_count integer;
  restored_count integer;
begin
  select count(*)
  into target_count
  from public.products
  where created_at = timestamptz '2026-07-02 12:00:19.947007+00'
    and updated_at = timestamptz '2026-07-02 13:06:17.32176+00'
    and department = 'pets'
    and product_type = 'standard'
    and coalesce(is_deleted, false) = true;

  if target_count <> expected_count then
    raise exception 'Refusing Tiernahrung restore: expected % rows, found % rows', expected_count, target_count;
  end if;

  update public.products
  set is_deleted = false
  where created_at = timestamptz '2026-07-02 12:00:19.947007+00'
    and updated_at = timestamptz '2026-07-02 13:06:17.32176+00'
    and department = 'pets'
    and product_type = 'standard'
    and coalesce(is_deleted, false) = true;

  get diagnostics restored_count = row_count;

  if restored_count <> expected_count then
    raise exception 'Tiernahrung restore row-count mismatch: expected %, restored %', expected_count, restored_count;
  end if;
end $$;

commit;

