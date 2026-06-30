-- DSGVO / GDPR RLS hardening for MarsPets+.
--
-- Review/apply note:
-- - Do not run directly on production without a backup and a short maintenance window.
-- - The Node backend uses the Supabase service-role key and bypasses RLS, so Express
--   route-level auth remains the primary control for the live app.
-- - Direct anon/authenticated table grants are revoked. The policies below document the
--   intended row model and provide defense-in-depth if direct grants are reintroduced.
-- - Through the Express API/UI, GL users intentionally retain read access to all markets;
--   market master data is treated as non-sensitive for this app.
-- - bug-screenshots, fragebogen-response-images, gl-profile-pictures, vorbesteller-lieferung, vorverkauf-wellen, and wellen-photos are made
--   private here. Do not flip wellen-images to private until those UI/API paths are also
--   migrated to signed URLs.

begin;

-- Fail fast instead of waiting behind production traffic locks.
set local lock_timeout = '5s';
set local statement_timeout = '5min';

create schema if not exists app_private;
revoke all on schema app_private from public, anon, authenticated;
grant usage on schema app_private to authenticated, service_role;

create or replace function app_private.app_gl_id_text()
returns text
language sql
stable
security definer
set search_path = ''
as $$
  select coalesce(nullif(u.gebietsleiter_id::text, ''), u.id::text)
  from public.users u
  where u.id = (select auth.uid())
  limit 1;
$$;

revoke execute on function app_private.app_gl_id_text() from public, anon;
grant execute on function app_private.app_gl_id_text() to authenticated, service_role;

revoke create on schema public from public;
revoke usage on schema public from public;
revoke usage on schema public from anon;
grant usage on schema public to authenticated, service_role;

-- Future public objects should not become direct Data API surface by default.
-- Default privileges are scoped by object owner. Supabase app schemas are normally
-- owned by postgres, and the unqualified statements also cover the applying role
-- when this script is run by another privileged owner.
alter default privileges in schema public revoke all on tables from anon, authenticated;
alter default privileges in schema public revoke all on sequences from anon, authenticated;
alter default privileges in schema public revoke execute on functions from public, anon, authenticated;
alter default privileges in schema public grant all on tables to service_role;
alter default privileges in schema public grant all on sequences to service_role;
alter default privileges in schema public grant execute on functions to service_role;
alter default privileges for role postgres in schema public revoke all on tables from anon, authenticated;
alter default privileges for role postgres in schema public revoke all on sequences from anon, authenticated;
alter default privileges for role postgres in schema public revoke execute on functions from public, anon, authenticated;
alter default privileges for role postgres in schema public grant all on tables to service_role;
alter default privileges for role postgres in schema public grant all on sequences to service_role;
alter default privileges for role postgres in schema public grant execute on functions to service_role;

create or replace function pg_temp.exec_if_table(p_table text, p_sql text)
returns void
language plpgsql
as $$
begin
  if to_regclass(p_table) is not null then
    execute p_sql;
  end if;
end;
$$;

-- Enable RLS on every public table the app uses or has local schema for.
do $$
declare
  t text;
begin
  foreach t in array array[
    'public.users',
    'public.gebietsleiter',
    'public.gl_onboarding_reads',
    'public.markets',
    'public.products',
    'public.products_update',
    'public.action_history',
    'public.bug_reports',
    'public.fb_questions',
    'public.fb_modules',
    'public.fb_module_questions',
    'public.fb_module_rules',
    'public.fb_fragebogen',
    'public.fb_fragebogen_modules',
    'public.fb_fragebogen_markets',
    'public.fb_responses',
    'public.fb_response_answers',
    'public.fb_zeiterfassung_submissions',
    'public.fb_zusatz_zeiterfassung',
    'public.fb_day_tracking',
    'public.zeiterfassung_wochen_checks',
    'public.wellen',
    'public.wellen_displays',
    'public.wellen_kartonware',
    'public.wellen_einzelprodukte',
    'public.wellen_kw_days',
    'public.wellen_markets',
    'public.wellen_paletten',
    'public.wellen_paletten_products',
    'public.wellen_schuetten',
    'public.wellen_schuetten_products',
    'public.wellen_gl_progress',
    'public.wellen_submissions',
    'public.wellen_photos',
    'public.wellen_photo_tags',
    'public.vorverkauf_entries',
    'public.vorverkauf_items',
    'public.vorverkauf_wellen',
    'public.vorverkauf_wellen_markets',
    'public.vorverkauf_submissions',
    'public.vorverkauf_submission_products',
    'public.nara_incentive_submissions',
    'public.nara_incentive_items',
    'public.market_visits'
  ]
  loop
    if to_regclass(t) is not null then
      execute format('alter table %s enable row level security', t);
    end if;
  end loop;
end $$;

-- Keep direct Data API grants aligned with the app architecture. The frontend does
-- not use Supabase table access directly; all app data goes through the Express API.
-- Therefore anon/authenticated get no direct table privileges, and the backend
-- service role keeps full access behind route-level authentication/authorization.
do $$
declare
  t text;
begin
  foreach t in array array[
    'public.users',
    'public.gebietsleiter',
    'public.gl_onboarding_reads',
    'public.markets',
    'public.products',
    'public.products_update',
    'public.action_history',
    'public.bug_reports',
    'public.fb_questions',
    'public.fb_modules',
    'public.fb_module_questions',
    'public.fb_module_rules',
    'public.fb_fragebogen',
    'public.fb_fragebogen_modules',
    'public.fb_fragebogen_markets',
    'public.fb_responses',
    'public.fb_response_answers',
    'public.fb_zeiterfassung_submissions',
    'public.fb_zusatz_zeiterfassung',
    'public.fb_day_tracking',
    'public.zeiterfassung_wochen_checks',
    'public.wellen',
    'public.wellen_displays',
    'public.wellen_kartonware',
    'public.wellen_einzelprodukte',
    'public.wellen_kw_days',
    'public.wellen_markets',
    'public.wellen_paletten',
    'public.wellen_paletten_products',
    'public.wellen_schuetten',
    'public.wellen_schuetten_products',
    'public.wellen_gl_progress',
    'public.wellen_submissions',
    'public.wellen_photos',
    'public.wellen_photo_tags',
    'public.vorverkauf_entries',
    'public.vorverkauf_items',
    'public.vorverkauf_wellen',
    'public.vorverkauf_wellen_markets',
    'public.vorverkauf_submissions',
    'public.vorverkauf_submission_products',
    'public.nara_incentive_submissions',
    'public.nara_incentive_items',
    'public.market_visits'
  ]
  loop
    if to_regclass(t) is not null then
      execute format('revoke all on table %s from anon, authenticated', t);
      execute format('grant all on table %s to service_role', t);
    end if;
  end loop;
end $$;

-- Sequences are uncommon in the current UUID-based schema, but revoke any exposed
-- public sequences as defense in depth for future/imported objects.
do $$
declare
  s text;
begin
  for s in
    select format('%I.%I', n.nspname, c.relname)
    from pg_class c
    join pg_namespace n on n.oid = c.relnamespace
    where n.nspname = 'public'
      and c.relkind = 'S'
  loop
    execute format('revoke all on sequence %s from anon, authenticated', s);
    execute format('grant all on sequence %s to service_role', s);
  end loop;
end $$;

-- Remove known broad or stale policies. Service-role bypasses RLS without policies.
do $$
declare
  r record;
begin
  for r in
    select * from (values
      ('public.users', 'users_select_own'),
      ('public.users', 'users_update_own'),
      ('public.users', 'users_insert_admin_only'),
      ('public.users', 'users_delete_admin_only'),
      ('public.gl_onboarding_reads', 'GLs can manage their own onboarding reads'),
      ('public.gebietsleiter', 'Allow authenticated users to read gebietsleiter'),
      ('public.gebietsleiter', 'Allow authenticated users to insert gebietsleiter'),
      ('public.gebietsleiter', 'Allow authenticated users to update gebietsleiter'),
      ('public.gebietsleiter', 'Allow authenticated users to delete gebietsleiter'),
      ('public.markets', 'Allow all operations for authenticated users'),
      ('public.markets', 'Allow all operations for service role'),
      ('public.action_history', 'Allow all operations for authenticated users'),
      ('public.bug_reports', 'GLs can insert bug reports'),
      ('public.bug_reports', 'GLs can view own bug reports'),
      ('public.bug_reports', 'Service role has full access to bug reports'),
      ('public.fb_zeiterfassung_submissions', 'GLs can manage own zeiterfassung_submissions'),
      ('public.fb_responses', 'GLs can manage own responses'),
      ('public.fb_response_answers', 'GLs can manage own response_answers'),
      ('public.fb_day_tracking', 'GLs can manage their own day tracking'),
      ('public.fb_day_tracking', 'Admins can view all day tracking'),
      ('public.wellen_gl_progress', 'GLs can manage their own progress'),
      ('public.wellen_submissions', 'GLs can view their own submissions'),
      ('public.wellen_submissions', 'GLs can create their own submissions'),
      ('public.wellen_photos', 'GLs can view photos'),
      ('public.wellen_photos', 'GLs can insert own photos'),
      ('public.vorverkauf_entries', 'Service role has full access to vorverkauf_entries'),
      ('public.vorverkauf_items', 'Service role has full access to vorverkauf_items'),
      ('public.vorverkauf_wellen', 'Service role has full access to vorverkauf_wellen'),
      ('public.vorverkauf_wellen_markets', 'Service role has full access to vorverkauf_wellen_markets'),
      ('public.vorverkauf_submissions', 'Service role has full access to vorverkauf_submissions'),
      ('public.vorverkauf_submission_products', 'Service role has full access to vorverkauf_submission_products')
    ) as v(table_name, policy_name)
  loop
    if to_regclass(r.table_name) is not null then
      execute format('drop policy if exists %I on %s', r.policy_name, r.table_name);
    end if;
  end loop;
end $$;

-- Remove policies created by earlier runs of this hardening script.
do $$
declare
  r record;
begin
  for r in
    select schemaname || '.' || tablename as table_name, policyname
    from pg_policies
    where schemaname = 'public'
      and policyname like 'dsgvo_%'
  loop
    execute format('drop policy if exists %I on %s', r.policyname, r.table_name);
  end loop;
end $$;

drop function if exists public.app_gl_id();
drop function if exists public.app_gl_id_text();

-- Users: direct authenticated access is self-only. Admin management stays behind backend routes.
select pg_temp.exec_if_table('public.users', $sql$
  create policy dsgvo_users_select_self
  on public.users
  for select
  to authenticated
  using ((select auth.uid()) = id)
$sql$);

select pg_temp.exec_if_table('public.gebietsleiter', $sql$
  create policy dsgvo_gebietsleiter_select_self
  on public.gebietsleiter
  for select
  to authenticated
  using (id::text = (select app_private.app_gl_id_text()) and coalesce(is_active, true) = true)
$sql$);

-- Non-sensitive catalog/master data. These policies only become reachable if direct
-- authenticated table grants are deliberately reintroduced later.
select pg_temp.exec_if_table('public.markets', $sql$
  create policy dsgvo_markets_select_authenticated
  on public.markets
  for select
  to authenticated
  using (true)
$sql$);

select pg_temp.exec_if_table('public.products', $sql$
  create policy dsgvo_products_select_authenticated
  on public.products
  for select
  to authenticated
  using (coalesce(is_deleted, false) = false)
$sql$);

select pg_temp.exec_if_table('public.bug_reports', $sql$
  create policy dsgvo_bug_reports_select_own
  on public.bug_reports
  for select
  to authenticated
  using (gebietsleiter_id::text = (select app_private.app_gl_id_text()))
$sql$);

select pg_temp.exec_if_table('public.bug_reports', $sql$
  create policy dsgvo_bug_reports_insert_own
  on public.bug_reports
  for insert
  to authenticated
  with check (gebietsleiter_id::text = (select app_private.app_gl_id_text()))
$sql$);

select pg_temp.exec_if_table('public.gl_onboarding_reads', $sql$
  create policy dsgvo_gl_onboarding_reads_own_all
  on public.gl_onboarding_reads
  for all
  to authenticated
  using (gl_id::text = (select app_private.app_gl_id_text()))
  with check (gl_id::text = (select app_private.app_gl_id_text()))
$sql$);

-- Authenticated read-only definition tables. Writes remain backend/admin only.
do $$
declare
  r record;
begin
  for r in
    select * from (values
      ('public.fb_questions', 'dsgvo_fb_questions_select_authenticated', 'coalesce(is_deleted, false) = false'),
      ('public.fb_modules', 'dsgvo_fb_modules_select_authenticated', 'coalesce(is_deleted, false) = false'),
      ('public.fb_module_questions', 'dsgvo_fb_module_questions_select_authenticated', 'true'),
      ('public.fb_module_rules', 'dsgvo_fb_module_rules_select_authenticated', 'true'),
      ('public.fb_fragebogen', 'dsgvo_fb_fragebogen_select_authenticated', 'coalesce(is_deleted, false) = false'),
      ('public.fb_fragebogen_modules', 'dsgvo_fb_fragebogen_modules_select_authenticated', 'true'),
      ('public.fb_fragebogen_markets', 'dsgvo_fb_fragebogen_markets_select_authenticated', 'true'),
      ('public.wellen', 'dsgvo_wellen_select_authenticated', 'true'),
      ('public.wellen_displays', 'dsgvo_wellen_displays_select_authenticated', 'true'),
      ('public.wellen_kartonware', 'dsgvo_wellen_kartonware_select_authenticated', 'true'),
      ('public.wellen_einzelprodukte', 'dsgvo_wellen_einzelprodukte_select_authenticated', 'true'),
      ('public.wellen_kw_days', 'dsgvo_wellen_kw_days_select_authenticated', 'true'),
      ('public.wellen_markets', 'dsgvo_wellen_markets_select_authenticated', 'true'),
      ('public.wellen_paletten', 'dsgvo_wellen_paletten_select_authenticated', 'true'),
      ('public.wellen_paletten_products', 'dsgvo_wellen_paletten_products_select_authenticated', 'true'),
      ('public.wellen_schuetten', 'dsgvo_wellen_schuetten_select_authenticated', 'true'),
      ('public.wellen_schuetten_products', 'dsgvo_wellen_schuetten_products_select_authenticated', 'true'),
      ('public.wellen_photo_tags', 'dsgvo_wellen_photo_tags_select_authenticated', 'true'),
      ('public.vorverkauf_wellen', 'dsgvo_vorverkauf_wellen_select_authenticated', 'true'),
      ('public.vorverkauf_wellen_markets', 'dsgvo_vorverkauf_wellen_markets_select_authenticated', 'true')
    ) as v(table_name, policy_name, using_sql)
  loop
    if to_regclass(r.table_name) is not null then
      execute format(
        'create policy %I on %s for select to authenticated using (%s)',
        r.policy_name,
        r.table_name,
        r.using_sql
      );
    end if;
  end loop;
end $$;

-- Owner-scoped tables with a direct gebietsleiter_id column.
do $$
declare
  r record;
begin
  for r in
    select * from (values
      ('public.fb_zeiterfassung_submissions', 'dsgvo_fb_zeit_own_all'),
      ('public.fb_responses', 'dsgvo_fb_responses_own_all'),
      ('public.fb_zusatz_zeiterfassung', 'dsgvo_fb_zusatz_own_all'),
      ('public.fb_day_tracking', 'dsgvo_fb_day_tracking_own_all'),
      ('public.zeiterfassung_wochen_checks', 'dsgvo_wochen_checks_own_all'),
      ('public.wellen_gl_progress', 'dsgvo_wellen_progress_own_all'),
      ('public.wellen_submissions', 'dsgvo_wellen_submissions_own_all'),
      ('public.wellen_photos', 'dsgvo_wellen_photos_own_all'),
      ('public.vorverkauf_entries', 'dsgvo_vorverkauf_entries_own_all'),
      ('public.vorverkauf_submissions', 'dsgvo_vorverkauf_submissions_own_all'),
      ('public.nara_incentive_submissions', 'dsgvo_nara_submissions_own_all'),
      ('public.market_visits', 'dsgvo_market_visits_own_all')
    ) as v(table_name, policy_name)
  loop
    if to_regclass(r.table_name) is not null then
      execute format(
        'create policy %I on %s for all to authenticated using (gebietsleiter_id::text = (select app_private.app_gl_id_text())) with check (gebietsleiter_id::text = (select app_private.app_gl_id_text()))',
        r.policy_name,
        r.table_name
      );
    end if;
  end loop;
end $$;

-- Child tables inherit ownership through their parent row.
do $$
begin
  if to_regclass('public.fb_response_answers') is not null
     and to_regclass('public.fb_responses') is not null then
    execute $sql$
      create policy dsgvo_fb_answers_own_all
      on public.fb_response_answers
      for all
      to authenticated
      using (
        exists (
          select 1 from public.fb_responses r
          where r.id = response_id
            and r.gebietsleiter_id::text = (select app_private.app_gl_id_text())
        )
      )
      with check (
        exists (
          select 1 from public.fb_responses r
          where r.id = response_id
            and r.gebietsleiter_id::text = (select app_private.app_gl_id_text())
        )
      )
    $sql$;
  end if;

  if to_regclass('public.vorverkauf_items') is not null
     and to_regclass('public.vorverkauf_entries') is not null then
    execute $sql$
      create policy dsgvo_vorverkauf_items_own_all
      on public.vorverkauf_items
      for all
      to authenticated
      using (
        exists (
          select 1 from public.vorverkauf_entries e
          where e.id = vorverkauf_entry_id
            and e.gebietsleiter_id::text = (select app_private.app_gl_id_text())
        )
      )
      with check (
        exists (
          select 1 from public.vorverkauf_entries e
          where e.id = vorverkauf_entry_id
            and e.gebietsleiter_id::text = (select app_private.app_gl_id_text())
        )
      )
    $sql$;
  end if;

  if to_regclass('public.vorverkauf_submission_products') is not null
     and to_regclass('public.vorverkauf_submissions') is not null then
    execute $sql$
      create policy dsgvo_vorverkauf_submission_products_own_all
      on public.vorverkauf_submission_products
      for all
      to authenticated
      using (
        exists (
          select 1 from public.vorverkauf_submissions s
          where s.id = submission_id
            and s.gebietsleiter_id::text = (select app_private.app_gl_id_text())
        )
      )
      with check (
        exists (
          select 1 from public.vorverkauf_submissions s
          where s.id = submission_id
            and s.gebietsleiter_id::text = (select app_private.app_gl_id_text())
        )
      )
    $sql$;
  end if;

  if to_regclass('public.nara_incentive_items') is not null
     and to_regclass('public.nara_incentive_submissions') is not null then
    execute $sql$
      create policy dsgvo_nara_items_own_all
      on public.nara_incentive_items
      for all
      to authenticated
      using (
        exists (
          select 1 from public.nara_incentive_submissions s
          where s.id = submission_id
            and s.gebietsleiter_id::text = (select app_private.app_gl_id_text())
        )
      )
      with check (
        exists (
          select 1 from public.nara_incentive_submissions s
          where s.id = submission_id
            and s.gebietsleiter_id::text = (select app_private.app_gl_id_text())
        )
      )
    $sql$;
  end if;
end $$;

-- Public views bypass underlying table policies unless made security_invoker.
-- Existing local schema grants these views to authenticated users, so remove direct API access.
do $$
declare
  v text;
begin
  foreach v in array array[
    'public.fb_modules_overview',
    'public.fb_fragebogen_overview',
    'public.fb_questions_usage',
    'public.wellen_overview',
    'public.wellen_gl_overview',
    'public.vorverkauf_wellen_overview'
  ]
  loop
    if to_regclass(v) is not null then
      execute format('revoke all on table %s from anon, authenticated', v);
      execute format('grant select on table %s to service_role', v);
    end if;
  end loop;
end $$;

-- Bug screenshots, Fotofragen response photos, GL profile pictures, delivery proof photos, and Fotowelle evidence photos can contain personal/contextual data.
-- The backend returns signed URLs for reads, so these buckets no longer need public
-- object URLs. The wellen-images bucket remains public only for wave/admin image
-- assets, and question-images remains public for admin-managed Fragebogen question images.
insert into storage.buckets (id, name, public)
values
  ('bug-screenshots', 'bug-screenshots', false),
  ('fragebogen-response-images', 'fragebogen-response-images', false),
  ('gl-profile-pictures', 'gl-profile-pictures', false),
  ('vorbesteller-lieferung', 'vorbesteller-lieferung', false),
  ('vorverkauf-wellen', 'vorverkauf-wellen', false),
  ('wellen-photos', 'wellen-photos', false),
  ('question-images', 'question-images', true),
  ('wellen-images', 'wellen-images', true)
on conflict (id) do update set public = excluded.public;

-- storage.objects is owned by Supabase's internal storage role in hosted projects.
-- This migration role cannot alter or replace those object policies. Bucket
-- privacy is hardened above, and backend access to private buckets goes through
-- service-role signed URLs.

do $$
declare
  fn text;
begin
  foreach fn in array array[
    'public.calculate_time_diff(time,time)',
    'public.get_last_market_visit(uuid,date)',
    'public.update_action_history_updated_at()',
    'public.update_bug_reports_updated_at()',
    'public.update_fb_day_tracking_updated_at()',
    'public.update_fb_updated_at_column()',
    'public.update_fragebogen_status()',
    'public.update_gebietsleiter_updated_at()',
    'public.update_products_updated_at()',
    'public.update_updated_at_column()',
    'public.update_users_updated_at()',
    'public.update_vorverkauf_welle_status()',
    'public.update_vorverkauf_wellen_updated_at()',
    'public.update_welle_status()',
    'public.update_zusatz_zeit_updated_at()'
  ]
  loop
    if to_regprocedure(fn) is not null then
      execute format('revoke all on function %s from public, anon, authenticated', fn);
      execute format('grant execute on function %s to service_role', fn);
    end if;
  end loop;
end $$;

commit;
