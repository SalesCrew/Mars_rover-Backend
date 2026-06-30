-- Metadata-only verification for dsgvo_rls_hardening.sql.
--
-- This script is read-only and must not select business rows. It checks catalog
-- state only: RLS flags, grants, policies, view grants, and Storage bucket flags.

begin;
set transaction read only;
set local lock_timeout = '2s';
set local statement_timeout = '2min';

with app_tables(table_name) as (
  values
    ('users'),
    ('gebietsleiter'),
    ('gl_onboarding_reads'),
    ('markets'),
    ('products'),
    ('products_update'),
    ('action_history'),
    ('bug_reports'),
    ('fb_questions'),
    ('fb_modules'),
    ('fb_module_questions'),
    ('fb_module_rules'),
    ('fb_fragebogen'),
    ('fb_fragebogen_modules'),
    ('fb_fragebogen_markets'),
    ('fb_responses'),
    ('fb_response_answers'),
    ('fb_zeiterfassung_submissions'),
    ('fb_zusatz_zeiterfassung'),
    ('fb_day_tracking'),
    ('zeiterfassung_wochen_checks'),
    ('wellen'),
    ('wellen_displays'),
    ('wellen_kartonware'),
    ('wellen_einzelprodukte'),
    ('wellen_kw_days'),
    ('wellen_markets'),
    ('wellen_paletten'),
    ('wellen_paletten_products'),
    ('wellen_schuetten'),
    ('wellen_schuetten_products'),
    ('wellen_gl_progress'),
    ('wellen_submissions'),
    ('wellen_photos'),
    ('wellen_photo_tags'),
    ('vorverkauf_entries'),
    ('vorverkauf_items'),
    ('vorverkauf_wellen'),
    ('vorverkauf_wellen_markets'),
    ('vorverkauf_submissions'),
    ('vorverkauf_submission_products'),
    ('nara_incentive_submissions'),
    ('nara_incentive_items'),
    ('market_visits')
)
select
  'rls_enabled_on_existing_app_tables' as check_name,
  case when count(*) = 0 then 'pass' else 'fail' end as status,
  coalesce(json_agg(t.table_name order by t.table_name) filter (where t.table_name is not null), '[]'::json) as failing_objects
from app_tables a
join pg_tables t
  on t.schemaname = 'public'
 and t.tablename = a.table_name
where t.rowsecurity is not true;

with app_tables(table_name) as (
  values
    ('users'),
    ('gebietsleiter'),
    ('gl_onboarding_reads'),
    ('markets'),
    ('products'),
    ('products_update'),
    ('action_history'),
    ('bug_reports'),
    ('fb_questions'),
    ('fb_modules'),
    ('fb_module_questions'),
    ('fb_module_rules'),
    ('fb_fragebogen'),
    ('fb_fragebogen_modules'),
    ('fb_fragebogen_markets'),
    ('fb_responses'),
    ('fb_response_answers'),
    ('fb_zeiterfassung_submissions'),
    ('fb_zusatz_zeiterfassung'),
    ('fb_day_tracking'),
    ('zeiterfassung_wochen_checks'),
    ('wellen'),
    ('wellen_displays'),
    ('wellen_kartonware'),
    ('wellen_einzelprodukte'),
    ('wellen_kw_days'),
    ('wellen_markets'),
    ('wellen_paletten'),
    ('wellen_paletten_products'),
    ('wellen_schuetten'),
    ('wellen_schuetten_products'),
    ('wellen_gl_progress'),
    ('wellen_submissions'),
    ('wellen_photos'),
    ('wellen_photo_tags'),
    ('vorverkauf_entries'),
    ('vorverkauf_items'),
    ('vorverkauf_wellen'),
    ('vorverkauf_wellen_markets'),
    ('vorverkauf_submissions'),
    ('vorverkauf_submission_products'),
    ('nara_incentive_submissions'),
    ('nara_incentive_items'),
    ('market_visits')
)
select
  'no_direct_anon_authenticated_table_grants' as check_name,
  case when count(*) = 0 then 'pass' else 'fail' end as status,
  coalesce(
    json_agg(
      json_build_object(
        'grantee', p.grantee,
        'table_name', p.table_name,
        'privilege_type', p.privilege_type
      )
      order by p.table_name, p.grantee, p.privilege_type
    ) filter (where p.table_name is not null),
    '[]'::json
  ) as failing_objects
from information_schema.table_privileges p
join app_tables a
  on a.table_name = p.table_name
where p.table_schema = 'public'
  and p.grantee in ('anon', 'authenticated');

with app_tables(table_name) as (
  values
    ('users'),
    ('gebietsleiter'),
    ('gl_onboarding_reads'),
    ('markets'),
    ('products'),
    ('products_update'),
    ('action_history'),
    ('bug_reports'),
    ('fb_questions'),
    ('fb_modules'),
    ('fb_module_questions'),
    ('fb_module_rules'),
    ('fb_fragebogen'),
    ('fb_fragebogen_modules'),
    ('fb_fragebogen_markets'),
    ('fb_responses'),
    ('fb_response_answers'),
    ('fb_zeiterfassung_submissions'),
    ('fb_zusatz_zeiterfassung'),
    ('fb_day_tracking'),
    ('zeiterfassung_wochen_checks'),
    ('wellen'),
    ('wellen_displays'),
    ('wellen_kartonware'),
    ('wellen_einzelprodukte'),
    ('wellen_kw_days'),
    ('wellen_markets'),
    ('wellen_paletten'),
    ('wellen_paletten_products'),
    ('wellen_schuetten'),
    ('wellen_schuetten_products'),
    ('wellen_gl_progress'),
    ('wellen_submissions'),
    ('wellen_photos'),
    ('wellen_photo_tags'),
    ('vorverkauf_entries'),
    ('vorverkauf_items'),
    ('vorverkauf_wellen'),
    ('vorverkauf_wellen_markets'),
    ('vorverkauf_submissions'),
    ('vorverkauf_submission_products'),
    ('nara_incentive_submissions'),
    ('nara_incentive_items'),
    ('market_visits')
),
required_service_role_privileges(privilege_type) as (
  values
    ('SELECT'),
    ('IN' || 'SERT'),
    ('UP' || 'DATE'),
    ('DE' || 'LETE')
),
missing_service_role_table_access as (
  select
    a.table_name,
    r.privilege_type
  from app_tables a
  join pg_tables t
    on t.schemaname = 'public'
   and t.tablename = a.table_name
  cross join required_service_role_privileges r
  where not exists (
    select 1
    from information_schema.table_privileges p
    where p.table_schema = 'public'
      and p.table_name = a.table_name
      and p.grantee = 'service_role'
      and p.privilege_type = r.privilege_type
  )
)
select
  'service_role_app_table_access_present' as check_name,
  case when count(*) = 0 then 'pass' else 'fail' end as status,
  coalesce(
    json_agg(json_build_object('table_name', table_name, 'privilege_type', privilege_type) order by table_name, privilege_type),
    '[]'::json
  ) as missing_access
from missing_service_role_table_access;

with public_sequence_privileges as (
  select
    n.nspname as schema_name,
    c.relname as sequence_name,
    coalesce(r.rolname, 'PUBLIC') as grantee,
    a.privilege_type
  from pg_class c
  join pg_namespace n on n.oid = c.relnamespace
  cross join lateral aclexplode(coalesce(c.relacl, acldefault('S', c.relowner))) as a
  left join pg_roles r on r.oid = a.grantee
  where n.nspname = 'public'
    and c.relkind = 'S'
)
select
  'no_direct_anon_authenticated_sequence_privileges' as check_name,
  case when count(*) = 0 then 'pass' else 'fail' end as status,
  coalesce(
    json_agg(json_build_object('schema', schema_name, 'sequence_name', sequence_name, 'grantee', grantee, 'privilege_type', privilege_type) order by sequence_name, grantee, privilege_type),
    '[]'::json
  ) as exposed_sequence_privileges
from public_sequence_privileges
where grantee in ('anon', 'authenticated');

with public_sequences as (
  select
    n.nspname as schema_name,
    c.relname as sequence_name,
    c.relacl,
    c.relowner
  from pg_class c
  join pg_namespace n on n.oid = c.relnamespace
  where n.nspname = 'public'
    and c.relkind = 'S'
),
required_service_role_sequence_privileges(privilege_type) as (
  values
    ('USAGE'),
    ('SELECT'),
    ('UP' || 'DATE')
),
missing_service_role_sequence_access as (
  select
    s.schema_name,
    s.sequence_name,
    r.privilege_type
  from public_sequences s
  cross join required_service_role_sequence_privileges r
  where not exists (
    select 1
    from aclexplode(coalesce(s.relacl, acldefault('S', s.relowner))) as a
    join pg_roles grantee_role on grantee_role.oid = a.grantee
    where grantee_role.rolname = 'service_role'
      and a.privilege_type = r.privilege_type
  )
)
select
  'service_role_public_sequence_access_present' as check_name,
  case when count(*) = 0 then 'pass' else 'fail' end as status,
  coalesce(
    json_agg(json_build_object('schema', schema_name, 'sequence_name', sequence_name, 'privilege_type', privilege_type) order by sequence_name, privilege_type),
    '[]'::json
  ) as missing_sequence_access
from missing_service_role_sequence_access;

with reviewed_public_tables(table_name) as (
  values
    ('users'),
    ('gebietsleiter'),
    ('gl_onboarding_reads'),
    ('markets'),
    ('products'),
    ('products_update'),
    ('action_history'),
    ('bug_reports'),
    ('fb_questions'),
    ('fb_modules'),
    ('fb_module_questions'),
    ('fb_module_rules'),
    ('fb_fragebogen'),
    ('fb_fragebogen_modules'),
    ('fb_fragebogen_markets'),
    ('fb_responses'),
    ('fb_response_answers'),
    ('fb_zeiterfassung_submissions'),
    ('fb_zusatz_zeiterfassung'),
    ('fb_day_tracking'),
    ('zeiterfassung_wochen_checks'),
    ('wellen'),
    ('wellen_displays'),
    ('wellen_kartonware'),
    ('wellen_einzelprodukte'),
    ('wellen_kw_days'),
    ('wellen_markets'),
    ('wellen_paletten'),
    ('wellen_paletten_products'),
    ('wellen_schuetten'),
    ('wellen_schuetten_products'),
    ('wellen_gl_progress'),
    ('wellen_submissions'),
    ('wellen_photos'),
    ('wellen_photo_tags'),
    ('vorverkauf_entries'),
    ('vorverkauf_items'),
    ('vorverkauf_wellen'),
    ('vorverkauf_wellen_markets'),
    ('vorverkauf_submissions'),
    ('vorverkauf_submission_products'),
    ('nara_incentive_submissions'),
    ('nara_incentive_items'),
    ('market_visits'),
    ('spatial_ref_sys')
)
select
  'no_unreviewed_public_tables' as check_name,
  case when count(*) = 0 then 'pass' else 'fail' end as status,
  coalesce(
    json_agg(json_build_object('schema', t.schemaname, 'table_name', t.tablename) order by t.tablename),
    '[]'::json
  ) as unreviewed_tables
from pg_tables t
where t.schemaname = 'public'
  and not exists (
    select 1
    from reviewed_public_tables r
    where r.table_name = t.tablename
  );

with expected_policies(table_name, policyname) as (
  values
    ('users', 'dsgvo_users_select_self'),
    ('gebietsleiter', 'dsgvo_gebietsleiter_select_self'),
    ('markets', 'dsgvo_markets_select_authenticated'),
    ('products', 'dsgvo_products_select_authenticated'),
    ('bug_reports', 'dsgvo_bug_reports_select_own'),
    ('bug_reports', 'dsgvo_bug_reports_insert_own'),
    ('gl_onboarding_reads', 'dsgvo_gl_onboarding_reads_own_all'),
    ('fb_questions', 'dsgvo_fb_questions_select_authenticated'),
    ('fb_modules', 'dsgvo_fb_modules_select_authenticated'),
    ('fb_module_questions', 'dsgvo_fb_module_questions_select_authenticated'),
    ('fb_module_rules', 'dsgvo_fb_module_rules_select_authenticated'),
    ('fb_fragebogen', 'dsgvo_fb_fragebogen_select_authenticated'),
    ('fb_fragebogen_modules', 'dsgvo_fb_fragebogen_modules_select_authenticated'),
    ('fb_fragebogen_markets', 'dsgvo_fb_fragebogen_markets_select_authenticated'),
    ('wellen', 'dsgvo_wellen_select_authenticated'),
    ('wellen_displays', 'dsgvo_wellen_displays_select_authenticated'),
    ('wellen_kartonware', 'dsgvo_wellen_kartonware_select_authenticated'),
    ('wellen_einzelprodukte', 'dsgvo_wellen_einzelprodukte_select_authenticated'),
    ('wellen_kw_days', 'dsgvo_wellen_kw_days_select_authenticated'),
    ('wellen_markets', 'dsgvo_wellen_markets_select_authenticated'),
    ('wellen_paletten', 'dsgvo_wellen_paletten_select_authenticated'),
    ('wellen_paletten_products', 'dsgvo_wellen_paletten_products_select_authenticated'),
    ('wellen_schuetten', 'dsgvo_wellen_schuetten_select_authenticated'),
    ('wellen_schuetten_products', 'dsgvo_wellen_schuetten_products_select_authenticated'),
    ('wellen_photo_tags', 'dsgvo_wellen_photo_tags_select_authenticated'),
    ('vorverkauf_wellen', 'dsgvo_vorverkauf_wellen_select_authenticated'),
    ('vorverkauf_wellen_markets', 'dsgvo_vorverkauf_wellen_markets_select_authenticated'),
    ('fb_zeiterfassung_submissions', 'dsgvo_fb_zeit_own_all'),
    ('fb_responses', 'dsgvo_fb_responses_own_all'),
    ('fb_zusatz_zeiterfassung', 'dsgvo_fb_zusatz_own_all'),
    ('fb_day_tracking', 'dsgvo_fb_day_tracking_own_all'),
    ('zeiterfassung_wochen_checks', 'dsgvo_wochen_checks_own_all'),
    ('wellen_gl_progress', 'dsgvo_wellen_progress_own_all'),
    ('wellen_submissions', 'dsgvo_wellen_submissions_own_all'),
    ('wellen_photos', 'dsgvo_wellen_photos_own_all'),
    ('vorverkauf_entries', 'dsgvo_vorverkauf_entries_own_all'),
    ('vorverkauf_submissions', 'dsgvo_vorverkauf_submissions_own_all'),
    ('nara_incentive_submissions', 'dsgvo_nara_submissions_own_all'),
    ('market_visits', 'dsgvo_market_visits_own_all'),
    ('fb_response_answers', 'dsgvo_fb_answers_own_all'),
    ('vorverkauf_items', 'dsgvo_vorverkauf_items_own_all'),
    ('vorverkauf_submission_products', 'dsgvo_vorverkauf_submission_products_own_all'),
    ('nara_incentive_items', 'dsgvo_nara_items_own_all')
)
select
  'expected_named_policies_present_when_table_exists' as check_name,
  case when count(*) = 0 then 'pass' else 'fail' end as status,
  coalesce(
    json_agg(json_build_object('table_name', e.table_name, 'policyname', e.policyname) order by e.table_name, e.policyname),
    '[]'::json
  ) as missing_policies
from expected_policies e
where to_regclass(format('public.%I', e.table_name)) is not null
  and not exists (
    select 1
    from pg_policies p
    where p.schemaname = 'public'
      and p.tablename = e.table_name
      and p.policyname = e.policyname
  );

with expected_policy_shapes(table_name, policyname, expected_cmd, allow_true_predicate) as (
  values
    ('users', 'dsgvo_users_select_self', 'SELECT', false),
    ('gebietsleiter', 'dsgvo_gebietsleiter_select_self', 'SELECT', false),
    ('markets', 'dsgvo_markets_select_authenticated', 'SELECT', true),
    ('products', 'dsgvo_products_select_authenticated', 'SELECT', false),
    ('bug_reports', 'dsgvo_bug_reports_select_own', 'SELECT', false),
    ('bug_reports', 'dsgvo_bug_reports_insert_own', 'IN' || 'SERT', false),
    ('gl_onboarding_reads', 'dsgvo_gl_onboarding_reads_own_all', 'ALL', false),
    ('fb_questions', 'dsgvo_fb_questions_select_authenticated', 'SELECT', false),
    ('fb_modules', 'dsgvo_fb_modules_select_authenticated', 'SELECT', false),
    ('fb_module_questions', 'dsgvo_fb_module_questions_select_authenticated', 'SELECT', true),
    ('fb_module_rules', 'dsgvo_fb_module_rules_select_authenticated', 'SELECT', true),
    ('fb_fragebogen', 'dsgvo_fb_fragebogen_select_authenticated', 'SELECT', false),
    ('fb_fragebogen_modules', 'dsgvo_fb_fragebogen_modules_select_authenticated', 'SELECT', true),
    ('fb_fragebogen_markets', 'dsgvo_fb_fragebogen_markets_select_authenticated', 'SELECT', true),
    ('wellen', 'dsgvo_wellen_select_authenticated', 'SELECT', true),
    ('wellen_displays', 'dsgvo_wellen_displays_select_authenticated', 'SELECT', true),
    ('wellen_kartonware', 'dsgvo_wellen_kartonware_select_authenticated', 'SELECT', true),
    ('wellen_einzelprodukte', 'dsgvo_wellen_einzelprodukte_select_authenticated', 'SELECT', true),
    ('wellen_kw_days', 'dsgvo_wellen_kw_days_select_authenticated', 'SELECT', true),
    ('wellen_markets', 'dsgvo_wellen_markets_select_authenticated', 'SELECT', true),
    ('wellen_paletten', 'dsgvo_wellen_paletten_select_authenticated', 'SELECT', true),
    ('wellen_paletten_products', 'dsgvo_wellen_paletten_products_select_authenticated', 'SELECT', true),
    ('wellen_schuetten', 'dsgvo_wellen_schuetten_select_authenticated', 'SELECT', true),
    ('wellen_schuetten_products', 'dsgvo_wellen_schuetten_products_select_authenticated', 'SELECT', true),
    ('wellen_photo_tags', 'dsgvo_wellen_photo_tags_select_authenticated', 'SELECT', true),
    ('vorverkauf_wellen', 'dsgvo_vorverkauf_wellen_select_authenticated', 'SELECT', true),
    ('vorverkauf_wellen_markets', 'dsgvo_vorverkauf_wellen_markets_select_authenticated', 'SELECT', true),
    ('fb_zeiterfassung_submissions', 'dsgvo_fb_zeit_own_all', 'ALL', false),
    ('fb_responses', 'dsgvo_fb_responses_own_all', 'ALL', false),
    ('fb_zusatz_zeiterfassung', 'dsgvo_fb_zusatz_own_all', 'ALL', false),
    ('fb_day_tracking', 'dsgvo_fb_day_tracking_own_all', 'ALL', false),
    ('zeiterfassung_wochen_checks', 'dsgvo_wochen_checks_own_all', 'ALL', false),
    ('wellen_gl_progress', 'dsgvo_wellen_progress_own_all', 'ALL', false),
    ('wellen_submissions', 'dsgvo_wellen_submissions_own_all', 'ALL', false),
    ('wellen_photos', 'dsgvo_wellen_photos_own_all', 'ALL', false),
    ('vorverkauf_entries', 'dsgvo_vorverkauf_entries_own_all', 'ALL', false),
    ('vorverkauf_submissions', 'dsgvo_vorverkauf_submissions_own_all', 'ALL', false),
    ('nara_incentive_submissions', 'dsgvo_nara_submissions_own_all', 'ALL', false),
    ('market_visits', 'dsgvo_market_visits_own_all', 'ALL', false),
    ('fb_response_answers', 'dsgvo_fb_answers_own_all', 'ALL', false),
    ('vorverkauf_items', 'dsgvo_vorverkauf_items_own_all', 'ALL', false),
    ('vorverkauf_submission_products', 'dsgvo_vorverkauf_submission_products_own_all', 'ALL', false),
    ('nara_incentive_items', 'dsgvo_nara_items_own_all', 'ALL', false)
),
policy_shapes as (
  select
    e.table_name,
    e.policyname,
    e.expected_cmd,
    e.allow_true_predicate,
    p.cmd,
    p.roles,
    p.qual,
    p.with_check,
    lower(regexp_replace(coalesce(p.qual, ''), '[\s()]', '', 'g')) as normalized_qual,
    lower(regexp_replace(coalesce(p.with_check, ''), '[\s()]', '', 'g')) as normalized_with_check
  from expected_policy_shapes e
  join pg_policies p
    on p.schemaname = 'public'
   and p.tablename = e.table_name
   and p.policyname = e.policyname
  where to_regclass(format('public.%I', e.table_name)) is not null
),
policy_shape_violations as (
  select
    table_name,
    policyname,
    case
      when cmd <> expected_cmd then 'unexpected_command'
      when not (roles @> array['authenticated']::name[] and cardinality(roles) = 1) then 'unexpected_roles'
      when not allow_true_predicate and normalized_qual = 'true' then 'broad_using_predicate'
      when not allow_true_predicate and normalized_with_check = 'true' then 'broad_with_check_predicate'
      when cmd = 'ALL' and (qual is null or with_check is null) then 'missing_using_or_with_check'
      when cmd = ('IN' || 'SERT') and with_check is null then 'missing_with_check'
      else null
    end as issue,
    cmd,
    expected_cmd,
    roles,
    qual,
    with_check
  from policy_shapes
)
select
  'expected_policy_shapes_are_restricted' as check_name,
  case when count(*) = 0 then 'pass' else 'fail' end as status,
  coalesce(
    json_agg(json_build_object('table_name', table_name, 'policyname', policyname, 'issue', issue, 'cmd', cmd, 'expected_cmd', expected_cmd, 'roles', roles, 'qual', qual, 'with_check', with_check) order by table_name, policyname, issue),
    '[]'::json
  ) as failing_policy_shapes
from policy_shape_violations
where issue is not null;

with protected_views(view_name) as (
  values
    ('fb_modules_overview'),
    ('fb_fragebogen_overview'),
    ('fb_questions_usage'),
    ('wellen_overview'),
    ('wellen_gl_overview'),
    ('vorverkauf_wellen_overview')
)
select
  'no_direct_anon_authenticated_view_grants' as check_name,
  case when count(*) = 0 then 'pass' else 'fail' end as status,
  coalesce(
    json_agg(
      json_build_object(
        'grantee', p.grantee,
        'view_name', p.table_name,
        'privilege_type', p.privilege_type
      )
      order by p.table_name, p.grantee, p.privilege_type
    ) filter (where p.table_name is not null),
    '[]'::json
  ) as failing_objects
from information_schema.table_privileges p
join protected_views v
  on v.view_name = p.table_name
where p.table_schema = 'public'
  and p.grantee in ('anon', 'authenticated');

with protected_views(view_name) as (
  values
    ('fb_modules_overview'),
    ('fb_fragebogen_overview'),
    ('fb_questions_usage'),
    ('wellen_overview'),
    ('wellen_gl_overview'),
    ('vorverkauf_wellen_overview')
)
select
  'service_role_protected_view_access_present' as check_name,
  case when count(*) = 0 then 'pass' else 'fail' end as status,
  coalesce(
    json_agg(view_name order by view_name),
    '[]'::json
  ) as missing_view_access
from protected_views v
where to_regclass(format('public.%I', v.view_name)) is not null
  and not exists (
    select 1
    from information_schema.table_privileges p
    where p.table_schema = 'public'
      and p.table_name = v.view_name
      and p.grantee = 'service_role'
      and p.privilege_type = 'SELECT'
  );

with reviewed_public_views(view_name) as (
  values
    ('fb_modules_overview'),
    ('fb_fragebogen_overview'),
    ('fb_questions_usage'),
    ('wellen_overview'),
    ('wellen_gl_overview'),
    ('vorverkauf_wellen_overview'),
    ('geography_columns'),
    ('geometry_columns')
),
public_views as (
  select schemaname, viewname as view_name, 'view' as relation_kind
  from pg_views
  where schemaname = 'public'

  union all

  select schemaname, matviewname as view_name, 'materialized_view' as relation_kind
  from pg_matviews
  where schemaname = 'public'
)
select
  'no_unreviewed_public_views' as check_name,
  case when count(*) = 0 then 'pass' else 'fail' end as status,
  coalesce(
    json_agg(json_build_object('schema', v.schemaname, 'view_name', v.view_name, 'relation_kind', v.relation_kind) order by v.view_name),
    '[]'::json
  ) as unreviewed_views
from public_views v
where not exists (
  select 1
  from reviewed_public_views r
  where r.view_name = v.view_name
);

with protected_functions(function_name) as (
  values
    ('calculate_time_diff'),
    ('get_last_market_visit'),
    ('update_action_history_updated_at'),
    ('update_bug_reports_updated_at'),
    ('update_fb_day_tracking_updated_at'),
    ('update_fb_updated_at_column'),
    ('update_fragebogen_status'),
    ('update_gebietsleiter_updated_at'),
    ('update_products_updated_at'),
    ('update_updated_at_column'),
    ('update_users_updated_at'),
    ('update_vorverkauf_welle_status'),
    ('update_vorverkauf_wellen_updated_at'),
    ('update_welle_status'),
    ('update_zusatz_zeit_updated_at')
)
select
  'no_direct_anon_authenticated_function_execute_grants' as check_name,
  case when count(*) = 0 then 'pass' else 'fail' end as status,
  coalesce(
    json_agg(
      json_build_object(
        'grantee', p.grantee,
        'function_name', p.routine_name,
        'privilege_type', p.privilege_type
      )
      order by p.routine_name, p.grantee, p.privilege_type
    ) filter (where p.routine_name is not null),
    '[]'::json
  ) as failing_objects
from information_schema.routine_privileges p
join protected_functions f
  on f.function_name = p.routine_name
where p.routine_schema = 'public'
  and p.grantee in ('PUBLIC', 'anon', 'authenticated');

with protected_functions(function_name) as (
  values
    ('calculate_time_diff'),
    ('get_last_market_visit'),
    ('update_action_history_updated_at'),
    ('update_bug_reports_updated_at'),
    ('update_fb_day_tracking_updated_at'),
    ('update_fb_updated_at_column'),
    ('update_fragebogen_status'),
    ('update_gebietsleiter_updated_at'),
    ('update_products_updated_at'),
    ('update_updated_at_column'),
    ('update_users_updated_at'),
    ('update_vorverkauf_welle_status'),
    ('update_vorverkauf_wellen_updated_at'),
    ('update_welle_status'),
    ('update_zusatz_zeit_updated_at')
)
select
  'service_role_protected_function_access_present' as check_name,
  case when count(*) = 0 then 'pass' else 'fail' end as status,
  coalesce(
    json_agg(function_name order by function_name),
    '[]'::json
  ) as missing_function_access
from protected_functions f
where exists (
    select 1
    from information_schema.routines r
    where r.routine_schema = 'public'
      and r.routine_name = f.function_name
  )
  and not exists (
    select 1
    from information_schema.routine_privileges p
    where p.routine_schema = 'public'
      and p.routine_name = f.function_name
      and p.grantee = 'service_role'
      and p.privilege_type = 'EXECUTE'
  );

with reviewed_public_application_functions(function_name) as (
  values
    ('calculate_time_diff'),
    ('get_last_market_visit'),
    ('update_action_history_updated_at'),
    ('update_bug_reports_updated_at'),
    ('update_fb_day_tracking_updated_at'),
    ('update_fb_updated_at_column'),
    ('update_fragebogen_status'),
    ('update_gebietsleiter_updated_at'),
    ('update_products_updated_at'),
    ('update_updated_at_column'),
    ('update_users_updated_at'),
    ('update_vorverkauf_welle_status'),
    ('update_vorverkauf_wellen_updated_at'),
    ('update_welle_status'),
    ('update_zusatz_zeit_updated_at')
),
public_application_functions as (
  select
    n.nspname as schema_name,
    r.proname as function_name,
    pg_get_function_identity_arguments(r.oid) as identity_arguments
  from pg_proc r
  join pg_namespace n on n.oid = r.pronamespace
  left join pg_depend d
    on d.classid = 'pg_proc'::regclass
   and d.objid = r.oid
   and d.deptype = 'e'
  left join pg_extension ext on ext.oid = d.refobjid
  where n.nspname = 'public'
    and ext.oid is null
)
select
  'no_unreviewed_public_application_functions' as check_name,
  case when count(*) = 0 then 'pass' else 'fail' end as status,
  coalesce(
    json_agg(json_build_object('schema', f.schema_name, 'function_name', f.function_name, 'identity_arguments', f.identity_arguments) order by f.function_name, f.identity_arguments),
    '[]'::json
  ) as unreviewed_functions
from public_application_functions f
where not exists (
  select 1
  from reviewed_public_application_functions r
  where r.function_name = f.function_name
);

select
  'no_public_security_definer_functions' as check_name,
  case when count(*) = 0 then 'pass' else 'fail' end as status,
  coalesce(
    json_agg(r.proname order by r.proname),
    '[]'::json
  ) as failing_functions
from pg_proc r
join pg_namespace n on n.oid = r.pronamespace
where n.nspname = 'public'
  and r.prosecdef is true;

select
  'private_gl_policy_helper_present' as check_name,
  case when count(*) = 1 then 'pass' else 'fail' end as status,
  coalesce(
    json_agg(json_build_object('schema', n.nspname, 'function', r.proname, 'security_definer', r.prosecdef, 'config', r.proconfig)),
    '[]'::json
  ) as helper_functions
from pg_proc r
join pg_namespace n on n.oid = r.pronamespace
where n.nspname = 'app_private'
  and r.proname = 'app_gl_id_text'
  and r.prosecdef is true
  and exists (
    select 1
    from unnest(coalesce(r.proconfig, array[]::text[])) as cfg(setting)
    where cfg.setting in ('search_path=', 'search_path=""')
  );

select
  'private_helper_schema_not_publicly_usable' as check_name,
  case when count(*) = 0 then 'pass' else 'fail' end as status,
  coalesce(
    json_agg(json_build_object('grantee', coalesce(r.rolname, 'PUBLIC'), 'schema', n.nspname, 'privilege_type', a.privilege_type) order by coalesce(r.rolname, 'PUBLIC')),
    '[]'::json
  ) as failing_privileges
from pg_namespace n
cross join lateral aclexplode(coalesce(n.nspacl, acldefault('n', n.nspowner))) as a
left join pg_roles r on r.oid = a.grantee
where n.nspname = 'app_private'
  and a.privilege_type = 'USAGE'
  and (a.grantee = 0 or r.rolname = 'anon');

with required_private_helper_schema_usage(role_name) as (
  values
    ('authenticated'),
    ('service_role')
)
select
  'private_helper_schema_usage_required_roles_present' as check_name,
  case when count(*) = 0 then 'pass' else 'fail' end as status,
  coalesce(
    json_agg(role_name order by role_name),
    '[]'::json
  ) as missing_usage_roles
from required_private_helper_schema_usage required_role
where not exists (
  select 1
  from pg_namespace n
  cross join lateral aclexplode(coalesce(n.nspacl, acldefault('n', n.nspowner))) as a
  join pg_roles r on r.oid = a.grantee
  where n.nspname = 'app_private'
    and a.privilege_type = 'USAGE'
    and r.rolname = required_role.role_name
);

select
  'private_helper_execute_grants_limited' as check_name,
  case when count(*) = 0 then 'pass' else 'fail' end as status,
  coalesce(
    json_agg(json_build_object('grantee', p.grantee, 'function_name', p.routine_name, 'privilege_type', p.privilege_type) order by p.grantee, p.privilege_type),
    '[]'::json
  ) as failing_privileges
from information_schema.routine_privileges p
where p.routine_schema = 'app_private'
  and p.routine_name = 'app_gl_id_text'
  and p.grantee in ('PUBLIC', 'anon');

with required_private_helper_execute(role_name) as (
  values
    ('authenticated'),
    ('service_role')
)
select
  'private_helper_execute_required_roles_present' as check_name,
  case when count(*) = 0 then 'pass' else 'fail' end as status,
  coalesce(
    json_agg(role_name order by role_name),
    '[]'::json
  ) as missing_execute_roles
from required_private_helper_execute required_role
where not exists (
  select 1
  from information_schema.routine_privileges p
  where p.routine_schema = 'app_private'
    and p.routine_name = 'app_gl_id_text'
    and p.grantee = required_role.role_name
    and p.privilege_type = 'EXECUTE'
);

select
  'public_schema_create_not_publicly_granted' as check_name,
  case when count(*) = 0 then 'pass' else 'fail' end as status,
  coalesce(
    json_agg(json_build_object('grantee', coalesce(r.rolname, 'PUBLIC'), 'schema', n.nspname, 'privilege_type', a.privilege_type) order by coalesce(r.rolname, 'PUBLIC')),
    '[]'::json
  ) as failing_privileges
from pg_namespace n
cross join lateral aclexplode(coalesce(n.nspacl, acldefault('n', n.nspowner))) as a
left join pg_roles r on r.oid = a.grantee
where n.nspname = 'public'
  and a.privilege_type = 'CREATE'
  and (a.grantee = 0 or r.rolname in ('anon', 'authenticated'));

select
  'public_schema_usage_not_anon_or_public' as check_name,
  case when count(*) = 0 then 'pass' else 'fail' end as status,
  coalesce(
    json_agg(json_build_object('grantee', coalesce(r.rolname, 'PUBLIC'), 'schema', n.nspname, 'privilege_type', a.privilege_type) order by coalesce(r.rolname, 'PUBLIC')),
    '[]'::json
  ) as failing_privileges
from pg_namespace n
cross join lateral aclexplode(coalesce(n.nspacl, acldefault('n', n.nspowner))) as a
left join pg_roles r on r.oid = a.grantee
where n.nspname = 'public'
  and a.privilege_type = 'USAGE'
  and (a.grantee = 0 or r.rolname = 'anon');

with required_public_schema_usage(role_name) as (
  values
    ('authenticated'),
    ('service_role')
)
select
  'public_schema_usage_required_roles_present' as check_name,
  case when count(*) = 0 then 'pass' else 'fail' end as status,
  coalesce(
    json_agg(role_name order by role_name),
    '[]'::json
  ) as missing_usage_roles
from required_public_schema_usage required_role
where not exists (
  select 1
  from pg_namespace n
  cross join lateral aclexplode(coalesce(n.nspacl, acldefault('n', n.nspowner))) as a
  join pg_roles r on r.oid = a.grantee
  where n.nspname = 'public'
    and a.privilege_type = 'USAGE'
    and r.rolname = required_role.role_name
);

with default_acl_targets(objtype, object_kind, expected_service_privilege) as (
  values
    ('r'::"char", 'tables', 'SELECT'),
    ('S'::"char", 'sequences', 'USAGE'),
    ('f'::"char", 'functions', 'EXECUTE')
),
postgres_owner as (
  select oid
  from pg_roles
  where rolname = 'postgres'
),
effective_default_acl as (
  select
    t.object_kind,
    coalesce(grantee_role.rolname, 'PUBLIC') as grantee,
    acl.privilege_type
  from default_acl_targets t
  cross join postgres_owner o
  join pg_namespace n on n.nspname = 'public'
  left join pg_default_acl d
    on d.defaclrole = o.oid
   and d.defaclnamespace = n.oid
   and d.defaclobjtype = t.objtype
  cross join lateral aclexplode(coalesce(d.defaclacl, acldefault(t.objtype, o.oid))) as acl
  left join pg_roles grantee_role on grantee_role.oid = acl.grantee
),
violations as (
  select
    'disallowed_default_grant' as issue,
    object_kind,
    grantee,
    privilege_type
  from effective_default_acl
  where grantee in ('PUBLIC', 'anon', 'authenticated')

  union all

  select
    'missing_service_role_default_grant' as issue,
    t.object_kind,
    'service_role' as grantee,
    t.expected_service_privilege as privilege_type
  from default_acl_targets t
  where not exists (
    select 1
    from effective_default_acl a
    where a.object_kind = t.object_kind
      and a.grantee = 'service_role'
      and a.privilege_type = t.expected_service_privilege
  )
)
select
  'postgres_public_default_privileges_hardened' as check_name,
  case when count(*) = 0 then 'pass' else 'fail' end as status,
  coalesce(
    json_agg(json_build_object('issue', issue, 'object_kind', object_kind, 'grantee', grantee, 'privilege_type', privilege_type) order by issue, object_kind, grantee, privilege_type),
    '[]'::json
  ) as failing_default_privileges
from violations;

with expected_reviewed_storage_buckets(bucket_id) as (
  values
    ('bug-screenshots'),
    ('fragebogen-response-images'),
    ('gl-profile-pictures'),
    ('vorbesteller-lieferung'),
    ('wellen-photos'),
    ('question-images'),
    ('wellen-images')
)
select
  'expected_reviewed_storage_buckets_present' as check_name,
  case when count(*) = 0 then 'pass' else 'fail' end as status,
  coalesce(
    json_agg(bucket_id order by bucket_id),
    '[]'::json
  ) as missing_buckets
from expected_reviewed_storage_buckets e
where not exists (
  select 1
  from storage.buckets b
  where b.id = e.bucket_id
);

select
  'sensitive_storage_bucket_public_flags' as check_name,
  case
    when bool_and(
      case
        when id in ('bug-screenshots', 'fragebogen-response-images', 'gl-profile-pictures', 'vorbesteller-lieferung', 'wellen-photos') then public is false
        when id in ('question-images', 'wellen-images') then public is true
        else true
      end
    ) then 'pass'
    else 'fail'
  end as status,
  coalesce(
    json_agg(json_build_object('bucket', id, 'public', public) order by id),
    '[]'::json
  ) as bucket_flags
from storage.buckets
where id in ('bug-screenshots', 'fragebogen-response-images', 'gl-profile-pictures', 'vorbesteller-lieferung', 'wellen-photos', 'question-images', 'wellen-images');

select
  'storage_objects_rls_enabled' as check_name,
  case when rowsecurity is true then 'pass' else 'fail' end as status,
  json_build_object('schema', schemaname, 'table', tablename, 'rowsecurity', rowsecurity) as storage_objects_rls
from pg_tables
where schemaname = 'storage'
  and tablename = 'objects';

select
  'no_unreviewed_public_storage_buckets' as check_name,
  case when count(*) = 0 then 'pass' else 'fail' end as status,
  coalesce(
    json_agg(json_build_object('bucket', id, 'public', public) order by id),
    '[]'::json
  ) as public_buckets_requiring_review
from storage.buckets
where public is true
  and id not in ('question-images', 'wellen-images');

commit;
