-- Catalog-only preflight snapshot before applying dsgvo_rls_hardening.sql.
--
-- This script must not read business rows. It captures only privileges,
-- policies, functions, views, schemas, and Storage bucket flags needed for
-- precise review or rollback.

begin;
set transaction read only;
set local lock_timeout = '2s';
set local statement_timeout = '2min';

select
  'table_privileges' as snapshot_name,
  p.grantee,
  p.table_schema,
  p.table_name,
  p.privilege_type
from information_schema.table_privileges p
where p.table_schema in ('public', 'storage')
  and p.grantee in ('PUBLIC', 'anon', 'authenticated', 'service_role')
order by p.table_schema, p.table_name, p.grantee, p.privilege_type;

select
  'sequence_privileges' as snapshot_name,
  p.grantee,
  p.object_schema,
  p.object_name,
  p.privilege_type
from information_schema.usage_privileges p
where p.object_schema = 'public'
  and p.object_type = 'SEQUENCE'
  and p.grantee in ('PUBLIC', 'anon', 'authenticated', 'service_role')
order by p.object_name, p.grantee, p.privilege_type;

select
  'schema_privileges' as snapshot_name,
  n.nspname as schema_name,
  r.rolname as role_name,
  has_schema_privilege(r.rolname, n.oid, 'USAGE') as has_usage,
  has_schema_privilege(r.rolname, n.oid, 'CREATE') as has_create
from pg_namespace n
cross join pg_roles r
where n.nspname in ('public', 'app_private')
  and r.rolname in ('anon', 'authenticated', 'service_role')
order by n.nspname, r.rolname;

select
  'rls_flags' as snapshot_name,
  t.schemaname,
  t.tablename,
  t.rowsecurity
from pg_tables t
where t.schemaname = 'public'
order by t.tablename;

select
  'policies' as snapshot_name,
  p.schemaname,
  p.tablename,
  p.policyname,
  p.permissive,
  p.roles,
  p.cmd,
  p.qual,
  p.with_check
from pg_policies p
where p.schemaname in ('public', 'storage')
order by p.schemaname, p.tablename, p.policyname;

select
  'views' as snapshot_name,
  n.nspname as schema_name,
  c.relname as view_name,
  c.relkind,
  c.relacl
from pg_class c
join pg_namespace n on n.oid = c.relnamespace
where n.nspname = 'public'
  and c.relkind in ('v', 'm')
order by n.nspname, c.relname;

select
  'functions' as snapshot_name,
  n.nspname as schema_name,
  p.proname as function_name,
  pg_get_function_identity_arguments(p.oid) as identity_arguments,
  p.prosecdef as security_definer,
  p.proconfig as function_config,
  p.proacl
from pg_proc p
join pg_namespace n on n.oid = p.pronamespace
where n.nspname in ('public', 'app_private')
order by n.nspname, p.proname, identity_arguments;

with expected_reviewed_storage_buckets(bucket_id, expected_public) as (
  values
    ('bug-screenshots', false),
    ('fragebogen-response-images', false),
    ('gl-profile-pictures', false),
    ('vorbesteller-lieferung', false),
    ('vorverkauf-wellen', false),
    ('wellen-photos', false),
    ('question-images', true),
    ('wellen-images', true)
)
select
  'storage_buckets' as snapshot_name,
  e.bucket_id as expected_bucket_id,
  e.expected_public,
  b.id is not null as exists_before_apply,
  b.name,
  b.public,
  b.file_size_limit,
  b.allowed_mime_types
from expected_reviewed_storage_buckets e
left join storage.buckets b
  on b.id = e.bucket_id
order by e.bucket_id;

commit;
