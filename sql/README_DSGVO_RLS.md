# DSGVO RLS Hardening

This folder contains the local database hardening script for the MarsPets+ production Supabase project:

- `dsgvo_rls_hardening.sql`
- `dsgvo_rls_preflight_metadata_snapshot.sql`
- `dsgvo_rls_verify_metadata.sql`
- `DSGVO_PRODUCTION_APPLY_CHECKLIST.md`
- `DSGVO_PRODUCTION_EVIDENCE_TEMPLATE.md`
- `../scripts/dsgvo-local-audit.ps1`

Do not run this casually against production. The app uses the backend service-role key for database access, so the live authorization boundary is the Express API. The SQL is defense-in-depth for Supabase Data API, view, function, and Storage exposure.

Supabase's current direction for API exposure is explicit grants plus RLS. This app does not need direct browser table access, so the hardening keeps `anon` and `authenticated` direct table grants closed and routes all app data through the authenticated Express API.

Current Supabase changelog review for this hardening pass: checked on 2026-06-30. The relevant breaking-change entry is `2026-04-28 - Tables not exposed to Data and GraphQL API automatically`, which reinforces the explicit-grant/RLS model. No recent changelog item requires keeping direct `anon` or `authenticated` table grants open for this app because the browser uses the Express API, not Supabase table access.

## What The Script Does

- Enables RLS on known public app tables.
- Revokes direct table and sequence privileges from `anon` and `authenticated`.
- Keeps full database access for `service_role`, which is used only by the backend.
- Does not insert, update, delete, truncate, merge, or copy rows in app business tables. The only intentional DML is Storage bucket metadata upsert/update for reviewed bucket privacy flags.
- Creates a locked `app_private.app_gl_id_text()` helper for RLS policy ownership checks that need the app's Auth-user-to-GL mapping. It is intentionally outside the exposed `public` schema.
- Creates owner-scoped RLS policies for GL-owned personal/activity tables.
- Keeps market read semantics documented as non-sensitive: GLs can still see all markets through the Express API/UI.
- Revokes direct access to known public views because views can bypass underlying table RLS unless made `security_invoker`.
- Revokes direct execute access to known public helper/RPC functions from `PUBLIC`, `anon`, and `authenticated`; backend `service_role` keeps execute where needed.
- Runs inside one transaction with a short `lock_timeout` and bounded `statement_timeout`, so production lock contention aborts the apply instead of hanging behind live traffic.
- Makes these Storage buckets private:
  - `bug-screenshots`
  - `fragebogen-response-images`
  - `gl-profile-pictures`
  - `vorbesteller-lieferung`
  - `wellen-photos`
- Intentionally leaves `wellen-images` public because current UI paths still use public URLs for wave/admin image assets, and leaves `question-images` public because Fragebogen question images are admin-managed UI assets returned as public URLs. New Wellen/Fotowelle evidence photo uploads use the private `wellen-photos` bucket and backend signed URLs; legacy evidence photo paths in `wellen-images` are still signed/read for backward compatibility. New GL profile-picture uploads use the private `gl-profile-pictures` bucket and backend signed URLs. The upload route can create that bucket as private if it is missing; this SQL still verifies and locks the bucket privacy flag during the controlled database hardening step.
- Redacts private image storage fields from custom Excel exports. Evidence/profile images should be viewed through authenticated backend signed URLs, not distributed as raw Storage object paths in spreadsheets.
- Enables and verifies RLS on `storage.objects`, removes old writable direct object policies for `anon`/`authenticated`, and then applies the restrictive Storage object policy, so bucket/object access remains policy-controlled.

## Apply Rules

For the actual production window, use `DSGVO_PRODUCTION_APPLY_CHECKLIST.md` as the short operator checklist.

1. Take a production backup first.
2. Use a short maintenance window.
3. Confirm the currently deployed backend includes the signed-URL, private GL profile-picture bucket, and route-auth changes before applying Storage privacy changes.
4. Confirm the frontend deployment is not configured with Supabase URL, anon, or service-role keys.
5. Confirm the backend deployment has `SUPABASE_SERVICE_KEY` or the legacy/common alias `SUPABASE_SERVICE_ROLE_KEY` only in backend/server environment variables, and that it is a Supabase service_role JWT or secret key, not an anon/publishable key.
6. Run `dsgvo_rls_preflight_metadata_snapshot.sql` and save the output with the backup/change record. It reads catalog metadata only, not business rows, and runs inside a read-only transaction with short local runtime timeouts so metadata reads do not wait indefinitely.
7. Apply the SQL as one transaction. The script sets a short lock timeout and bounded statement timeout; if it times out, stop and inspect/reschedule instead of retrying in a loop.
8. Run `dsgvo_rls_verify_metadata.sql` for catalog-only verification. It also runs inside a read-only transaction with short local runtime timeouts and should be rerun only after inspecting/rescheduling if it times out.
9. Do not run business-data smoke tests on production.
10. Use `DSGVO_PRODUCTION_EVIDENCE_TEMPLATE.md` to record backup id, deployment ids, preflight output location, apply result, and verifier result without pasting personal or business data.

## Local Pre-Apply Audit

Run this before deploying/applying the SQL. It checks local route order, SQL safety patterns, frontend Supabase key exposure, and builds without touching production:

```bash
npm run dsgvo:audit
```

For a faster static-only pass:

```bash
npm run dsgvo:audit:static
```

The audit also fails common UTF-8 mojibake markers in backend source, SQL, audit scripts, frontend source, and setup docs. This keeps German labels, export strings, and operational logs readable after scripted edits or Windows shell rewrites.

The audit also compares backend Supabase `.from('...')` database object references against the actual hardened table list and protected-view list in `dsgvo_rls_hardening.sql`. If a new table or view is added to the backend, the audit fails until the RLS/grant/view handling is reviewed and added to the SQL. Storage buckets are handled separately because bucket privacy is controlled through `storage.buckets` and Storage object policies.

Dynamic Supabase `.from(table)` helper calls are separately bounded. The audit permits only reviewed helper locations and requires every caller of `requireOwnedRowOrAdmin`, `fetchRowsByIdsInChunks`, `fetchValueMap`, `fetchRowsByIdChunks`, and `fetchAdminRowsByIdChunks` to pass literal table names. This prevents future user-controlled or computed table names from bypassing the static RLS/object coverage review.

The audit also parses local `database_schema*.sql` files for created tables, views, and functions. Any local schema object missing from the hardening SQL fails the audit, so historical schema files cannot quietly define an exposed public object that the production hardening plan ignores.

Local SQL files are checked for user/password seed operations such as `INSERT INTO users`, `DELETE FROM users`, `admin123`, `password123`, and embedded bcrypt hashes. Users and password changes must go through Supabase Auth and authenticated backend admin APIs, not ad-hoc SQL files.

Historical root-level `database_schema*.sql` files are also checked for legacy broad RLS snippets such as `auth.role()`, `FOR ALL USING (true)`, broad authenticated GL policies, or public `FOR SELECT USING (true)` examples. Production RLS/grant handling must live in this reviewed `backend/sql/dsgvo_rls_hardening.sql` flow.

It also verifies that the frontend installs the authenticated `fetch` wrapper before React renders and that no other frontend file overrides or bypasses `window.fetch`. This matters because the backend uses a service-role Supabase client behind Express route auth.

Backend `500` responses are checked for direct `error.message`/exception-object exposure. Backend logs are also checked so database, Storage, and provider exception objects are not printed raw; clients should receive generic failure messages.

Request logging must also avoid raw dynamic path identifiers. Global and route-level request logs sanitize UUID and long numeric path segments before printing, so operational logs keep route shape without retaining GL/user/response IDs from URLs.

Backend Supabase reads are also checked for wildcard `.select('*')` usage. Routes should request explicit columns so adding a future column to a table does not automatically widen API responses, exports, logs, or in-memory processing beyond the current purpose of the route.

Because service-role database access bypasses RLS, read and write routes are checked for explicit authorization review. Routes either need an admin/self/owner middleware, live in a router mounted with `requireAdmin`, or be listed in the audit as an intentionally reviewed authenticated workflow. Those reviewed route allowlists are drift-checked against current route definitions, so stale allowlist entries fail the audit. For GL-owned data, the handler must derive the GL/user id from `req.user` or perform an equivalent owner check. Catalog and market/wave definition reads can remain authenticated-only where the current UI intentionally exposes them to GLs.

The route audit also verifies that every `router.get/post/put/patch/delete` definition is written in a shape the authorization parser can see. If a future endpoint uses a different handler pattern, the audit fails until the parser or route review list is updated. This prevents new service-role routes from silently bypassing the local authorization review.

The audit also guards the distinction between market master data and GL personal master data. GL users intentionally keep current UI access to all markets. GL profile/contact records, profile pictures, password changes, and chat context that includes GL profile fields must remain admin-only or self-scoped.

The local schema files are parsed beyond table names: RLS policy column assumptions such as `gebietsleiter_id`, parent foreign keys, `is_deleted`, `is_active`, and GL mapping columns must exist in the local schema references. The audit also checks that `market_visits.source` values written by backend routes are represented in `database_schema_market_visits.sql`, so route behavior and schema constraints do not drift apart.

Every backend Supabase Storage bucket reference is resolved from code, including `*_BUCKET` constants, and must appear in both `dsgvo_rls_hardening.sql` and `dsgvo_rls_verify_metadata.sql`. Adding a new bucket therefore requires an explicit public/private review and metadata verifier coverage before the local audit passes.

The frontend is also checked for direct Supabase REST/Auth/Storage access. Browser code must not contain Supabase URLs, `/rest/v1`, `/auth/v1`, `/storage/v1`, `apikey` headers, `@supabase/supabase-js`, or browser Supabase environment variables. The frontend must use the Express API boundary.

Private Storage reads must use short-lived signed URLs. The audit checks every `createSignedUrl(...)` call and requires a reviewed `*_SIGNED_URL_SECONDS` constant or a safe inline numeric expiry between 1 and 3600 seconds. It also checks that signed URL bucket selection is locally reviewable and limited to reviewed media buckets. Current private image URLs are capped at one hour.

Public Storage URLs are intentionally limited to reviewed public asset buckets. The audit checks every backend `getPublicUrl(...)` call and only permits `question-images` and `wellen-images`; private evidence, response, delivery, bug, and profile buckets must use signed URLs instead.

Runtime bucket creation/update calls are also checked. If backend code creates or updates a Storage bucket, the options must keep `public: false`, use a reviewed MIME allowlist, and set a reviewed file size limit. This prevents sensitive upload buckets from becoming public or unbounded through route-level helper code.

The backend Supabase SDK dependency must stay pinned to an exact version in both `package.json` and `package-lock.json`. This follows Supabase's package security guidance and avoids unreviewed security-sensitive SDK changes during install/deploy.

The backend Supabase client config also fails startup when neither `SUPABASE_SERVICE_KEY` nor `SUPABASE_SERVICE_ROLE_KEY` is configured, or when the configured key appears to be an anon/publishable key. The local audit checks that this guard stays in place.

The hardening SQL DML boundary is intentionally narrow. The audit fails any `INSERT`, `UPDATE`, `DELETE`, `TRUNCATE`, `MERGE`, `COPY`, or dynamic DML in `dsgvo_rls_hardening.sql` except the reviewed `storage.buckets` metadata insert/update statements used to lock bucket public/private flags.

The HTTP boundary is also checked because the browser talks only to the Express API. CORS must stay on an explicit allowlist, wildcard/credentialed CORS must not be enabled, localhost origins must stay development-only unless `ALLOW_LOCAL_CORS=true` is explicitly set, `x-powered-by` must stay disabled, and basic no-sniff/frame/referrer/cache headers must remain set.

Unauthenticated auth routes are checked separately because `/api/auth` is mounted before global business-data auth. Login and refresh must remain rate-limited, caller IPs, login identifiers, and refresh tokens must only be used as hashes in the limiter key, the in-memory limiter bucket count must remain bounded, and login failures must not distinguish between missing accounts and wrong passwords.

## Route-Level Auth Assumptions

The backend uses the Supabase service-role key, which bypasses RLS. This is acceptable only while route-level authentication and authorization remain true:

- `src/index.ts` mounts `/api/auth` before the global business-data auth middleware.
- `src/index.ts` then mounts `app.use('/api', authenticateToken)` before every business-data router.
- Health routes (`/`, `/health`, `/api/health`) stay unauthenticated and must not return business data.
- Admin-only routers are mounted with `requireAdmin` where the whole router is administrative (`action-history`, `activities`, `export`, `products-update`).
- Sensitive row-level operations use `requireSelfOrAdmin`, `requireOwnedRowOrAdmin`, or `getAuthenticatedGlId(req.user)` so GL users cannot choose another GL by request parameter/body.
- GL market visibility is intentionally unchanged: GLs may see all markets through the API/UI because market master data is treated as non-sensitive for this app.
- GL personal profile/contact data is separate from market master data and must stay admin/self scoped.
- Account lifecycle changes must invalidate route-level authorization even if an old access token still exists: admin deletion removes the `users` profile used by `authenticateToken`, and GL deactivation is blocked by the active `gebietsleiter` profile check.
- GL deactivation must pseudonymize personal profile/contact fields while keeping the GL id for historical reporting joins. This keeps dashboards/exports stable without retaining inactive-account contact data.
- CORS remains allowlist-based, does not use wildcard or credentialed browser access, and does not keep localhost origins open in production unless `ALLOW_LOCAL_CORS=true` is explicitly set.
- `/api/auth/login` and `/api/auth/refresh` remain rate-limited before calling Supabase Auth, and login errors remain generic.

Before applying the SQL to production, verify those assumptions against the deployed backend revision, not just the local working tree.

## Rollback Guidance

Prefer restoring the production backup taken immediately before applying this SQL.

Do not use a generic rollback script for this change unless the exact previous grants, policies, function privileges, view grants, and bucket privacy flags have been captured first. Re-opening broad `anon` or `authenticated` access by hand can accidentally create a bigger data exposure than the original issue.

Before applying, run `dsgvo_rls_preflight_metadata_snapshot.sql` and save its output. It captures these metadata snapshots so a rollback can be precise without reading business data. The preflight and verifier scripts run in read-only transactions and set local `lock_timeout` and `statement_timeout` for bounded metadata-only execution:

```sql
select *
from information_schema.table_privileges
where table_schema = 'public'
  and grantee in ('anon', 'authenticated', 'service_role')
order by table_name, grantee, privilege_type;

select *
from pg_policies
where schemaname = 'public'
order by tablename, policyname;

select id, public
from storage.buckets
where id in ('bug-screenshots', 'fragebogen-response-images', 'gl-profile-pictures', 'vorbesteller-lieferung', 'wellen-photos', 'question-images', 'wellen-images')
order by id;
```

If rollback is needed because the backend fails after deployment, restore from backup first. If backup restore is impossible, only reapply the exact captured metadata state for the affected object and keep the production API behind authenticated route checks.

## Catalog-Only Verification

`dsgvo_rls_verify_metadata.sql` currently emits 31 catalog-only checks. These read metadata, not business rows:

```sql
select schemaname, tablename, rowsecurity
from pg_tables
where schemaname = 'public'
order by tablename;

select grantee, table_schema, table_name, privilege_type
from information_schema.table_privileges
where table_schema = 'public'
  and grantee in ('anon', 'authenticated', 'service_role')
order by table_name, grantee, privilege_type;

select schemaname, tablename, policyname, roles, cmd
from pg_policies
where schemaname = 'public'
order by tablename, policyname;

select id, public
from storage.buckets
where id in ('bug-screenshots', 'fragebogen-response-images', 'gl-profile-pictures', 'vorbesteller-lieferung', 'wellen-photos', 'question-images', 'wellen-images')
order by id;
```

Expected high-level result:

- `anon` and `authenticated` have no direct table privileges on app tables.
- `anon` and `authenticated` have no direct privileges on public sequences.
- `service_role` keeps read/write access on app tables, public sequence access, plus the reviewed view/function access needed by the authenticated Express backend after direct browser roles are closed.
- RLS is enabled on app tables.
- No extra public tables, views, materialized views, or non-extension application functions exist outside the reviewed app lists; known extension metadata (`spatial_ref_sys`, `geography_columns`, `geometry_columns`) and extension-owned functions are excluded.
- Owner policies exist for personal/activity tables.
- Every expected `dsgvo_*` table and Storage policy from the hardening script is present where its table exists.
- Expected `dsgvo_*` policies keep the reviewed command, `authenticated` role scope, and restricted predicates; broad `USING (true)` is only allowed on reviewed non-sensitive catalog/definition reads.
- The local audit checks that every expected policy also has a matching verifier shape entry, preventing policy-presence checks and policy-shape checks from drifting apart.
- The metadata verifier uses only deterministic `pass`/`fail` statuses; missing expected policies and unreviewed public buckets fail the production gate.
- No `SECURITY DEFINER` functions exist in `public`; the GL policy helper exists only in `app_private`.
- The private GL policy helper keeps a locked empty `search_path`, is not executable by `PUBLIC` or `anon`, its private schema is not usable by `PUBLIC` or `anon`, and `authenticated`/`service_role` retain the required schema usage and helper execute privileges.
- The `public` schema does not grant `CREATE` to `PUBLIC`, `anon`, or `authenticated`.
- The `public` schema does not grant `USAGE` to `PUBLIC` or `anon`; `authenticated` and `service_role` retain explicit schema usage for reviewed policies/backend access.
- Future `postgres`-owned objects in `public` do not default-grant table/sequence/function access to `PUBLIC`, `anon`, or `authenticated`, while `service_role` keeps the expected default access for backend-owned migrations.
- Every reviewed Storage bucket exists before public/private flags are evaluated.
- The sensitive photo buckets are private.
- `storage.objects` has RLS enabled.
- No direct writable `storage.objects` policies remain for `anon` or `authenticated`, except the reviewed restrictive bucket-boundary policy.
- The reviewed Storage object policy remains `RESTRICTIVE`, scoped only to `anon`/`authenticated`, and bounded to `question-images`/`wellen-images`; private photo/evidence buckets must not appear in its `USING` or `WITH CHECK` predicate.
- `wellen-photos` is private for new Wellen/Fotowelle evidence photos.
- `wellen-images` and `question-images` remain the only reviewed public Storage buckets for UI/admin image assets and legacy compatibility until all of those UI/API paths use signed URLs; any other public bucket is a verifier failure.

## References

- GDPR Article 25 and 32: https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A32016R0679
- Supabase changelog: https://supabase.com/changelog.md
- Supabase changelog entry, 2026-04-28, tables not exposed automatically: https://supabase.com/changelog/45329-breaking-change-tables-not-exposed-to-data-and-graphql-api-automatically
- Supabase RLS: https://supabase.com/docs/guides/database/postgres/row-level-security
- Supabase API security: https://supabase.com/docs/guides/api/securing-your-api
