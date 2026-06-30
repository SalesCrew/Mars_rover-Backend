# DSGVO Production Apply Checklist

Use this only for the controlled production hardening window. Do not run business-data smoke tests or production data queries as part of this checklist.

## Stop Conditions

Stop before applying SQL if any item is false:

- A fresh production backup exists and restore ownership is clear.
- The deployed backend revision contains the route-auth, signed-URL, private bucket, generic error, and service-key validation changes from this hardening package.
- The frontend production environment has no Supabase URL, anon, publishable, secret, or service-role key.
- The frontend revision does not call Supabase REST/Auth/Storage endpoints directly; browser data access goes through the Express API.
- The backend production environment has `SUPABASE_SERVICE_KEY` or the legacy/common alias `SUPABASE_SERVICE_ROLE_KEY` only on the server/backend, and it is a Supabase `service_role` JWT or `sb_secret_` key, not an anon or publishable key.
- `npm run dsgvo:audit` passes locally against the exact revision being deployed, including the backend/frontend mojibake guard.
- The operator has access to save SQL result output for rollback evidence.
- A copy of `DSGVO_PRODUCTION_EVIDENCE_TEMPLATE.md` is ready for the change record.

## Production Sequence

1. Put the app in the agreed maintenance window.
2. Confirm the latest backend deployment is healthy using only normal health endpoints.
3. Run `dsgvo_rls_preflight_metadata_snapshot.sql`. It is metadata-only and runs in a read-only transaction with bounded local runtime timeouts.
4. Save the full preflight output with the backup/change record.
5. Apply `dsgvo_rls_hardening.sql` as one transaction. The script sets `lock_timeout` and `statement_timeout`; if it times out, stop and inspect/reschedule instead of retrying in a loop.
6. Run `dsgvo_rls_verify_metadata.sql`. It is metadata-only and runs in a read-only transaction with bounded local runtime timeouts.
7. Confirm the verifier emits 31 checks and every verifier result has `status = 'pass'`.
8. Keep the preflight output, verifier output, backend deployment id, frontend deployment id, and backup id together in the change record.
9. Complete the copied evidence template without adding business rows, personal data, photo paths, tokens, or service keys.

## If Verification Fails

- Do not run exploratory business-data queries.
- Keep the app behind authenticated backend routes.
- Do not "fix" verifier failures by opening `anon`/`authenticated` table grants or making private buckets public.
- Prefer restoring the production backup taken for this change.
- If backup restore is not possible, use only the saved preflight metadata output to restore the exact affected grants, policies, function privileges, view grants, schema privileges, or bucket privacy flags.

## Expected App Semantics After Apply

- GL users still see the same market/wellen/question definition data through the UI/API.
- GL users do not gain direct Supabase table access from the browser.
- Sensitive response, evidence, delivery, bug, and profile images stay private and are read through backend signed URLs.
- `question-images` and `wellen-images` remain the reviewed public asset buckets.
- Backend service-role access still works, but route-level auth remains the live authorization boundary.
