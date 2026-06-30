# DSGVO Hardening Handoff

This is the concise index for the MarsPets+ DSGVO/RLS hardening package. It is not a replacement for `README_DSGVO_RLS.md`; use it to orient a reviewer or operator quickly.

## Local Files

- `dsgvo_rls_hardening.sql`: transactional Supabase RLS/grant/Storage hardening.
- `dsgvo_rls_preflight_metadata_snapshot.sql`: catalog-only rollback/preflight snapshot; runs in a read-only transaction with bounded local timeouts.
- `dsgvo_rls_verify_metadata.sql`: catalog-only verifier; runs in a read-only transaction with bounded local timeouts and currently emits 28 checks.
- `DSGVO_PRODUCTION_APPLY_CHECKLIST.md`: short production operator checklist.
- `DSGVO_PRODUCTION_EVIDENCE_TEMPLATE.md`: fill-in production change record template; keep business rows, personal data, photo paths, tokens, and service keys out of it.
- `../scripts/dsgvo-local-audit.ps1`: local static/build guardrail.

## Required Local Gate

Run from `backend/` before deployment or production SQL work:

```bash
npm run dsgvo:audit
```

This builds backend/frontend and checks route auth, reviewed route allowlist drift, frontend Supabase key/direct REST exposure, Storage privacy, signed URLs, verifier coverage, SQL safety, env-file tracking, mojibake/encoding corruption markers, bounded dynamic Supabase `.from(table)` helper calls, and documentation invariants.

## Production Order

1. Deploy the matching backend/frontend revision.
2. Take and identify a fresh production backup.
3. Run `dsgvo_rls_preflight_metadata_snapshot.sql` and save the full output.
4. Apply `dsgvo_rls_hardening.sql` as one transaction.
5. Run `dsgvo_rls_verify_metadata.sql`.
6. Confirm all 28 verifier checks return `status = 'pass'`.

## Hard Boundaries

- Do not query production business rows for smoke tests.
- Do not expose Supabase URL, anon, publishable, secret, or service-role keys in the frontend.
- Do not add direct browser calls to Supabase REST/Auth/Storage endpoints; the frontend must use the Express API boundary.
- Do not re-open direct `anon` or `authenticated` table grants for the browser.
- Do not add hardening SQL DML outside the reviewed `storage.buckets` metadata public/private flag updates.
- Keep GL market visibility unchanged through the Express API/UI.
- Keep backend route-level authorization as the live control because service-role access bypasses RLS.

## Still Not Complete Until

- The production backup exists.
- The matching backend/frontend deployment is live.
- The preflight snapshot is saved.
- The hardening SQL is applied.
- The metadata verifier output proves all 28 checks pass.
