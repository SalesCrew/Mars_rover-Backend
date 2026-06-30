# DSGVO Production Evidence Template

Use one copy of this template for the controlled production hardening window. Do not paste business rows, user records, photo paths, tokens, service keys, or other personal data into this record.

## Change Window

- Date:
- Operator:
- Reviewer:
- Backend deployment id:
- Frontend deployment id:
- Supabase project reference:
- Maintenance window start:
- Maintenance window end:

## Pre-Apply Gates

- Production backup id:
- Backup timestamp:
- Backup restore owner:
- `npm run dsgvo:audit` commit/revision:
- `npm run dsgvo:audit` result:
- Backend health endpoint result:
- Frontend Supabase env/key exposure checked:
- Backend `SUPABASE_SERVICE_KEY` / `SUPABASE_SERVICE_ROLE_KEY` server-only checked:

## Preflight Metadata Snapshot

- Script: `dsgvo_rls_preflight_metadata_snapshot.sql`
- Started at:
- Finished at:
- Result saved at:
- Notes:

Paste or attach the full metadata-only output outside this template if it is long. Do not add business-data queries.

## Apply

- Script: `dsgvo_rls_hardening.sql`
- Started at:
- Finished at:
- Transaction result:
- Errors:
- Notes:

## Metadata Verifier

- Script: `dsgvo_rls_verify_metadata.sql`
- Started at:
- Finished at:
- Check count observed:
- Passing checks:
- Failing checks:
- Result saved at:

Expected result: exactly 31 checks and every row has `status = 'pass'`.

## Rollback Decision

- Rollback needed:
- Reason:
- Action taken:
- Backup restore id, if used:
- Metadata-only rollback notes, if backup restore was not used:

## Final Record

- Preflight output stored with change record:
- Verifier output stored with change record:
- Backup id stored with change record:
- Backend/frontend deployment ids stored with change record:
- No production business-data smoke tests run:
- Final decision:
