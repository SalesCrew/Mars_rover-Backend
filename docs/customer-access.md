# Kundenzugang (local review, 2026-11-17)

Based on Coke Spark's profile popover and customer-access permission matrix. Rover uses a separate `customer` role and explicit `view` / `export` permissions. Export implies view; customer writes are never permitted.

## Local preview

Use the sibling frontend/backend worktrees. From backend:

```sh
node --import tsx scripts/customer-access-preview.mjs
```

Preview: http://127.0.0.1:5187 (API 4087). Synthetic accounts: `admin@example.test` and `bettina@example.test`, password `Preview-Only-2026!`. The admin opens the top-left profile menu, then **Kundenzugänge**. The seeded customer has dashboard, revenue-wave and photo-wave access; revenue Excel export is enabled. New accounts start without page grants.

The preview imports the real Auth/customer routers and applies the real migration to disposable PGlite through a loopback Auth/PostgREST adapter. It never imports the production server or starts its scheduled jobs. All accounts, prices, bookings and images are generated fixtures. No production environment files are copied into the worktrees. The adapter is test code, not an alternative authentication implementation for deployment.

## Enforcement and scope

- Every business request validates the Supabase access token with `getUser` and loads the current database role. Every customer request reloads active status and permissions. User metadata and browser state cannot authorize access.
- All legacy business routes reject customer roles. Dedicated customer endpoints check the specific page and action before querying data. Management is admin-only. Account/profile updates and permission audits are atomic; failed creation compensates the Auth record. Version checks reject stale edits. Email identity is immutable after creation.
- The migration adds restrictive RLS policies to existing public tables with RLS and storage.objects, in addition to the existing policies. Managed storage metadata tables have no permissive policies and remain denied without modifying their schema. The private, fixed-search-path role helper permits only internal admin/GL roles. Customers and users without an internal profile cannot bypass page permissions through direct Supabase REST access. New access/audit tables and the management RPC are service-only. Future exposed tables must keep this restriction.
- Dashboard/revenue/photo views include non-deleted waves whose date range includes the current Vienna calendar date; test GL bookings/photos are excluded. Revenue uses stored booking prices, marks missing prices and does not substitute current catalog prices. Other explicitly granted pages provide whitelisted read-only reports, rather than admin editing tools.
- Photos are proxied through an authenticated endpoint. Customer responses never expose storage URLs. Storage fetches accept only the configured project, fixed bucket and JPEG/PNG/WebP. Photo export has its own permission check, prepares the entire ZIP before responding and is bounded to 100 photos / 50 MB. Search applies to downloads as well as the UI. The UI paginates photos/tables; data reads paginate independently of the configured Supabase row cap and fail rather than return partial oversized totals.

## Verification

Backend: `node --import tsx --test tests/*.test.mjs`.
Frontend: `node --import ../backend/node_modules/tsx/dist/loader.mjs --test tests/*.test.mjs` (existing cross-repository tests require the local `frontend/backend -> ../backend` symlink).
Browser: `node --import ../backend/node_modules/tsx/dist/loader.mjs tests/customer-access-browser.mjs` from frontend. This uses an independent disposable fixture/API and port 5197, not the manual preview. Existing external font CSS requests are stubbed; unexpected external requests are blocked. Screenshots, synthetic Excel/ZIP and results are in frontend `outputs/customer-access-e2e/`.

Both builds, changed React component lint, the existing regression suites, permission/RLS tests and desktop/mobile browser flows were checked. The Auth adapter cannot certify hosted Supabase email deliverability or infrastructure configuration; no real email is sent.

## Deployment boundary

The production release was authorized on 2026-11-17. No customer account is provisioned as part of deployment. Before deployment: review the restrictive-policy change against the deployed schema, apply the migration before provisioning any customer, deploy the backend/frontend together, then create the actual account through the admin UI. Do not seed customer credentials or run the fixture adapter in production. Ordinary admin/GL behavior is retained; accounts without a valid internal profile are intentionally denied direct database access.
