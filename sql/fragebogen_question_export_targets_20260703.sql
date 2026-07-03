-- Adds export-only target flags for Perfect Store/Fragebogen yes-no items.
-- These flags do not affect question rendering, response storage, or scoring
-- outside the dedicated distribution export.

begin;

set local lock_timeout = '5s';
set local statement_timeout = '2min';

alter table public.fb_questions
  add column if not exists distributionsziel boolean not null default false,
  add column if not exists qualitaetsziel boolean not null default false;

commit;

