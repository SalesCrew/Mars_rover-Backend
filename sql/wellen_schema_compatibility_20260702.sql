-- Wellen schema compatibility for the deployed Wellen API.
-- Adds nullable metadata columns only; no production data rows are inserted,
-- updated, deleted, or backfilled by this migration.

begin;

set local lock_timeout = '5s';
set local statement_timeout = '2min';

alter table public.wellen
  add column if not exists description text;

alter table public.wellen_displays
  add column if not exists description text,
  add column if not exists image_url text,
  add column if not exists ve integer,
  add column if not exists ve_size integer,
  add column if not exists vpe integer;

alter table public.wellen_kartonware
  add column if not exists description text,
  add column if not exists image_url text,
  add column if not exists ve integer,
  add column if not exists ve_size integer,
  add column if not exists vpe integer;

alter table public.wellen_einzelprodukte
  add column if not exists description text,
  add column if not exists image_url text,
  add column if not exists ve integer,
  add column if not exists ve_size integer,
  add column if not exists vpe integer;

alter table public.wellen_photo_tags
  add column if not exists tag_order integer;

alter table public.wellen_paletten
  add column if not exists description text,
  add column if not exists image_url text;

alter table public.wellen_paletten_products
  add column if not exists target_number integer,
  add column if not exists item_value numeric,
  add column if not exists description text,
  add column if not exists image_url text,
  add column if not exists picture_url text;

alter table public.wellen_schuetten
  add column if not exists description text,
  add column if not exists image_url text;

alter table public.wellen_schuetten_products
  add column if not exists target_number integer,
  add column if not exists item_value numeric,
  add column if not exists description text,
  add column if not exists image_url text,
  add column if not exists picture_url text;

alter table public.wellen_submissions
  add column if not exists photo_url text,
  add column if not exists parent_palette_id uuid;

commit;
