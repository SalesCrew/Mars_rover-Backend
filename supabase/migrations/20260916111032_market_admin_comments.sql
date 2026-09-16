-- One editable, admin-only note per market. Do not put this on public.markets:
-- authenticated users currently have direct SELECT access to that table.
create table public.market_admin_comments (
  market_id varchar(50) primary key references public.markets(id) on delete cascade,
  comment text not null default '',
  updated_at timestamptz not null default now()
);

alter table public.market_admin_comments enable row level security;
revoke all on table public.market_admin_comments from public, anon, authenticated;
grant select, insert, update, delete on table public.market_admin_comments to service_role;
