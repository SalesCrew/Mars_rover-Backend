-- Harden day tracking KM handling without deleting or rewriting historical rows.
-- Existing incomplete history remains readable. NOT VALID constraints still
-- protect new/updated rows after this migration is applied.

alter table public.fb_day_tracking
  add column if not exists km_stand_start_deferred boolean not null default false;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'fb_day_tracking_completed_requires_km'
      and conrelid = 'public.fb_day_tracking'::regclass
  ) then
    alter table public.fb_day_tracking
      add constraint fb_day_tracking_completed_requires_km
      check (
        created_at < timestamptz '2026-07-07 00:00:00+00'
        or
        status not in ('completed', 'force_closed')
        or (
          day_start_time is not null
          and day_end_time is not null
          and km_stand_start is not null
          and km_stand_end is not null
        )
      ) not valid;
  end if;

  if not exists (
    select 1
    from pg_constraint
    where conname = 'fb_day_tracking_active_missing_start_km_marked'
      and conrelid = 'public.fb_day_tracking'::regclass
  ) then
    alter table public.fb_day_tracking
      add constraint fb_day_tracking_active_missing_start_km_marked
      check (
        created_at < timestamptz '2026-07-07 00:00:00+00'
        or
        status <> 'active'
        or km_stand_start is not null
        or km_stand_start_deferred = true
      ) not valid;
  end if;

  if not exists (
    select 1
    from pg_constraint
    where conname = 'fb_day_tracking_km_values_valid'
      and conrelid = 'public.fb_day_tracking'::regclass
  ) then
    alter table public.fb_day_tracking
      add constraint fb_day_tracking_km_values_valid
      check (
        created_at < timestamptz '2026-07-07 00:00:00+00'
        or
        (km_stand_start is null or km_stand_start >= 0)
        and (km_stand_end is null or km_stand_end >= 0)
        and (
          km_stand_start is null
          or km_stand_end is null
          or km_stand_end >= km_stand_start
        )
      ) not valid;
  end if;
end $$;
