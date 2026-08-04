begin;

alter table public.wellen_submissions
  add column if not exists submission_batch_id uuid;

comment on column public.wellen_submissions.submission_batch_id is
  'Client-generated idempotency key. Historical submissions remain NULL.';

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conrelid = 'public.wellen_submissions'::regclass
      and conname = 'wellen_submissions_submission_batch_item_key'
  ) then
    alter table public.wellen_submissions
      add constraint wellen_submissions_submission_batch_item_key
      unique (submission_batch_id, item_type, item_id);
  end if;
end
$$;

commit;
