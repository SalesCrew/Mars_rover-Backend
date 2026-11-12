-- Additive only: installing this migration never revalues existing bookings.
create table public.quarter_price_imports (
  id uuid primary key default gen_random_uuid(),
  year integer not null check (year between 2000 and 2100),
  quarter integer not null check (quarter between 1 and 4),
  filename text not null,
  sha256 text not null check (sha256 ~ '^[0-9a-f]{64}$'),
  actor_id uuid not null,
  created_at timestamptz not null default now()
);
create table public.quarter_price_catalog (
  id uuid primary key default gen_random_uuid(),
  year integer not null check (year between 2000 and 2100),
  quarter integer not null check (quarter between 1 and 4),
  department text not null check (department in ('food','pets')),
  item_type text not null check (item_type in ('display','kartonware','einzelprodukt')),
  artikel_nr text not null check (length(trim(artikel_nr)) between 1 and 80),
  catalog_key text generated always as (department || ':' || item_type || ':' || artikel_nr) stored,
  name text not null check (length(trim(name)) between 1 and 500),
  units_per_ve integer not null check (units_per_ve between 1 and 100000),
  price_per_order numeric(16,4) not null check (price_per_order >= 0 and price_per_order < 100000000),
  import_id uuid not null references public.quarter_price_imports(id),
  source_cell text not null,
  scope_note text not null default '',
  unique (year,quarter,catalog_key)
);
create index quarter_price_catalog_article_idx on public.quarter_price_catalog(year,quarter,item_type,artikel_nr);
create index quarter_price_catalog_import_idx on public.quarter_price_catalog(import_id);
alter table public.quarter_price_imports enable row level security;
alter table public.quarter_price_catalog enable row level security;
revoke all on public.quarter_price_imports,public.quarter_price_catalog from public,anon,authenticated,service_role;
grant select,insert on public.quarter_price_imports,public.quarter_price_catalog to service_role;

alter table public.wellen add column if not exists catalog_pricing_required boolean not null default false;
alter table public.wellen_displays add column if not exists artikel_nr text;
alter table public.wellen_kartonware add column if not exists artikel_nr text;
alter table public.wellen_displays add column if not exists catalog_key text;
alter table public.wellen_kartonware add column if not exists catalog_key text;
alter table public.wellen_einzelprodukte add column if not exists catalog_key text;
alter table public.wellen_displays add column if not exists catalog_ve_size integer;
alter table public.wellen_kartonware add column if not exists catalog_ve_size integer;
alter table public.wellen_einzelprodukte add column if not exists catalog_ve_size integer;
alter table public.wellen_displays add column if not exists catalog_order_price numeric;
alter table public.wellen_kartonware add column if not exists catalog_order_price numeric;
alter table public.wellen_einzelprodukte add column if not exists catalog_order_price numeric;

alter table public.wellen_submissions add column if not exists artikel_nr text;
alter table public.wellen_submissions add column if not exists order_quantity numeric;
alter table public.wellen_submissions add column if not exists order_unit text;
alter table public.wellen_submissions add column if not exists order_unit_price numeric;
alter table public.wellen_submissions add column if not exists ve_size integer;
alter table public.wellen_submissions add column if not exists price_year integer;
alter table public.wellen_submissions add column if not exists price_quarter integer;
alter table public.wellen_submissions add column if not exists price_source_id uuid references public.quarter_price_catalog(id);
create index if not exists wellen_submissions_quarter_price_idx on public.wellen_submissions(price_source_id) where price_source_id is not null;

-- Immutable quarterly evidence. Re-importing identical prices is safe; a conflicting
-- price/VE for the same article and quarter must be reviewed, never silently replaced.
create or replace function public.import_quarter_prices(
  p_year integer,p_quarter integer,p_filename text,p_sha256 text,p_rows jsonb,p_actor uuid
) returns jsonb language plpgsql security invoker set search_path='' as $$
declare v_import uuid; v_count integer; v_existing integer;
begin
  if p_actor is null or p_year is null or p_quarter is null or p_filename is null or p_sha256 is null or p_rows is null or p_year not between 2000 and 2100 or p_quarter not between 1 and 4
    or p_sha256 !~ '^[0-9a-f]{64}$' or length(trim(p_filename)) not between 1 and 500
    or jsonb_typeof(p_rows) <> 'array' or jsonb_array_length(p_rows) not between 1 and 5000 then
    raise exception 'Ungültige Quartalspreisliste.' using errcode='22023';
  end if;
  if exists(select 1 from jsonb_to_recordset(p_rows) r(department text,item_type text,artikel_nr text,name text,units_per_ve integer,price_per_order numeric,source_cell text)
    where r.department is null or r.department not in ('food','pets') or r.item_type is null or r.item_type not in ('display','kartonware','einzelprodukt')
      or r.artikel_nr is null or length(trim(r.artikel_nr)) not between 1 and 80 or r.name is null or length(trim(r.name)) not between 1 and 500
      or r.units_per_ve is null or r.units_per_ve not between 1 and 100000 or r.price_per_order is null or r.price_per_order<0 or r.price_per_order>=100000000
      or r.price_per_order::text in ('NaN','Infinity','-Infinity') or coalesce(r.source_cell,'')='') then
    raise exception 'Artikelnummer, VE, Preis und Quellenzeile sind erforderlich.' using errcode='22023';
  end if;
  if (select count(*) from jsonb_to_recordset(p_rows) r(department text,item_type text,artikel_nr text)) <>
     (select count(distinct (r.department,r.item_type,r.artikel_nr)) from jsonb_to_recordset(p_rows) r(department text,item_type text,artikel_nr text)) then
    raise exception 'Doppelte Artikelnummer in derselben Artikelart.' using errcode='22023';
  end if;
  perform pg_advisory_xact_lock(hashtext('quarter-prices:'||p_year||':'||p_quarter));
  if exists(select 1 from jsonb_to_recordset(p_rows) r(department text,item_type text,artikel_nr text,units_per_ve integer,price_per_order numeric)
    join public.quarter_price_catalog c on c.year=p_year and c.quarter=p_quarter and c.department=r.department and c.item_type=r.item_type and c.artikel_nr=r.artikel_nr
    where c.units_per_ve<>r.units_per_ve or c.price_per_order<>r.price_per_order) then
    raise exception 'Für dieses Quartal ist bereits ein anderer Preis oder eine andere VE hinterlegt. Bitte prüfen.' using errcode='22023';
  end if;
  insert into public.quarter_price_imports(year,quarter,filename,sha256,actor_id) values(p_year,p_quarter,p_filename,p_sha256,p_actor) returning id into v_import;
  insert into public.quarter_price_catalog(year,quarter,department,item_type,artikel_nr,name,units_per_ve,price_per_order,import_id,source_cell,scope_note)
    select p_year,p_quarter,r.department,r.item_type,r.artikel_nr,r.name,r.units_per_ve,r.price_per_order,v_import,r.source_cell,coalesce(r.scope_note,'')
    from jsonb_to_recordset(p_rows) r(department text,item_type text,artikel_nr text,name text,units_per_ve integer,price_per_order numeric,source_cell text,scope_note text)
    on conflict(year,quarter,catalog_key) do nothing;
  get diagnostics v_count=row_count;
  return jsonb_build_object('importId',v_import,'inserted',v_count,'unchanged',jsonb_array_length(p_rows)-v_count);
end; $$;
revoke all on function public.import_quarter_prices(integer,integer,text,text,jsonb,uuid) from public,anon,authenticated;
grant execute on function public.import_quarter_prices(integer,integer,text,text,jsonb,uuid) to service_role;

-- Only INSERT: historical rows are untouched. Never trust a client-supplied price.
-- quantity stays in pieces for individual products, preserving all existing progress
-- and goal calculations. The actual ordered VE quantity/price is stored separately.
create or replace function public.snapshot_wave_quarter_price() returns trigger
language plpgsql security invoker set search_path='' as $$
declare v_key text; v_required boolean; v_price public.quarter_price_catalog; v_divisor integer; v_date timestamp;
begin
  if new.item_type not in ('display','kartonware','einzelprodukt') then return new; end if;
  select catalog_pricing_required into v_required from public.wellen where id=new.welle_id;
  if new.item_type='display' then select catalog_key into v_key from public.wellen_displays where id=new.item_id and welle_id=new.welle_id;
  elsif new.item_type='kartonware' then select catalog_key into v_key from public.wellen_kartonware where id=new.item_id and welle_id=new.welle_id;
  else select catalog_key into v_key from public.wellen_einzelprodukte where id=new.item_id and welle_id=new.welle_id;
  end if;
  if v_key is null then
    if v_required then raise exception 'Artikel ist nicht mit dem Quartalskatalog verknüpft. Bitte Welle neu laden.' using errcode='22023'; end if;
    -- Legacy orders remain possible; missing historic evidence is not fabricated.
    return new;
  end if;
  v_date := statement_timestamp() at time zone 'Europe/Vienna';
  select * into v_price from public.quarter_price_catalog where catalog_key=v_key and year=extract(year from v_date)::integer and quarter=extract(quarter from v_date)::integer;
  if not found or v_price.item_type<>new.item_type then raise exception 'Für den Artikel fehlt der freigegebene Preis im aktuellen Quartal. Es wurde nichts gespeichert.' using errcode='22023'; end if;
  v_divisor := case when new.item_type='einzelprodukt' then v_price.units_per_ve else 1 end;
  if new.quantity is null or new.quantity<=0 or new.quantity % v_divisor<>0 then
    raise exception 'Einzelprodukte können nur in ganzen VEs bestellt werden. Bitte die Welle neu laden.' using errcode='22023';
  end if;
  new.value_per_unit:=v_price.price_per_order/v_divisor;
  new.artikel_nr:=v_price.artikel_nr;
  new.order_quantity:=new.quantity/v_divisor;
  new.order_unit:=case when new.item_type='display' then 'Display' else 'VE' end;
  new.order_unit_price:=v_price.price_per_order;
  new.ve_size:=v_price.units_per_ve;
  new.price_year:=v_price.year; new.price_quarter:=v_price.quarter; new.price_source_id:=v_price.id;
  return new;
end; $$;
revoke all on function public.snapshot_wave_quarter_price() from public,anon,authenticated;
create trigger snapshot_wave_quarter_price before insert on public.wellen_submissions
  for each row execute function public.snapshot_wave_quarter_price();

create or replace function public.guard_wave_quarter_price_update() returns trigger
language plpgsql security invoker set search_path='' as $$
declare v_price public.quarter_price_catalog; v_divisor integer;
begin
  if new.price_source_id is null then
    if old.price_source_id is not null then raise exception 'Gespeicherter Quartalspreis darf nicht entfernt werden.' using errcode='22023'; end if;
    return new;
  end if;
  select * into v_price from public.quarter_price_catalog where id=new.price_source_id;
  v_divisor:=case when new.item_type='einzelprodukt' then v_price.units_per_ve else 1 end;
  if not found or v_price.item_type<>new.item_type or new.value_per_unit is distinct from v_price.price_per_order/v_divisor then
    raise exception 'Buchungspreise müssen dem belegten Quartalspreis entsprechen.' using errcode='22023';
  end if;
  if new.quantity<=0 or (new.order_unit<>'Stück' and new.quantity % v_divisor<>0) then
    raise exception 'Bitte nur ganze VEs eingeben.' using errcode='22023';
  end if;
  new.order_quantity:=case when new.order_unit='Stück' then new.quantity else new.quantity/v_divisor end;
  new.order_unit_price:=case when new.order_unit='Stück' then new.value_per_unit else v_price.price_per_order end;
  new.artikel_nr:=v_price.artikel_nr;new.ve_size:=v_price.units_per_ve;new.price_year:=v_price.year;new.price_quarter:=v_price.quarter;
  return new;
end; $$;
revoke all on function public.guard_wave_quarter_price_update() from public,anon,authenticated;
create trigger guard_wave_quarter_price_update before update on public.wellen_submissions
  for each row execute function public.guard_wave_quarter_price_update();
grant execute on function public.snapshot_wave_quarter_price(),public.guard_wave_quarter_price_update() to service_role;

-- Historical pricing is a separate explicit, audited transaction. Match only the
-- exact saved article/type/department key, never a fuzzy name or a current price.
create table public.wave_quarter_price_audit (
  id uuid primary key default gen_random_uuid(),actor_id uuid not null,created_at timestamptz not null default now(),changes jsonb not null
);
alter table public.wave_quarter_price_audit enable row level security;
revoke all on public.wave_quarter_price_audit from public,anon,authenticated,service_role;
grant select,insert on public.wave_quarter_price_audit to service_role;

create or replace function public.normalized_quarter_article_name(p_name text) returns text
language sql immutable security invoker set search_path='' as $$
  select trim(regexp_replace(replace(replace(replace(replace(lower(coalesce(p_name,'')),'™',''),'®',''),'’',chr(39)),'‘',chr(39)),'[[:space:]]+',' ','g'));
$$;
revoke all on function public.normalized_quarter_article_name(text) from public,anon,authenticated;
grant execute on function public.normalized_quarter_article_name(text) to service_role;

create or replace function public.reconcile_wave_quarter_prices(p_apply boolean default false,p_token text default null,p_actor uuid default null)
returns jsonb language plpgsql security invoker set search_path='' as $$
declare v_snapshot jsonb; v_token text; v_changes jsonb; v_updated integer:=0;
begin
  if p_apply and (p_actor is null or p_token is null) then raise exception 'Bestätigung erforderlich.' using errcode='22023'; end if;
  if p_apply then
    perform id from public.wellen_submissions order by id for update;
    perform id from public.wellen_displays order by id for share;
    perform id from public.wellen_kartonware order by id for share;
    perform id from public.wellen_einzelprodukte order by id for share;
  end if;
  with items as (
    select id,welle_id,'display'::text as item_type,catalog_key,artikel_nr,name from public.wellen_displays
    union all select id,welle_id,'kartonware',catalog_key,artikel_nr,name from public.wellen_kartonware
    union all select id,welle_id,'einzelprodukt',catalog_key,artikel_nr,name from public.wellen_einzelprodukte
  ), candidates as (
    select s.*,w.name as wave_name,coalesce(i.name,p.name,'Unbekannt') as article_name,coalesce(s.artikel_nr,i.artikel_nr,p.artikel_nr) as known_article,c.id as source_id,c.artikel_nr as source_article,
      coalesce(s.price_year,extract(year from s.created_at at time zone 'Europe/Vienna')::integer) as source_year,
      coalesce(s.price_quarter,extract(quarter from s.created_at at time zone 'Europe/Vienna')::integer) as source_quarter,c.units_per_ve,c.price_per_order,
      case when s.item_type='einzelprodukt' then c.units_per_ve else 1 end as divisor,
      case when i.catalog_key is null and coalesce(s.artikel_nr,i.artikel_nr,p.artikel_nr) is null then 'Artikelzuordnung fehlt' when c.id is null then 'Quartalspreis oder eindeutige Artikelzuordnung fehlt' else null end as reason
    from public.wellen_submissions s
    join public.wellen w on w.id=s.welle_id and not coalesce(w.is_deleted,false)
    left join items i on i.id=s.item_id and i.welle_id=s.welle_id and i.item_type=s.item_type
    left join public.products p on s.item_type='einzelprodukt' and p.id=s.item_id::text
    left join lateral (
      select case when count(*)=1 then min(candidate.id::text)::uuid else null end as id
      from public.quarter_price_catalog candidate
      where candidate.year=coalesce(s.price_year,extract(year from s.created_at at time zone 'Europe/Vienna')::integer)
        and candidate.quarter=coalesce(s.price_quarter,extract(quarter from s.created_at at time zone 'Europe/Vienna')::integer)
        and candidate.item_type=s.item_type
        and case
          when s.price_source_id is not null then candidate.id=s.price_source_id
          when s.artikel_nr is not null then candidate.artikel_nr=s.artikel_nr
          when i.catalog_key is not null then candidate.catalog_key=i.catalog_key
          else candidate.artikel_nr=coalesce(i.artikel_nr,p.artikel_nr)
            and public.normalized_quarter_article_name(candidate.name)=public.normalized_quarter_article_name(coalesce(i.name,p.name))
        end
    ) match on true
    left join public.quarter_price_catalog c on c.id=match.id
    where s.item_type in ('display','kartonware','einzelprodukt')
  ) select coalesce(jsonb_agg(jsonb_build_object('id',id,'welleId',welle_id,'quantity',quantity,'oldPrice',value_per_unit,
      'newPrice',price_per_order/divisor,'article',coalesce(source_article,known_article),'name',article_name,'waveName',wave_name,'year',source_year,'quarter',source_quarter,'sourceId',source_id,
      'veSize',units_per_ve,'orderQuantity',case when divisor is not null and quantity % divisor=0 then quantity/divisor else quantity end,
      'orderUnit',case when item_type='display' then 'Display' when divisor is not null and quantity % divisor=0 then 'VE' else 'Stück' end,
      'orderPrice',case when divisor is not null and quantity % divisor=0 then price_per_order else price_per_order/divisor end,
      'reason',reason,'oldSource',price_source_id,'oldArticle',artikel_nr,'oldOrderQuantity',order_quantity,'oldOrderPrice',order_unit_price,
      'oldUnit',order_unit,'oldVeSize',ve_size,'oldYear',price_year,'oldQuarter',price_quarter) order by id),'[]'::jsonb)
    into v_snapshot from candidates;
  v_token:=encode(sha256(convert_to(v_snapshot::text,'UTF8')),'hex');
  if p_apply and p_token is distinct from v_token then raise exception 'Vorschau ist veraltet. Bitte neu prüfen.' using errcode='40001'; end if;
  select coalesce(jsonb_agg(e),'[]'::jsonb) into v_changes from jsonb_array_elements(v_snapshot)e where e->>'reason' is null
    and (e->'oldPrice' is distinct from e->'newPrice' or e->'oldSource' is distinct from e->'sourceId' or e->'oldArticle' is distinct from e->'article'
      or e->'oldOrderQuantity' is distinct from e->'orderQuantity' or e->'oldOrderPrice' is distinct from e->'orderPrice'
      or e->'oldUnit' is distinct from e->'orderUnit' or e->'oldVeSize' is distinct from e->'veSize'
      or e->'oldYear' is distinct from e->'year' or e->'oldQuarter' is distinct from e->'quarter');
  if p_apply and jsonb_array_length(v_changes)>0 then
    update public.wellen_submissions s set value_per_unit=(e->>'newPrice')::numeric,artikel_nr=e->>'article',
      order_quantity=(e->>'orderQuantity')::numeric,order_unit=e->>'orderUnit',order_unit_price=(e->>'orderPrice')::numeric,
      ve_size=(e->>'veSize')::integer,price_year=(e->>'year')::integer,price_quarter=(e->>'quarter')::integer,price_source_id=(e->>'sourceId')::uuid
      from jsonb_array_elements(v_changes)e where s.id=(e->>'id')::uuid;
    get diagnostics v_updated=row_count;
    if v_updated<>jsonb_array_length(v_changes) then raise exception 'Abgleich unvollständig.'; end if;
    insert into public.wave_quarter_price_audit(actor_id,changes) values(p_actor,v_changes);
  end if;
  return jsonb_build_object('token',v_token,'count',jsonb_array_length(v_changes),'updated',v_updated,'changes',v_changes,
    'unresolved',(select coalesce(jsonb_agg(e),'[]'::jsonb) from jsonb_array_elements(v_snapshot)e where e->>'reason' is not null));
end; $$;
revoke all on function public.reconcile_wave_quarter_prices(boolean,text,uuid) from public,anon,authenticated;
grant execute on function public.reconcile_wave_quarter_prices(boolean,text,uuid) to service_role;
notify pgrst,'reload schema';
