-- Provenance records the supplier document date separately from the business quarter.
-- Explicit source notes retain approval for older lists or intra-quarter documents.
alter table public.quarter_price_imports add column source_valid_from date;
alter table public.quarter_price_imports add column source_note text not null default '';
alter table public.quarter_price_catalog add column source_valid_from date;
alter table public.quarter_price_catalog add column source_note text not null default '';

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
  if exists(select 1 from jsonb_to_recordset(p_rows) r(department text,item_type text,artikel_nr text,name text,units_per_ve integer,price_per_order numeric,source_cell text,source_valid_from date,source_note text)
    where r.department is null or r.department not in ('food','pets') or r.item_type is null or r.item_type not in ('display','kartonware','einzelprodukt')
      or r.artikel_nr is null or length(trim(r.artikel_nr)) not between 1 and 80 or r.name is null or length(trim(r.name)) not between 1 and 500
      or r.units_per_ve is null or r.units_per_ve not between 1 and 100000 or r.price_per_order is null or r.price_per_order<0 or r.price_per_order>=100000000
      or length(coalesce(r.source_note,''))>2000
      or r.price_per_order::text in ('NaN','Infinity','-Infinity') or coalesce(r.source_cell,'')='') then
    raise exception 'Artikelnummer, VE, Preis und Quellenzeile sind erforderlich.' using errcode='22023';
  end if;
  if exists(select 1 from jsonb_to_recordset(p_rows) r(source_valid_from date,source_note text)
    where r.source_valid_from is not null and r.source_valid_from<>make_date(p_year,(p_quarter-1)*3+1,1) and coalesce(trim(r.source_note),'')='') then
    raise exception 'Bei abweichendem Gültigkeitsbeginn ist eine ausdrückliche Freigabe für das gesamte Quartal erforderlich.' using errcode='22023';
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
  insert into public.quarter_price_imports(year,quarter,filename,sha256,actor_id,source_valid_from,source_note) values(p_year,p_quarter,p_filename,p_sha256,p_actor,(p_rows->0->>'source_valid_from')::date,coalesce(p_rows->0->>'source_note','')) returning id into v_import;
  insert into public.quarter_price_catalog(year,quarter,department,item_type,artikel_nr,name,units_per_ve,price_per_order,import_id,source_cell,scope_note,source_valid_from,source_note)
    select p_year,p_quarter,r.department,r.item_type,r.artikel_nr,r.name,r.units_per_ve,r.price_per_order,v_import,r.source_cell,coalesce(r.scope_note,''),r.source_valid_from,coalesce(r.source_note,'')
    from jsonb_to_recordset(p_rows) r(department text,item_type text,artikel_nr text,name text,units_per_ve integer,price_per_order numeric,source_cell text,scope_note text,source_valid_from date,source_note text)
    on conflict(year,quarter,catalog_key) do nothing;
  get diagnostics v_count=row_count;
  return jsonb_build_object('importId',v_import,'inserted',v_count,'unchanged',jsonb_array_length(p_rows)-v_count);
end; $$;

create or replace function public.normalized_quarter_article_name(p_name text) returns text
language sql immutable security invoker set search_path='' as $$
  select trim(regexp_replace(replace(replace(replace(replace(replace(lower(coalesce(p_name,'')),'™',''),'®',''),'’',chr(39)),'‘',chr(39)),'´',chr(39)),'[[:space:]]+',' ','g'));
$$;

alter table public.quarter_price_catalog add column normalized_name text generated always as (public.normalized_quarter_article_name(name)) stored;
create index quarter_price_catalog_name_lookup_idx on public.quarter_price_catalog(year,quarter,item_type,normalized_name);

-- Unique full-name fallback is allowed only when no article number is recorded.
-- Ambiguous names and conflicting recorded article numbers remain unresolved.
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
      case when s.price_source_id is not null then 'Gespeicherter Quartalspreis' when s.artikel_nr is not null or i.catalog_key is not null then 'Gespeicherte Artikelzuordnung' when coalesce(i.artikel_nr,p.artikel_nr) is not null then 'Artikelnummer und vollständiger Name' else 'Eindeutiger vollständiger Name und Artikelart im Quartal' end as match_method,
      coalesce(s.price_year,extract(year from s.created_at at time zone 'Europe/Vienna')::integer) as source_year,
      coalesce(s.price_quarter,extract(quarter from s.created_at at time zone 'Europe/Vienna')::integer) as source_quarter,c.units_per_ve,c.price_per_order,
      case when s.item_type='einzelprodukt' then c.units_per_ve else 1 end as divisor,
      case when c.id is not null then null when i.catalog_key is null and coalesce(s.artikel_nr,i.artikel_nr,p.artikel_nr) is null then 'Artikelzuordnung fehlt' when c.id is null then 'Quartalspreis oder eindeutige Artikelzuordnung fehlt' else null end as reason
    from public.wellen_submissions s
    join public.wellen w on w.id=s.welle_id and not coalesce(w.is_deleted,false)
    left join items i on i.id=s.item_id and i.welle_id=s.welle_id and i.item_type=s.item_type
    left join public.products p on s.item_type='einzelprodukt' and p.id=s.item_id::text
    left join lateral (
      select case when count(*)=1 then min(candidate.id::text)::uuid else null end as id
      from (
      select candidate.id from public.quarter_price_catalog candidate where candidate.year=coalesce(s.price_year,extract(year from s.created_at at time zone 'Europe/Vienna')::integer)
        and candidate.quarter=coalesce(s.price_quarter,extract(quarter from s.created_at at time zone 'Europe/Vienna')::integer)
        and candidate.item_type=s.item_type
        and s.price_source_id is not null and candidate.id=s.price_source_id
      union all
      select candidate.id from public.quarter_price_catalog candidate where candidate.year=coalesce(s.price_year,extract(year from s.created_at at time zone 'Europe/Vienna')::integer)
        and candidate.quarter=coalesce(s.price_quarter,extract(quarter from s.created_at at time zone 'Europe/Vienna')::integer)
        and candidate.item_type=s.item_type
        and s.price_source_id is null and s.artikel_nr is not null and candidate.artikel_nr=s.artikel_nr
      union all
      select candidate.id from public.quarter_price_catalog candidate where candidate.year=coalesce(s.price_year,extract(year from s.created_at at time zone 'Europe/Vienna')::integer)
        and candidate.quarter=coalesce(s.price_quarter,extract(quarter from s.created_at at time zone 'Europe/Vienna')::integer)
        and candidate.item_type=s.item_type
        and s.price_source_id is null and s.artikel_nr is null and i.catalog_key is not null and candidate.catalog_key=i.catalog_key
      union all
      select candidate.id from public.quarter_price_catalog candidate where candidate.year=coalesce(s.price_year,extract(year from s.created_at at time zone 'Europe/Vienna')::integer)
        and candidate.quarter=coalesce(s.price_quarter,extract(quarter from s.created_at at time zone 'Europe/Vienna')::integer)
        and candidate.item_type=s.item_type
        and s.price_source_id is null and s.artikel_nr is null and i.catalog_key is null and coalesce(i.artikel_nr,p.artikel_nr) is not null and candidate.artikel_nr=coalesce(i.artikel_nr,p.artikel_nr) and candidate.normalized_name=public.normalized_quarter_article_name(coalesce(i.name,p.name))
      union all
      select candidate.id from public.quarter_price_catalog candidate where candidate.year=coalesce(s.price_year,extract(year from s.created_at at time zone 'Europe/Vienna')::integer)
        and candidate.quarter=coalesce(s.price_quarter,extract(quarter from s.created_at at time zone 'Europe/Vienna')::integer)
        and candidate.item_type=s.item_type
        and s.price_source_id is null and s.artikel_nr is null and i.catalog_key is null and coalesce(i.artikel_nr,p.artikel_nr) is null and coalesce(i.name,p.name) is not null and candidate.normalized_name=public.normalized_quarter_article_name(coalesce(i.name,p.name))
      ) candidate
    ) match on true
    left join public.quarter_price_catalog c on c.id=match.id
    where s.item_type in ('display','kartonware','einzelprodukt')
  ) select coalesce(jsonb_agg(jsonb_build_object('id',id,'welleId',welle_id,'quantity',quantity,'oldPrice',value_per_unit,
      'newPrice',price_per_order/divisor,'article',coalesce(source_article,known_article),'name',article_name,'waveName',wave_name,'year',source_year,'quarter',source_quarter,'sourceId',source_id,
      'matchMethod',match_method,'veSize',units_per_ve,'orderQuantity',case when divisor is not null and quantity % divisor=0 then quantity/divisor else quantity end,
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
-- Existing RPC signatures and least-privilege grants are retained.
notify pgrst,'reload schema';
