// Generates reviewed SQL only. No connections, credentials, file reads or network.
// Run the preview read-only first; the apply requires its exact snapshot token.
const uuid=/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const quote=v=>"'"+String(v).replaceAll("'","''")+"'";
const priceFields=['value_per_unit','artikel_nr','order_quantity','order_unit','order_unit_price','ve_size','price_year','price_quarter','price_source_id'];
export function validatePlan(plan){
 if(plan?.version!==1||!Number.isInteger(plan.year)||plan.year<2000||plan.year>2100||!uuid.test(plan.actorId)||!/^[a-f0-9]{64}$/.test(plan.source?.sha256)||!plan.source?.filename||!plan.source?.sheet||!Array.isArray(plan.mappings)||!plan.mappings.length||plan.mappings.length>100)throw Error('Invalid reviewed mapping plan');
 const seen=new Set();
 for(const m of plan.mappings){
  const key=m.item_type+':'+m.item_id;
  if(!uuid.test(m.wave_id)||!uuid.test(m.item_id)||!['display','kartonware'].includes(m.item_type)||!/^\d{6}$/.test(m.article)||!m.expected_item_name||!/^\s*(billa\s*plus|b\+)/i.test(m.expected_wave_name)||!Number.isInteger(m.source_row)||m.source_row<2||!m.source_period||!Number.isInteger(m.expected_booking_count)||m.expected_booking_count<1||seen.has(key))throw Error('Invalid or duplicate reviewed article mapping');
  seen.add(key);
 }
 return plan;
}
const input=expr=>`select * from jsonb_to_recordset(${expr}->'mappings') as m(wave_id uuid,item_id uuid,item_type text,expected_wave_name text,expected_item_name text,article text,source_row int,source_period text,expected_booking_count int)`;
function stateSQL(expr){
 return `with mapping as (${input(expr)}),
 items as (
 select d.id,d.welle_id,'display'::text as item_type,to_jsonb(d) as before from public.wellen_displays d
 union all select d.id,d.welle_id,'kartonware',to_jsonb(d) from public.wellen_kartonware d
 ), definitions as (
 select m.*,i.before,w.name as wave_name,to_jsonb(w) as wave_before,
 (select count(*) from public.wellen_submissions s where s.welle_id=m.wave_id and s.item_type=m.item_type and s.item_id=m.item_id) as booking_count,
 case when i.id is null or w.id is null then 'Missing wave/article'
 when w.is_deleted then 'Deleted wave'
 when w.name is distinct from m.expected_wave_name or i.before->>'name' is distinct from m.expected_item_name then 'Wave/article label changed'
 when coalesce(i.before->>'artikel_nr',m.article)<>m.article then 'Conflicting recorded article'
 when coalesce(i.before->>'catalog_key','pets:'||m.item_type||':'||m.article)<>'pets:'||m.item_type||':'||m.article then 'Conflicting catalog identity'
 else null end as issue
 from mapping m left join items i on i.id=m.item_id and i.welle_id=m.wave_id and i.item_type=m.item_type
 left join public.wellen w on w.id=m.wave_id
 ), bookings as (
 select d.wave_id,d.item_id,d.item_type,d.article,d.source_row,d.source_period,to_jsonb(s) as before,to_jsonb(c) as catalog,
 extract(year from s.created_at at time zone 'Europe/Vienna')::int as year,
 extract(quarter from s.created_at at time zone 'Europe/Vienna')::int as quarter,
 case when c.id is null then 'Missing exact article/type/quarter price'
 when extract(year from s.created_at at time zone 'Europe/Vienna')::int<>(${expr}->>'year')::int then 'Unexpected booking year'
 when s.quantity<=0 then 'Nonpositive booking quantity'
 when s.price_source_id is not null and (s.price_source_id<>c.id or s.artikel_nr is distinct from d.article) then 'Conflicting frozen article'
 when s.artikel_nr is not null and s.artikel_nr<>d.article then 'Conflicting saved article'
 else null end as issue
 from definitions d join public.wellen_submissions s on s.welle_id=d.wave_id and s.item_type=d.item_type and s.item_id=d.item_id
 left join public.quarter_price_catalog c on c.department='pets' and c.item_type=d.item_type and c.artikel_nr=d.article
 and c.year=extract(year from s.created_at at time zone 'Europe/Vienna')::int
 and c.quarter=extract(quarter from s.created_at at time zone 'Europe/Vienna')::int
 ), state as (
 select jsonb_build_object('plan',${expr},
 'definitions',(select jsonb_agg(to_jsonb(d) order by d.item_type,d.item_id) from definitions d),
 'bookings',coalesce((select jsonb_agg(to_jsonb(b) order by b.before->>'id') from bookings b),'[]'::jsonb)) as snapshot
 ) select snapshot from state`;
}
const errorsSQL=`select coalesce(jsonb_agg(x),'[]'::jsonb) from (
 select jsonb_build_object('itemId',e->>'item_id','reason',e->>'issue') as x from jsonb_array_elements(v_state->'definitions')e where e->>'issue' is not null
 union all select jsonb_build_object('itemId',e->>'item_id','reason','Booking count changed') from jsonb_array_elements(v_state->'definitions')e where e->>'booking_count' is distinct from e->>'expected_booking_count'
 union all select jsonb_build_object('bookingId',e->'before'->>'id','reason',e->>'issue') from jsonb_array_elements(v_state->'bookings')e where e->>'issue' is not null
 ) issues`;
export function previewSQL(plan){
 validatePlan(plan);
 return `with state as (${stateSQL(quote(JSON.stringify(plan))+'::jsonb')}), counted as (
 select snapshot as v_state from state
 ) select jsonb_build_object(
 'token',md5(v_state::text),'errors',(${errorsSQL}),
 'definitionChanges',(select count(*) from jsonb_array_elements(v_state->'definitions')e where e->'before'->>'artikel_nr' is distinct from e->>'article' or e->'before'->>'catalog_key' is distinct from 'pets:'||(e->>'item_type')||':'||(e->>'article')),
 'bookingChanges',(select count(*) from jsonb_array_elements(v_state->'bookings')e where e->'before'->>'price_source_id' is null),
 'quarters',(select jsonb_agg(to_jsonb(q) order by year,quarter,item_type) from (
 select (e->>'year')::int as year,(e->>'quarter')::int as quarter,e->>'item_type' as item_type,count(*) as bookings,sum((e->'before'->>'quantity')::numeric*(e->'catalog'->>'price_per_order')::numeric) as total
 from jsonb_array_elements(v_state->'bookings')e where e->'before'->>'price_source_id' is null group by 1,2,3)q),
 'definitions',(select jsonb_agg(jsonb_build_object('itemId',e->>'item_id','wave',e->>'wave_name','oldName',e->'before'->>'name','article',e->>'article','sourceRow',e->>'source_row','bookings',e->>'booking_count')) from jsonb_array_elements(v_state->'definitions')e)
 ) as preview from counted;`;
}
export function applySQL(plan,token){
 validatePlan(plan);if(!/^[a-f0-9]{32}$/.test(token))throw Error('Exact preview token required');
 let delimiter='$mapping_apply$';
 while(JSON.stringify(plan).includes(delimiter))delimiter=delimiter.slice(0,-1)+'_x$';
 const stripped="array["+priceFields.map(quote).join(',')+"]::text[]";
 return `begin;
 set local lock_timeout='5s';set local statement_timeout='45s';
 do ${delimiter}
 declare v_plan jsonb:=${quote(JSON.stringify(plan))}::jsonb;v_state jsonb;v_errors jsonb;v_n int;v_expected int;v_audit uuid;v_changes jsonb;v_definitions int:=0;
 begin
 if not exists(select 1 from public.users where id=(v_plan->>'actorId')::uuid and role='admin') then raise exception 'Verified admin actor required';end if;
 -- Same lock order as the existing reconciliation RPC: bookings, definitions, waves.
 perform s.id from public.wellen_submissions s join (${input('v_plan')})m on m.wave_id=s.welle_id and m.item_type=s.item_type and m.item_id=s.item_id order by s.id for update of s;
 perform d.id from public.wellen_displays d join (${input('v_plan')})m on m.item_id=d.id and m.wave_id=d.welle_id and m.item_type='display' order by d.id for update of d;
 perform d.id from public.wellen_kartonware d join (${input('v_plan')})m on m.item_id=d.id and m.wave_id=d.welle_id and m.item_type='kartonware' order by d.id for update of d;
 -- Parent UPDATE locks also block new FK references while the snapshot is applied.
 perform w.id from public.wellen w where w.id in(select wave_id from (${input('v_plan')})m) order by w.id for update;
 perform c.id from public.quarter_price_catalog c join (${input('v_plan')})m on c.department='pets' and c.item_type=m.item_type and c.artikel_nr=m.article where c.year=(v_plan->>'year')::int order by c.id for share of c;
 select snapshot into v_state from (${stateSQL('v_plan')})s;
 if md5(v_state::text)<> ${quote(token)} then raise exception 'Reviewed snapshot changed; obtain a fresh preview' using errcode='40001';end if;
 select (${errorsSQL}) into v_errors;
 if jsonb_array_length(v_errors)>0 then raise exception 'Mapping evidence is incomplete: %',v_errors using errcode='22023';end if;
 select count(*) into v_expected from jsonb_array_elements(v_state->'bookings')e where e->'before'->>'price_source_id' is null;
 -- Audit full booking before-images, exact catalog source and mapping workbook provenance.
 select coalesce(jsonb_agg(jsonb_build_object(
 'id',e->'before'->>'id','welleId',e->>'wave_id','itemId',e->>'item_id','itemType',e->>'item_type',
 'name',d->'before'->>'name','waveName',d->>'wave_name','quantity',e->'before'->'quantity',
 'oldPrice',e->'before'->'value_per_unit','newPrice',e->'catalog'->'price_per_order',
 'oldSource',e->'before'->'price_source_id','sourceId',e->'catalog'->'id','article',e->>'article',
 'year',e->'year','quarter',e->'quarter','orderQuantity',e->'before'->'quantity',
 'orderUnit',case when e->>'item_type'='display' then 'Display' else 'VE' end,'orderPrice',e->'catalog'->'price_per_order',
 'veSize',e->'catalog'->'units_per_ve','before',e->'before',
 'matchMethod','Reviewed customer/week/variant mapping',
 'mappingSource',(v_plan->'source')||jsonb_build_object('row',e->'source_row','period',e->>'source_period'),
 'waveItemBefore',d->'before','catalogEvidence',e->'catalog'
 ) order by e->'before'->>'id'),'[]'::jsonb) into v_changes
 from jsonb_array_elements(v_state->'bookings')e join jsonb_array_elements(v_state->'definitions')d on d->>'item_id'=e->>'item_id' and d->>'item_type'=e->>'item_type'
 where e->'before'->>'price_source_id' is null;
 update public.wellen_submissions s set
 value_per_unit=(e->'catalog'->>'price_per_order')::numeric,artikel_nr=e->>'article',
 order_quantity=(e->'before'->>'quantity')::numeric,order_unit=case when e->>'item_type'='display' then 'Display' else 'VE' end,
 order_unit_price=(e->'catalog'->>'price_per_order')::numeric,ve_size=(e->'catalog'->>'units_per_ve')::int,
 price_year=(e->>'year')::int,price_quarter=(e->>'quarter')::int,price_source_id=(e->'catalog'->>'id')::uuid
 from jsonb_array_elements(v_state->'bookings')e where s.id=(e->'before'->>'id')::uuid and e->'before'->>'price_source_id' is null;
 get diagnostics v_n=row_count;
 if v_n<>v_expected then raise exception 'Incomplete booking update';end if;
 update public.wellen_displays d set artikel_nr=e->>'article',catalog_key='pets:display:'||(e->>'article')
 from jsonb_array_elements(v_state->'definitions')e where e->>'item_type'='display' and d.id=(e->>'item_id')::uuid and (d.artikel_nr is distinct from e->>'article' or d.catalog_key is distinct from 'pets:display:'||(e->>'article'));
 get diagnostics v_n=row_count;v_definitions:=v_definitions+v_n;
 update public.wellen_kartonware d set artikel_nr=e->>'article',catalog_key='pets:kartonware:'||(e->>'article')
 from jsonb_array_elements(v_state->'definitions')e where e->>'item_type'='kartonware' and d.id=(e->>'item_id')::uuid and (d.artikel_nr is distinct from e->>'article' or d.catalog_key is distinct from 'pets:kartonware:'||(e->>'article'));
 get diagnostics v_n=row_count;v_definitions:=v_definitions+v_n;
 if exists(select 1 from public.wellen_submissions s join jsonb_array_elements(v_state->'bookings')e on s.id=(e->'before'->>'id')::uuid
 where (to_jsonb(s)-${stripped}) is distinct from ((e->'before')-${stripped})) then raise exception 'Non-price booking data changed';end if;
 if exists(select 1 from (
 select 'display'::text as type,to_jsonb(d) as row from public.wellen_displays d
 union all select 'kartonware',to_jsonb(d) from public.wellen_kartonware d)actual
 join jsonb_array_elements(v_state->'definitions')e on actual.type=e->>'item_type' and actual.row->>'id'=e->>'item_id'
 where actual.row-array['artikel_nr','catalog_key']::text[] is distinct from (e->'before')-array['artikel_nr','catalog_key']::text[]) then raise exception 'Other wave definition data changed';end if;
 if exists(select 1 from public.wellen_submissions s join jsonb_array_elements(v_state->'bookings')e on s.id=(e->'before'->>'id')::uuid
 where s.price_source_id is distinct from (e->'catalog'->>'id')::uuid or s.artikel_nr is distinct from e->>'article'
 or s.price_year is distinct from (e->>'year')::int or s.price_quarter is distinct from (e->>'quarter')::int
 or s.value_per_unit is distinct from (e->'catalog'->>'price_per_order')::numeric
 or s.order_quantity is distinct from s.quantity::numeric or s.order_unit_price is distinct from s.value_per_unit
 or s.ve_size is distinct from (e->'catalog'->>'units_per_ve')::int
 or s.order_unit is distinct from case when s.item_type='display' then 'Display' else 'VE' end) then raise exception 'Price/source verification failed';end if;
 if v_expected>0 then
 insert into public.wave_quarter_price_audit(actor_id,changes) values((v_plan->>'actorId')::uuid,v_changes) returning id into v_audit;
 elsif v_definitions>0 then raise exception 'Definition-only changes need a separate audited review';
 end if;
 perform set_config('mars.mapping_result',jsonb_build_object('bookingsUpdated',v_expected,'definitionsUpdated',v_definitions,'auditId',v_audit,'sourceSha256',v_plan->'source'->>'sha256','nonPriceDataVerified',true)::text,true);
 end ${delimiter};
 select current_setting('mars.mapping_result')::jsonb as result;
 commit;`;
}

