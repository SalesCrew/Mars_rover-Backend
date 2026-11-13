// One-time, explicit business-confirmed VE corrections. No connection or credentials.
const quote=s=>"'"+String(s).replaceAll("'","''")+"'";
const uuid=/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
export function validateCorrection(plan){
 if(!uuid.test(plan?.actorId)||!uuid.test(plan?.waveId)||!plan.approval?.text||!plan.approval?.at||!Array.isArray(plan.batchIds)||!plan.batchIds.length||!plan.batchIds.every(x=>uuid.test(x))||!Array.isArray(plan.entries)||!plan.entries.length||plan.entries.length>100)throw Error('Invalid explicit VE correction');
 const ids=new Set();
 for(const e of plan.entries){if(!uuid.test(e.id)||ids.has(e.id)||!Number.isInteger(e.ve)||e.ve<1||e.ve>100000||!/^\d{6}$/.test(e.article)||!/^\d+(?:\.\d+)?$/.test(String(e.unitPrice))||(e.sourceId!==null&&!uuid.test(e.sourceId)))throw Error('Invalid VE correction row');ids.add(e.id);}
 return plan;
}
const input=p=>`select * from jsonb_to_recordset(${p}->'entries')e(id uuid,ve integer,article text,"unitPrice" numeric,"sourceId" uuid)`;
function state(p){return `with requested as(${input(p)}), entries as(
 select e.*,to_jsonb(s) as before,to_jsonb(c) as catalog,public.wave_single_order_details(s.welle_id,s.item_id) as authority,
 (s.quantity=e.ve and s.order_unit='VE' and s.order_quantity=1 and s.order_unit_price=e."unitPrice"*e.ve and s.ve_size=e.ve and s.artikel_nr=e.article and s.price_source_id is not distinct from e."sourceId") as done,
 case when s.id is null then 'Missing booking'
 when s.welle_id<>(${p}->>'waveId')::uuid or not(s.submission_batch_id::text in(select jsonb_array_elements_text(${p}->'batchIds'))) or s.item_type<>'einzelprodukt' then 'Unexpected booking scope'
 when coalesce(s.ve_size,(public.wave_single_order_details(s.welle_id,s.item_id)->>'units')::int) is distinct from e.ve then 'VE evidence changed'
 when s.price_source_id is not null and s.price_source_id is distinct from e."sourceId" then 'Frozen price source changed'
 when e."sourceId" is null and coalesce(s.value_per_unit,(public.wave_single_order_details(s.welle_id,s.item_id)->>'unitPrice')::numeric) is distinct from e."unitPrice" then 'Existing booking price changed'
 when e."sourceId" is not null and (c.id is null or c.item_type<>'einzelprodukt' or c.artikel_nr<>e.article or c.units_per_ve<>e.ve or c.price_per_order<>e."unitPrice"*e.ve or c.year<>extract(year from s.created_at at time zone 'Europe/Vienna')::int or c.quarter<>extract(quarter from s.created_at at time zone 'Europe/Vienna')::int) then 'Quarter price evidence changed'
 when s.quantity<>1 and not(s.quantity=e.ve and s.order_unit='VE' and s.order_quantity=1) then 'Original quantity is no longer one'
 else null end as issue
 from requested e left join public.wellen_submissions s on s.id=e.id left join public.quarter_price_catalog c on c.id=e."sourceId"
) select jsonb_build_object('plan',${p},'entries',(select jsonb_agg(to_jsonb(e) order by e.id) from entries e)) as snapshot`;}
export function correctionPreviewSQL(plan){validateCorrection(plan);return `with state as(${state(quote(JSON.stringify(plan))+'::jsonb')}) select jsonb_build_object('token',md5(snapshot::text),'count',(select count(*) from jsonb_array_elements(snapshot->'entries')e where not coalesce((e->>'done')::boolean,false)),'issues',(select coalesce(jsonb_agg(jsonb_build_object('id',e->>'id','reason',e->>'issue')),'[]'::jsonb) from jsonb_array_elements(snapshot->'entries')e where e->>'issue' is not null),'quantityBefore',(select sum((e->'before'->>'quantity')::numeric) from jsonb_array_elements(snapshot->'entries')e),'quantityAfter',(select sum((e->>'ve')::numeric) from jsonb_array_elements(snapshot->'entries')e),'totalAfter',(select sum((e->>'ve')::numeric*(e->>'unitPrice')::numeric) from jsonb_array_elements(snapshot->'entries')e)) as preview from state;`;}
export function correctionApplySQL(plan,token){
 validateCorrection(plan);if(!/^[a-f0-9]{32}$/.test(token))throw Error('Exact preview token required');
 let tag='$confirmed_ve$';while(JSON.stringify(plan).includes(tag))tag=tag.slice(0,-1)+'_x$';
 return `begin;set local lock_timeout='5s';set local statement_timeout='30s';
 do ${tag}
 declare p jsonb:=${quote(JSON.stringify(plan))}::jsonb;v jsonb;changes jsonb;problems int;updated int;expected int;audit_id uuid;
 begin
 if not exists(select 1 from public.users where id=(p->>'actorId')::uuid and role='admin') then raise exception 'Admin actor required';end if;
 perform s.id from public.wellen_submissions s join (${input('p')})e on e.id=s.id order by s.id for update of s;
 perform w.id from public.wellen w where id=(p->>'waveId')::uuid for update;
 perform c.id from public.quarter_price_catalog c where c.id in(select "sourceId" from (${input('p')})e) order by c.id for share;
 select snapshot into v from (${state('p')})q;
 if md5(v::text)<> ${quote(token)} then raise exception 'Reviewed correction changed' using errcode='40001';end if;
 select count(*) into problems from jsonb_array_elements(v->'entries')e where e->>'issue' is not null;
 if problems>0 then raise exception 'VE correction evidence is incomplete';end if;
 select count(*) into expected from jsonb_array_elements(v->'entries')e where not coalesce((e->>'done')::bool,false);
 update public.wellen_submissions s set quantity=(e->>'ve')::int,ve_size=(e->>'ve')::int,order_unit='VE',order_quantity=1,
 value_per_unit=(e->>'unitPrice')::numeric,order_unit_price=(e->>'unitPrice')::numeric*(e->>'ve')::int,
 artikel_nr=e->>'article',price_source_id=(e->>'sourceId')::uuid
 from jsonb_array_elements(v->'entries')e where s.id=(e->>'id')::uuid and not coalesce((e->>'done')::bool,false);
 get diagnostics updated=row_count;if updated<>expected then raise exception 'Incomplete VE correction';end if;
 if exists(select 1 from public.wellen_submissions s join jsonb_array_elements(v->'entries')e on s.id=(e->>'id')::uuid
 where (to_jsonb(s)-array['quantity','ve_size','order_unit','order_quantity','value_per_unit','order_unit_price','artikel_nr','price_source_id','price_year','price_quarter']::text[]) is distinct from ((e->'before')-array['quantity','ve_size','order_unit','order_quantity','value_per_unit','order_unit_price','artikel_nr','price_source_id','price_year','price_quarter']::text[])
 or s.quantity<>(e->>'ve')::int or s.order_quantity<>1 or s.order_unit<>'VE' or s.order_unit_price<>(e->>'unitPrice')::numeric*(e->>'ve')::int or s.value_per_unit<>(e->>'unitPrice')::numeric or s.artikel_nr<>e->>'article' or s.price_source_id is distinct from (e->>'sourceId')::uuid) then raise exception 'VE correction verification failed';end if;
 if updated>0 then
 select jsonb_agg(jsonb_build_object('id',e->>'id','correctionType','confirmed_ve_quantity','approval',p->'approval','before',e->'before','after',to_jsonb(s),'catalogEvidence',e->'catalog','veEvidence',e->'authority') order by e->>'id') into changes
 from jsonb_array_elements(v->'entries')e join public.wellen_submissions s on s.id=(e->>'id')::uuid where not coalesce((e->>'done')::bool,false);
 insert into public.wave_quarter_price_audit(actor_id,changes) values((p->>'actorId')::uuid,changes) returning id into audit_id;
 end if;
 perform set_config('mars.ve_correction_result',jsonb_build_object('updated',updated,'auditId',audit_id,'otherFieldsVerified',true)::text,true);
 end ${tag};select current_setting('mars.ve_correction_result')::jsonb as result;commit;`;
}

