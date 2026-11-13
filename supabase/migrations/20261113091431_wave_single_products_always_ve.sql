-- Existing quantities and historical prices are untouched by installation.
-- Individual-product orders use whole VEs, including legacy waves and cached clients.
create or replace function public.parse_wave_ve(p_text text) returns integer
language plpgsql immutable security invoker set search_path='' as $$
declare v_text text;v_number numeric;
begin
 v_text:=trim(regexp_replace(coalesce(p_text,''),'^[[:space:]]*VE[[:space:]]*:[[:space:]]*','','i'));
 if v_text !~* '^[0-9]+([.,]0+)?([[:space:]]*(stk|stück|stueck|einheit|einheiten))?$' then return null;end if;
 v_number:=(regexp_match(v_text,'^[0-9]+'))[1]::numeric;
 if v_number between 1 and 100000 then return v_number::integer;end if;
 return null;
end;$$;
revoke all on function public.parse_wave_ve(text) from public,anon,authenticated;
grant execute on function public.parse_wave_ve(text) to service_role;

create or replace function public.wave_single_order_details(p_wave uuid,p_item uuid) returns jsonb
language plpgsql stable security invoker set search_path='' as $$
declare v_item jsonb;v_product jsonb;v_units integer;v_count integer;v_price numeric;v_article text;
begin
 select to_jsonb(e) into v_item from public.wellen_einzelprodukte e where e.id=p_item and e.welle_id=p_wave;
 if v_item is not null then
  v_units:=coalesce(public.parse_wave_ve(v_item->>'ve'),public.parse_wave_ve(v_item->>'ve_size'),public.parse_wave_ve(v_item->>'vpe'));
  v_price:=(v_item->>'item_value')::numeric;v_article:=v_item->>'artikel_nr';
  if v_units is null then
   select count(distinct units),min(units) into v_count,v_units from (
    select public.parse_wave_ve(p.content) as units
    from public.products p where
    case when nullif(v_article,'') is not null then p.artikel_nr=v_article
    else public.normalized_quarter_article_name(p.name)=public.normalized_quarter_article_name(v_item->>'name') end
   ) candidates where units is not null;
   if v_count<>1 then v_units:=null;end if;
  end if;
 else
  select to_jsonb(p) into v_product from public.products p where p.id=p_item::text;
  v_units:=public.parse_wave_ve(v_product->>'content');
  v_price:=(v_product->>'price')::numeric;v_article:=v_product->>'artikel_nr';
 end if;
 return jsonb_build_object('units',v_units,'unitPrice',v_price,'article',v_article);
end;$$;
revoke all on function public.wave_single_order_details(uuid,uuid) from public,anon,authenticated;
grant execute on function public.wave_single_order_details(uuid,uuid) to service_role;

create or replace function public.snapshot_wave_quarter_price() returns trigger
language plpgsql security invoker set search_path='' as $$
declare v_key text;v_required boolean;v_price public.quarter_price_catalog;v_divisor integer;v_date timestamp;v_order jsonb;
begin
 if new.item_type not in ('display','kartonware','einzelprodukt') then return new;end if;
 select catalog_pricing_required into v_required from public.wellen where id=new.welle_id;
 if new.item_type='display' then select catalog_key into v_key from public.wellen_displays where id=new.item_id and welle_id=new.welle_id;
 elsif new.item_type='kartonware' then select catalog_key into v_key from public.wellen_kartonware where id=new.item_id and welle_id=new.welle_id;
 else select catalog_key into v_key from public.wellen_einzelprodukte where id=new.item_id and welle_id=new.welle_id;
 end if;
 if v_key is null then
  if v_required then raise exception 'Artikel ist nicht mit dem Quartalskatalog verknüpft. Bitte Welle neu laden.' using errcode='22023';end if;
  if new.item_type='einzelprodukt' then
   v_order:=public.wave_single_order_details(new.welle_id,new.item_id);v_divisor:=(v_order->>'units')::integer;
   if v_divisor is null then raise exception 'Für diesen Artikel fehlt eine eindeutige VE. Bitte die VE beim Artikel hinterlegen lassen.' using errcode='22023';end if;
   if new.quantity is null or new.quantity<=0 or new.quantity % v_divisor<>0 then
    raise exception 'Einzelprodukte bitte in ganzen VEs erfassen. Lade Rover neu und gib die Anzahl der VEs ein.' using errcode='22023';
   end if;
   -- Legacy unit prices remain explicitly unconfirmed, but are frozen from server data.
   new.value_per_unit:=(v_order->>'unitPrice')::numeric;
   new.artikel_nr:=v_order->>'article';new.ve_size:=v_divisor;
   new.order_quantity:=new.quantity/v_divisor;new.order_unit:='VE';
   new.order_unit_price:=new.value_per_unit*v_divisor;
  end if;
  return new;
 end if;
 v_date:=statement_timestamp() at time zone 'Europe/Vienna';
 select * into v_price from public.quarter_price_catalog where catalog_key=v_key and year=extract(year from v_date)::integer and quarter=extract(quarter from v_date)::integer;
 if not found or v_price.item_type<>new.item_type then raise exception 'Für den Artikel fehlt der freigegebene Preis im aktuellen Quartal. Es wurde nichts gespeichert.' using errcode='22023';end if;
 v_divisor:=case when new.item_type='einzelprodukt' then v_price.units_per_ve else 1 end;
 if new.quantity is null or new.quantity<=0 or new.quantity % v_divisor<>0 then raise exception 'Einzelprodukte können nur in ganzen VEs bestellt werden. Bitte die Welle neu laden.' using errcode='22023';end if;
 new.value_per_unit:=v_price.price_per_order/v_divisor;new.artikel_nr:=v_price.artikel_nr;
 new.order_quantity:=new.quantity/v_divisor;new.order_unit:=case when new.item_type='display' then 'Display' else 'VE' end;
 new.order_unit_price:=v_price.price_per_order;new.ve_size:=v_price.units_per_ve;
 new.price_year:=v_price.year;new.price_quarter:=v_price.quarter;new.price_source_id:=v_price.id;
 return new;
end;$$;
revoke all on function public.snapshot_wave_quarter_price() from public,anon,authenticated;
grant execute on function public.snapshot_wave_quarter_price() to service_role;

create or replace function public.guard_wave_quarter_price_update() returns trigger
language plpgsql security invoker set search_path='' as $$
declare v_price public.quarter_price_catalog;v_divisor integer;
begin
 if new.price_source_id is null then
  if old.price_source_id is not null then raise exception 'Gespeicherter Quartalspreis darf nicht entfernt werden.' using errcode='22023';end if;
  if new.item_type='einzelprodukt' and new.order_unit='VE' and new.ve_size>0 then
   if old.order_unit='VE' and old.ve_size>0 and new.ve_size is distinct from old.ve_size then raise exception 'Die gespeicherte VE darf nicht geändert werden.' using errcode='22023';end if;
   if new.quantity<=0 or new.quantity % new.ve_size<>0 then raise exception 'Bitte nur ganze VEs eingeben.' using errcode='22023';end if;
   new.order_quantity:=new.quantity/new.ve_size;new.order_unit_price:=new.value_per_unit*new.ve_size;
  end if;
  return new;
 end if;
 select * into v_price from public.quarter_price_catalog where id=new.price_source_id;
 v_divisor:=case when new.item_type='einzelprodukt' then v_price.units_per_ve else 1 end;
 if not found or v_price.item_type<>new.item_type or new.value_per_unit is distinct from v_price.price_per_order/v_divisor then raise exception 'Buchungspreise müssen dem belegten Quartalspreis entsprechen.' using errcode='22023';end if;
 if new.quantity<=0 or (new.order_unit<>'Stück' and new.quantity % v_divisor<>0) then raise exception 'Bitte nur ganze VEs eingeben.' using errcode='22023';end if;
 new.order_quantity:=case when new.order_unit='Stück' then new.quantity else new.quantity/v_divisor end;
 new.order_unit_price:=case when new.order_unit='Stück' then new.value_per_unit else v_price.price_per_order end;
 new.artikel_nr:=v_price.artikel_nr;new.ve_size:=v_price.units_per_ve;new.price_year:=v_price.year;new.price_quarter:=v_price.quarter;
 return new;
end;$$;
revoke all on function public.guard_wave_quarter_price_update() from public,anon,authenticated;
grant execute on function public.guard_wave_quarter_price_update() to service_role;
notify pgrst,'reload schema';

