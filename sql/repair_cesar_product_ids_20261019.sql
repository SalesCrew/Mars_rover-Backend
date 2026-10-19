-- One-off, guarded repair of the seven unreferenced Cesar catalogue rows.
-- Run only after saving the full original rows and this mapping externally.
-- No submission/history rows are updated. Existing archived IDs are retained.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';
LOCK TABLE public.products IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE public.nara_incentive_items, public.vorverkauf_items,
  public.vorverkauf_submission_products, public.wellen_submissions,
  public.wellen_gl_progress, public.products_update IN SHARE MODE;
CREATE TEMP TABLE product_uuid_repair_mapping(old_id text PRIMARY KEY, new_id text UNIQUE, artikel_nr text) ON COMMIT DROP;
INSERT INTO product_uuid_repair_mapping VALUES
  ('cesar-schale-150g-8-pack-klassik-terrine-mit-rind----leber--huhn---truthahn--kalb---gefl-gel--huhn---rind-5', '96775e66-afaf-4372-8b4b-d95112e2fe86', '334822'),
  ('cesar-schale-150g-8-pack-landragout-mit-rind--truthahn--gefl-gel--lamm-in-sauce-4', '4b6809bd-3b1a-4e74-90cf-83a0dd6dcd30', '359886'),
  ('cesar-schale-150g-8-pack-senior-8--mit-rind--truthahn--gefl-gel-und-lamm-in-sauce-6', '464e95e5-edf2-42c0-ba35-62c7ee3042b9', '451963'),
  ('cesar-frischebeutel-12x100g-creations-in-sauce-rind--huhn--lamm-und-truthahn-8', 'ea31e1aa-542c-406c-a5a9-92896ad18e4e', '611172'),
  ('cesar-frischebeutel-12x100g-creations-in-gelee-rind--huhn--lamm-und-truthahn-7', '0c0cd13d-89b2-439a-9014-10bee901d7b6', '611176'),
  ('cesar-schale-150g-garten-terrine-truthahn-und-lamm-2', 'd6becf2f-02f9-4d25-9d8d-fc699d84d0bb', '616113'),
  ('cesar-schale-150g-garten-terrine-rind-und-gem-se-3', '4624241a-456e-40e4-aa09-6bcfa66de042', '617371');
CREATE TEMP TABLE product_uuid_repair_before ON COMMIT DROP AS SELECT * FROM public.products;
CREATE TEMP TABLE product_uuid_repair_history(table_name text, row_count bigint, digest text) ON COMMIT DROP;
DO $repair$
DECLARE
  table_name text;
  before_row record;
  actual_count bigint;
  actual_digest text;
  updated_count integer;
BEGIN
  IF (SELECT count(*) FROM public.products p JOIN product_uuid_repair_mapping m ON p.id=m.new_id AND p.artikel_nr=m.artikel_nr)=7
     AND NOT EXISTS (SELECT 1 FROM public.products p JOIN product_uuid_repair_mapping m ON p.id=m.old_id) THEN
    RAISE NOTICE 'All seven product UUIDs already repaired; no changes.';
    RETURN;
  END IF;
  IF (SELECT count(*) FROM public.products p JOIN product_uuid_repair_mapping m ON p.id=m.old_id AND p.artikel_nr=m.artikel_nr
      WHERE p.is_deleted=false AND p.is_active=true AND p.created_at='2026-08-26T14:20:26.276961Z')<>7 THEN
    RAISE EXCEPTION 'Expected exactly the seven original active Cesar rows; aborting';
  END IF;
  IF EXISTS (SELECT 1 FROM public.products p JOIN product_uuid_repair_mapping m ON p.id=m.new_id) THEN
    RAISE EXCEPTION 'Target UUID already exists; aborting';
  END IF;
  IF EXISTS (SELECT 1 FROM public.nara_incentive_items x JOIN product_uuid_repair_mapping m ON x.product_id=m.old_id)
     OR EXISTS (SELECT 1 FROM public.vorverkauf_items x JOIN product_uuid_repair_mapping m ON x.product_id=m.old_id)
     OR EXISTS (SELECT 1 FROM public.vorverkauf_submission_products x JOIN product_uuid_repair_mapping m ON x.product_id=m.old_id)
     OR EXISTS (SELECT 1 FROM public.products x JOIN product_uuid_repair_mapping m ON x.palette_products::text LIKE '%'||m.old_id||'%')
     OR EXISTS (SELECT 1 FROM public.products_update x JOIN product_uuid_repair_mapping m ON x.id=m.old_id OR x.palette_products::text LIKE '%'||m.old_id||'%') THEN
    RAISE EXCEPTION 'A legacy ID is referenced; aborting without modifying history';
  END IF;
  FOR table_name IN SELECT unnest(ARRAY['nara_incentive_items','vorverkauf_items','vorverkauf_submission_products','wellen_submissions','wellen_gl_progress','products_update']) LOOP
    EXECUTE format('SELECT count(*), md5(coalesce(string_agg(to_jsonb(t)::text, %L ORDER BY id), %L)) FROM public.%I t','','',table_name)
      INTO actual_count, actual_digest;
    INSERT INTO product_uuid_repair_history VALUES(table_name,actual_count,actual_digest);
  END LOOP;
  UPDATE public.products p SET id=m.new_id FROM product_uuid_repair_mapping m WHERE p.id=m.old_id;
  GET DIAGNOSTICS updated_count=ROW_COUNT;
  IF updated_count<>7 THEN RAISE EXCEPTION 'Expected seven updates, got %',updated_count; END IF;
  IF (SELECT count(*) FROM public.products)<>(SELECT count(*) FROM product_uuid_repair_before)
    OR EXISTS (
      SELECT 1 FROM product_uuid_repair_before b
      LEFT JOIN product_uuid_repair_mapping m ON m.old_id=b.id
      LEFT JOIN public.products p ON p.id=coalesce(m.new_id,b.id)
      WHERE p.id IS NULL
        OR (m.old_id IS NULL AND to_jsonb(p)<>to_jsonb(b))
        OR (m.old_id IS NOT NULL AND (to_jsonb(p)-'id'-'updated_at')<>(to_jsonb(b)-'id'-'updated_at'))
    ) THEN RAISE EXCEPTION 'Product data preservation check failed'; END IF;
  FOR before_row IN SELECT * FROM product_uuid_repair_history LOOP
    EXECUTE format('SELECT count(*), md5(coalesce(string_agg(to_jsonb(t)::text, %L ORDER BY id), %L)) FROM public.%I t','','',before_row.table_name)
      INTO actual_count, actual_digest;
    IF actual_count<>before_row.row_count OR actual_digest<>before_row.digest THEN
      RAISE EXCEPTION 'History changed in %; rolling back',before_row.table_name;
    END IF;
  END LOOP;
END;
$repair$;
SELECT m.artikel_nr, m.old_id, p.id AS new_id, p.name
FROM product_uuid_repair_mapping m JOIN public.products p ON p.id=m.new_id ORDER BY m.artikel_nr;
SELECT * FROM product_uuid_repair_history ORDER BY table_name;
COMMIT;
