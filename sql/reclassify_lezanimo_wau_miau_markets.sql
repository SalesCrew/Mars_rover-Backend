-- Approved market-chain correction, 2026-09-28, MarsRover production.
-- Only these 11 verified market IDs may change. Banner/subgroup remain intact.
-- Safe to run again: rows already using the intended chain are left unchanged.
BEGIN ISOLATION LEVEL REPEATABLE READ;
SET LOCAL statement_timeout = '10s';
SET LOCAL lock_timeout = '3s';

DO $market_chain_correction$
DECLARE
  target RECORD;
  previous_row JSONB;
  corrected_row JSONB;
  other_markets_before TEXT;
  other_markets_after TEXT;
  target_ids TEXT[] := ARRAY[
    '65165', '534', '16783', '23765', '20507', '70368',
    '72285', '75131', '72283', '649', '68462'
  ];
BEGIN
  SELECT md5(string_agg(to_jsonb(m)::text, E'\n' ORDER BY m.id))
  INTO other_markets_before
  FROM public.markets m WHERE NOT (m.id = ANY(target_ids));

  FOR target IN
    SELECT * FROM (VALUES
      ('65165', 'Lezanimo', 'Lezanimo'),
      ('534', 'Lezanimo', 'Lezanimo'),
      ('16783', 'Lezanimo', 'Lezanimo'),
      ('23765', 'Wau Miau', 'Wau,Miau'),
      ('20507', 'Wau Miau', 'Wau,Miau'),
      ('70368', 'Wau Miau', 'Wau,Miau'),
      ('72285', 'Wau Miau', 'Wau,Miau'),
      ('75131', 'Wau Miau', 'Wau,Miau'),
      ('72283', 'Wau Miau', 'Wau,Miau'),
      ('649', 'Wau Miau', 'Wau,Miau'),
      ('68462', 'Wau Miau', 'Wau,Miau')
    ) AS targets(id, expected_subgroup, new_chain)
    ORDER BY id
  LOOP
    SELECT to_jsonb(m) INTO previous_row
    FROM public.markets m WHERE m.id = target.id FOR UPDATE;

    IF previous_row IS NULL
      OR previous_row->>'subgroup' IS DISTINCT FROM target.expected_subgroup
      OR previous_row->>'chain' IS NULL
      OR previous_row->>'chain' NOT IN ('Zoofachhandel', target.new_chain)
      OR previous_row->>'is_active' IS DISTINCT FROM 'true'
      OR position(lower(target.expected_subgroup) IN lower(coalesce(previous_row->>'name', ''))) = 0
    THEN
      RAISE EXCEPTION 'Market % no longer matches the verified classification; correction aborted', target.id;
    END IF;

    UPDATE public.markets SET chain = target.new_chain
    WHERE id = target.id AND chain IS DISTINCT FROM target.new_chain;

    SELECT to_jsonb(m) INTO corrected_row
    FROM public.markets m WHERE m.id = target.id;
    IF corrected_row->>'chain' IS DISTINCT FROM target.new_chain
      OR (corrected_row - 'chain' - 'updated_at') IS DISTINCT FROM (previous_row - 'chain' - 'updated_at')
    THEN
      RAISE EXCEPTION 'Unexpected change to market %; correction aborted', target.id;
    END IF;
  END LOOP;

  SELECT md5(string_agg(to_jsonb(m)::text, E'\n' ORDER BY m.id))
  INTO other_markets_after
  FROM public.markets m WHERE NOT (m.id = ANY(target_ids));
  IF other_markets_after IS DISTINCT FROM other_markets_before THEN
    RAISE EXCEPTION 'A market outside the approved IDs changed; correction aborted';
  END IF;
END;
$market_chain_correction$;

SELECT id, name, chain, banner, subgroup, is_active
FROM public.markets
WHERE id IN ('65165', '534', '16783', '23765', '20507', '70368',
             '72285', '75131', '72283', '649', '68462')
ORDER BY chain, name;
COMMIT;
