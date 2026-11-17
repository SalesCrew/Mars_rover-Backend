-- Customer access: schema and authorization only; no accounts or business records are seeded.
SET LOCAL lock_timeout = '3s';
SET LOCAL statement_timeout = '20s';
DO $$ DECLARE role_check text; BEGIN
  -- Fail closed if the inspected role constraint has become missing or ambiguous.
  SELECT conname INTO STRICT role_check FROM pg_constraint
    WHERE conrelid='public.users'::regclass AND contype='c'
    AND pg_get_constraintdef(oid) LIKE '%role%';
  EXECUTE format('ALTER TABLE public.users DROP CONSTRAINT %I',role_check);
END $$;
ALTER TABLE public.users ADD CONSTRAINT users_role_check CHECK (role IN ('admin','gl','customer'));

CREATE TABLE public.customer_access (
  user_id uuid PRIMARY KEY REFERENCES public.users(id) ON DELETE CASCADE,
  email text NOT NULL UNIQUE,
  is_active boolean NOT NULL DEFAULT true,
  permissions jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(permissions)='object'),
  version integer NOT NULL DEFAULT 1,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE public.customer_access ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.customer_access FROM PUBLIC, anon, authenticated;
GRANT ALL ON public.customer_access TO service_role;
CREATE TABLE public.customer_access_audit (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  actor_id uuid NOT NULL,
  user_id uuid NOT NULL,
  before_data jsonb,
  after_data jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE public.customer_access_audit ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.customer_access_audit FROM PUBLIC, anon, authenticated;
GRANT ALL ON public.customer_access_audit TO service_role;

-- Atomic profile/access update, optimistic locking and an audit without credentials.
CREATE FUNCTION public.save_customer_access(p_actor_id uuid,p_user_id uuid,p_email text,
  p_first_name text,p_last_name text,p_is_active boolean,p_permissions jsonb,p_version integer)
RETURNS jsonb LANGUAGE plpgsql SECURITY INVOKER SET search_path='' AS $$
DECLARE old_access public.customer_access; new_access public.customer_access;
  raw jsonb; page text;
BEGIN
  IF NOT EXISTS(SELECT 1 FROM public.users WHERE id=p_actor_id AND role='admin') THEN
    RAISE EXCEPTION 'CUSTOMER_ADMIN_REQUIRED';
  END IF;
  IF jsonb_typeof(p_permissions) IS DISTINCT FROM 'object' OR length(trim(p_first_name))=0
    OR length(trim(p_last_name))=0 OR p_is_active IS NULL OR length(p_email)>254 THEN
    RAISE EXCEPTION 'CUSTOMER_INVALID_INPUT';
  END IF;
  FOR page,raw IN SELECT key,value FROM jsonb_each(p_permissions) LOOP
    IF page NOT IN ('dashboard','vorbesteller','fotos','markets','gebietsleiter','vorverkauf',
      'produktersatz','nara-incentive','fragebogen','zeiterfassung','produkte')
      OR jsonb_typeof(raw) IS DISTINCT FROM 'object'
      OR (raw->'view') IS DISTINCT FROM 'true'::jsonb
      OR jsonb_typeof(raw->'export') IS DISTINCT FROM 'boolean'
      OR (SELECT count(*) FROM jsonb_object_keys(raw))<>2 THEN
      RAISE EXCEPTION 'CUSTOMER_INVALID_PERMISSIONS';
    END IF;
  END LOOP;
  SELECT * INTO old_access FROM public.customer_access WHERE user_id=p_user_id FOR UPDATE;
  IF p_version IS NULL THEN
    IF old_access.user_id IS NOT NULL OR EXISTS(SELECT 1 FROM public.users WHERE id=p_user_id) THEN
      RAISE EXCEPTION 'CUSTOMER_CONFLICT';
    END IF;
    INSERT INTO public.users(id,role,first_name,last_name) VALUES(p_user_id,'customer',trim(p_first_name),trim(p_last_name));
    INSERT INTO public.customer_access(user_id,email,is_active,permissions)
      VALUES(p_user_id,lower(trim(p_email)),p_is_active,p_permissions) RETURNING * INTO new_access;
  ELSE
    IF old_access.user_id IS NULL OR old_access.version<>p_version THEN RAISE EXCEPTION 'CUSTOMER_CONFLICT'; END IF;
    IF old_access.email<>lower(trim(p_email)) OR NOT EXISTS(SELECT 1 FROM public.users WHERE id=p_user_id AND role='customer') THEN
      RAISE EXCEPTION 'CUSTOMER_INVALID_INPUT';
    END IF;
    UPDATE public.users SET first_name=trim(p_first_name),last_name=trim(p_last_name),updated_at=now() WHERE id=p_user_id;
    UPDATE public.customer_access SET is_active=p_is_active,permissions=p_permissions,version=version+1,updated_at=now()
      WHERE user_id=p_user_id RETURNING * INTO new_access;
  END IF;
  INSERT INTO public.customer_access_audit(actor_id,user_id,before_data,after_data)
    VALUES(p_actor_id,p_user_id,CASE WHEN old_access.user_id IS NULL THEN NULL ELSE to_jsonb(old_access) END,to_jsonb(new_access));
  RETURN to_jsonb(new_access)||jsonb_build_object('firstName',trim(p_first_name),'lastName',trim(p_last_name));
END $$;
REVOKE ALL ON FUNCTION public.save_customer_access(uuid,uuid,text,text,text,boolean,jsonb,integer) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION public.save_customer_access(uuid,uuid,text,text,text,boolean,jsonb,integer) TO service_role;

-- Existing authenticated-wide policies must not give customers a direct REST bypass.
-- A narrow private definer lookup avoids recursive users RLS; it exposes only a boolean
-- about the authenticated caller and never accepts a caller-selected user ID.
CREATE SCHEMA IF NOT EXISTS app_private;
CREATE FUNCTION app_private.app_has_internal_role() RETURNS boolean LANGUAGE sql STABLE
  SECURITY DEFINER SET search_path='' AS $$
  SELECT EXISTS(SELECT 1 FROM public.users WHERE id=(SELECT auth.uid()) AND role IN ('admin','gl'));
$$;
REVOKE ALL ON FUNCTION app_private.app_has_internal_role() FROM PUBLIC,anon;
GRANT USAGE ON SCHEMA app_private TO authenticated;
GRANT EXECUTE ON FUNCTION app_private.app_has_internal_role() TO authenticated;
-- Other managed storage tables have no permissive policies and already deny API reads.
-- Leave their schemas/policies under Supabase management.
DO $$ DECLARE t record; BEGIN
  FOR t IN SELECT n.nspname,c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE c.relkind IN ('r','p') AND c.relrowsecurity
    AND (n.nspname='public' OR (n.nspname='storage' AND c.relname='objects'))
  LOOP
    EXECUTE format('CREATE POLICY customer_backend_only ON %I.%I AS RESTRICTIVE FOR ALL TO authenticated USING ((SELECT app_private.app_has_internal_role())) WITH CHECK ((SELECT app_private.app_has_internal_role()))',t.nspname,t.relname);
  END LOOP;
END $$;
