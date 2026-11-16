-- FresherFlow schema for Neon PostgreSQL.
-- Run once in the Neon SQL Editor or with a DIRECT (non-pooled) connection.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS public.jobs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  title text NOT NULL,
  company text NOT NULL,
  city text,
  state text,
  country text NOT NULL DEFAULT 'India',
  location_text text,
  job_type text,
  role_category text,
  experience_level text DEFAULT 'Fresher',
  salary_min numeric,
  salary_max numeric,
  salary_currency text DEFAULT 'INR',
  salary_period text,
  description text,
  skills text[] NOT NULL DEFAULT '{}',
  source_name text NOT NULL,
  source_url text NOT NULL UNIQUE,
  source_posted_at timestamptz,
  collected_at timestamptz NOT NULL DEFAULT now(),
  expires_at timestamptz,
  is_fresher_friendly boolean NOT NULL DEFAULT false,
  is_active boolean NOT NULL DEFAULT true,
  ai_confidence numeric CHECK (ai_confidence BETWEEN 0 AND 1),
  content_hash text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS jobs_active_posted_idx ON public.jobs (is_active, source_posted_at DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS jobs_city_idx ON public.jobs (lower(city));
CREATE INDEX IF NOT EXISTS jobs_role_idx ON public.jobs (role_category);
CREATE INDEX IF NOT EXISTS jobs_type_idx ON public.jobs (job_type);
CREATE INDEX IF NOT EXISTS jobs_skills_idx ON public.jobs USING gin (skills);

CREATE TABLE IF NOT EXISTS public.ingestion_runs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  started_at timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz,
  status text NOT NULL DEFAULT 'running' CHECK (status IN ('running','success','partial','failed')),
  sources jsonb NOT NULL DEFAULT '[]'::jsonb,
  fetched_count integer NOT NULL DEFAULT 0,
  inserted_count integer NOT NULL DEFAULT 0,
  updated_count integer NOT NULL DEFAULT 0,
  skipped_count integer NOT NULL DEFAULT 0,
  error_count integer NOT NULL DEFAULT 0,
  error_summary text,
  estimated_ai_input_tokens bigint NOT NULL DEFAULT 0,
  estimated_ai_output_tokens bigint NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS public.job_reviews (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  job_id uuid REFERENCES public.jobs(id) ON DELETE CASCADE,
  reviewer_label text,
  checked_at timestamptz NOT NULL DEFAULT now(),
  is_relevant boolean NOT NULL,
  role_correct boolean,
  city_correct boolean,
  fresher_correct boolean,
  notes text
);

CREATE OR REPLACE FUNCTION public.touch_job_updated_at()
RETURNS trigger LANGUAGE plpgsql SET search_path = ''
AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS jobs_touch_updated_at ON public.jobs;
CREATE TRIGGER jobs_touch_updated_at
BEFORE UPDATE ON public.jobs
FOR EACH ROW EXECUTE FUNCTION public.touch_job_updated_at();
