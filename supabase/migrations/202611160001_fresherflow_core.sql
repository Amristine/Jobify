create extension if not exists pgcrypto;

create table if not exists public.jobs (
  id uuid primary key default gen_random_uuid(),
  title text not null,
  company text not null,
  city text,
  state text,
  country text not null default 'India',
  location_text text,
  job_type text,
  role_category text,
  experience_level text default 'Fresher',
  salary_min numeric,
  salary_max numeric,
  salary_currency text default 'INR',
  salary_period text,
  description text,
  skills text[] not null default '{}',
  source_name text not null,
  source_url text not null unique,
  source_posted_at timestamptz,
  collected_at timestamptz not null default now(),
  expires_at timestamptz,
  is_fresher_friendly boolean not null default false,
  is_active boolean not null default true,
  ai_confidence numeric check (ai_confidence between 0 and 1),
  content_hash text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists jobs_active_posted_idx on public.jobs (is_active, source_posted_at desc nulls last);
create index if not exists jobs_city_idx on public.jobs (lower(city));
create index if not exists jobs_role_idx on public.jobs (role_category);
create index if not exists jobs_type_idx on public.jobs (job_type);
create index if not exists jobs_skills_idx on public.jobs using gin (skills);

create table if not exists public.ingestion_runs (
  id uuid primary key default gen_random_uuid(),
  started_at timestamptz not null default now(),
  finished_at timestamptz,
  status text not null default 'running' check (status in ('running','success','partial','failed')),
  sources jsonb not null default '[]'::jsonb,
  fetched_count integer not null default 0,
  inserted_count integer not null default 0,
  updated_count integer not null default 0,
  skipped_count integer not null default 0,
  error_count integer not null default 0,
  error_summary text,
  estimated_ai_input_tokens bigint not null default 0,
  estimated_ai_output_tokens bigint not null default 0
);

create table if not exists public.job_reviews (
  id uuid primary key default gen_random_uuid(),
  job_id uuid references public.jobs(id) on delete cascade,
  reviewer_label text,
  checked_at timestamptz not null default now(),
  is_relevant boolean not null,
  role_correct boolean,
  city_correct boolean,
  fresher_correct boolean,
  notes text
);

alter table public.jobs enable row level security;
alter table public.ingestion_runs enable row level security;
alter table public.job_reviews enable row level security;

drop policy if exists "Public can read active fresher jobs" on public.jobs;
create policy "Public can read active fresher jobs"
  on public.jobs for select to anon, authenticated
  using (is_active = true and is_fresher_friendly = true);

grant select on public.jobs to anon, authenticated;
revoke all on public.ingestion_runs from anon, authenticated;
revoke all on public.job_reviews from anon, authenticated;

create or replace function public.touch_job_updated_at()
returns trigger
language plpgsql
set search_path = ''
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

drop trigger if exists jobs_touch_updated_at on public.jobs;
create trigger jobs_touch_updated_at
before update on public.jobs
for each row execute function public.touch_job_updated_at();
