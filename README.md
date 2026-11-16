# FresherFlow

Fresher-first job discovery app: Next.js frontend, Neon PostgreSQL jobs API, and a Python ingestion worker using JobSpy + Gemini.

## Included
- Responsive job search, filters, bookmarks and job cards
- GET /api/jobs reads active fresher-friendly listings from Neon
- PostgreSQL schema for jobs, ingestion runs and manual quality reviews
- Python worker: authorised JobSpy sources plus public Greenhouse, Lever, Ashby and SmartRecruiters job-board APIs; Gemini enrichment and URL-based deduplication
- Daily GitHub Actions workflow at 02:30 UTC and manual dispatch
- Neon pooled connections for Vercel serverless functions and short-lived worker connections

## Database setup (Neon)
1. Create a Neon project and database.
2. In Neon Console, open **Connect**, select your branch/database/role, and turn **Connection pooling** on.
3. Copy the pooled connection string (the hostname must contain `-pooler`).
4. Run `db/schema.sql` once against the database using the Neon SQL Editor or a direct (non-pooled) connection.
5. Add the pooled connection string as `DATABASE_URL` in Vercel and GitHub Actions secrets.

Do not reuse an unrelated CRM database. Never commit connection strings or expose them through a `NEXT_PUBLIC_` variable.

## Local setup
1. Copy `.env.example` to `.env.local` and set `DATABASE_URL` and `GEMINI_API_KEY`.
2. Run `npm install`, then `npm run dev`.
3. Install worker dependencies: `pip install -r worker/requirements.txt`.
4. Run `python ingest.py` from the `worker` directory.

## GitHub Actions configuration
Repository Settings → Secrets and variables → Actions:
- Secrets: `DATABASE_URL`, `GEMINI_API_KEY`; optional `PROXY_INDEED`, `PROXY_NAUKRI`, `PROXY_GLASSDOOR`, `PROXY_LINKEDIN`.
- Variables: `GEMINI_MODEL`, `JOB_SOURCES` (start with `indeed,ats`), `ATS_COMPANIES`, `JOB_SEARCH_TERMS`, `ENABLE_LINKEDIN`, `AUTHORIZED_PROXY_SOURCES`, `RESULTS_PER_SEARCH`, `MAX_JOBS_PER_RUN`.

`ATS_COMPANIES` is a comma-separated list of public employer board identifiers in `provider:slug` form, for example `greenhouse:companyslug,lever:companyslug,ashby:boardname,smartrecruiters:companyslug`. Only add company boards whose public job-board endpoints and reuse terms permit automated access. The worker filters to India-located postings, deduplicates by canonical apply URL, and classifies fresher eligibility with Gemini. It does not bypass access controls.

The workflow runs daily at 02:30 UTC (08:00 IST) and can also be run manually from Actions.

## Neon connection behaviour
Neon Free compute suspends after 5 minutes without activity. Stored data remains available; the next connection automatically wakes the compute. Vercel uses Neon's pooled endpoint and a small client pool to avoid opening excessive connections. The first request after an idle period can be slightly slower. The API returns a retryable 503 instead of exposing database connection details.

## Source access and proxy policy
Enable only sources for which automated collection is expressly permitted. LinkedIn is disabled unless explicitly enabled. A proxy is used only when the source is named in `AUTHORIZED_PROXY_SOURCES` and the corresponding `PROXY_SOURCE` variable is configured. No proxy rotation or retry is performed to defeat blocks, CAPTCHAs, authentication, or rate limits. JobSpy's MIT licence does not grant rights to third-party job-board content.

## Evaluation
Use `job_reviews` to hand-check 50 records. Role accuracy = correct role labels / reviewed role labels. City accuracy = correct city labels / reviewed city labels. Fresher precision = correct fresher-eligible inclusions / reviewed inclusions. Calculate cost per 1,000 using actual Gemini billing; ingestion run token counts are estimates only.

## Verification before calling it live
Run `npm run build`, apply `db/schema.sql`, add secrets, run one manual ingestion, verify jobs and ingestion_runs, check GET /api/jobs, then run the GitHub Actions workflow manually. The frontend intentionally falls back to clearly labelled demo listings until live data is available.
