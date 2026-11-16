# FresherFlow

Fresher-first job discovery app: Next.js frontend, Supabase jobs API, and Python ingestion worker using JobSpy + Gemini.

## Included
- Responsive job search, filters, bookmarks and job cards
- GET /api/jobs reads active fresher-friendly listings from Supabase
- Supabase schema for jobs, ingestion runs and manual quality reviews
- Python worker: authorised JobSpy sources, Gemini enrichment, URL-based deduplication
- Daily GitHub Actions workflow at 02:30 UTC and manual dispatch
- Per-source proxy configuration, gated by AUTHORIZED_PROXY_SOURCES

## Supabase setup
Create a dedicated FresherFlow project; do not reuse the unrelated ali-crm database. Run supabase/migrations/202611160001_fresherflow_core.sql in its SQL Editor.

Set SUPABASE_URL and SUPABASE_SECRET_KEY for the Next.js server. Set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_SECRET_KEY) for the Python worker. Never expose either secret key in browser code or commit secrets.

## Local setup
1. Copy .env.example to .env.local and fill the server-side values.
2. Run npm install.
3. Run npm run dev and open http://localhost:3000.
4. Install Python dependencies with pip install -r worker/requirements.txt.
5. Run the worker from the worker directory with the environment variables loaded: python ingest.py.

## GitHub Actions configuration
Repository Settings > Secrets and variables > Actions:
Secrets: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, GEMINI_API_KEY; optional PROXY_INDEED, PROXY_NAUKRI, PROXY_GLASSDOOR, PROXY_LINKEDIN.
Variables: GEMINI_MODEL, JOB_SOURCES, JOB_SEARCH_TERMS, ENABLE_LINKEDIN, AUTHORIZED_PROXY_SOURCES, RESULTS_PER_SEARCH.
The daily workflow can also be run manually from Actions.

## Source access and proxy policy
Enable only sources for which automated collection is expressly permitted. LinkedIn is disabled unless explicitly enabled. A proxy is used only when the source is named in AUTHORIZED_PROXY_SOURCES and the corresponding PROXY_SOURCE variable is configured. No proxy rotation or retry is performed to defeat blocks, CAPTCHAs, authentication, or rate limits. JobSpy's MIT licence does not grant rights to third-party job-board content.

## Evaluation
Use job_reviews to hand-check 50 records. Role accuracy = correct role labels / reviewed role labels. City accuracy = correct city labels / reviewed city labels. Fresher precision = correct fresher-eligible inclusions / reviewed inclusions. Calculate cost per 1,000 from actual Gemini billing; ingestion_runs token counts are estimates only.

## Verification before calling it live
Apply the migration to the dedicated project, configure keys, run npm run build, run one manual ingestion, verify jobs and ingestion_runs, check GET /api/jobs, then run the GitHub Actions workflow manually. The frontend intentionally falls back to clearly labelled demo listings until live data is available.