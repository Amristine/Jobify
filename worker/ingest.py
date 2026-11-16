"""Authorised JobSpy -> Gemini -> Neon PostgreSQL ingestion worker."""
from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import pandas as pd
import psycopg
from dotenv import load_dotenv
from google import genai
from jobspy import scrape_jobs
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

load_dotenv(Path(__file__).resolve().parents[1] / ".env.local")
load_dotenv()
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("fresherflow-ingest")

DATABASE_URL = os.environ["DATABASE_URL"]
GEMINI_KEY = os.environ["GEMINI_API_KEY"]
MODEL = os.getenv("GEMINI_MODEL") or "gemini-2.5-flash"
ai = genai.Client(api_key=GEMINI_KEY)
pool = ConnectionPool(
    conninfo=DATABASE_URL,
    min_size=0,
    max_size=2,
    timeout=20,
    kwargs={"sslmode": "require", "row_factory": dict_row, "prepare_threshold": None},
    open=True,
)

TERMS = [x.strip() for x in (os.getenv("JOB_SEARCH_TERMS") or "software engineer fresher,frontend developer fresher,backend developer fresher,data analyst fresher,AI ML intern,graduate engineer trainee").split(",") if x.strip()]
SOURCES = [x.strip().lower() for x in (os.getenv("JOB_SOURCES") or "indeed,ats").split(",") if x.strip()]
_ats_config = [x.strip() for x in os.getenv("ATS_COMPANIES", "").split(",") if x.strip()]
ATS_COMPANIES = [
    x for x in _ats_config
    if ":" in x and x.split(":", 1)[0].strip().lower() in {"greenhouse", "lever", "ashby", "smartrecruiters"}
    and x.split(":", 1)[1].strip()
]
if not ATS_COMPANIES:
    ATS_COMPANIES = ["lever:drivetrain", "lever:meesho", "lever:paytm"]
if os.getenv("ENABLE_LINKEDIN", "false").lower() == "true" and "linkedin" not in SOURCES:
    SOURCES.append("linkedin")
if "ats" in SOURCES:
    SOURCES = ["ats"] + [source for source in SOURCES if source != "ats"]
AUTHORIZED_PROXY_SOURCES = {x.strip().lower() for x in os.getenv("AUTHORIZED_PROXY_SOURCES", "").split(",") if x.strip()}
RESULTS_PER_SEARCH = max(1, min(int(os.getenv("RESULTS_PER_SEARCH") or "40"), 100))
MAX_JOBS = max(1, int(os.getenv("MAX_JOBS_PER_RUN") or "500"))


def clean(value) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return str(value).strip()


def canonical_url(value: str) -> str:
    value = clean(value)
    if not value:
        return ""
    parts = urlsplit(value)
    if parts.scheme not in ("http", "https") or not parts.netloc:
        return ""
    query = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True)
             if not k.lower().startswith("utm_") and k.lower() not in {"trk", "trackingid", "refid"}]
    return urlunsplit((parts.scheme.lower(), parts.netloc.lower(), parts.path.rstrip("/"), urlencode(query), ""))


def proxy_for(source: str):
    if source not in AUTHORIZED_PROXY_SOURCES:
        return None
    proxy = os.getenv(f"PROXY_{source.upper()}", "").strip()
    return [proxy] if proxy else None



def fetch_ats_jobs() -> pd.DataFrame:
    """Read public employer job-board APIs. Configure boards as provider:board-slug."""
    rows = []
    india_places = {
        "india", "bengaluru", "bangalore", "mumbai", "pune", "hyderabad",
        "chennai", "delhi", "gurugram", "gurgaon", "noida", "kolkata",
        "ahmedabad", "jaipur", "kochi", "coimbatore", "indore"
    }

    def get_json(url: str):
        request = urllib.request.Request(
            url, headers={"User-Agent": "FresherFlow/1.0 (public careers API reader)", "Accept": "application/json"}
        )
        with urllib.request.urlopen(request, timeout=25) as response:
            return json.loads(response.read().decode("utf-8"))

    for entry in ATS_COMPANIES:
        try:
            provider, board = entry.split(":", 1)
            provider, board = provider.strip().lower(), board.strip()
            if not board:
                raise ValueError("missing board slug")
            jobs = []
            if provider == "greenhouse":
                payload = get_json(f"https://boards-api.greenhouse.io/v1/boards/{board}/jobs?content=true")
                for item in payload.get("jobs", []):
                    jobs.append({
                        "title": item.get("title"), "company": board,
                        "location": (item.get("location") or {}).get("name", ""),
                        "job_url": item.get("absolute_url"), "description": item.get("content"),
                        "date_posted": item.get("updated_at"), "job_type": "Unknown"
                    })
            elif provider == "lever":
                payload = get_json(f"https://api.lever.co/v0/postings/{board}?mode=json")
                for item in payload if isinstance(payload, list) else []:
                    categories = item.get("categories") or {}
                    jobs.append({
                        "title": item.get("text"), "company": board,
                        "location": categories.get("location", ""),
                        "job_url": item.get("hostedUrl"), "description": item.get("descriptionPlain") or item.get("description"),
                        "date_posted": item.get("createdAt"), "job_type": categories.get("commitment", "Unknown")
                    })
            elif provider == "ashby":
                payload = get_json(f"https://api.ashbyhq.com/posting-api/job-board/{board}?includeCompensation=false")
                for item in payload.get("jobs", []):
                    jobs.append({
                        "title": item.get("title"), "company": board,
                        "location": item.get("location", ""),
                        "job_url": item.get("jobUrl"), "description": item.get("descriptionPlain") or item.get("description"),
                        "date_posted": item.get("publishedAt"), "job_type": item.get("employmentType", "Unknown")
                    })
            elif provider == "smartrecruiters":
                payload = get_json(f"https://api.smartrecruiters.com/v1/companies/{board}/postings?limit=100")
                for item in payload.get("content", []):
                    loc = item.get("location") or {}
                    jobs.append({
                        "title": item.get("name"), "company": board,
                        "location": ", ".join(x for x in [loc.get("city"), loc.get("region"), loc.get("country")] if x),
                        "job_url": (item.get("ref") or item.get("applyUrl")),
                        "description": "", "date_posted": item.get("releasedDate"), "job_type": "Unknown"
                    })
            else:
                log.warning("Unsupported ATS provider %r in ATS_COMPANIES; skipping", provider)
                continue

            for job in jobs:
                loc = clean(job.get("location")).lower()
                if loc and not any(place in loc for place in india_places):
                    continue
                if not loc:
                    continue
                job["company"] = job.get("company") or board
                rows.append(job)
            log.info("ATS board %s:%s returned %s India-location postings", provider, board, sum(1 for j in jobs if clean(j.get("location")).lower() and any(p in clean(j.get("location")).lower() for p in india_places)))
        except Exception as exc:
            log.warning("Could not read ATS board %s: %s", entry, str(exc)[:500])
    return pd.DataFrame(rows)


def classify(job: dict) -> dict:
    description = clean(job.get("description")) or "No description provided by source. Classify conservatively using only title, company, and location."
    prompt = f"""
You classify Indian job postings for a fresher job board. Return ONLY valid JSON.
Do not infer facts that are not in the posting. Mark fresher_eligible true only if
the posting explicitly targets interns, fresh graduates, entry-level candidates,
0-2 years experience, graduate trainees, or equivalent. If unclear, false.
Allowed role_category values: Software Engineering, Frontend Development,
Backend Development, AI & Machine Learning, Data & Analytics, UI/UX Design,
Business & Sales, QA & Testing, DevOps & Cloud, Other.
Allowed job_type values: Internship, Full-time, Contract, Part-time, Unknown.
Return keys: role_category, city, job_type, experience_level, fresher_eligible,
skills (array of up to 8 strings), confidence (0 to 1).
POSTING:
Title: {job.get('title','')}
Company: {job.get('company','')}
Location: {job.get('location_text','')}
Description: {description[:7000]}
"""
    response = ai.models.generate_content(
        model=MODEL,
        contents=prompt,
        config={"response_mime_type": "application/json"},
    )
    parsed = json.loads(response.text or "{}")
    return {
        "role_category": clean(parsed.get("role_category")) or "Other",
        "city": clean(parsed.get("city")) or job.get("city") or None,
        "job_type": clean(parsed.get("job_type")) or job.get("job_type") or "Unknown",
        "experience_level": clean(parsed.get("experience_level")) or "Fresher",
        "is_fresher_friendly": parsed.get("fresher_eligible") is True,
        "skills": [clean(x) for x in parsed.get("skills", []) if clean(x)][:8],
        "ai_confidence": max(0.0, min(float(parsed.get("confidence", 0.0)), 1.0)),
    }


def to_iso(value):
    if not value:
        return None
    try:
        stamp = pd.to_datetime(value, utc=True, errors="coerce")
        return stamp.to_pydatetime() if not pd.isna(stamp) else None
    except Exception:
        return None


def normalise_row(row, source: str) -> dict | None:
    title, company = clean(row.get("title")), clean(row.get("company"))
    url = canonical_url(row.get("job_url"))
    if not title or not company or not url:
        return None
    location = clean(row.get("location"))
    bits = [x.strip() for x in re.split(r"[,|]", location) if x.strip()]

    def number_or_none(value):
        try:
            return float(value) if pd.notna(value) else None
        except (TypeError, ValueError):
            return None

    return {
        "title": title[:500],
        "company": company[:300],
        "location_text": location[:500] or None,
        "city": bits[0][:150] if bits else None,
        "state": bits[1][:150] if len(bits) > 1 else None,
        "country": "India",
        "job_type": clean(row.get("job_type")) or "Unknown",
        "description": clean(row.get("description"))[:12000] or None,
        "source_name": source,
        "source_url": url,
        "source_posted_at": to_iso(row.get("date_posted")),
        "salary_min": number_or_none(row.get("min_amount")),
        "salary_max": number_or_none(row.get("max_amount")),
        "salary_currency": clean(row.get("currency")) or "INR",
        "salary_period": clean(row.get("interval")) or None,
        "content_hash": hashlib.sha256((title.lower() + "|" + company.lower() + "|" + location.lower()).encode()).hexdigest(),
    }


def main():
    started = datetime.now(timezone.utc)
    fetched = inserted = updated = skipped = errors = input_tokens = output_tokens = 0
    source_results = []
    seen_urls = set()
    ai_quota_exhausted = False

    with pool.connection() as conn:
        run_id = conn.execute(
            "INSERT INTO ingestion_runs (started_at, status, sources) VALUES (%s, 'running', %s) RETURNING id",
            (started, Jsonb(SOURCES)),
        ).fetchone()["id"]
        conn.commit()

    try:
        for source in SOURCES:
            if ai_quota_exhausted:
                log.warning("Stopping remaining sources because Gemini quota is exhausted; rerun after quota resets or upgrade billing.")
                break
            source_count = 0
            source_error = None
            for term_index, term in enumerate(TERMS):
                if source == "ats" and term_index > 0:
                    break
                if fetched >= MAX_JOBS:
                    break
                try:
                    if source == "ats":
                        if not ATS_COMPANIES:
                            log.warning("ATS source selected but ATS_COMPANIES is empty; configure provider:board-slug entries")
                            break
                        frame = fetch_ats_jobs()
                    else:
                        kwargs = {
                            "site_name": [source],
                            "search_term": term,
                            "location": "India",
                            "results_wanted": min(RESULTS_PER_SEARCH, MAX_JOBS - fetched),
                            "hours_old": 168,
                            "linkedin_fetch_description": False,
                        }
                        if source == "indeed":
                            kwargs["country_indeed"] = "India"
                        proxy = proxy_for(source)
                        if proxy:
                            kwargs["proxies"] = proxy
                        frame = scrape_jobs(**kwargs)
                    if frame is None or frame.empty:
                        continue
                    for _, series in frame.iterrows():
                        fetched += 1
                        base = normalise_row(series.to_dict(), source)
                        if not base or base["source_url"] in seen_urls:
                            skipped += 1
                            continue
                        seen_urls.add(base["source_url"])
                        try:
                            ai_fields = classify(base)
                            input_tokens += max(1, (len(base.get("description") or "") + len(base["title"]) + 800) // 4)
                            output_tokens += 100
                            record = {**base, **ai_fields}
                            if not record["is_fresher_friendly"]:
                                skipped += 1
                                continue
                            with pool.connection() as conn:
                                prior = conn.execute("SELECT id FROM jobs WHERE source_url = %s", (base["source_url"],)).fetchone()
                                conn.execute(
                                    """
                                    INSERT INTO jobs (
                                      title, company, city, state, country, location_text, job_type,
                                      role_category, experience_level, salary_min, salary_max,
                                      salary_currency, salary_period, description, skills, source_name,
                                      source_url, source_posted_at, is_fresher_friendly, is_active,
                                      ai_confidence, content_hash, collected_at, updated_at
                                    ) VALUES (
                                      %(title)s, %(company)s, %(city)s, %(state)s, %(country)s, %(location_text)s,
                                      %(job_type)s, %(role_category)s, %(experience_level)s, %(salary_min)s,
                                      %(salary_max)s, %(salary_currency)s, %(salary_period)s, %(description)s,
                                      %(skills)s, %(source_name)s, %(source_url)s, %(source_posted_at)s,
                                      %(is_fresher_friendly)s, true, %(ai_confidence)s, %(content_hash)s, now(), now()
                                    )
                                    ON CONFLICT (source_url) DO UPDATE SET
                                      title = EXCLUDED.title, company = EXCLUDED.company,
                                      city = EXCLUDED.city, state = EXCLUDED.state,
                                      location_text = EXCLUDED.location_text, job_type = EXCLUDED.job_type,
                                      role_category = EXCLUDED.role_category, experience_level = EXCLUDED.experience_level,
                                      salary_min = EXCLUDED.salary_min, salary_max = EXCLUDED.salary_max,
                                      salary_currency = EXCLUDED.salary_currency, salary_period = EXCLUDED.salary_period,
                                      description = EXCLUDED.description, skills = EXCLUDED.skills,
                                      source_posted_at = EXCLUDED.source_posted_at,
                                      is_fresher_friendly = EXCLUDED.is_fresher_friendly,
                                      ai_confidence = EXCLUDED.ai_confidence, content_hash = EXCLUDED.content_hash,
                                      is_active = true, collected_at = now(), updated_at = now()
                                    """,
                                    {**record, "skills": record["skills"]},
                                )
                                conn.commit()
                            if prior:
                                updated += 1
                            else:
                                inserted += 1
                            source_count += 1
                        except Exception as exc:
                            errors += 1
                            if "429" in str(exc) or "RESOURCE_EXHAUSTED" in str(exc) or "quota" in str(exc).lower():
                                ai_quota_exhausted = True
                                log.error("Gemini quota exhausted; stopping classification to avoid repeated failed requests.")
                                break
                            log.exception("Could not classify/store one listing from %s: %s", source, exc)
                except Exception as exc:
                    errors += 1
                    source_error = str(exc)[:1000]
                    log.exception("Source %s failed for search term %r", source, term)
                    # A blocked source should not generate hundreds of identical failures.
                    if "403" in str(exc) or "blocked" in str(exc).lower() or "forbidden" in str(exc).lower():
                        log.warning("Stopping source %s after an access-denied response; use an authorised feed/API instead.", source)
                    break
            source_results.append({"source": source, "saved": source_count, "error": source_error})

        status = "failed" if errors and not (inserted + updated) else ("partial" if errors else "success")
        with pool.connection() as conn:
            conn.execute(
                """
                UPDATE ingestion_runs SET finished_at = %s, status = %s, sources = %s,
                  fetched_count = %s, inserted_count = %s, updated_count = %s,
                  skipped_count = %s, error_count = %s, estimated_ai_input_tokens = %s,
                  estimated_ai_output_tokens = %s, error_summary = %s
                WHERE id = %s
                """,
                (datetime.now(timezone.utc), status, Jsonb(source_results), fetched,
                 inserted, updated, skipped, errors, input_tokens, output_tokens,
                 json.dumps(source_results)[:5000] if errors else None, run_id),
            )
            conn.commit()
        log.info("Ingestion complete: fetched=%s inserted=%s updated=%s skipped=%s errors=%s", fetched, inserted, updated, skipped, errors)
        if errors and not (inserted + updated):
            sys.exit(1)
    except Exception as exc:
        with pool.connection() as conn:
            conn.execute(
                "UPDATE ingestion_runs SET finished_at = %s, status = 'failed', error_count = %s, error_summary = %s WHERE id = %s",
                (datetime.now(timezone.utc), errors + 1, str(exc)[:5000], run_id),
            )
            conn.commit()
        raise
    finally:
        pool.close()


if __name__ == "__main__":
    main()
