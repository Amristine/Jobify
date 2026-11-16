"""Authorised JobSpy -> Gemini -> Supabase ingestion worker.

A source is queried once per search term. There is deliberately no retry or
proxy rotation after an access-denied/rate-limit response. Proxies are supplied
only for source names explicitly listed in AUTHORIZED_PROXY_SOURCES.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import sys
from datetime import datetime, timezone
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import pandas as pd
from dotenv import load_dotenv
from google import genai
from jobspy import scrape_jobs
from supabase import create_client

load_dotenv()
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("fresherflow-ingest")

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.getenv("SUPABASE_SECRET_KEY") or os.environ["SUPABASE_SERVICE_ROLE_KEY"]
GEMINI_KEY = os.environ["GEMINI_API_KEY"]
MODEL = os.getenv("GEMINI_MODEL") or "gemini-2.5-flash"
db = create_client(SUPABASE_URL, SUPABASE_KEY)
ai = genai.Client(api_key=GEMINI_KEY)

TERMS = [x.strip() for x in (os.getenv("JOB_SEARCH_TERMS") or "software engineer fresher,frontend developer fresher,backend developer fresher,data analyst fresher,AI ML intern,graduate engineer trainee").split(",") if x.strip()]
SOURCES = [x.strip().lower() for x in (os.getenv("JOB_SOURCES") or "indeed,naukri,glassdoor").split(",") if x.strip()]
if os.getenv("ENABLE_LINKEDIN", "false").lower() == "true" and "linkedin" not in SOURCES:
    SOURCES.append("linkedin")
AUTHORIZED_PROXY_SOURCES = {x.strip().lower() for x in os.getenv("AUTHORIZED_PROXY_SOURCES", "").split(",") if x.strip()}
RESULTS_PER_SEARCH = max(1, min(int(os.getenv("RESULTS_PER_SEARCH") or "40"), 100))
MAX_JOBS = max(1, int(os.getenv("MAX_JOBS_PER_RUN", "500")))

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

def classify(job: dict) -> dict:
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
Description: {job.get('description','')[:7000]}
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
        return stamp.isoformat() if not pd.isna(stamp) else None
    except Exception:
        return None

def normalise_row(row, source: str) -> dict | None:
    title, company = clean(row.get("title")), clean(row.get("company"))
    url = canonical_url(row.get("job_url"))
    if not title or not company or not url:
        return None
    location = clean(row.get("location"))
    bits = [x.strip() for x in re.split(r"[,|]", location) if x.strip()]
    amount_min = row.get("min_amount")
    amount_max = row.get("max_amount")
    def number_or_none(x):
        try:
            return float(x) if pd.notna(x) else None
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
        "salary_min": number_or_none(amount_min),
        "salary_max": number_or_none(amount_max),
        "salary_currency": clean(row.get("currency")) or "INR",
        "salary_period": clean(row.get("interval")) or None,
        "content_hash": hashlib.sha256((title.lower() + "|" + company.lower() + "|" + location.lower()).encode()).hexdigest(),
    }

def main():
    started = datetime.now(timezone.utc)
    run = db.table("ingestion_runs").insert({"started_at": started.isoformat(), "status": "running", "sources": SOURCES}).execute()
    run_id = run.data[0]["id"]
    fetched = inserted = updated = skipped = errors = input_tokens = output_tokens = 0
    source_results = []
    seen_urls = set()

    try:
        for source in SOURCES:
            source_count = 0
            source_error = None
            for term in TERMS:
                if fetched >= MAX_JOBS:
                    break
                try:
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
                            # Token counts are estimates for the cost worksheet, not billing data.
                            input_tokens += max(1, (len(base.get("description") or "") + len(base["title"]) + 800) // 4)
                            output_tokens += 100
                            record = {**base, **ai_fields}
                            if not record["is_fresher_friendly"]:
                                skipped += 1
                                continue
                            existing = db.table("jobs").select("id").eq("source_url", base["source_url"]).limit(1).execute()
                            db.table("jobs").upsert(record, on_conflict="source_url").execute()
                            if existing.data:
                                updated += 1
                            else:
                                inserted += 1
                            source_count += 1
                        except Exception as exc:
                            errors += 1
                            log.exception("Could not classify/store one listing from %s: %s", source, exc)
                except Exception as exc:
                    errors += 1
                    source_error = str(exc)[:1000]
                    # Intentionally do not retry or rotate proxy on access errors / rate limits.
                    log.exception("Source %s failed for search term %r", source, term)
                    break
            source_results.append({"source": source, "saved": source_count, "error": source_error})
        status = "failed" if errors and not (inserted + updated) else ("partial" if errors else "success")
        db.table("ingestion_runs").update({
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "status": status,
            "sources": source_results,
            "fetched_count": fetched,
            "inserted_count": inserted,
            "updated_count": updated,
            "skipped_count": skipped,
            "error_count": errors,
            "estimated_ai_input_tokens": input_tokens,
            "estimated_ai_output_tokens": output_tokens,
            "error_summary": json.dumps(source_results)[:5000] if errors else None,
        }).eq("id", run_id).execute()
        log.info("Ingestion complete: fetched=%s inserted=%s updated=%s skipped=%s errors=%s", fetched, inserted, updated, skipped, errors)
        if errors and not (inserted + updated):
            sys.exit(1)
    except Exception as exc:
        db.table("ingestion_runs").update({
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "status": "failed",
            "error_count": errors + 1,
            "error_summary": str(exc)[:5000],
        }).eq("id", run_id).execute()
        raise

if __name__ == "__main__":
    main()
