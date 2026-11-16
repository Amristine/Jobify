import { NextRequest, NextResponse } from "next/server";
import { getDb } from "@/lib/db";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

export async function GET(request: NextRequest) {
  try {
    const params = request.nextUrl.searchParams;
    const search = params.get("q")?.trim();
    const city = params.get("city");
    const type = params.get("type");
    const role = params.get("role");
    const limit = Math.min(Math.max(Number(params.get("limit") || 100), 1), 250);
    const db = getDb();

    const conditions = [
      db`is_active = true`,
      db`is_fresher_friendly = true`,
    ];

    if (city && city !== "All cities") conditions.push(db`city ILIKE ${city}`);
    if (type && type !== "All types") conditions.push(db`job_type ILIKE ${type}`);
    if (role && role !== "All opportunities") conditions.push(db`role_category ILIKE ${role}`);
    if (search) {
      const term = `%${search.replace(/[\\%_]/g, " ").trim()}%`;
      conditions.push(db`(title ILIKE ${term} OR company ILIKE ${term} OR city ILIKE ${term} OR role_category ILIKE ${term})`);
    }

    const jobs = await db`
      SELECT id, title, company, city, state, country, location_text,
             job_type, role_category, experience_level, salary_min, salary_max,
             salary_currency, salary_period, description, skills, source_name,
             source_url, source_posted_at, collected_at, ai_confidence
      FROM jobs
      WHERE ${db`.join(conditions, db` AND `)`}
      ORDER BY source_posted_at DESC NULLS LAST, collected_at DESC
      LIMIT ${limit}
    `;

    return NextResponse.json({ jobs, count: jobs.length, source: "neon" });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unable to load jobs";
    console.error("GET /api/jobs failed:", message);
    return NextResponse.json(
      { error: "Job listings are temporarily unavailable. Please retry shortly.", jobs: [] },
      { status: 503 },
    );
  }
}