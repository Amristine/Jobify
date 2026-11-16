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

    const limit = Math.min(
      Math.max(Number(params.get("limit") || 100), 1),
      250
    );

    const db = getDb();

    const conditions = [
      "is_active = true",
      "is_fresher_friendly = true",
    ];

    const values: (string | number)[] = [];

    const addFilter = (column: string, value: string) => {
      values.push(value);
      conditions.push(`${column} ILIKE $${values.length}`);
    };

    if (city && city !== "All cities") {
      addFilter("city", city);
    }

    if (type && type !== "All types") {
      addFilter("job_type", type);
    }

    if (role && role !== "All opportunities") {
      addFilter("role_category", role);
    }

    if (search) {
      values.push(`%${search.replace(/[%_\\]/g, " ").trim()}%`);
      const p = `$${values.length}`;

      conditions.push(
        `(title ILIKE ${p} OR company ILIKE ${p} OR city ILIKE ${p} OR role_category ILIKE ${p})`
      );
    }

    values.push(limit);

    const query = `
      SELECT id, title, company, city, state, country, location_text,
             job_type, role_category, experience_level, salary_min, salary_max,
             salary_currency, salary_period, description, skills, source_name,
             source_url, source_posted_at, collected_at, ai_confidence
      FROM jobs
      WHERE ${conditions.join(" AND ")}
      ORDER BY source_posted_at DESC NULLS LAST, collected_at DESC
      LIMIT $${values.length}
    `;

    const jobs = await db.unsafe(query, values);

    return NextResponse.json({
      jobs,
      count: jobs.length,
      source: "neon",
    });
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Unable to load jobs";

    console.error("GET /api/jobs failed:", message);

    return NextResponse.json(
      {
        error: "Job listings are temporarily unavailable. Please retry shortly.",
        jobs: [],
      },
      { status: 503 }
    );
  }
}
