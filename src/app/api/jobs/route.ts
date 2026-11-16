import { NextRequest, NextResponse } from "next/server";
import { getSupabaseAdmin } from "@/lib/supabase-admin";

export const dynamic = "force-dynamic";

export async function GET(request: NextRequest) {
  try {
    const params = request.nextUrl.searchParams;
    const search = params.get("q")?.trim();
    const city = params.get("city");
    const type = params.get("type");
    const role = params.get("role");
    const limit = Math.min(Math.max(Number(params.get("limit") || 100), 1), 250);

    let query = getSupabaseAdmin()
      .from("jobs")
      .select("id,title,company,city,state,country,location_text,job_type,role_category,experience_level,salary_min,salary_max,salary_currency,salary_period,description,skills,source_name,source_url,source_posted_at,collected_at,ai_confidence")
      .eq("is_active", true)
      .eq("is_fresher_friendly", true)
      .order("source_posted_at", { ascending: false, nullsFirst: false })
      .limit(limit);

    if (city && city !== "All cities") query = query.ilike("city", city);
    if (type && type !== "All types") query = query.ilike("job_type", type);
    if (role && role !== "All opportunities") query = query.ilike("role_category", role);
    if (search) {
      const safe = search.replace(/[,%()]/g, " ").trim();
      if (safe) query = query.or(`title.ilike.%${safe}%,company.ilike.%${safe}%,city.ilike.%${safe}%,role_category.ilike.%${safe}%`);
    }

    const { data, error } = await query;
    if (error) throw error;
    return NextResponse.json({ jobs: data ?? [], count: data?.length ?? 0, source: "supabase" });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unable to load jobs";
    const status = message.includes("not configured") ? 503 : 500;
    return NextResponse.json({ error: message, jobs: [] }, { status });
  }
}
