import "server-only";
import postgres from "postgres";

declare global {
  // Reuse a single small client during local development hot reloads.
  // Vercel instances keep this client warm between invocations when possible.
  // eslint-disable-next-line no-var
  var fresherFlowSql: ReturnType<typeof postgres> | undefined;
}

export function getDb() {
  const connectionString = process.env.DATABASE_URL;
  if (!connectionString) {
    throw new Error("DATABASE_URL is missing. Add your Neon pooled connection string.");
  }

  if (!globalThis.fresherFlowSql) {
    globalThis.fresherFlowSql = postgres(connectionString, {
      // Use Neon's pooled (-pooler) endpoint for Vercel/serverless workloads.
      max: 1,
      idle_timeout: 10,
      connect_timeout: 15,
      prepare: false,
      ssl: "require",
    });
  }

  return globalThis.fresherFlowSql;
}