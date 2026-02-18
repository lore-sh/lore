import { z } from "zod";
import { TossError } from "../errors";

const sqlInputSchema = z.string().trim().min(1, "SQL must not be empty");

const FORBIDDEN_KEYWORDS = [
  "INSERT",
  "UPDATE",
  "DELETE",
  "DROP",
  "ALTER",
  "CREATE",
  "REPLACE",
  "TRUNCATE",
  "ATTACH",
  "DETACH",
  "PRAGMA",
  "VACUUM",
  "BEGIN",
  "COMMIT",
  "ROLLBACK",
] as const;

function stripStringLiterals(sql: string): string {
  return sql
    .replace(/'([^']|'')*'/g, "''")
    .replace(/\"([^\"\\]|\\.)*\"/g, '""')
    .replace(/`([^`]|``)*`/g, "``")
    .replace(/--.*$/gm, "")
    .replace(/\/\*[\s\S]*?\*\//g, "");
}

export function validateReadSql(inputSql: string): string {
  const parsed = sqlInputSchema.safeParse(inputSql);
  if (!parsed.success) {
    throw new TossError("INVALID_SQL", parsed.error.issues.map((issue) => issue.message).join("; "));
  }

  let sql = parsed.data.trim();
  if (sql.endsWith(";")) {
    sql = sql.slice(0, -1).trim();
  }

  const stripped = stripStringLiterals(sql);
  if (stripped.includes(";")) {
    throw new TossError("INVALID_SQL", "Multiple SQL statements are not allowed");
  }

  const upper = stripped.trim().toUpperCase();
  if (!(upper.startsWith("SELECT") || upper.startsWith("WITH"))) {
    throw new TossError("INVALID_SQL", "Only SELECT / WITH ... SELECT queries are allowed");
  }

  for (const keyword of FORBIDDEN_KEYWORDS) {
    const regex = new RegExp(`\\b${keyword}\\b`, "i");
    if (regex.test(stripped)) {
      throw new TossError("INVALID_SQL", `Forbidden keyword in read-only query: ${keyword}`);
    }
  }

  return sql;
}
