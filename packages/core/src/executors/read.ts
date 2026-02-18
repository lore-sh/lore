import type { Database } from "bun:sqlite";

export function executeReadSql(db: Database, sql: string): Record<string, unknown>[] {
  const statement = db.query(sql);
  return statement.all() as Record<string, unknown>[];
}
