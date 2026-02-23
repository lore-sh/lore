import type { Database } from "./engine/db";
import { executeReadSql } from "./engine/execute";
import { validateReadSql } from "./engine/sql";

export function query(db: Database, sqlInput: string): Record<string, unknown>[] {
  const sql = validateReadSql(sqlInput);
  return executeReadSql(db, sql);
}
