import type { Database } from "bun:sqlite";
import { executeReadSql } from "./engine/execute";
import { validateReadSql } from "./engine/sql";

export function readQuery(db: Database, sqlInput: string): Record<string, unknown>[] {
  const sql = validateReadSql(sqlInput);
  return executeReadSql(db, sql);
}
