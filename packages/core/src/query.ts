import { assertInitialized, closeDatabase, openDatabase } from "./db";
import { executeReadSql } from "./executors/read";
import type { DatabaseOptions } from "./types";
import { validateReadSql } from "./validators/sql";

export function readQuery(sqlInput: string, options: DatabaseOptions = {}): Record<string, unknown>[] {
  const sql = validateReadSql(sqlInput);
  const { db, dbPath } = openDatabase(options.dbPath);
  try {
    assertInitialized(db, dbPath);
    return executeReadSql(db, sql);
  } finally {
    closeDatabase(db);
  }
}
