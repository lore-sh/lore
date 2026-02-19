import { withInitializedDatabase } from "./db";
import { executeReadSql } from "./executors/read";
import type { DatabaseOptions } from "./types";
import { validateReadSql } from "./validators/sql";

export function readQuery(sqlInput: string, options: DatabaseOptions = {}): Record<string, unknown>[] {
  const sql = validateReadSql(sqlInput);
  return withInitializedDatabase(options, ({ db }) => {
    return executeReadSql(db, sql);
  });
}
