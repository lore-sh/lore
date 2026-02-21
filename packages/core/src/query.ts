import { withInitializedDatabase } from "./engine/db";
import { executeReadSql } from "./engine/execute";
import { validateReadSql } from "./engine/sql";

export function readQuery(sqlInput: string): Record<string, unknown>[] {
  const sql = validateReadSql(sqlInput);
  return withInitializedDatabase(({ db }) => {
    return executeReadSql(db, sql);
  });
}
