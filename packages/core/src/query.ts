import { withInitializedDatabase } from "./db";
import { executeReadSql } from "./executors/read";
import { validateReadSql } from "./validators/sql";

export function readQuery(sqlInput: string): Record<string, unknown>[] {
  const sql = validateReadSql(sqlInput);
  return withInitializedDatabase(({ db }) => {
    return executeReadSql(db, sql);
  });
}
