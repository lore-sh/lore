import { sql } from "drizzle-orm";
import { headCommit } from "./commit";
import {
  LAST_VERIFIED_AT_META_KEY,
  LAST_VERIFIED_OK_META_KEY,
  getMetaValue,
  listUserTables,
  type Database,
} from "./db";
import { commitSize, historySize } from "./history";
import { countRows } from "./inspect";
import { CommitTable, SnapshotTable } from "./schema";
import { syncStatus } from "./sync";

export function status(db: Database) {
  const tables = listUserTables(db).map((table) => ({
    name: table,
    count: countRows(db, table),
  }));

  const head = headCommit(db);
  const snapshotCountRow = db.select({ c: sql<number>`count(*)` }).from(SnapshotTable).get();
  const verifiedOkRaw = getMetaValue(db, LAST_VERIFIED_OK_META_KEY);
  const commitCount = db.select({ n: sql<number>`count(*)` }).from(CommitTable).get()?.n ?? 0;
  const latestCommitEstimatedBytes = head ? commitSize(db, head.commitId) : null;

  return {
    dbPath: db.$client.filename,
    tableCount: tables.length,
    tables,
    headCommit: head,
    snapshotCount: snapshotCountRow?.c ?? 0,
    lastVerifiedAt: getMetaValue(db, LAST_VERIFIED_AT_META_KEY),
    lastVerifiedOk: verifiedOkRaw === null ? null : verifiedOkRaw === "1",
    sync: syncStatus(db),
    storage: {
      commitCount,
      estimatedHistoryBytes: historySize(db),
      latestCommitEstimatedBytes,
    },
  };
}
