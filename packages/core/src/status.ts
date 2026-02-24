import { sql } from "drizzle-orm";
import {
  LAST_VERIFIED_AT_META_KEY,
  LAST_VERIFIED_OK_META_KEY,
  getMetaValue,
  listUserTables,
  type Database,
} from "./db";
import {
  getCommitCount,
  getHeadCommit,
} from "./commit";
import { estimateCommitSizeBytes, estimateHistorySizeBytes } from "./history";
import { countRows } from "./inspect";
import { SnapshotTable, type CommitKind } from "./schema";
import { syncStatus, type SyncStatus } from "./sync";

export interface StorageEstimate {
  commitCount: number;
  estimatedHistoryBytes: number;
  latestCommitEstimatedBytes: number | null;
}

export interface StatusTable {
  name: string;
  count: number;
}

export interface Status {
  dbPath: string;
  tableCount: number;
  tables: StatusTable[];
  headCommit: {
    commitId: string;
    seq: number;
    kind: CommitKind;
    message: string;
    createdAt: number;
  } | null;
  snapshotCount: number;
  lastVerifiedAt: string | null;
  lastVerifiedOk: boolean | null;
  sync: SyncStatus;
  storage: StorageEstimate;
}

export function status(db: Database): Status {
  const tables = listUserTables(db).map((table) => ({
    name: table,
    count: countRows(db, table),
  }));

  const head = getHeadCommit(db);
  const snapshotCountRow = db.select({ c: sql<number>`count(*)` }).from(SnapshotTable).get();
  const verifiedOkRaw = getMetaValue(db, LAST_VERIFIED_OK_META_KEY);
  const commitCount = getCommitCount(db);
  const latestCommitEstimatedBytes = head ? estimateCommitSizeBytes(db, head.commitId) : null;

  return {
    dbPath: db.$client.filename,
    tableCount: tables.length,
    tables,
    headCommit: head
      ? {
          commitId: head.commitId,
          seq: head.seq,
          kind: head.kind,
          message: head.message,
          createdAt: head.createdAt,
        }
      : null,
    snapshotCount: snapshotCountRow?.c ?? 0,
    lastVerifiedAt: getMetaValue(db, LAST_VERIFIED_AT_META_KEY),
    lastVerifiedOk: verifiedOkRaw === null ? null : verifiedOkRaw === "1",
    sync: syncStatus(db),
    storage: {
      commitCount,
      estimatedHistoryBytes: estimateHistorySizeBytes(db),
      latestCommitEstimatedBytes,
    },
  };
}
