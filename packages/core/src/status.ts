import type { Database } from "bun:sqlite";
import { sql } from "drizzle-orm";
import type { CommitKind } from "./history";
import type { SyncStatus } from "./sync";
import { LAST_VERIFIED_AT_META_KEY, LAST_VERIFIED_OK_META_KEY, getMetaValue, getRow, listUserTables } from "./engine/db";
import { createEngineDb } from "./engine/client";
import { SnapshotTable } from "./engine/schema.sql";
import { syncStatus } from "./sync";
import { estimateCommitSizeBytes, estimateHistorySizeBytes, getCommitCount, getHeadCommit } from "./engine/log";
import { quoteIdentifier } from "./engine/sql";

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
  const tables = listUserTables(db).map((table) => {
    const row = getRow<{ c: number }>(db, `SELECT COUNT(*) AS c FROM ${quoteIdentifier(table, { unsafe: true })}`);
    return { name: table, count: row?.c ?? 0 };
  });

  const head = getHeadCommit(db);
  const snapshotCountRow = createEngineDb(db).select({ c: sql<number>`count(*)` }).from(SnapshotTable).get();
  const verifiedOkRaw = getMetaValue(db, LAST_VERIFIED_OK_META_KEY);
  const commitCount = getCommitCount(db);
  const latestCommitEstimatedBytes = head ? estimateCommitSizeBytes(db, head.commitId) : null;

  return {
    dbPath: db.filename,
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
