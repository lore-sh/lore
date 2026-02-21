import { sql } from "drizzle-orm";
import {
  getMetaValue,
  getRow,
  listUserTables,
  withInitializedDatabase,
} from "./engine/db";
import { createEngineDb } from "./engine/client";
import { SnapshotTable } from "./engine/schema.sql";
import { buildSyncStatus } from "./sync";
import { estimateCommitSizeBytes, estimateHistorySizeBytes, getCommitCount, getHeadCommit, listCommits } from "./engine/log";
import { quoteIdentifier } from "./engine/sql";
import type { CommitEntry, TossStatus } from "./types";

export function getStatus(): TossStatus {
  return withInitializedDatabase(({ db, dbPath }) => {
    const tables = listUserTables(db).map((table) => {
      const row = getRow<{ c: number }>(db, `SELECT COUNT(*) AS c FROM ${quoteIdentifier(table, { unsafe: true })}`);
      return { name: table, count: row?.c ?? 0 };
    });

    const head = getHeadCommit(db);
    const snapshotCountRow = createEngineDb(db).select({ c: sql<number>`count(*)` }).from(SnapshotTable).get();
    const verifiedOkRaw = getMetaValue(db, "last_verified_ok");
    const commitCount = getCommitCount(db);
    const latestCommitEstimatedBytes = head ? estimateCommitSizeBytes(db, head.commitId) : null;

    return {
      dbPath,
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
      lastVerifiedAt: getMetaValue(db, "last_verified_at"),
      lastVerifiedOk: verifiedOkRaw === null ? null : verifiedOkRaw === "1",
      sync: buildSyncStatus(db),
      storage: {
        commitCount,
        estimatedHistoryBytes: estimateHistorySizeBytes(db),
        latestCommitEstimatedBytes,
      },
    };
  });
}

export function getHistory(): CommitEntry[] {
  return withInitializedDatabase(({ db }) => {
    return listCommits(db, true);
  });
}
