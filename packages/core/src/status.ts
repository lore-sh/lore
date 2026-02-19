import {
  assertInitialized,
  closeDatabase,
  FORMAT_GENERATION,
  getMetaValue,
  HISTORY_ENGINE,
  listUserTables,
  openDatabase,
  SNAPSHOT_TABLE,
  SQLITE_MIN_VERSION,
} from "./db";
import { getHeadCommit, listCommits } from "./log";
import { quoteIdentifier } from "./sql";
import type { CommitEntry, ServiceOptions, TossStatus } from "./types";

export function getStatus(options: ServiceOptions = {}): TossStatus {
  const { db, dbPath } = openDatabase(options.dbPath);
  try {
    assertInitialized(db, dbPath);

    const tables = listUserTables(db).map((table) => {
      const row = db.query(`SELECT COUNT(*) AS c FROM ${quoteIdentifier(table)}`).get() as { c: number };
      return { name: table, count: row.c };
    });

    const head = getHeadCommit(db);
    const snapshotCountRow = db.query(`SELECT COUNT(*) AS c FROM ${SNAPSHOT_TABLE}`).get() as { c: number };

    return {
      dbPath,
      historyEngine: getMetaValue(db, "history_engine") ?? HISTORY_ENGINE,
      formatGeneration: Number(getMetaValue(db, "format_generation") ?? FORMAT_GENERATION),
      sqliteMinVersion: getMetaValue(db, "sqlite_min_version") ?? SQLITE_MIN_VERSION,
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
      snapshotCount: snapshotCountRow.c,
      lastVerifiedAt: getMetaValue(db, "last_verified_at"),
    };
  } finally {
    closeDatabase(db);
  }
}

export function getHistory(
  options: ServiceOptions & {
    verbose?: boolean;
  } = {},
): CommitEntry[] {
  const { db, dbPath } = openDatabase(options.dbPath);
  try {
    assertInitialized(db, dbPath);
    return listCommits(db, true);
  } finally {
    closeDatabase(db);
  }
}
