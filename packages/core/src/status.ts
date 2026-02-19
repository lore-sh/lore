import {
  assertInitialized,
  closeDatabase,
  getMetaValue,
  listUserTables,
  openDatabase,
  SNAPSHOT_TABLE,
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
      lastVerifiedOk:
        getMetaValue(db, "last_verified_ok") === null
          ? null
          : getMetaValue(db, "last_verified_ok") === "1",
      lastVerifiedOkAt: getMetaValue(db, "last_verified_ok_at"),
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
