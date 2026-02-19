import {
  getRow,
  getMetaValue,
  listUserTables,
  SNAPSHOT_TABLE,
  withInitializedDatabase,
} from "./db";
import { getHeadCommit, listCommits } from "./log";
import { quoteIdentifier } from "./sql";
import type { CommitEntry, DatabaseOptions, TossStatus } from "./types";

export function getStatus(options: DatabaseOptions = {}): TossStatus {
  return withInitializedDatabase(options, ({ db, dbPath }) => {
    const tables = listUserTables(db).map((table) => {
      const row = getRow<{ c: number }>(db, `SELECT COUNT(*) AS c FROM ${quoteIdentifier(table)}`);
      return { name: table, count: row?.c ?? 0 };
    });

    const head = getHeadCommit(db);
    const snapshotCountRow = getRow<{ c: number }>(db, `SELECT COUNT(*) AS c FROM ${SNAPSHOT_TABLE}`);
    const verifiedOkRaw = getMetaValue(db, "last_verified_ok");

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
      lastVerifiedOkAt: getMetaValue(db, "last_verified_ok_at"),
    };
  });
}

export function getHistory(
  options: DatabaseOptions & {
    verbose?: boolean;
  } = {},
): CommitEntry[] {
  return withInitializedDatabase(options, ({ db }) => {
    return listCommits(db, true);
  });
}
