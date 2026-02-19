import { createHash } from "node:crypto";
import { cp, mkdir, readFile, rm } from "node:fs/promises";
import { dirname, join } from "node:path";
import {
  assertInitialized,
  closeDatabase,
  DEFAULT_SNAPSHOT_INTERVAL,
  DEFAULT_SNAPSHOT_RETAIN,
  getMetaValue,
  listUserTables,
  openDatabase,
  runInTransaction,
} from "./db";
import { TossError } from "./errors";
import { buildCommitOperationsResult } from "./commit";
import { removeExistingDbFiles } from "./init";
import { quoteIdentifier } from "./sql";
import type { CommitEntry, Operation, ServiceOptions, SnapshotEntry } from "./types";

export function hashFile(path: string): Promise<string> {
  return readFile(path).then((buffer) => createHash("sha256").update(buffer).digest("hex"));
}

export function getSnapshotInterval(db: import("bun:sqlite").Database): number {
  const value = getMetaValue(db, "snapshot_interval");
  return value ? Number(value) : DEFAULT_SNAPSHOT_INTERVAL;
}

export function getSnapshotRetain(db: import("bun:sqlite").Database): number {
  const value = getMetaValue(db, "snapshot_retain");
  return value ? Number(value) : DEFAULT_SNAPSHOT_RETAIN;
}

export async function maybeCreateSnapshot(dbPath: string, commit: CommitEntry): Promise<void> {
  const { db } = openDatabase(dbPath);
  try {
    const interval = getSnapshotInterval(db);
    if (interval <= 0 || commit.seq % interval !== 0) {
      return;
    }
  } finally {
    closeDatabase(db);
  }

  const snapshotsDir = join(dirname(dbPath), ".toss", "snapshots");
  await mkdir(snapshotsDir, { recursive: true });
  const snapshotPath = join(snapshotsDir, `${commit.seq}-${commit.commitId}.db`);

  const { db: snapshotDb } = openDatabase(dbPath);
  try {
    snapshotDb.run(`VACUUM INTO '${snapshotPath.replaceAll("'", "''")}'`);
  } finally {
    closeDatabase(snapshotDb);
  }

  const digest = await hashFile(snapshotPath);
  const { db: writeDb } = openDatabase(dbPath);
  try {
    const rowCountHint = listUserTables(writeDb).reduce((acc, table) => {
      const row = writeDb.query(`SELECT COUNT(*) AS c FROM ${quoteIdentifier(table)}`).get() as { c: number };
      return acc + row.c;
    }, 0);
    writeDb
      .query("INSERT OR REPLACE INTO _toss_snapshot(commit_id, file_path, file_sha256, created_at, row_count_hint) VALUES(?, ?, ?, ?, ?)")
      .run(commit.commitId, snapshotPath, digest, new Date().toISOString(), rowCountHint);

    const retain = getSnapshotRetain(writeDb);
    const stale = writeDb
      .query(
        `
        SELECT commit_id, file_path FROM _toss_snapshot
        ORDER BY created_at DESC
        LIMIT -1 OFFSET ?
        `,
      )
      .all(retain) as Array<{ commit_id: string; file_path: string }>;
    for (const row of stale) {
      await rm(row.file_path, { force: true });
      writeDb.query("DELETE FROM _toss_snapshot WHERE commit_id=?").run(row.commit_id);
    }
  } finally {
    closeDatabase(writeDb);
  }
}

export function listSnapshots(options: ServiceOptions = {}): SnapshotEntry[] {
  const { db, dbPath } = openDatabase(options.dbPath);
  try {
    assertInitialized(db, dbPath);
    const rows = db
      .query("SELECT commit_id, file_path, file_sha256, created_at, row_count_hint FROM _toss_snapshot ORDER BY created_at DESC")
      .all() as Array<{
      commit_id: string;
      file_path: string;
      file_sha256: string;
      created_at: string;
      row_count_hint: number;
    }>;
    return rows.map((row) => ({
      commitId: row.commit_id,
      filePath: row.file_path,
      fileSha256: row.file_sha256,
      createdAt: row.created_at,
      rowCountHint: row.row_count_hint,
    }));
  } finally {
    closeDatabase(db);
  }
}

export async function recoverFromSnapshot(
  commitId: string,
  options: ServiceOptions = {},
): Promise<{ dbPath: string; restoredCommitId: string; replayedCommits: number }> {
  const { db, dbPath } = openDatabase(options.dbPath);
  let snapshotPath: string | null = null;
  let targetSeq = 0;
  let replayCommits: Array<{ message: string; operations: Operation[] }> = [];
  try {
    assertInitialized(db, dbPath);
    const snapshot = db
      .query(
        `
        SELECT s.file_path, c.seq
        FROM _toss_snapshot s
        JOIN _toss_commit c ON c.commit_id = s.commit_id
        WHERE s.commit_id=?
        LIMIT 1
        `,
      )
      .get(commitId) as { file_path: string; seq: number } | null;
    if (!snapshot) {
      throw new TossError("NOT_FOUND", `Snapshot not found for commit: ${commitId}`);
    }
    snapshotPath = snapshot.file_path;
    targetSeq = snapshot.seq;

    const laterRows = db
      .query("SELECT commit_id, message FROM _toss_commit WHERE seq > ? ORDER BY seq ASC")
      .all(targetSeq) as Array<{ commit_id: string; message: string }>;
    replayCommits = laterRows.map((row) => ({
      message: row.message,
      operations: db
        .query("SELECT op_json FROM _toss_op WHERE commit_id=? ORDER BY op_index ASC")
        .all(row.commit_id)
        .map((op) => JSON.parse((op as { op_json: string }).op_json) as Operation),
    }));
  } finally {
    closeDatabase(db);
  }

  if (!snapshotPath) {
    throw new TossError("RECOVER_FAILED", `Snapshot path missing for commit: ${commitId}`);
  }

  await removeExistingDbFiles(dbPath);
  await cp(snapshotPath, dbPath);

  if (replayCommits.length === 0) {
    return { dbPath, restoredCommitId: commitId, replayedCommits: 0 };
  }

  for (const replay of replayCommits) {
    const { db: replayDb, dbPath: replayDbPath } = openDatabase(dbPath);
    try {
      assertInitialized(replayDb, replayDbPath);
      runInTransaction(replayDb, () =>
        buildCommitOperationsResult(replayDb, replay.operations, "apply", `[replay] ${replay.message}`, null),
      );
    } finally {
      closeDatabase(replayDb);
    }
  }

  return { dbPath, restoredCommitId: commitId, replayedCommits: replayCommits.length };
}
