import { createHash } from "node:crypto";
import { cp, mkdir, readFile, rename, rm } from "node:fs/promises";
import { dirname, join } from "node:path";
import { Database } from "bun:sqlite";
import { sha256Hex } from "./checksum";
import {
  assertInitialized,
  closeDatabase,
  COMMIT_TABLE,
  DEFAULT_SNAPSHOT_INTERVAL,
  DEFAULT_SNAPSHOT_RETAIN,
  getMetaValue,
  listUserTables,
  MAIN_REF_NAME,
  openDatabase,
  runInTransaction,
  SNAPSHOT_TABLE,
} from "./db";
import { TossError } from "./errors";
import { executeOperation } from "./executors/apply";
import {
  appendCommitExact,
  getCommitById,
  getRowEffectsByCommitId,
  getSchemaEffectsByCommitId,
  type RowEffect,
  type SchemaEffect,
} from "./log";
import { schemaHash, stateHash } from "./rows";
import { quoteIdentifier } from "./sql";
import type { CommitEntry, CommitKind, Operation, ServiceOptions, SnapshotEntry } from "./types";

interface ReplayCommit {
  commitId: string;
  seq: number;
  kind: CommitKind;
  message: string;
  createdAt: string;
  parentIds: string[];
  schemaHashBefore: string;
  schemaHashAfter: string;
  stateHashAfter: string;
  planHash: string;
  inverseReady: boolean;
  revertedTargetId: string | null;
  operations: Operation[];
  rowEffects: RowEffect[];
  schemaEffects: SchemaEffect[];
}

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

async function removeFileWithSidecars(path: string): Promise<void> {
  await rm(path, { force: true });
  await rm(`${path}-wal`, { force: true });
  await rm(`${path}-shm`, { force: true });
}

function openStagingWritableDatabase(stagingPath: string): Database {
  const db = new Database(stagingPath);
  db.run("PRAGMA journal_mode=DELETE");
  db.run("PRAGMA synchronous=FULL");
  db.run("PRAGMA foreign_keys=ON");
  db.run("PRAGMA busy_timeout=5000");
  return db;
}

function countRows(db: import("bun:sqlite").Database): number {
  return listUserTables(db).reduce((acc, table) => {
    const row = db.query(`SELECT COUNT(*) AS c FROM ${quoteIdentifier(table)}`).get() as { c: number };
    return acc + row.c;
  }, 0);
}

function readSnapshotHead(snapshotDbPath: string): { commitId: string; seq: number; rowCountHint: number } {
  const db = new Database(snapshotDbPath, { readonly: true });
  try {
    assertInitialized(db, snapshotDbPath);
    const head = db
      .query(`SELECT commit_id FROM _toss_ref WHERE name=? LIMIT 1`)
      .get(MAIN_REF_NAME) as { commit_id: string | null } | null;
    const commitId = head?.commit_id;
    if (!commitId) {
      throw new TossError("SNAPSHOT_FAILED", "Snapshot DB has no HEAD commit");
    }
    const seqRow = db.query(`SELECT seq FROM ${COMMIT_TABLE} WHERE commit_id=? LIMIT 1`).get(commitId) as { seq: number } | null;
    if (!seqRow) {
      throw new TossError("SNAPSHOT_FAILED", `Snapshot commit not found in ${COMMIT_TABLE}: ${commitId}`);
    }
    return { commitId, seq: seqRow.seq, rowCountHint: countRows(db) };
  } finally {
    db.close(false);
  }
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
  const tmpSnapshotPath = join(snapshotsDir, `tmp-${crypto.randomUUID().replaceAll("-", "")}.db`);

  const { db: snapshotDb } = openDatabase(dbPath);
  try {
    snapshotDb.run(`VACUUM INTO '${tmpSnapshotPath.replaceAll("'", "''")}'`);
  } finally {
    closeDatabase(snapshotDb);
  }

  const snapshotHead = readSnapshotHead(tmpSnapshotPath);
  const snapshotPath = join(snapshotsDir, `${snapshotHead.seq}-${snapshotHead.commitId}.db`);
  await removeFileWithSidecars(snapshotPath);
  await rename(tmpSnapshotPath, snapshotPath);
  await removeFileWithSidecars(tmpSnapshotPath);
  const digest = await hashFile(snapshotPath);

  const { db: writeDb } = openDatabase(dbPath);
  try {
    writeDb
      .query(
        `INSERT OR REPLACE INTO ${SNAPSHOT_TABLE}(commit_id, file_path, file_sha256, created_at, row_count_hint) VALUES(?, ?, ?, ?, ?)`,
      )
      .run(snapshotHead.commitId, snapshotPath, digest, new Date().toISOString(), snapshotHead.rowCountHint);

    const retain = getSnapshotRetain(writeDb);
    const stale = writeDb
      .query(
        `
        SELECT commit_id, file_path FROM ${SNAPSHOT_TABLE}
        ORDER BY created_at DESC
        LIMIT -1 OFFSET ?
        `,
      )
      .all(retain) as Array<{ commit_id: string; file_path: string }>;
    for (const row of stale) {
      await removeFileWithSidecars(row.file_path);
      writeDb.query(`DELETE FROM ${SNAPSHOT_TABLE} WHERE commit_id=?`).run(row.commit_id);
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
      .query(`SELECT commit_id, file_path, file_sha256, created_at, row_count_hint FROM ${SNAPSHOT_TABLE} ORDER BY created_at DESC`)
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

function loadReplayCommits(db: import("bun:sqlite").Database, fromSeqExclusive: number): ReplayCommit[] {
  const commitRows = db
    .query(`SELECT commit_id FROM ${COMMIT_TABLE} WHERE seq > ? ORDER BY seq ASC`)
    .all(fromSeqExclusive) as Array<{ commit_id: string }>;
  const replayCommits: ReplayCommit[] = [];
  for (const row of commitRows) {
    const commit = getCommitById(db, row.commit_id);
    if (!commit) {
      throw new TossError("RECOVER_FAILED", `Replay commit not found: ${row.commit_id}`);
    }
    replayCommits.push({
      commitId: commit.commitId,
      seq: commit.seq,
      kind: commit.kind,
      message: commit.message,
      createdAt: commit.createdAt,
      parentIds: commit.parentIds,
      schemaHashBefore: commit.schemaHashBefore,
      schemaHashAfter: commit.schemaHashAfter,
      stateHashAfter: commit.stateHashAfter,
      planHash: commit.planHash,
      inverseReady: commit.inverseReady,
      revertedTargetId: commit.revertedTargetId,
      operations: commit.operations,
      rowEffects: getRowEffectsByCommitId(db, commit.commitId),
      schemaEffects: getSchemaEffectsByCommitId(db, commit.commitId),
    });
  }
  return replayCommits;
}

async function promotePreparedDatabase(preparedDbPath: string, dbPath: string): Promise<void> {
  try {
    await rm(`${dbPath}-wal`, { force: true });
    await rm(`${dbPath}-shm`, { force: true });
    await rename(preparedDbPath, dbPath);
    await rm(`${dbPath}-wal`, { force: true });
    await rm(`${dbPath}-shm`, { force: true });
  } catch (error) {
    await removeFileWithSidecars(preparedDbPath);
    throw error;
  }
}

function replayCommitExactly(db: import("bun:sqlite").Database, replay: ReplayCommit): void {
  const beforeSchemaHash = schemaHash(db);
  if (beforeSchemaHash !== replay.schemaHashBefore) {
    throw new TossError(
      "RECOVER_FAILED",
      `schema_hash_before mismatch for replay ${replay.commitId}: expected ${replay.schemaHashBefore}, got ${beforeSchemaHash}`,
    );
  }
  for (const operation of replay.operations) {
    executeOperation(db, operation);
  }

  const computedPlanHash = sha256Hex(replay.operations);
  if (computedPlanHash !== replay.planHash) {
    throw new TossError(
      "RECOVER_FAILED",
      `plan_hash mismatch for replay ${replay.commitId}: expected ${replay.planHash}, got ${computedPlanHash}`,
    );
  }

  const afterSchemaHash = schemaHash(db);
  if (afterSchemaHash !== replay.schemaHashAfter) {
    throw new TossError(
      "RECOVER_FAILED",
      `schema_hash_after mismatch for replay ${replay.commitId}: expected ${replay.schemaHashAfter}, got ${afterSchemaHash}`,
    );
  }

  const afterStateHash = stateHash(db);
  if (afterStateHash !== replay.stateHashAfter) {
    throw new TossError(
      "RECOVER_FAILED",
      `state_hash_after mismatch for replay ${replay.commitId}: expected ${replay.stateHashAfter}, got ${afterStateHash}`,
    );
  }

  appendCommitExact(db, replay);
}

export async function recoverFromSnapshot(
  commitId: string,
  options: ServiceOptions = {},
): Promise<{ dbPath: string; restoredCommitId: string; replayedCommits: number }> {
  const { db, dbPath } = openDatabase(options.dbPath);
  let snapshotPath: string | null = null;
  let targetSeq = 0;
  let replayCommits: ReplayCommit[] = [];
  try {
    assertInitialized(db, dbPath);
    const snapshot = db
      .query(
        `
        SELECT s.file_path, c.seq
        FROM ${SNAPSHOT_TABLE} s
        JOIN ${COMMIT_TABLE} c ON c.commit_id = s.commit_id
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
    replayCommits = loadReplayCommits(db, targetSeq);
  } finally {
    closeDatabase(db);
  }

  if (!snapshotPath) {
    throw new TossError("RECOVER_FAILED", `Snapshot path missing for commit: ${commitId}`);
  }

  const stagingPath = `${dbPath}.recover-${crypto.randomUUID().replaceAll("-", "")}.staging.db`;
  await removeFileWithSidecars(stagingPath);
  await cp(snapshotPath, stagingPath);
  const replayDb = openStagingWritableDatabase(stagingPath);
  let promoted = false;
  try {
    assertInitialized(replayDb, stagingPath);
    for (const replay of replayCommits) {
      runInTransaction(replayDb, () => {
        replayCommitExactly(replayDb, replay);
      });
    }
    closeDatabase(replayDb);
    await promotePreparedDatabase(stagingPath, dbPath);
    promoted = true;
  } finally {
    if (!promoted) {
      try {
        closeDatabase(replayDb);
      } catch {
        // no-op
      }
      await removeFileWithSidecars(stagingPath);
    }
  }

  return { dbPath, restoredCommitId: commitId, replayedCommits: replayCommits.length };
}
