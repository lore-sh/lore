import { Database } from "bun:sqlite";
import { mkdir, rename } from "fs/promises";
import { dirname, resolve } from "node:path";
import { sha256Hex } from "./checksum";
import { closeClient } from "./engine/client";
import {
  assertInitialized,
  COMMIT_TABLE,
  DEFAULT_SNAPSHOT_INTERVAL,
  DEFAULT_SNAPSHOT_RETAIN,
  getMetaValue,
  getRow,
  getRows,
  listUserTables,
  MAIN_REF_NAME,
  runInTransactionWithDeferredForeignKeys,
  SNAPSHOT_TABLE,
  withDatabaseAtPath,
  withDatabaseAsyncAtPath,
  withInitializedDatabase,
} from "./db";
import { TossError } from "./errors";
import { deleteWalAndShm, deleteWithSidecars } from "./fsx";
import {
  appendCommitExact,
  type CommitReplayInput,
  getCommitById,
  getRowEffectsByCommitId,
  getSchemaEffectsByCommitId,
} from "./log";
import {
  applyRowEffectsWithOptions,
  applyUserRowAndSchemaEffects,
  assertNoForeignKeyViolations,
} from "./observed";
import { schemaHash, stateHash } from "./rows";
import { quoteIdentifier } from "./sql";
import type { CommitEntry, SnapshotEntry } from "./types";

export async function hashFile(path: string): Promise<string> {
  const hasher = new Bun.CryptoHasher("sha256");
  const stream = Bun.file(path).stream();
  for await (const chunk of stream) {
    hasher.update(chunk);
  }
  return hasher.digest("hex");
}

export function getSnapshotInterval(db: Database): number {
  const value = getMetaValue(db, "snapshot_interval");
  return value ? Number(value) : DEFAULT_SNAPSHOT_INTERVAL;
}

export function getSnapshotRetain(db: Database): number {
  const value = getMetaValue(db, "snapshot_retain");
  return value ? Number(value) : DEFAULT_SNAPSHOT_RETAIN;
}

function openStagingWritableDatabase(stagingPath: string): Database {
  const db = new Database(stagingPath);
  db.run("PRAGMA journal_mode=DELETE");
  db.run("PRAGMA synchronous=FULL");
  db.run("PRAGMA foreign_keys=ON");
  db.run("PRAGMA busy_timeout=5000");
  return db;
}

function countRows(db: Database): number {
  return listUserTables(db).reduce((acc, table) => {
    const row = getRow<{ c: number }>(db, `SELECT COUNT(*) AS c FROM ${quoteIdentifier(table)}`);
    return acc + (row?.c ?? 0);
  }, 0);
}

function readSnapshotHead(snapshotDbPath: string): { commitId: string; seq: number; rowCountHint: number } {
  const db = new Database(snapshotDbPath, { readonly: true });
  try {
    assertInitialized(db, snapshotDbPath);
    const head = getRow<{ commit_id: string | null }>(db, `SELECT commit_id FROM _toss_ref WHERE name=? LIMIT 1`, MAIN_REF_NAME);
    const commitId = head?.commit_id;
    if (!commitId) {
      throw new TossError("SNAPSHOT_FAILED", "Snapshot DB has no HEAD commit");
    }
    const seqRow = getRow<{ seq: number }>(db, `SELECT seq FROM ${COMMIT_TABLE} WHERE commit_id=? LIMIT 1`, commitId);
    if (!seqRow) {
      throw new TossError("SNAPSHOT_FAILED", `Snapshot commit not found in ${COMMIT_TABLE}: ${commitId}`);
    }
    return { commitId, seq: seqRow.seq, rowCountHint: countRows(db) };
  } finally {
    db.close(false);
  }
}

export async function maybeCreateSnapshot(dbPath: string, commit: CommitEntry): Promise<void> {
  const shouldCreate = withDatabaseAtPath(dbPath, ({ db }) => {
    const interval = getSnapshotInterval(db);
    return interval > 0 && commit.seq % interval === 0;
  });
  if (!shouldCreate) {
    return;
  }

  const snapshotsDir = resolve(dirname(dbPath), "snapshots");
  await mkdir(snapshotsDir, { recursive: true });
  const tmpSnapshotPath = resolve(snapshotsDir, `tmp-${crypto.randomUUID().replaceAll("-", "")}.db`);

  withDatabaseAtPath(dbPath, ({ db: snapshotDb }) => {
    snapshotDb.run(`VACUUM INTO '${tmpSnapshotPath.replaceAll("'", "''")}'`);
  });

  const snapshotHead = readSnapshotHead(tmpSnapshotPath);
  const snapshotPath = resolve(snapshotsDir, `${snapshotHead.seq}-${snapshotHead.commitId}.db`);
  await deleteWithSidecars(snapshotPath);
  await rename(tmpSnapshotPath, snapshotPath);
  const [digest] = await Promise.all([hashFile(snapshotPath), deleteWithSidecars(tmpSnapshotPath)]);

  await withDatabaseAsyncAtPath(dbPath, async ({ db: writeDb }) => {
    writeDb
      .query(
        `INSERT OR REPLACE INTO ${SNAPSHOT_TABLE}(commit_id, file_path, file_sha256, created_at, row_count_hint) VALUES(?, ?, ?, ?, ?)`,
      )
      .run(snapshotHead.commitId, snapshotPath, digest, new Date().toISOString(), snapshotHead.rowCountHint);

    const retain = getSnapshotRetain(writeDb);
    const stale = getRows<{ commit_id: string; file_path: string }>(
      writeDb,
      `
        SELECT commit_id, file_path FROM ${SNAPSHOT_TABLE}
        ORDER BY created_at DESC
        LIMIT -1 OFFSET ?
        `,
      retain,
    );
    for (const row of stale) {
      await deleteWithSidecars(row.file_path);
      writeDb.query(`DELETE FROM ${SNAPSHOT_TABLE} WHERE commit_id=?`).run(row.commit_id);
    }
  });
}

export function listSnapshots(): SnapshotEntry[] {
  return withInitializedDatabase(({ db }) => {
    const rows = getRows<{
      commit_id: string;
      file_path: string;
      file_sha256: string;
      created_at: string;
      row_count_hint: number;
    }>(db, `SELECT commit_id, file_path, file_sha256, created_at, row_count_hint FROM ${SNAPSHOT_TABLE} ORDER BY created_at DESC`);
    return rows.map((row) => ({
      commitId: row.commit_id,
      filePath: row.file_path,
      fileSha256: row.file_sha256,
      createdAt: row.created_at,
      rowCountHint: row.row_count_hint,
    }));
  });
}

function loadCommitReplayInputs(db: Database, fromSeqExclusive: number): CommitReplayInput[] {
  const commitRows = getRows<{ commit_id: string }>(db, `SELECT commit_id FROM ${COMMIT_TABLE} WHERE seq > ? ORDER BY seq ASC`, fromSeqExclusive);
  const replayCommits: CommitReplayInput[] = [];
  for (const row of commitRows) {
    const commit = getCommitById(db, row.commit_id);
    if (!commit) {
      throw new TossError("RECOVER_FAILED", `Replay commit not found: ${row.commit_id}`);
    }
    const { parentCount: _, ...base } = commit;
    replayCommits.push({
      ...base,
      rowEffects: getRowEffectsByCommitId(db, commit.commitId),
      schemaEffects: getSchemaEffectsByCommitId(db, commit.commitId),
    });
  }
  return replayCommits;
}

async function promotePreparedDatabase(preparedDbPath: string, dbPath: string): Promise<void> {
  try {
    closeClient({ resetPath: false });
    await deleteWalAndShm(dbPath);
    await rename(preparedDbPath, dbPath);
    await deleteWalAndShm(dbPath);
  } catch (error) {
    await deleteWithSidecars(preparedDbPath);
    throw error;
  }
}

function replayCommitExactly(db: Database, replay: CommitReplayInput): void {
  const beforeSchemaHash = schemaHash(db);
  if (beforeSchemaHash !== replay.schemaHashBefore) {
    throw new TossError(
      "RECOVER_FAILED",
      `schema_hash_before mismatch for replay ${replay.commitId}: expected ${replay.schemaHashBefore}, got ${beforeSchemaHash}`,
    );
  }

  const computedPlanHash = sha256Hex(replay.operations);
  if (computedPlanHash !== replay.planHash) {
    throw new TossError(
      "RECOVER_FAILED",
      `plan_hash mismatch for replay ${replay.commitId}: expected ${replay.planHash}, got ${computedPlanHash}`,
    );
  }

  applyUserRowAndSchemaEffects(db, replay.rowEffects, replay.schemaEffects, "forward", {
    disableTableTriggers: true,
  });
  applyRowEffectsWithOptions(db, replay.rowEffects, "forward", {
    disableTableTriggers: true,
    includeUserEffects: false,
    includeSystemEffects: true,
    systemPolicy: "reconcile",
  });
  assertNoForeignKeyViolations(db, "RECOVER_FAILED", `replay ${replay.commitId}`);

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

function resolveSnapshotForRecovery(commitId: string): { dbPath: string; snapshotPath: string; replayCommits: CommitReplayInput[] } {
  return withInitializedDatabase(({ db, dbPath }) => {
    const snapshot = getRow<{ file_path: string; seq: number }>(
      db,
      `
        SELECT s.file_path, c.seq
        FROM ${SNAPSHOT_TABLE} s
        JOIN ${COMMIT_TABLE} c ON c.commit_id = s.commit_id
        WHERE s.commit_id=?
        LIMIT 1
        `,
      commitId,
    );
    if (!snapshot) {
      throw new TossError("NOT_FOUND", `Snapshot not found for commit: ${commitId}`);
    }
    return {
      dbPath,
      snapshotPath: snapshot.file_path,
      replayCommits: loadCommitReplayInputs(db, snapshot.seq),
    };
  });
}

export async function recoverFromSnapshot(
  commitId: string,
): Promise<{ dbPath: string; restoredCommitId: string; replayedCommits: number }> {
  const { dbPath, snapshotPath, replayCommits } = resolveSnapshotForRecovery(commitId);

  const stagingPath = `${dbPath}.recover-${crypto.randomUUID().replaceAll("-", "")}.staging.db`;
  await deleteWithSidecars(stagingPath);
  await Bun.write(stagingPath, Bun.file(snapshotPath));
  const replayDb = openStagingWritableDatabase(stagingPath);
  let closed = false;
  let promoted = false;
  try {
    assertInitialized(replayDb, stagingPath);
    for (const replay of replayCommits) {
      runInTransactionWithDeferredForeignKeys(replayDb, () => {
        replayCommitExactly(replayDb, replay);
      });
    }
    replayDb.close(false);
    closed = true;
    await promotePreparedDatabase(stagingPath, dbPath);
    promoted = true;
  } finally {
    if (!closed) {
      replayDb.close(false);
    }
    if (!promoted) {
      await deleteWithSidecars(stagingPath);
    }
  }

  return { dbPath, restoredCommitId: commitId, replayedCommits: replayCommits.length };
}
