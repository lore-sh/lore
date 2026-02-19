import { Database } from "bun:sqlite";
import { mkdir, rename } from "fs/promises";
import { deleteWalAndShm, deleteWithSidecars } from "./fsx";
import { dirnameOf, joinPath } from "./pathing";
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
  resolveDbPath,
  runInTransactionWithDeferredForeignKeys,
  SNAPSHOT_TABLE,
} from "./db";
import { TossError } from "./errors";
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
import type { CommitEntry, DatabaseOptions, SnapshotEntry } from "./types";

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

  const snapshotsDir = joinPath(dirnameOf(dbPath), ".toss", "snapshots");
  await mkdir(snapshotsDir, { recursive: true });
  const tmpSnapshotPath = joinPath(snapshotsDir, `tmp-${crypto.randomUUID().replaceAll("-", "")}.db`);

  const { db: snapshotDb } = openDatabase(dbPath);
  try {
    snapshotDb.run(`VACUUM INTO '${tmpSnapshotPath.replaceAll("'", "''")}'`);
  } finally {
    closeDatabase(snapshotDb);
  }

  const snapshotHead = readSnapshotHead(tmpSnapshotPath);
  const snapshotPath = joinPath(snapshotsDir, `${snapshotHead.seq}-${snapshotHead.commitId}.db`);
  await deleteWithSidecars(snapshotPath);
  await rename(tmpSnapshotPath, snapshotPath);
  const [digest] = await Promise.all([hashFile(snapshotPath), deleteWithSidecars(tmpSnapshotPath)]);

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
      await deleteWithSidecars(row.file_path);
      writeDb.query(`DELETE FROM ${SNAPSHOT_TABLE} WHERE commit_id=?`).run(row.commit_id);
    }
  } finally {
    closeDatabase(writeDb);
  }
}

export function listSnapshots(options: DatabaseOptions = {}): SnapshotEntry[] {
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

function loadCommitReplayInputs(db: Database, fromSeqExclusive: number): CommitReplayInput[] {
  const commitRows = db
    .query(`SELECT commit_id FROM ${COMMIT_TABLE} WHERE seq > ? ORDER BY seq ASC`)
    .all(fromSeqExclusive) as Array<{ commit_id: string }>;
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

function resolveSnapshotForRecovery(
  dbPath: string,
  commitId: string,
): { snapshotPath: string; replayCommits: CommitReplayInput[] } {
  const { db } = openDatabase(dbPath);
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
    return {
      snapshotPath: snapshot.file_path,
      replayCommits: loadCommitReplayInputs(db, snapshot.seq),
    };
  } finally {
    closeDatabase(db);
  }
}

export async function recoverFromSnapshot(
  commitId: string,
  options: DatabaseOptions = {},
): Promise<{ dbPath: string; restoredCommitId: string; replayedCommits: number }> {
  const dbPath = resolveDbPath(options.dbPath);
  const { snapshotPath, replayCommits } = resolveSnapshotForRecovery(dbPath, commitId);

  const stagingPath = `${dbPath}.recover-${crypto.randomUUID().replaceAll("-", "")}.staging.db`;
  await deleteWithSidecars(stagingPath);
  await Bun.write(stagingPath, Bun.file(snapshotPath));
  const replayDb = openStagingWritableDatabase(stagingPath);
  let promoted = false;
  try {
    assertInitialized(replayDb, stagingPath);
    for (const replay of replayCommits) {
      runInTransactionWithDeferredForeignKeys(replayDb, () => {
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
      await deleteWithSidecars(stagingPath);
    }
  }

  return { dbPath, restoredCommitId: commitId, replayedCommits: replayCommits.length };
}
