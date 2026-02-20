import { Database } from "bun:sqlite";
import { asc, desc, eq, gt } from "drizzle-orm";
import { mkdir, rename } from "fs/promises";
import { dirname, resolve } from "node:path";
import { sha256Hex } from "./checksum";
import { closeClient, createEngineDb } from "./engine/client";
import {
  assertInitialized,
  configureDatabase,
  DEFAULT_SNAPSHOT_INTERVAL,
  DEFAULT_SNAPSHOT_RETAIN,
  getMetaValue,
  getRow,
  listUserTables,
  MAIN_REF_NAME,
  runInTransactionWithDeferredForeignKeys,
  withInitializedDatabase,
  withInitializedDatabaseAsync,
} from "./db";
import { CommitTable, RefTable, SnapshotTable } from "./engine/schema.sql";
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
    const engineDb = createEngineDb(db);
    const head = engineDb.select({ commitId: RefTable.commitId }).from(RefTable).where(eq(RefTable.name, MAIN_REF_NAME)).limit(1).get();
    const commitId = head?.commitId;
    if (!commitId) {
      throw new TossError("SNAPSHOT_FAILED", "Snapshot DB has no HEAD commit");
    }
    const commit = engineDb.select({ seq: CommitTable.seq }).from(CommitTable).where(eq(CommitTable.commitId, commitId)).limit(1).get();
    if (!commit) {
      throw new TossError("SNAPSHOT_FAILED", `Snapshot commit not found in _toss_commit: ${commitId}`);
    }
    return { commitId, seq: commit.seq, rowCountHint: countRows(db) };
  } finally {
    db.close(false);
  }
}

export async function maybeCreateSnapshot(commit: CommitEntry): Promise<void> {
  const runtime = withInitializedDatabase(({ db, dbPath }) => {
    const interval = getSnapshotInterval(db);
    if (interval <= 0 || commit.seq % interval !== 0) {
      return null;
    }
    return { dbPath };
  });
  if (!runtime) {
    return;
  }
  const { dbPath } = runtime;

  const snapshotsDir = resolve(dirname(dbPath), "snapshots");
  await mkdir(snapshotsDir, { recursive: true });
  const tmpSnapshotPath = resolve(snapshotsDir, `tmp-${crypto.randomUUID().replaceAll("-", "")}.db`);

  withInitializedDatabase(({ db: snapshotDb, dbPath: currentPath }) => {
    if (currentPath !== dbPath) {
      throw new TossError("SNAPSHOT_FAILED", `Snapshot target moved: expected ${dbPath}, got ${currentPath}`);
    }
    snapshotDb.run(`VACUUM INTO '${tmpSnapshotPath.replaceAll("'", "''")}'`);
  });

  const snapshotHead = readSnapshotHead(tmpSnapshotPath);
  const snapshotPath = resolve(snapshotsDir, `${snapshotHead.seq}-${snapshotHead.commitId}.db`);
  await deleteWithSidecars(snapshotPath);
  await rename(tmpSnapshotPath, snapshotPath);
  const [digest] = await Promise.all([hashFile(snapshotPath), deleteWithSidecars(tmpSnapshotPath)]);

  await withInitializedDatabaseAsync(async ({ db: writeDb, dbPath: currentPath }) => {
    if (currentPath !== dbPath) {
      throw new TossError("SNAPSHOT_FAILED", `Snapshot target moved: expected ${dbPath}, got ${currentPath}`);
    }
    const engineDb = createEngineDb(writeDb);
    engineDb
      .insert(SnapshotTable)
      .values({
        commitId: snapshotHead.commitId,
        filePath: snapshotPath,
        fileSha256: digest,
        createdAt: Date.now(),
        rowCountHint: snapshotHead.rowCountHint,
      })
      .onConflictDoUpdate({
        target: SnapshotTable.commitId,
        set: {
          filePath: snapshotPath,
          fileSha256: digest,
          createdAt: Date.now(),
          rowCountHint: snapshotHead.rowCountHint,
        },
      })
      .run();

    const retain = getSnapshotRetain(writeDb);
    const allSnapshots = engineDb
      .select({ commitId: SnapshotTable.commitId, filePath: SnapshotTable.filePath })
      .from(SnapshotTable)
      .orderBy(desc(SnapshotTable.createdAt))
      .all();
    const stale = allSnapshots.slice(Math.max(retain, 0));
    for (const row of stale) {
      await deleteWithSidecars(row.filePath);
      engineDb.delete(SnapshotTable).where(eq(SnapshotTable.commitId, row.commitId)).run();
    }
  });
}

export function listSnapshots(): SnapshotEntry[] {
  return withInitializedDatabase(({ db }) => {
    const rows = createEngineDb(db).select().from(SnapshotTable).orderBy(desc(SnapshotTable.createdAt)).all();
    return rows.map((row) => ({
      commitId: row.commitId,
      filePath: row.filePath,
      fileSha256: row.fileSha256,
      createdAt: row.createdAt,
      rowCountHint: row.rowCountHint,
    }));
  });
}

function loadCommitReplayInputs(db: Database, fromSeqExclusive: number): CommitReplayInput[] {
  const commitRows = createEngineDb(db)
    .select({ commitId: CommitTable.commitId })
    .from(CommitTable)
    .where(gt(CommitTable.seq, fromSeqExclusive))
    .orderBy(asc(CommitTable.seq))
    .all();
  const replayCommits: CommitReplayInput[] = [];
  for (const row of commitRows) {
    const commit = getCommitById(db, row.commitId);
    if (!commit) {
      throw new TossError("RECOVER_FAILED", `Replay commit not found: ${row.commitId}`);
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

export async function promotePreparedDatabase(preparedDbPath: string, dbPath: string): Promise<void> {
  closeClient({ resetPath: false });
  try {
    await rename(preparedDbPath, dbPath);
    await deleteWalAndShm(dbPath);
    configureDatabase(dbPath);
  } catch (error) {
    await deleteWithSidecars(preparedDbPath);
    try {
      configureDatabase(dbPath);
    } catch {
      // Best effort: preserve runtime continuity on the original path.
    }
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
    const snapshot = createEngineDb(db)
      .select({
        filePath: SnapshotTable.filePath,
        seq: CommitTable.seq,
      })
      .from(SnapshotTable)
      .innerJoin(CommitTable, eq(CommitTable.commitId, SnapshotTable.commitId))
      .where(eq(SnapshotTable.commitId, commitId))
      .limit(1)
      .get();
    if (!snapshot) {
      throw new TossError("NOT_FOUND", `Snapshot not found for commit: ${commitId}`);
    }
    return {
      dbPath,
      snapshotPath: snapshot.filePath,
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
