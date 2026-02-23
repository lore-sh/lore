import { Database } from "bun:sqlite";
import { desc, eq } from "drizzle-orm";
import { mkdir, rename } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { createEngineDb } from "./engine/client";
import {
  assertInitialized,
  DEFAULT_SNAPSHOT_INTERVAL,
  DEFAULT_SNAPSHOT_RETAIN,
  getRow,
  listUserTables,
  MAIN_REF_NAME,
  openDb,
  resolveDbPath,
  runInTransactionWithDeferredForeignKeys,
} from "./engine/db";
import { CommitTable, RefTable, SnapshotTable } from "./engine/schema.sql";
import { CodedError } from "./error";
import { deleteWalAndShm, deleteWithSidecars } from "./engine/files";
import type { CommitReplayInput } from "./engine/log";
import { loadCommitReplayInputs, replayCommitExactly } from "./engine/replay";
import { quoteIdentifier } from "./engine/sql";
import type { CommitEntry, SnapshotEntry } from "./types";

export async function hashFile(path: string): Promise<string> {
  const hasher = new Bun.CryptoHasher("sha256");
  const stream = Bun.file(path).stream();
  for await (const chunk of stream) {
    hasher.update(chunk);
  }
  return hasher.digest("hex");
}

function openStagingWritableDatabase(stagingPath: string): Database {
  const db = new Database(stagingPath, { strict: true });
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
  const db = new Database(snapshotDbPath, { readonly: true, strict: true });
  try {
    assertInitialized(db);
    const engineDb = createEngineDb(db);
    const head = engineDb.select({ commitId: RefTable.commitId }).from(RefTable).where(eq(RefTable.name, MAIN_REF_NAME)).limit(1).get();
    const commitId = head?.commitId;
    if (!commitId) {
      throw new CodedError("SNAPSHOT_FAILED", "Snapshot DB has no HEAD commit");
    }
    const commit = engineDb.select({ seq: CommitTable.seq }).from(CommitTable).where(eq(CommitTable.commitId, commitId)).limit(1).get();
    if (!commit) {
      throw new CodedError("SNAPSHOT_FAILED", `Snapshot commit not found in _toss_commit: ${commitId}`);
    }
    return { commitId, seq: commit.seq, rowCountHint: countRows(db) };
  } finally {
    db.close(false);
  }
}

export async function maybeCreateSnapshot(db: Database, commit: CommitEntry): Promise<void> {
  const interval = DEFAULT_SNAPSHOT_INTERVAL;
  if (interval <= 0 || commit.seq % interval !== 0) {
    return;
  }

  const dbPath = db.filename;
  const snapshotsDir = resolve(dirname(dbPath), "snapshots");
  await mkdir(snapshotsDir, { recursive: true });
  const tmpSnapshotPath = resolve(snapshotsDir, `tmp-${crypto.randomUUID().replaceAll("-", "")}.db`);

  db.run(`VACUUM INTO '${tmpSnapshotPath.replaceAll("'", "''")}'`);

  const snapshotHead = readSnapshotHead(tmpSnapshotPath);
  const snapshotPath = resolve(snapshotsDir, `${snapshotHead.seq}-${snapshotHead.commitId}.db`);
  await deleteWithSidecars(snapshotPath);
  await rename(tmpSnapshotPath, snapshotPath);
  const [digest] = await Promise.all([hashFile(snapshotPath), deleteWithSidecars(tmpSnapshotPath)]);

  const engineDb = createEngineDb(db);
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

  const retain = DEFAULT_SNAPSHOT_RETAIN;
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
}

export function listSnapshots(db: Database): SnapshotEntry[] {
  return createEngineDb(db).select().from(SnapshotTable).orderBy(desc(SnapshotTable.createdAt)).all();
}

export async function promotePreparedDatabase(preparedDbPath: string, dbPath: string): Promise<void> {
  try {
    await rename(preparedDbPath, dbPath);
    await deleteWalAndShm(dbPath);
  } catch (error) {
    await deleteWithSidecars(preparedDbPath);
    throw error;
  }
}

function resolveSnapshotForRecovery(
  dbPath: string,
  commitId: string,
): { snapshotPath: string; replayCommits: CommitReplayInput[] } {
  const db = openDb(dbPath);
  try {
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
      throw new CodedError("NOT_FOUND", `Snapshot not found for commit: ${commitId}`);
    }
    return {
      snapshotPath: snapshot.filePath,
      replayCommits: loadCommitReplayInputs(db, snapshot.seq),
    };
  } finally {
    db.close(false);
  }
}

export async function recoverFromSnapshot(
  dbPathInput: string,
  commitId: string,
): Promise<{ dbPath: string; restoredCommitId: string; replayedCommits: number }> {
  const dbPath = resolveDbPath(dbPathInput);
  const { snapshotPath, replayCommits } = resolveSnapshotForRecovery(dbPath, commitId);

  const stagingPath = `${dbPath}.recover-${crypto.randomUUID().replaceAll("-", "")}.staging.db`;
  await deleteWithSidecars(stagingPath);
  await Bun.write(stagingPath, Bun.file(snapshotPath));
  const replayDb = openStagingWritableDatabase(stagingPath);
  let closed = false;
  let promoted = false;
  try {
    assertInitialized(replayDb);
    for (const replay of replayCommits) {
      runInTransactionWithDeferredForeignKeys(replayDb, () => {
        replayCommitExactly(replayDb, replay, { errorCode: "RECOVER_FAILED" });
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
