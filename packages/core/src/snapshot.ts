import { desc, eq } from "drizzle-orm";
import { drizzle } from "drizzle-orm/bun-sqlite";
import { mkdir, rename } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import {
  DEFAULT_SNAPSHOT_INTERVAL,
  DEFAULT_SNAPSHOT_RETAIN,
  MAIN_REF_NAME,
  assertInitialized,
  deleteWalAndShm,
  deleteWithSidecars,
  listUserTables,
  openDb,
  resolveDbPath,
  runInDeferredTransaction,
  type Database,
} from "./db";
import { CommitTable, RefTable, SnapshotTable } from "./schema";
import * as schema from "./schema";
import type { Commit } from "./schema";
import { CodedError } from "./error";
import { loadCommitReplayInputs, replayCommitExactly } from "./commit";
import { quoteIdentifier } from "./sql";

async function hashFile(path: string): Promise<string> {
  const hasher = new Bun.CryptoHasher("sha256");
  const stream = Bun.file(path).stream();
  for await (const chunk of stream) {
    hasher.update(chunk);
  }
  return hasher.digest("hex");
}

function openStagingWritableDatabase(stagingPath: string): Database {
  const db = drizzle({ connection: { source: stagingPath }, schema });
  db.$client.run("PRAGMA journal_mode=DELETE");
  db.$client.run("PRAGMA synchronous=FULL");
  db.$client.run("PRAGMA foreign_keys=ON");
  db.$client.run("PRAGMA busy_timeout=5000");
  return db;
}

function countRows(db: Database): number {
  return listUserTables(db).reduce((acc, table) => {
    const row = db.$client.query<{ c: number }, []>(`SELECT COUNT(*) AS c FROM ${quoteIdentifier(table)}`).get();
    return acc + (row?.c ?? 0);
  }, 0);
}

function readSnapshotHead(snapshotDbPath: string): { commitId: string; seq: number; rowCountHint: number } {
  const db = drizzle({ connection: { source: snapshotDbPath, readonly: true }, schema });
  try {
    assertInitialized(db);
    const head = db.select({ commitId: RefTable.commitId }).from(RefTable).where(eq(RefTable.name, MAIN_REF_NAME)).limit(1).get();
    const commitId = head?.commitId;
    if (!commitId) {
      throw new CodedError("SNAPSHOT_FAILED", "Snapshot DB has no HEAD commit");
    }
    const commit = db.select({ seq: CommitTable.seq }).from(CommitTable).where(eq(CommitTable.commitId, commitId)).limit(1).get();
    if (!commit) {
      throw new CodedError("SNAPSHOT_FAILED", `Snapshot commit not found in _toss_commit: ${commitId}`);
    }
    return { commitId, seq: commit.seq, rowCountHint: countRows(db) };
  } finally {
    db.$client.close(false);
  }
}

export async function maybeCreateSnapshot(db: Database, commit: Commit): Promise<void> {
  if (DEFAULT_SNAPSHOT_INTERVAL <= 0 || commit.seq % DEFAULT_SNAPSHOT_INTERVAL !== 0) {
    return;
  }

  const dbPath = db.$client.filename;
  const snapshotsDir = resolve(dirname(dbPath), "snapshots");
  await mkdir(snapshotsDir, { recursive: true });
  const tmpSnapshotPath = resolve(snapshotsDir, `tmp-${crypto.randomUUID().replaceAll("-", "")}.db`);

  db.$client.run(`VACUUM INTO '${tmpSnapshotPath.replaceAll("'", "''")}'`);

  const snapshotHead = readSnapshotHead(tmpSnapshotPath);
  const snapshotPath = resolve(snapshotsDir, `${snapshotHead.seq}-${snapshotHead.commitId}.db`);
  await deleteWithSidecars(snapshotPath);
  await rename(tmpSnapshotPath, snapshotPath);
  const [digest] = await Promise.all([hashFile(snapshotPath), deleteWithSidecars(tmpSnapshotPath)]);

  db.insert(SnapshotTable)
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

  const allSnapshots = db
    .select({ commitId: SnapshotTable.commitId, filePath: SnapshotTable.filePath })
    .from(SnapshotTable)
    .orderBy(desc(SnapshotTable.createdAt))
    .all();
  const stale = allSnapshots.slice(DEFAULT_SNAPSHOT_RETAIN);
  for (const row of stale) {
    await deleteWithSidecars(row.filePath);
    db.delete(SnapshotTable).where(eq(SnapshotTable.commitId, row.commitId)).run();
  }
}

export async function promotePrepared(preparedDbPath: string, dbPath: string): Promise<void> {
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
): { snapshotPath: string; replayCommits: ReturnType<typeof loadCommitReplayInputs> } {
  const db = openDb(dbPath);
  try {
    const snapshot = db
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
    db.$client.close(false);
  }
}

export async function recover(
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
      runInDeferredTransaction(replayDb, () => {
        replayCommitExactly(replayDb, replay, { errorCode: "RECOVER_FAILED" });
      });
    }
    replayDb.$client.close(false);
    closed = true;
    await promotePrepared(stagingPath, dbPath);
    promoted = true;
  } finally {
    if (!closed) {
      replayDb.$client.close(false);
    }
    if (!promoted) {
      await deleteWithSidecars(stagingPath);
    }
  }

  return { dbPath, restoredCommitId: commitId, replayedCommits: replayCommits.length };
}
