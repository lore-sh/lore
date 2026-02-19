import { AsyncLocalStorage } from "async_hooks";
import { mkdtempSync, rmSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import { Database } from "bun:sqlite";
import { initDatabase } from "../src";
import { ENGINE_META_TABLE } from "../src/db";
import { schemaHash } from "../src/rows";

const tmpDirScopeStorage = new AsyncLocalStorage<Set<string>>();

function currentTmpDirScope(): Set<string> {
  const scope = tmpDirScopeStorage.getStore();
  if (!scope) {
    throw new Error("createTestContext must be called inside withTmpDirCleanup");
  }
  return scope;
}

function cleanupTmpDirScope(scope: Iterable<string>): void {
  for (const dir of scope) {
    rmSync(dir, { recursive: true, force: true });
  }
}

export function withTmpDirCleanup<T>(fn: () => T | Promise<T>): () => Promise<T> {
  return async () => {
    const scope = new Set<string>();
    return await tmpDirScopeStorage.run(scope, async () => {
      try {
        return await fn();
      } finally {
        cleanupTmpDirScope(scope);
      }
    });
  };
}

export function createTestContext(): { dir: string; dbPath: string } {
  const scope = currentTmpDirScope();
  const dir = mkdtempSync(join(tmpdir(), "toss-core-test-"));
  scope.add(dir);
  const dbPath = join(dir, "toss.db");
  return { dir, dbPath };
}

export async function writePlanFile(dir: string, name: string, payload: unknown): Promise<string> {
  const path = `${dir}/${name}`;
  await Bun.write(path, JSON.stringify(payload));
  return path;
}

export async function computeSchemaHash(statements: string[]): Promise<string> {
  const { dbPath } = createTestContext();
  await initDatabase({ dbPath });
  const db = new Database(dbPath);
  for (const sql of statements) {
    db.run(sql);
  }
  const hash = schemaHash(db);
  db.close(false);
  return hash;
}

export function enableSnapshotEveryCommit(dbPath: string): void {
  const db = new Database(dbPath);
  db.query(`UPDATE ${ENGINE_META_TABLE} SET value='1' WHERE key='snapshot_interval'`).run();
  db.query(`UPDATE ${ENGINE_META_TABLE} SET value='10' WHERE key='snapshot_retain'`).run();
  db.close(false);
}
