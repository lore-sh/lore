import { AsyncLocalStorage } from "async_hooks";
import { mkdtempSync, rmSync } from "fs";
import { tmpdir } from "os";
import { dirname, join } from "path";
import { Database } from "bun:sqlite";
import { configureDatabase, initDatabase } from "../src";
import { closeClient, getClientPath } from "../src/engine/client";
import { schemaHash } from "../src/engine/rows";

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
        closeClient();
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
  closeClient();
  const { dbPath } = createTestContext();
  await initDatabase({ dbPath });
  const db = new Database(dbPath);
  try {
    for (const sql of statements) {
      db.run(sql);
    }
    return schemaHash(db);
  } finally {
    db.close(false);
    closeClient();
  }
}

interface EnvSnapshot {
  HOME?: string | undefined;
  USERPROFILE?: string | undefined;
  TURSO_AUTH_TOKEN?: string | undefined;
}

function captureEnv(): EnvSnapshot {
  return {
    HOME: process.env.HOME,
    USERPROFILE: process.env.USERPROFILE,
    TURSO_AUTH_TOKEN: process.env.TURSO_AUTH_TOKEN,
  };
}

function restoreEnv(snapshot: EnvSnapshot): void {
  for (const key of ["HOME", "USERPROFILE", "TURSO_AUTH_TOKEN"] as const) {
    if (snapshot[key] === undefined) {
      delete process.env[key];
    } else {
      process.env[key] = snapshot[key];
    }
  }
}

export function withTestHome<T>(home: string, run: () => T): T {
  const snapshot = captureEnv();
  process.env.HOME = home;
  process.env.USERPROFILE = home;
  delete process.env.TURSO_AUTH_TOKEN;
  try {
    return run();
  } finally {
    restoreEnv(snapshot);
  }
}

export async function withDbPath<T>(dbPath: string, run: () => Promise<T>): Promise<T> {
  const previousClientPath = getClientPath();
  const snapshot = captureEnv();
  closeClient();
  process.env.HOME = dirname(dbPath);
  process.env.USERPROFILE = dirname(dbPath);
  delete process.env.TURSO_AUTH_TOKEN;
  configureDatabase(dbPath);
  try {
    return await run();
  } finally {
    closeClient();
    if (previousClientPath) {
      configureDatabase(previousClientPath);
    }
    restoreEnv(snapshot);
  }
}
