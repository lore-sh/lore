import { AsyncLocalStorage } from "async_hooks";
import { mkdtempSync, rmSync } from "fs";
import { tmpdir } from "os";
import { dirname, join } from "path";
import { drizzle } from "drizzle-orm/bun-sqlite";
import { apply, check, initDb, openDb, parsePlan } from "../src";
import { CodedError } from "../src/error";
import type { Database } from "../src/db";
import { schemaHash } from "../src/inspect";
import * as schema from "../src/schema";

const tmpDirScopeStorage = new AsyncLocalStorage<Set<string>>();
const scopedDbStorage = new AsyncLocalStorage<Database>();
let persistentDb: Database | null = null;
let lastDbPath: string | null = null;

function isClosedDatabaseError(error: unknown): boolean {
  return error instanceof RangeError && error.message.includes("closed database");
}

function isOpenDatabase(db: Database): boolean {
  try {
    db.$client.query("SELECT 1").get();
    return true;
  } catch (error) {
    if (isClosedDatabaseError(error)) {
      return false;
    }
    throw error;
  }
}

function openTestDb(dbPath: string): Database {
  try {
    return openDb(dbPath);
  } catch (error) {
    if (!CodedError.hasCode(error, "NOT_INITIALIZED")) {
      throw error;
    }
    const db = drizzle({ connection: { source: dbPath }, schema });
    db.$client.run("PRAGMA foreign_keys=ON");
    db.$client.run("PRAGMA busy_timeout=5000");
    return db;
  }
}

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

function closePersistentDb(): void {
  if (!persistentDb) {
    return;
  }
  try {
    persistentDb.$client.close(false);
  } catch {
    // ignore duplicate-close during test cleanup
  }
  persistentDb = null;
}

export function currentDb(): Database {
  const scoped = scopedDbStorage.getStore();
  if (scoped) {
    if (isOpenDatabase(scoped)) {
      return scoped;
    }
    if (persistentDb && isOpenDatabase(persistentDb)) {
      return persistentDb;
    }
    if (lastDbPath) {
      if (persistentDb) {
        try {
          persistentDb.$client.close(false);
        } catch {
          // ignore duplicate-close
        }
      }
      const reopened = openTestDb(lastDbPath);
      persistentDb = reopened;
      return reopened;
    }
  }
  if (persistentDb && isOpenDatabase(persistentDb)) {
    return persistentDb;
  }
  persistentDb = null;
  if (lastDbPath) {
    try {
      persistentDb = openTestDb(lastDbPath);
    } catch {
      // keep default error below
    }
  }
  if (!persistentDb) {
    throw new Error("Database is not set for this test. Call initDb(...) or withDbPath(...). ");
  }
  return persistentDb;
}

export function replacePersistentDb(db: Database): void {
  if (persistentDb && persistentDb !== db) {
    try {
      persistentDb.$client.close(false);
    } catch {
      // ignore duplicate-close
    }
  }
  persistentDb = db;
}

export function withTmpDirCleanup<T>(fn: () => T | Promise<T>): () => Promise<T> {
  return async () => {
    const scope = new Set<string>();
    return await tmpDirScopeStorage.run(scope, async () => {
      try {
        return await fn();
      } finally {
        closePersistentDb();
        cleanupTmpDirScope(scope);
      }
    });
  };
}

export function createTestContext(): { dir: string; dbPath: string } {
  const scope = currentTmpDirScope();
  const dir = mkdtempSync(join(tmpdir(), "lore-core-test-"));
  scope.add(dir);
  const dbPath = join(dir, "lore.db");
  lastDbPath = dbPath;
  return { dir, dbPath };
}

export async function writePlanFile(dir: string, name: string, payload: unknown): Promise<string> {
  const path = `${dir}/${name}`;
  await Bun.write(path, JSON.stringify(payload));
  return path;
}

async function readPlanInput(planRef: string): Promise<string> {
  if (planRef === "-") {
    return Bun.stdin.text();
  }
  return Bun.file(planRef).text();
}

function normalizePlanForTest(db: Database, payload: string): string {
  const parsed: unknown = JSON.parse(payload);
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    return payload;
  }
  if ("baseSchemaHash" in parsed) {
    return payload;
  }
  const withHash = {
    ...parsed,
    baseSchemaHash: schemaHash(db),
  };
  return JSON.stringify(withHash);
}

export async function applyPlan(db: Database, planRef: string) {
  const payload = await readPlanInput(planRef);
  const plan = parsePlan(normalizePlanForTest(db, payload));
  return apply(db, plan);
}

export async function planCheck(db: Database, planRef: string) {
  const payload = await readPlanInput(planRef);
  const plan = parsePlan(normalizePlanForTest(db, payload));
  return check(db, plan);
}

export async function computeSchemaHash(statements: string[]): Promise<string> {
  const { dbPath } = createTestContext();
  await initDb({ dbPath });
  const db = drizzle({ connection: { source: dbPath }, schema });
  try {
    for (const sql of statements) {
      db.$client.run(sql);
    }
    return schemaHash(db);
  } finally {
    db.$client.close(false);
    closePersistentDb();
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

export async function withDbPath<T>(dbPath: string, run: ((db: Database) => Promise<T>) | (() => Promise<T>)): Promise<T> {
  const snapshot = captureEnv();
  process.env.HOME = dirname(dbPath);
  process.env.USERPROFILE = dirname(dbPath);
  delete process.env.TURSO_AUTH_TOKEN;
  lastDbPath = dbPath;

  const db = openTestDb(dbPath);
  const previous = persistentDb;
  persistentDb = db;
  try {
    return await scopedDbStorage.run(db, async () => {
      if (run.length > 0) {
        return await (run as (db: Database) => Promise<T>)(db);
      }
      return await (run as () => Promise<T>)();
    });
  } finally {
    const activeDb = persistentDb;
    persistentDb = previous;
    try {
      if (activeDb && activeDb !== previous) {
        activeDb.$client.close(false);
      }
    } catch {
      // ignore duplicate-close
    }
    restoreEnv(snapshot);
  }
}
