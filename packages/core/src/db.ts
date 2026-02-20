import type { SQLQueryBindings, Database } from "bun:sqlite";
import { existsSync, mkdirSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { migrate } from "drizzle-orm/bun-sqlite/migrator";
import { getClientPath, getSqlite, hasClient, initClient, withClient } from "./engine/client";
import { TossError } from "./errors";
import { resolveHomeDir } from "./fsx";

export const DEFAULT_DB_DIR = ".toss";
export const DEFAULT_DB_NAME = "toss.db";
export const DEFAULT_SNAPSHOT_INTERVAL = 100;
export const DEFAULT_SNAPSHOT_RETAIN = 20;
export const MAIN_REF_NAME = "main";
export const ENGINE_META_TABLE = "_toss_engine_meta";
export const COMMIT_TABLE = "_toss_commit";
export const COMMIT_PARENT_TABLE = "_toss_commit_parent";
export const REF_TABLE = "_toss_ref";
export const REFLOG_TABLE = "_toss_reflog";
export const OP_TABLE = "_toss_op";
export const EFFECT_ROW_TABLE = "_toss_effect_row";
export const EFFECT_SCHEMA_TABLE = "_toss_effect_schema";
export const SNAPSHOT_TABLE = "_toss_snapshot";
const ENGINE_MIGRATIONS_DIR = resolve(import.meta.dir, "../migration");

export interface DatabaseContext {
  db: Database;
  dbPath: string;
}

function defaultDbPath(): string {
  return resolve(resolveHomeDir(), DEFAULT_DB_DIR, DEFAULT_DB_NAME);
}

export function resolveDbPath(pathFromArg?: string): string {
  const candidate = pathFromArg ?? Bun.env.TOSS_DB_PATH ?? defaultDbPath();
  return resolve(candidate);
}

function notInitializedError(dbPath: string): TossError {
  return new TossError("NOT_INITIALIZED", `Database is not initialized: ${dbPath}. Run \`toss init --force-new\`.`);
}

function ensureDatabaseDirectory(dbPath: string): void {
  mkdirSync(dirname(dbPath), { recursive: true });
}

function assertDatabaseFileExists(dbPath: string): void {
  if (!existsSync(dbPath)) {
    throw notInitializedError(dbPath);
  }
}

function runtimeDbPath(path?: string): string {
  if (path) {
    return resolveDbPath(path);
  }
  const currentPath = getClientPath();
  if (currentPath) {
    return currentPath;
  }
  return resolveDbPath();
}

function openDatabase(path?: string): DatabaseContext {
  const dbPath = runtimeDbPath(path);
  ensureDatabaseDirectory(dbPath);
  if (path) {
    initClient(dbPath);
  } else if (!hasClient()) {
    initClient(dbPath);
  }
  const db = getSqlite();
  return { db, dbPath };
}

export function getRow<T>(db: Database, sql: string, ...bindings: SQLQueryBindings[]): T | null {
  return db.query<T, SQLQueryBindings[]>(sql).get(...bindings);
}

export function getRows<T>(db: Database, sql: string, ...bindings: SQLQueryBindings[]): T[] {
  return db.query<T, SQLQueryBindings[]>(sql).all(...bindings);
}

export function withDatabaseAtPath<T>(dbPath: string, run: (ctx: DatabaseContext) => T): T {
  const ctx = openDatabase(dbPath);
  return run(ctx);
}

export async function withDatabaseAsyncAtPath<T>(dbPath: string, run: (ctx: DatabaseContext) => Promise<T>): Promise<T> {
  const ctx = openDatabase(dbPath);
  return await run(ctx);
}

export function withInitializedDatabase<T>(run: (ctx: DatabaseContext) => T): T {
  const resolvedPath = runtimeDbPath();
  assertDatabaseFileExists(resolvedPath);
  return withDatabaseAtPath(resolvedPath, (ctx) => {
    assertInitialized(ctx.db, ctx.dbPath);
    return run(ctx);
  });
}

export async function withInitializedDatabaseAsync<T>(
  run: (ctx: DatabaseContext) => Promise<T>,
): Promise<T> {
  const resolvedPath = runtimeDbPath();
  assertDatabaseFileExists(resolvedPath);
  return withDatabaseAsyncAtPath(resolvedPath, (ctx) => {
    assertInitialized(ctx.db, ctx.dbPath);
    return run(ctx);
  });
}

export function initializeStorage(): void {
  if (!existsSync(ENGINE_MIGRATIONS_DIR)) {
    throw new TossError("CONFIG_ERROR", `Engine migrations directory not found: ${ENGINE_MIGRATIONS_DIR}`);
  }
  withClient((db) => {
    migrate(db, { migrationsFolder: ENGINE_MIGRATIONS_DIR });
    const sqlite = db.$client;
    sqlite
      .query(
        `INSERT INTO ${ENGINE_META_TABLE}(key, value) VALUES(?, ?)
         ON CONFLICT(key) DO UPDATE SET value=excluded.value`,
      )
      .run("snapshot_interval", String(DEFAULT_SNAPSHOT_INTERVAL));
    sqlite
      .query(
        `INSERT INTO ${ENGINE_META_TABLE}(key, value) VALUES(?, ?)
         ON CONFLICT(key) DO UPDATE SET value=excluded.value`,
      )
      .run("snapshot_retain", String(DEFAULT_SNAPSHOT_RETAIN));
    sqlite
      .query(`INSERT INTO ${REF_TABLE}(name, commit_id, updated_at) VALUES(?, NULL, ?) ON CONFLICT(name) DO NOTHING`)
      .run(MAIN_REF_NAME, Date.now());
  });
}

export function tableExists(db: Database, name: string): boolean {
  const row = getRow<{ ok?: number }>(db, "SELECT 1 AS ok FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", name);
  return row?.ok === 1;
}

export function isInitialized(db: Database): boolean {
  const requiredTables = [
    ENGINE_META_TABLE,
    COMMIT_TABLE,
    COMMIT_PARENT_TABLE,
    REF_TABLE,
    REFLOG_TABLE,
    OP_TABLE,
    EFFECT_ROW_TABLE,
    EFFECT_SCHEMA_TABLE,
    SNAPSHOT_TABLE,
  ];
  if (requiredTables.some((table) => !tableExists(db, table))) {
    return false;
  }
  const hasMainRef = getRow<{ ok?: number }>(db, `SELECT 1 AS ok FROM ${REF_TABLE} WHERE name=? LIMIT 1`, MAIN_REF_NAME);
  if (hasMainRef?.ok !== 1) {
    return false;
  }
  for (const key of ["snapshot_interval", "snapshot_retain"]) {
    const meta = getRow<{ ok?: number }>(db, `SELECT 1 AS ok FROM ${ENGINE_META_TABLE} WHERE key=? LIMIT 1`, key);
    if (meta?.ok !== 1) {
      return false;
    }
  }
  return true;
}

export function assertInitialized(db: Database, dbPath: string): void {
  if (!isInitialized(db)) {
    throw notInitializedError(dbPath);
  }
}

export function runInTransaction<T>(db: Database, fn: () => T): T {
  db.run("BEGIN IMMEDIATE");
  try {
    const result = fn();
    db.run("COMMIT");
    return result;
  } catch (error) {
    db.run("ROLLBACK");
    throw error;
  }
}

export function runInTransactionWithDeferredForeignKeys<T>(db: Database, fn: () => T): T {
  db.run("PRAGMA foreign_keys=ON");
  db.run("BEGIN IMMEDIATE");
  try {
    db.run("PRAGMA defer_foreign_keys=ON");
    const result = fn();
    db.run("COMMIT");
    return result;
  } catch (error) {
    db.run("ROLLBACK");
    throw error;
  }
}

export function runInSavepoint<T>(
  db: Database,
  name: string,
  run: () => T,
  options: { rollbackOnSuccess?: boolean } = {},
): T {
  db.run(`SAVEPOINT ${name}`);
  try {
    const result = run();
    if (options.rollbackOnSuccess) {
      db.run(`ROLLBACK TO ${name}`);
    }
    db.run(`RELEASE ${name}`);
    return result;
  } catch (error) {
    db.run(`ROLLBACK TO ${name}`);
    db.run(`RELEASE ${name}`);
    throw error;
  }
}

export function listUserTables(db: Database): string[] {
  const rows = getRows<{ name: string }>(
    db,
    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT GLOB '_toss_*' AND name NOT GLOB '__drizzle_*' AND name NOT GLOB 'sqlite_*' ORDER BY name",
  );
  return rows.map((row) => row.name);
}

export function getMetaValue(db: Database, key: string): string | null {
  const row = getRow<{ value?: string }>(db, `SELECT value FROM ${ENGINE_META_TABLE} WHERE key=?`, key);
  return row?.value ?? null;
}
