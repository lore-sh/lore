import type { SQLQueryBindings, Database } from "bun:sqlite";
import { existsSync, mkdirSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { migrate } from "drizzle-orm/bun-sqlite/migrator";
import { getClientPath, getSqlite, hasClient, initClient, withClient } from "./engine/client";
import { EngineMetaTable, RefTable } from "./engine/schema.sql";
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

function runtimeDbPath(): string {
  return getClientPath() ?? resolveDbPath();
}

function openDatabase(dbPath: string, options: { recreateClient?: boolean } = {}): DatabaseContext {
  ensureDatabaseDirectory(dbPath);
  if (!hasClient()) {
    initClient(dbPath);
  } else if (getClientPath() !== dbPath) {
    initClient(dbPath, { recreate: options.recreateClient ?? false });
  }
  const db = getSqlite();
  return { db, dbPath };
}

export function configureDatabase(dbPathFromArg?: string): DatabaseContext {
  return openDatabase(resolveDbPath(dbPathFromArg), { recreateClient: true });
}

export function getRow<T>(db: Database, sql: string, ...bindings: SQLQueryBindings[]): T | null {
  return db.query<T, SQLQueryBindings[]>(sql).get(...bindings);
}

export function getRows<T>(db: Database, sql: string, ...bindings: SQLQueryBindings[]): T[] {
  return db.query<T, SQLQueryBindings[]>(sql).all(...bindings);
}

function openInitializedDatabase(): DatabaseContext {
  const resolvedPath = runtimeDbPath();
  assertDatabaseFileExists(resolvedPath);
  const ctx = openDatabase(resolvedPath);
  assertInitialized(ctx.db, ctx.dbPath);
  return ctx;
}

export function withInitializedDatabase<T>(run: (ctx: DatabaseContext) => T): T {
  return run(openInitializedDatabase());
}

export async function withInitializedDatabaseAsync<T>(
  run: (ctx: DatabaseContext) => Promise<T>,
): Promise<T> {
  return await run(openInitializedDatabase());
}

export function initializeStorage(): void {
  if (!existsSync(ENGINE_MIGRATIONS_DIR)) {
    throw new TossError("CONFIG_ERROR", `Engine migrations directory not found: ${ENGINE_MIGRATIONS_DIR}`);
  }
  withClient((db) => {
    migrate(db, { migrationsFolder: ENGINE_MIGRATIONS_DIR });
    for (const [key, value] of [
      ["snapshot_interval", DEFAULT_SNAPSHOT_INTERVAL],
      ["snapshot_retain", DEFAULT_SNAPSHOT_RETAIN],
    ] as const) {
      db.insert(EngineMetaTable)
        .values({ key, value: String(value) })
        .onConflictDoUpdate({ target: EngineMetaTable.key, set: { value: String(value) } })
        .run();
    }
    db.insert(RefTable)
      .values({ name: MAIN_REF_NAME, commitId: null, updatedAt: Date.now() })
      .onConflictDoNothing({ target: RefTable.name })
      .run();
  });
}

export function tableExists(db: Database, name: string): boolean {
  const row = getRow<{ ok?: number }>(db, "SELECT 1 AS ok FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", name);
  return row?.ok === 1;
}

export function isInitialized(db: Database): boolean {
  const requiredTables = [
    ENGINE_META_TABLE, COMMIT_TABLE, COMMIT_PARENT_TABLE, REF_TABLE,
    REFLOG_TABLE, OP_TABLE, EFFECT_ROW_TABLE, EFFECT_SCHEMA_TABLE, SNAPSHOT_TABLE,
  ];
  if (requiredTables.some((table) => !tableExists(db, table))) {
    return false;
  }
  function hasRow(table: string, column: string, value: string): boolean {
    return getRow<{ ok?: number }>(db, `SELECT 1 AS ok FROM ${table} WHERE ${column}=? LIMIT 1`, value)?.ok === 1;
  }
  return hasRow(REF_TABLE, "name", MAIN_REF_NAME)
    && hasRow(ENGINE_META_TABLE, "key", "snapshot_interval")
    && hasRow(ENGINE_META_TABLE, "key", "snapshot_retain");
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
