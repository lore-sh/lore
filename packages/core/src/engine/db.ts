import type { SQLQueryBindings } from "bun:sqlite";
import { Database } from "bun:sqlite";
import { existsSync, mkdirSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { migrate } from "drizzle-orm/bun-sqlite/migrator";
import { CodedError } from "../error";
import { deleteWithSidecars, resolveHomeDir } from "./files";
import { createEngineDb } from "./client";
import { MetaTable, RefTable } from "./schema.sql";

export const DEFAULT_DB_DIR = ".toss";
export const DEFAULT_DB_NAME = "toss.db";
export const DEFAULT_SNAPSHOT_INTERVAL = 100;
export const DEFAULT_SNAPSHOT_RETAIN = 20;
export const DEFAULT_SYNC_PROTOCOL_VERSION = "1";
export const MAIN_REF_NAME = "main";
export const META_TABLE = "_toss_meta";
export const COMMIT_TABLE = "_toss_commit";
export const COMMIT_PARENT_TABLE = "_toss_commit_parent";
export const REF_TABLE = "_toss_ref";
export const REFLOG_TABLE = "_toss_reflog";
export const OP_TABLE = "_toss_op";
export const ROW_EFFECT_TABLE = "_toss_row_effect";
export const SCHEMA_EFFECT_TABLE = "_toss_schema_effect";
export const SNAPSHOT_TABLE = "_toss_snapshot";
export const LAST_PUSHED_COMMIT_META_KEY = "last_pushed_commit";
export const LAST_PULLED_COMMIT_META_KEY = "last_pulled_commit";
export const LAST_SYNC_STATE_META_KEY = "last_sync_state";
export const LAST_SYNC_ERROR_META_KEY = "last_sync_error";
export const SYNC_PROTOCOL_VERSION_META_KEY = "sync_protocol_version";
export const LAST_MATERIALIZED_COMMIT_META_KEY = "last_materialized_commit";
export const LAST_MATERIALIZED_AT_META_KEY = "last_materialized_at";
export const LAST_MATERIALIZED_ERROR_META_KEY = "last_materialized_error";
export const LAST_VERIFIED_AT_META_KEY = "last_verified_at";
export const LAST_VERIFIED_OK_META_KEY = "last_verified_ok";
export const RESETTABLE_META_DEFAULTS: ReadonlyArray<readonly [string, string]> = [];
export const PRESERVED_META_DEFAULTS = [
  [LAST_PUSHED_COMMIT_META_KEY, ""],
  [LAST_PULLED_COMMIT_META_KEY, ""],
  [LAST_SYNC_STATE_META_KEY, "offline"],
  [LAST_SYNC_ERROR_META_KEY, ""],
  [SYNC_PROTOCOL_VERSION_META_KEY, DEFAULT_SYNC_PROTOCOL_VERSION],
  [LAST_MATERIALIZED_COMMIT_META_KEY, ""],
  [LAST_MATERIALIZED_AT_META_KEY, ""],
  [LAST_MATERIALIZED_ERROR_META_KEY, ""],
] as const;
const ENGINE_MIGRATIONS_DIR = resolve(import.meta.dir, "../../migration");

export interface OpenDbOptions {
  busyTimeoutMs?: number | undefined;
}

function defaultDbPath(): string {
  return resolve(resolveHomeDir(), DEFAULT_DB_DIR, DEFAULT_DB_NAME);
}

export function resolveDbPath(pathFromArg?: string): string {
  const candidate = pathFromArg ?? defaultDbPath();
  return resolve(candidate);
}

function notInitializedError(dbPath: string): CodedError {
  return new CodedError("NOT_INITIALIZED", `Database is not initialized: ${dbPath}`);
}

function ensureDatabaseDirectory(dbPath: string): void {
  mkdirSync(dirname(dbPath), { recursive: true });
}

function assertDatabaseFileExists(dbPath: string): void {
  if (!existsSync(dbPath)) {
    throw notInitializedError(dbPath);
  }
}

function applyPragmas(db: Database, options: OpenDbOptions = {}): void {
  db.run("PRAGMA foreign_keys=ON");
  db.run(`PRAGMA busy_timeout=${Math.max(0, Math.floor(options.busyTimeoutMs ?? 5000))}`);
  db.run("PRAGMA journal_mode=WAL");
  db.run("PRAGMA synchronous=NORMAL");
  db.run("PRAGMA optimize=0x10002");
}

export function openDb(pathFromArg?: string, options: OpenDbOptions = {}): Database {
  const dbPath = resolveDbPath(pathFromArg);
  assertDatabaseFileExists(dbPath);
  const db = new Database(dbPath, { strict: true });
  try {
    applyPragmas(db, options);
    assertInitialized(db);
    return db;
  } catch (error) {
    db.close(false);
    throw error;
  }
}

function initializeStorage(db: Database): void {
  if (!existsSync(ENGINE_MIGRATIONS_DIR)) {
    throw new CodedError("CONFIG", `Engine migrations directory not found: ${ENGINE_MIGRATIONS_DIR}`);
  }
  const engineDb = createEngineDb(db);
  migrate(engineDb, { migrationsFolder: ENGINE_MIGRATIONS_DIR });
  for (const [key, value] of RESETTABLE_META_DEFAULTS) {
    engineDb.insert(MetaTable)
      .values({ key, value })
      .onConflictDoUpdate({ target: MetaTable.key, set: { value } })
      .run();
  }
  for (const [key, value] of PRESERVED_META_DEFAULTS) {
    engineDb.insert(MetaTable)
      .values({ key, value })
      .onConflictDoNothing({ target: MetaTable.key })
      .run();
  }
  engineDb.insert(RefTable)
    .values({ name: MAIN_REF_NAME, commitId: null, updatedAt: Date.now() })
    .onConflictDoNothing({ target: RefTable.name })
    .run();
}

export async function initDb(
  options: { dbPath?: string | undefined; forceNew?: boolean | undefined } = {},
): Promise<{ path: string }> {
  const dbPath = resolveDbPath(options.dbPath);
  ensureDatabaseDirectory(dbPath);
  if (options.forceNew) {
    await deleteWithSidecars(dbPath);
  }
  const db = new Database(dbPath, { strict: true });
  try {
    applyPragmas(db);
    initializeStorage(db);
    assertInitialized(db);
  } finally {
    db.close(false);
  }
  return { path: dbPath };
}

export function getRow<T>(db: Database, sql: string, ...bindings: SQLQueryBindings[]): T | null {
  return db.query<T, SQLQueryBindings[]>(sql).get(...bindings);
}

export function getRows<T>(db: Database, sql: string, ...bindings: SQLQueryBindings[]): T[] {
  return db.query<T, SQLQueryBindings[]>(sql).all(...bindings);
}

export function tableExists(db: Database, name: string): boolean {
  const row = getRow<{ ok?: number }>(db, "SELECT 1 AS ok FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", name);
  return row?.ok === 1;
}

export function isInitialized(db: Database): boolean {
  const requiredTables = [
    META_TABLE,
    COMMIT_TABLE,
    COMMIT_PARENT_TABLE,
    REF_TABLE,
    REFLOG_TABLE,
    OP_TABLE,
    ROW_EFFECT_TABLE,
    SCHEMA_EFFECT_TABLE,
    SNAPSHOT_TABLE,
  ];
  if (requiredTables.some((table) => !tableExists(db, table))) {
    return false;
  }
  function hasRow(table: string, column: string, value: string): boolean {
    return getRow<{ ok?: number }>(db, `SELECT 1 AS ok FROM ${table} WHERE ${column}=? LIMIT 1`, value)?.ok === 1;
  }
  return hasRow(REF_TABLE, "name", MAIN_REF_NAME);
}

export function assertInitialized(db: Database): void {
  if (!isInitialized(db)) {
    throw notInitializedError(db.filename);
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

export function runInDeferredTransaction<T>(db: Database, fn: () => T): T {
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
  const row = getRow<{ value?: string }>(db, `SELECT value FROM ${META_TABLE} WHERE key=?`, key);
  return row?.value ?? null;
}

export function normalizeMetaString(value: string | null): string | null {
  const trimmed = value?.trim();
  return trimmed?.length ? trimmed : null;
}

export function setMetaValue(db: Database, key: string, value: string): void {
  createEngineDb(db)
    .insert(MetaTable)
    .values({ key, value })
    .onConflictDoUpdate({
      target: MetaTable.key,
      set: { value },
    })
    .run();
}
