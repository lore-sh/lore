import { type SQLQueryBindings, Database } from "bun:sqlite";
import { resolve } from "node:path";
import { TossError } from "./errors";
import type { DatabaseOptions } from "./types";

export const DEFAULT_DB_NAME = "toss.db";
export const SCHEMA_FINGERPRINT = "toss-canonical-observed-2026-02-19";
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

export interface DatabaseContext {
  db: Database;
  dbPath: string;
}

export function resolveDbPath(pathFromArg?: string): string {
  const candidate = pathFromArg ?? Bun.env.TOSS_DB_PATH ?? DEFAULT_DB_NAME;
  return resolve(process.cwd(), candidate);
}

function applyPragmas(db: Database): void {
  db.run("PRAGMA journal_mode=WAL");
  db.run("PRAGMA synchronous=FULL");
  db.run("PRAGMA foreign_keys=ON");
  db.run("PRAGMA busy_timeout=5000");
}

function openDatabase(pathFromArg?: string): DatabaseContext {
  const dbPath = resolveDbPath(pathFromArg);
  const db = new Database(dbPath);
  applyPragmas(db);
  return { db, dbPath };
}

function closeDatabase(db: Database): void {
  db.close(false);
}

export function getRow<T>(db: Database, sql: string, ...bindings: SQLQueryBindings[]): T | null {
  return db.query<T, SQLQueryBindings[]>(sql).get(...bindings);
}

export function getRows<T>(db: Database, sql: string, ...bindings: SQLQueryBindings[]): T[] {
  return db.query<T, SQLQueryBindings[]>(sql).all(...bindings);
}

export function withDatabase<T>(options: DatabaseOptions | undefined, run: (ctx: DatabaseContext) => T): T {
  const ctx = openDatabase(options?.dbPath);
  try {
    return run(ctx);
  } finally {
    closeDatabase(ctx.db);
  }
}

export async function withDatabaseAsync<T>(
  options: DatabaseOptions | undefined,
  run: (ctx: DatabaseContext) => Promise<T>,
): Promise<T> {
  const ctx = openDatabase(options?.dbPath);
  try {
    return await run(ctx);
  } finally {
    closeDatabase(ctx.db);
  }
}

export function withInitializedDatabase<T>(options: DatabaseOptions | undefined, run: (ctx: DatabaseContext) => T): T {
  return withDatabase(options, (ctx) => {
    assertInitialized(ctx.db, ctx.dbPath);
    return run(ctx);
  });
}

export async function withInitializedDatabaseAsync<T>(
  options: DatabaseOptions | undefined,
  run: (ctx: DatabaseContext) => Promise<T>,
): Promise<T> {
  return withDatabaseAsync(options, (ctx) => {
    assertInitialized(ctx.db, ctx.dbPath);
    return run(ctx);
  });
}

export function initializeStorage(db: Database): void {
  db.run(`
    CREATE TABLE IF NOT EXISTS ${ENGINE_META_TABLE} (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL
    );
  `);

  db.run(`
    CREATE TABLE IF NOT EXISTS ${COMMIT_TABLE} (
      commit_id TEXT PRIMARY KEY,
      seq INTEGER NOT NULL UNIQUE,
      kind TEXT NOT NULL,
      message TEXT NOT NULL,
      created_at TEXT NOT NULL,
      parent_count INTEGER NOT NULL,
      schema_hash_before TEXT NOT NULL,
      schema_hash_after TEXT NOT NULL,
      state_hash_after TEXT NOT NULL,
      plan_hash TEXT NOT NULL,
      inverse_ready INTEGER NOT NULL,
      reverted_target_id TEXT
    );
  `);

  db.run(`
    CREATE TABLE IF NOT EXISTS ${COMMIT_PARENT_TABLE} (
      commit_id TEXT NOT NULL,
      parent_commit_id TEXT NOT NULL,
      ord INTEGER NOT NULL,
      PRIMARY KEY (commit_id, ord)
    );
  `);

  db.run(`
    CREATE TABLE IF NOT EXISTS ${REF_TABLE} (
      name TEXT PRIMARY KEY,
      commit_id TEXT,
      updated_at TEXT NOT NULL
    );
  `);

  db.run(`
    CREATE TABLE IF NOT EXISTS ${REFLOG_TABLE} (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ref_name TEXT NOT NULL,
      old_commit_id TEXT,
      new_commit_id TEXT,
      reason TEXT NOT NULL,
      created_at TEXT NOT NULL
    );
  `);

  db.run(`
    CREATE TABLE IF NOT EXISTS ${OP_TABLE} (
      commit_id TEXT NOT NULL,
      op_index INTEGER NOT NULL,
      op_type TEXT NOT NULL,
      op_json TEXT NOT NULL,
      PRIMARY KEY (commit_id, op_index)
    );
  `);

  db.run(`
    CREATE TABLE IF NOT EXISTS ${EFFECT_ROW_TABLE} (
      commit_id TEXT NOT NULL,
      effect_index INTEGER NOT NULL,
      table_name TEXT NOT NULL,
      pk_json TEXT NOT NULL,
      op_kind TEXT NOT NULL,
      before_row_json TEXT,
      after_row_json TEXT,
      before_hash TEXT,
      after_hash TEXT,
      PRIMARY KEY (commit_id, effect_index)
    );
  `);

  db.run(`
    CREATE INDEX IF NOT EXISTS idx_toss_effect_row_table_pk
      ON ${EFFECT_ROW_TABLE}(table_name, pk_json);
  `);

  db.run(`
    CREATE TABLE IF NOT EXISTS ${EFFECT_SCHEMA_TABLE} (
      commit_id TEXT NOT NULL,
      effect_index INTEGER NOT NULL,
      table_name TEXT NOT NULL,
      before_table_json TEXT,
      after_table_json TEXT,
      PRIMARY KEY (commit_id, effect_index)
    );
  `);

  db.run(`
    CREATE INDEX IF NOT EXISTS idx_toss_effect_schema_table_column
      ON ${EFFECT_SCHEMA_TABLE}(table_name);
  `);

  db.run(`
    CREATE TABLE IF NOT EXISTS ${SNAPSHOT_TABLE} (
      commit_id TEXT PRIMARY KEY,
      file_path TEXT NOT NULL,
      file_sha256 TEXT NOT NULL,
      created_at TEXT NOT NULL,
      row_count_hint INTEGER NOT NULL
    );
  `);

  const upsertMeta = db.query(`
    INSERT INTO ${ENGINE_META_TABLE}(key, value)
    VALUES(?, ?)
    ON CONFLICT(key) DO UPDATE SET value=excluded.value;
  `);
  upsertMeta.run("schema_fingerprint", SCHEMA_FINGERPRINT);
  upsertMeta.run("snapshot_interval", String(DEFAULT_SNAPSHOT_INTERVAL));
  upsertMeta.run("snapshot_retain", String(DEFAULT_SNAPSHOT_RETAIN));

  const now = new Date().toISOString();
  db.query(`
    INSERT INTO ${REF_TABLE}(name, commit_id, updated_at)
    VALUES(?, NULL, ?)
    ON CONFLICT(name) DO NOTHING;
  `).run(MAIN_REF_NAME, now);
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
  for (const table of requiredTables) {
    if (!tableExists(db, table)) {
      return false;
    }
  }

  const fingerprint = getRow<{ value?: string }>(db, `SELECT value FROM ${ENGINE_META_TABLE} WHERE key='schema_fingerprint'`);
  return fingerprint?.value === SCHEMA_FINGERPRINT;
}

export function assertInitialized(db: Database, dbPath: string): void {
  if (!isInitialized(db)) {
    throw new TossError("NOT_INITIALIZED", `Database is not initialized: ${dbPath}. Run \`toss init --force-new\`.`);
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
    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE '_toss_%' AND name NOT LIKE 'sqlite_%' ORDER BY name",
  );
  return rows.map((row) => row.name);
}

export function getMetaValue(db: Database, key: string): string | null {
  const row = getRow<{ value?: string }>(db, `SELECT value FROM ${ENGINE_META_TABLE} WHERE key=?`, key);
  return row?.value ?? null;
}
