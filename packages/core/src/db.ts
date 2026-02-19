import { existsSync } from "node:fs";
import { resolve } from "node:path";
import { Database } from "bun:sqlite";
import { TossError } from "./errors";

export const DEFAULT_DB_NAME = "toss.db";
export const HISTORY_ENGINE = "gitlike";
export const FORMAT_GENERATION = 1;
export const SQLITE_MIN_VERSION = "3.51.2";
export const DEFAULT_SNAPSHOT_INTERVAL = 100;
export const DEFAULT_SNAPSHOT_RETAIN = 20;
export const MAIN_REF_NAME = "main";

export interface DatabaseContext {
  db: Database;
  dbPath: string;
}

export function resolveDbPath(pathFromArg?: string): string {
  const candidate = pathFromArg ?? process.env.TOSS_DB_PATH ?? DEFAULT_DB_NAME;
  return resolve(process.cwd(), candidate);
}

function applyPragmas(db: Database): void {
  db.run("PRAGMA journal_mode=WAL");
  db.run("PRAGMA synchronous=FULL");
  db.run("PRAGMA foreign_keys=ON");
  db.run("PRAGMA busy_timeout=5000");
}

export function openDatabase(pathFromArg?: string): DatabaseContext {
  const dbPath = resolveDbPath(pathFromArg);
  const db = new Database(dbPath);
  applyPragmas(db);
  return { db, dbPath };
}

export function closeDatabase(db: Database): void {
  db.close(false);
}

export function initializeStorage(db: Database): void {
  db.run(`
    CREATE TABLE IF NOT EXISTS _toss_repo_meta (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL
    );
  `);

  db.run(`
    CREATE TABLE IF NOT EXISTS _toss_commit (
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
    CREATE TABLE IF NOT EXISTS _toss_commit_parent (
      commit_id TEXT NOT NULL,
      parent_commit_id TEXT NOT NULL,
      ord INTEGER NOT NULL,
      PRIMARY KEY (commit_id, ord)
    );
  `);

  db.run(`
    CREATE TABLE IF NOT EXISTS _toss_ref (
      name TEXT PRIMARY KEY,
      commit_id TEXT,
      updated_at TEXT NOT NULL
    );
  `);

  db.run(`
    CREATE TABLE IF NOT EXISTS _toss_reflog (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ref_name TEXT NOT NULL,
      old_commit_id TEXT,
      new_commit_id TEXT,
      reason TEXT NOT NULL,
      created_at TEXT NOT NULL
    );
  `);

  db.run(`
    CREATE TABLE IF NOT EXISTS _toss_op (
      commit_id TEXT NOT NULL,
      op_index INTEGER NOT NULL,
      op_type TEXT NOT NULL,
      op_json TEXT NOT NULL,
      PRIMARY KEY (commit_id, op_index)
    );
  `);

  db.run(`
    CREATE TABLE IF NOT EXISTS _toss_effect_row (
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
      ON _toss_effect_row(table_name, pk_json);
  `);

  db.run(`
    CREATE TABLE IF NOT EXISTS _toss_effect_schema (
      commit_id TEXT NOT NULL,
      effect_index INTEGER NOT NULL,
      table_name TEXT NOT NULL,
      column_name TEXT,
      op_kind TEXT NOT NULL,
      ddl_before_sql TEXT,
      ddl_after_sql TEXT,
      table_rows_before_json TEXT,
      PRIMARY KEY (commit_id, effect_index)
    );
  `);

  db.run(`
    CREATE INDEX IF NOT EXISTS idx_toss_effect_schema_table_column
      ON _toss_effect_schema(table_name, column_name);
  `);

  db.run(`
    CREATE TABLE IF NOT EXISTS _toss_snapshot (
      commit_id TEXT PRIMARY KEY,
      file_path TEXT NOT NULL,
      file_sha256 TEXT NOT NULL,
      created_at TEXT NOT NULL,
      row_count_hint INTEGER NOT NULL
    );
  `);

  const upsertMeta = db.query(`
    INSERT INTO _toss_repo_meta(key, value)
    VALUES(?, ?)
    ON CONFLICT(key) DO UPDATE SET value=excluded.value;
  `);
  upsertMeta.run("history_engine", HISTORY_ENGINE);
  upsertMeta.run("format_generation", String(FORMAT_GENERATION));
  upsertMeta.run("sqlite_min_version", SQLITE_MIN_VERSION);
  upsertMeta.run("snapshot_interval", String(DEFAULT_SNAPSHOT_INTERVAL));
  upsertMeta.run("snapshot_retain", String(DEFAULT_SNAPSHOT_RETAIN));

  const now = new Date().toISOString();
  db.query(`
    INSERT INTO _toss_ref(name, commit_id, updated_at)
    VALUES(?, NULL, ?)
    ON CONFLICT(name) DO NOTHING;
  `).run(MAIN_REF_NAME, now);
}

function hasTable(db: Database, name: string): boolean {
  const row = db
    .query("SELECT 1 AS ok FROM sqlite_master WHERE type='table' AND name=? LIMIT 1")
    .get(name) as { ok?: number } | null;
  return row?.ok === 1;
}

export function isInitialized(db: Database): boolean {
  const requiredTables = [
    "_toss_repo_meta",
    "_toss_commit",
    "_toss_commit_parent",
    "_toss_ref",
    "_toss_reflog",
    "_toss_op",
    "_toss_effect_row",
    "_toss_effect_schema",
    "_toss_snapshot",
  ];
  for (const table of requiredTables) {
    if (!hasTable(db, table)) {
      return false;
    }
  }

  const generation = db
    .query("SELECT value FROM _toss_repo_meta WHERE key='format_generation'")
    .get() as { value?: string } | null;
  const engine = db
    .query("SELECT value FROM _toss_repo_meta WHERE key='history_engine'")
    .get() as { value?: string } | null;
  return generation?.value === String(FORMAT_GENERATION) && engine?.value === HISTORY_ENGINE;
}

export function detectLegacySchema(db: Database): boolean {
  return hasTable(db, "_toss_log") || hasTable(db, "_toss_meta");
}

export function assertInitialized(db: Database, dbPath: string): void {
  if (!existsSync(dbPath) || !isInitialized(db)) {
    if (detectLegacySchema(db)) {
      throw new TossError(
        "FORMAT_MISMATCH",
        `Legacy toss format detected at ${dbPath}. Use \`toss init --force-new\` to reinitialize.`,
      );
    }
    throw new TossError("NOT_INITIALIZED", `Database is not initialized: ${dbPath}. Run \`toss init\`.`);
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

export function listUserTables(db: Database): string[] {
  const rows = db
    .query(
      "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE '_toss_%' AND name NOT LIKE 'sqlite_%' ORDER BY name",
    )
    .all() as Array<{ name: string }>;
  return rows.map((row) => row.name);
}

export function getMetaValue(db: Database, key: string): string | null {
  const row = db.query("SELECT value FROM _toss_repo_meta WHERE key=?").get(key) as { value?: string } | null;
  return row?.value ?? null;
}

