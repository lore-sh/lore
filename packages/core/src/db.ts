import { existsSync } from "node:fs";
import { resolve } from "node:path";
import { Database } from "bun:sqlite";
import { TossError } from "./errors";

export const DEFAULT_DB_NAME = "toss.db";
export const SCHEMA_VERSION = 1;

export interface DatabaseContext {
  db: Database;
  dbPath: string;
}

export function resolveDbPath(pathFromArg?: string): string {
  const candidate = pathFromArg ?? process.env.TOSS_DB_PATH ?? DEFAULT_DB_NAME;
  return resolve(process.cwd(), candidate);
}

export function openDatabase(pathFromArg?: string): DatabaseContext {
  const dbPath = resolveDbPath(pathFromArg);
  const db = new Database(dbPath);
  db.exec("PRAGMA foreign_keys=ON");
  return { db, dbPath };
}

export function closeDatabase(db: Database): void {
  db.close(false);
}

export function initializeStorage(db: Database): void {
  db.exec(`
    CREATE TABLE IF NOT EXISTS _toss_log (
      id TEXT PRIMARY KEY,
      timestamp TEXT NOT NULL,
      kind TEXT NOT NULL,
      message TEXT NOT NULL,
      operations TEXT NOT NULL,
      schema_version INTEGER NOT NULL,
      checksum TEXT NOT NULL,
      reverted_target_id TEXT
    );
  `);
  db.exec(`
    CREATE TABLE IF NOT EXISTS _toss_meta (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL
    );
  `);

  const upsert = db.query(`
    INSERT INTO _toss_meta(key, value)
    VALUES(?, ?)
    ON CONFLICT(key) DO UPDATE SET value=excluded.value;
  `);
  upsert.run("schema_version", String(SCHEMA_VERSION));
}

export function isInitialized(db: Database): boolean {
  const hasLog = db
    .query("SELECT 1 AS ok FROM sqlite_master WHERE type='table' AND name='_toss_log' LIMIT 1")
    .get() as { ok?: number } | null;
  const hasMeta = db
    .query("SELECT 1 AS ok FROM sqlite_master WHERE type='table' AND name='_toss_meta' LIMIT 1")
    .get() as { ok?: number } | null;
  if (!hasLog?.ok || !hasMeta?.ok) {
    return false;
  }

  const schema = db.query("SELECT value FROM _toss_meta WHERE key='schema_version'").get() as { value?: string } | null;
  return schema?.value === String(SCHEMA_VERSION);
}

export function assertInitialized(db: Database, dbPath: string): void {
  if (!existsSync(dbPath) || !isInitialized(db)) {
    throw new TossError("NOT_INITIALIZED", `Database is not initialized: ${dbPath}. Run \`toss init\`.`);
  }
}

export function runInTransaction<T>(db: Database, fn: () => T): T {
  db.exec("BEGIN IMMEDIATE");
  try {
    const result = fn();
    db.exec("COMMIT");
    return result;
  } catch (error) {
    db.exec("ROLLBACK");
    throw error;
  }
}

export function getSchemaVersion(db: Database): number {
  const row = db.query("SELECT value FROM _toss_meta WHERE key='schema_version'").get() as { value?: string } | null;
  return Number(row?.value ?? SCHEMA_VERSION);
}

export function listUserTables(db: Database): string[] {
  const rows = db
    .query(
      "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE '_toss_%' AND name NOT LIKE 'sqlite_%' ORDER BY name",
    )
    .all() as Array<{ name: string }>;
  return rows.map((row) => row.name);
}
