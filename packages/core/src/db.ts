import { existsSync, mkdirSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { drizzle } from "drizzle-orm/bun-sqlite";
import { CodedError } from "./error";
import { DRIZZLE_MIGRATIONS_TABLE, DRIZZLE_MIGRATIONS_TABLE_SQL, loadEngineMigrations, pendingEngineMigrations, type EngineMigration } from "./migration";
import { MetaTable, RefTable } from "./schema";
import * as schema from "./schema";
import { validateReadSql } from "./sql";

export type Database = ReturnType<typeof drizzle<typeof schema>>;

export type SkillPlatform = "claude" | "cursor" | "codex" | "opencode" | "openclaw";

export const DEFAULT_DB_DIR = ".lore";
export const DEFAULT_DB_NAME = "lore.db";
export const DEFAULT_SNAPSHOT_INTERVAL = 100;
export const DEFAULT_SNAPSHOT_RETAIN = 20;
export const DEFAULT_SYNC_PROTOCOL_VERSION = "1";
export const MAIN_REF_NAME = "main";
export const META_TABLE = "_lore_meta";
export const COMMIT_TABLE = "_lore_commit";
export const COMMIT_PARENT_TABLE = "_lore_commit_parent";
export const REF_TABLE = "_lore_ref";
export const REFLOG_TABLE = "_lore_reflog";
export const OP_TABLE = "_lore_op";
export const ROW_EFFECT_TABLE = "_lore_row_effect";
export const SCHEMA_EFFECT_TABLE = "_lore_schema_effect";
export const SNAPSHOT_TABLE = "_lore_snapshot";
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

export function isEnoent(error: unknown): boolean {
  return error instanceof Error && "code" in error && error.code === "ENOENT";
}

export function resolveHomeDir(): string {
  const home = process.env.HOME;
  if (!home) {
    throw new CodedError("CONFIG", "HOME is required to resolve the home directory.");
  }
  return resolve(home);
}

export async function deleteIfExists(path: string): Promise<void> {
  try {
    await Bun.file(path).delete();
  } catch (error) {
    if (isEnoent(error)) {
      return;
    }
    throw error;
  }
}

export async function deleteWithSidecars(path: string): Promise<void> {
  await Promise.all([deleteIfExists(path), deleteIfExists(`${path}-wal`), deleteIfExists(`${path}-shm`)]);
}

export async function deleteWalAndShm(path: string): Promise<void> {
  await Promise.all([deleteIfExists(`${path}-wal`), deleteIfExists(`${path}-shm`)]);
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

function ensureMigrationsTable(db: Database): void {
  db.$client.run(DRIZZLE_MIGRATIONS_TABLE_SQL);
}

function readAppliedMigrationHashesById(db: Database): Map<string, string> {
  const rows = db.$client.query<{ id: unknown; hash: unknown }, []>(`SELECT id, hash FROM "${DRIZZLE_MIGRATIONS_TABLE}"`).all();
  const applied = new Map<string, string>();
  for (const row of rows) {
    if (typeof row.id !== "string" || typeof row.hash !== "string") {
      throw new CodedError(
        "NOT_INITIALIZED",
        `Database uses an unsupported ${DRIZZLE_MIGRATIONS_TABLE} format. Recreate it with lore init --force-new.`,
      );
    }
    applied.set(row.id, row.hash);
  }
  return applied;
}

function applyMigration(db: Database, migration: EngineMigration): void {
  db.$client.run("BEGIN IMMEDIATE");
  try {
    db.$client.exec(migration.sql);
    db.$client
      .query<{ id: string; hash: string; created_at: number }, [string, string, number]>(
        `INSERT INTO "${DRIZZLE_MIGRATIONS_TABLE}" (id, hash, created_at) VALUES (?, ?, ?)`,
      )
      .run(migration.id, migration.hash, Date.now());
    db.$client.run("COMMIT");
  } catch (error) {
    db.$client.run("ROLLBACK");
    throw error;
  }
}

export async function applyEngineMigrations(db: Database): Promise<void> {
  ensureMigrationsTable(db);
  const migrations = await loadEngineMigrations();
  const appliedById = readAppliedMigrationHashesById(db);
  const pending = pendingEngineMigrations(migrations, appliedById);
  for (const migration of pending) {
    applyMigration(db, migration);
  }
}

function assertDatabaseFileExists(dbPath: string): void {
  if (!existsSync(dbPath)) {
    throw notInitializedError(dbPath);
  }
}

function applyPragmas(
  db: Database,
  options: {
    busyTimeoutMs?: number | undefined;
  } = {},
): void {
  db.$client.run("PRAGMA foreign_keys=ON");
  db.$client.run("PRAGMA legacy_alter_table=0");
  db.$client.run(`PRAGMA busy_timeout=${Math.max(0, Math.floor(options.busyTimeoutMs ?? 5000))}`);
  db.$client.run("PRAGMA journal_mode=WAL");
  db.$client.run("PRAGMA synchronous=NORMAL");
  db.$client.run("PRAGMA optimize=0x10002");
}

export function openDb(
  pathFromArg?: string,
  options: {
    busyTimeoutMs?: number | undefined;
  } = {},
): Database {
  const dbPath = resolveDbPath(pathFromArg);
  assertDatabaseFileExists(dbPath);
  const db = drizzle({ connection: { source: dbPath }, schema });
  try {
    applyPragmas(db, options);
    assertInitialized(db);
    return db;
  } catch (error) {
    db.$client.close(false);
    throw error;
  }
}

async function initializeStorage(db: Database): Promise<void> {
  await applyEngineMigrations(db);
  for (const [key, value] of PRESERVED_META_DEFAULTS) {
    db.insert(MetaTable)
      .values({ key, value })
      .onConflictDoNothing({ target: MetaTable.key })
      .run();
  }
  db.insert(RefTable)
    .values({ name: MAIN_REF_NAME, commitId: null, updatedAt: Date.now() })
    .onConflictDoNothing({ target: RefTable.name })
    .run();
}

export async function initDb(
  options: {
    dbPath?: string;
    forceNew?: boolean;
  } = {},
): Promise<{ path: string }> {
  const dbPath = resolveDbPath(options.dbPath);
  ensureDatabaseDirectory(dbPath);
  if (options.forceNew) {
    await deleteWithSidecars(dbPath);
  }
  const db = drizzle({ connection: { source: dbPath }, schema });
  try {
    applyPragmas(db);
    await initializeStorage(db);
    assertInitialized(db);
  } finally {
    db.$client.close(false);
  }
  return { path: dbPath };
}

export function tableExists(db: Database, name: string): boolean {
  const row = db.$client
    .query<{ ok?: number }, [string]>("SELECT 1 AS ok FROM sqlite_master WHERE type='table' AND name=? LIMIT 1")
    .get(name);
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
    return db.$client.query<{ ok?: number }, [string]>(`SELECT 1 AS ok FROM ${table} WHERE ${column}=? LIMIT 1`).get(value)?.ok === 1;
  }
  return hasRow(REF_TABLE, "name", MAIN_REF_NAME);
}

export function assertInitialized(db: Database): void {
  if (!isInitialized(db)) {
    throw notInitializedError(db.$client.filename);
  }
}

export function runSchemaAwareTransaction<T>(
  db: Database,
  fn: () => T,
  options: { hasSchemaChanges: boolean | (() => boolean); context?: string },
): T {
  const context = options.context ?? "schema transaction";
  db.$client.run("PRAGMA foreign_keys=ON");
  db.$client.run("PRAGMA defer_foreign_keys=ON");
  db.$client.run("BEGIN IMMEDIATE");
  try {
    const result = fn();
    const hasSchemaChanges =
      typeof options.hasSchemaChanges === "function"
        ? options.hasSchemaChanges()
        : options.hasSchemaChanges;
    if (hasSchemaChanges) {
      const fkRows = db.$client.query<{ table: string; rowid: number; parent: string; fkid: number }, []>(
        "PRAGMA foreign_key_check",
      ).all();
      if (fkRows.length > 0) {
        const fk = fkRows[0]!;
        throw new CodedError(
          "FK_VIOLATION",
          `${context}: foreign_key_check failed at ${fk.table} rowid=${fk.rowid} parent=${fk.parent} fk=${fk.fkid}`,
        );
      }
      const qcRow = db.$client.query<Record<string, unknown>, []>("PRAGMA quick_check(1)").get();
      const qcResult = qcRow ? Object.values(qcRow)[0] : undefined;
      if (qcResult !== "ok") {
        throw new CodedError("INTEGRITY_ERROR", `${context}: quick_check returned ${String(qcResult ?? "no rows")}`);
      }
    }
    db.$client.run("COMMIT");
    return result;
  } catch (error) {
    db.$client.run("ROLLBACK");
    throw error;
  }
}

export function runInSavepoint<T>(
  db: Database,
  name: string,
  run: () => T,
  options: { rollbackOnSuccess?: boolean } = {},
): T {
  db.$client.run(`SAVEPOINT ${name}`);
  try {
    const result = run();
    if (options.rollbackOnSuccess) {
      db.$client.run(`ROLLBACK TO ${name}`);
    }
    db.$client.run(`RELEASE ${name}`);
    return result;
  } catch (error) {
    db.$client.run(`ROLLBACK TO ${name}`);
    db.$client.run(`RELEASE ${name}`);
    throw error;
  }
}

export function listUserTables(db: Database): string[] {
  const rows = db.$client
    .query<{ name: string }, []>(
      "SELECT name FROM sqlite_master WHERE type='table' AND name NOT GLOB '_lore_*' AND name NOT GLOB '__drizzle_*' AND name NOT GLOB 'sqlite_*' ORDER BY name",
    )
    .all();
  return rows.map((row) => row.name);
}

export function listUserViews(db: Database): string[] {
  const rows = db.$client
    .query<{ name: string }, []>(
      "SELECT name FROM sqlite_master WHERE type='view' AND name NOT GLOB '_lore_*' AND name NOT GLOB '__drizzle_*' AND name NOT GLOB 'sqlite_*' ORDER BY name",
    )
    .all();
  return rows.map((row) => row.name);
}

export function getMetaValue(db: Database, key: string): string | null {
  const row = db.$client.query<{ value?: string }, [string]>(`SELECT value FROM ${META_TABLE} WHERE key=?`).get(key);
  return row?.value ?? null;
}

export function normalizeMetaString(value: string | null): string | null {
  const trimmed = value?.trim();
  return trimmed?.length ? trimmed : null;
}

export function setMetaValue(db: Database, key: string, value: string): void {
  db.insert(MetaTable)
    .values({ key, value })
    .onConflictDoUpdate({
      target: MetaTable.key,
      set: { value },
    })
    .run();
}

export function query(db: Database, sqlInput: string): Record<string, unknown>[] {
  const sql = validateReadSql(sqlInput);
  return db.$client.query<Record<string, unknown>, []>(sql).all();
}
