import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { CodedError } from "./error";

export type EngineMigration = Readonly<{
  id: string;
  hash: string;
  sql: string;
}>;

export const DRIZZLE_MIGRATIONS_TABLE = "__drizzle_migrations";
const STATEMENT_BREAKPOINT = "--> statement-breakpoint";
export const DRIZZLE_MIGRATIONS_TABLE_SQL = `CREATE TABLE IF NOT EXISTS "${DRIZZLE_MIGRATIONS_TABLE}" (
  id TEXT PRIMARY KEY,
  hash TEXT NOT NULL,
  created_at INTEGER NOT NULL
)`;

const CORE_MIGRATION_DIR = join(import.meta.dir, "../migration");

function hashSql(sql: string): string {
  const hasher = new Bun.CryptoHasher("sha256");
  hasher.update(sql);
  return hasher.digest("hex");
}

function fsMigrations(): EngineMigration[] {
  const files = readdirSync(CORE_MIGRATION_DIR)
    .filter((file) => file.endsWith(".sql"))
    .sort((a, b) => a.localeCompare(b));
  const migrations: EngineMigration[] = [];
  for (const file of files) {
    const id = file.slice(0, -4);
    const sql = readFileSync(join(CORE_MIGRATION_DIR, file), "utf8");
    migrations.push({
      id,
      hash: hashSql(sql),
      sql,
    });
  }
  return migrations;
}

function assertUniqueMigrationIds(migrations: ReadonlyArray<EngineMigration>): void {
  const seen = new Set<string>();
  for (const migration of migrations) {
    if (seen.has(migration.id)) {
      throw new CodedError("CONFIG", `Duplicate migration id: ${migration.id}`);
    }
    seen.add(migration.id);
  }
}

function isEngineMigration(value: unknown): value is EngineMigration {
  if (!value || typeof value !== "object") {
    return false;
  }
  const id = Reflect.get(value, "id");
  const hash = Reflect.get(value, "hash");
  const sql = Reflect.get(value, "sql");
  return typeof id === "string" && typeof hash === "string" && typeof sql === "string";
}

function readEmbeddedMigrationsFromModule(value: unknown): EngineMigration[] | null {
  if (!value || typeof value !== "object") {
    return null;
  }
  const raw = Reflect.get(value, "ENGINE_MIGRATIONS");
  if (!Array.isArray(raw)) {
    return null;
  }
  const migrations: EngineMigration[] = [];
  for (const migration of raw) {
    if (!isEngineMigration(migration)) {
      return null;
    }
    migrations.push({ ...migration });
  }
  return migrations;
}

async function embeddedMigrations(): Promise<EngineMigration[] | null> {
  try {
    const embeddedMigrationsModulePath = "./generated/embedded-migrations";
    const mod = await import(embeddedMigrationsModulePath);
    return readEmbeddedMigrationsFromModule(mod);
  } catch {
    return null;
  }
}

export async function loadEngineMigrations(): Promise<EngineMigration[]> {
  const migrations = (await embeddedMigrations()) ?? fsMigrations();
  assertUniqueMigrationIds(migrations);
  return migrations;
}

export function migrationStatements(sql: string): string[] {
  return sql
    .split(STATEMENT_BREAKPOINT)
    .map((statement) => statement.trim())
    .filter((statement) => statement.length > 0);
}

export function pendingEngineMigrations(
  migrations: ReadonlyArray<EngineMigration>,
  appliedById: ReadonlyMap<string, string>,
): EngineMigration[] {
  const pending: EngineMigration[] = [];
  for (const migration of migrations) {
    const appliedHash = appliedById.get(migration.id);
    if (appliedHash === undefined) {
      pending.push(migration);
      continue;
    }
    if (appliedHash === migration.hash) {
      continue;
    }
    throw new CodedError(
      "IMMUTABLE_MIGRATION_EDITED",
      `Migration "${migration.id}" was edited after it was already applied.`,
    );
  }
  return pending;
}
