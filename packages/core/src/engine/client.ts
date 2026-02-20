import { Database } from "bun:sqlite";
import { drizzle } from "drizzle-orm/bun-sqlite";
import { resolve } from "node:path";
import { TossError } from "../errors";
import * as schema from "./schema.sql";

type EngineSchema = typeof schema;
function createDrizzle(sqlite: Database) {
  return drizzle({ client: sqlite, schema });
}
type EngineDb = ReturnType<typeof createDrizzle>;
type EngineTx = Parameters<EngineDb["transaction"]>[0] extends (tx: infer T) => unknown ? T : never;

export interface EngineClient {
  readonly path: string;
  readonly db: EngineDb;
  close(): void;
}

let client: EngineClient | null = null;
let configuredPath: string | null = null;

function applyPragmas(db: Database, isReadonly = false): void {
  if (!isReadonly) {
    db.run("PRAGMA journal_mode=WAL");
    db.run("PRAGMA synchronous=FULL");
  }
  db.run("PRAGMA foreign_keys=ON");
  db.run("PRAGMA busy_timeout=5000");
}

function createClient(path: string, isReadonly = false): EngineClient {
  const sqlite = isReadonly ? new Database(path, { readonly: true }) : new Database(path);
  applyPragmas(sqlite, isReadonly);
  const db = createDrizzle(sqlite);
  return {
    path,
    db,
    close() {
      sqlite.close(false);
    },
  };
}

export function initClient(dbPath: string): EngineClient {
  const path = resolve(dbPath);
  if (configuredPath && configuredPath !== path) {
    throw new TossError(
      "CONFIG_ERROR",
      `Database client already initialized for ${configuredPath}. Refusing to switch to ${path}.`,
    );
  }
  configuredPath = path;
  if (!client) {
    client = createClient(path);
  }
  return client;
}

export function getClient(): EngineClient {
  if (!client) {
    throw new TossError("CONFIG_ERROR", "Database client is not initialized. Call initClient(dbPath) first.");
  }
  return client;
}

export function hasClient(): boolean {
  return client !== null;
}

export function getClientPath(): string | null {
  return configuredPath;
}

export function getSqlite(): Database {
  return getClient().db.$client;
}

export function closeClient(options: { resetPath?: boolean } = {}): void {
  if (client) {
    client.close();
    client = null;
  }
  if (options.resetPath ?? true) {
    configuredPath = null;
  }
}

export function withClient<T>(run: (db: EngineDb) => T): T {
  return run(getClient().db);
}

export function withTransaction<T>(run: (tx: EngineTx) => T): T {
  return getClient().db.transaction((tx) => run(tx), { behavior: "immediate" });
}

export function openIsolatedClient(dbPath: string, options: { readonly?: boolean } = {}): EngineClient {
  return createClient(resolve(dbPath), options.readonly ?? false);
}
