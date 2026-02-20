import { Database } from "bun:sqlite";
import { drizzle } from "drizzle-orm/bun-sqlite";
import { resolve } from "node:path";
import { TossError } from "../errors";
import * as schema from "./schema.sql";

export function createEngineDb(sqlite: Database) {
  return drizzle({ client: sqlite, schema });
}
type EngineDb = ReturnType<typeof createEngineDb>;
type EngineTx = Parameters<EngineDb["transaction"]>[0] extends (tx: infer T) => unknown ? T : never;

export interface EngineClient {
  readonly path: string;
  readonly sqlite: Database;
  close(): void;
}

let client: EngineClient | null = null;
let configuredPath: string | null = null;

function applyPragmas(db: Database, isReadonly = false): void {
  db.run("PRAGMA foreign_keys=ON");
  db.run("PRAGMA busy_timeout=5000");
  if (isReadonly) {
    return;
  }
  db.run("PRAGMA journal_mode=WAL");
  db.run("PRAGMA synchronous=NORMAL");
  db.run("PRAGMA optimize=0x10002");
}

function createClient(path: string, isReadonly = false): EngineClient {
  const sqlite = new Database(path, { readonly: isReadonly, strict: true });
  applyPragmas(sqlite, isReadonly);
  return {
    path,
    sqlite,
    close() {
      sqlite.close(false);
    },
  };
}

export function initClient(dbPath: string, options: { recreate?: boolean } = {}): EngineClient {
  const path = resolve(dbPath);
  if (client && configuredPath === path) {
    return client;
  }
  if (client && !options.recreate) {
    throw new TossError(
      "CONFIG_ERROR",
      `Database client already initialized for ${configuredPath}. Refusing to switch to ${path}.`,
    );
  }
  closeClient();
  client = createClient(path);
  configuredPath = path;
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
  return getClient().sqlite;
}

export function closeClient({ resetPath = true }: { resetPath?: boolean } = {}): void {
  if (client) {
    client.close();
    client = null;
  }
  if (resetPath) {
    configuredPath = null;
  }
}

export function withClient<T>(run: (db: EngineDb) => T): T {
  return run(createEngineDb(getClient().sqlite));
}

export function withTransaction<T>(run: (tx: EngineTx) => T): T {
  return createEngineDb(getClient().sqlite).transaction((tx) => run(tx), { behavior: "immediate" });
}

export function openIsolatedClient(dbPath: string, options: { readonly?: boolean } = {}): EngineClient {
  return createClient(resolve(dbPath), options.readonly ?? false);
}
