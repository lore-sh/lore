import { Database } from "bun:sqlite";
import { mock } from "bun:test";
import type {
  Client,
  InArgs,
  InStatement,
  InValue,
  Replicated,
  ResultSet,
  Row,
  Transaction,
  TransactionMode,
} from "@libsql/client";

type NormalizedStatement = {
  sql: string;
  args: InArgs | undefined;
};

type QueryBindings =
  | { kind: "none" }
  | { kind: "positional"; values: unknown[] }
  | { kind: "named"; values: Record<string, unknown> };

type QueryRunResult = {
  changes?: number;
  lastInsertRowid?: number | bigint;
};

type PreparedQuery = {
  all: (...params: unknown[]) => unknown[];
  run: (...params: unknown[]) => QueryRunResult;
  columnNames?: string[];
  columnTypes?: string[];
};

const registeredRemoteByUrl = new Map<string, string>();
const registeredUrlByPath = new Map<string, string>();
let fixtureCounter = 0;
let fixtureInstalled = false;

function normalizeStatement(stmtOrSql: InStatement | string, args?: InArgs): NormalizedStatement {
  if (typeof stmtOrSql === "string") {
    return { sql: stmtOrSql, args };
  }
  return { sql: stmtOrSql.sql, args: stmtOrSql.args };
}

function normalizeBatchStatement(stmt: InStatement | [string, InArgs?]): NormalizedStatement {
  if (Array.isArray(stmt)) {
    const [sql, args] = stmt;
    return { sql, args };
  }
  return normalizeStatement(stmt);
}

function normalizeBindingValue(value: InValue): unknown {
  if (value instanceof Date) {
    return value.toISOString();
  }
  if (value instanceof ArrayBuffer) {
    return new Uint8Array(value);
  }
  return value;
}

function normalizeBindings(args: InArgs | undefined): QueryBindings {
  if (args === undefined) {
    return { kind: "none" };
  }
  if (Array.isArray(args)) {
    return { kind: "positional", values: args.map((value) => normalizeBindingValue(value)) };
  }

  const values: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(args)) {
    const normalizedValue = normalizeBindingValue(value);
    if (key.startsWith("$") || key.startsWith(":") || key.startsWith("@")) {
      values[key] = normalizedValue;
      continue;
    }
    values[`$${key}`] = normalizedValue;
    values[`:${key}`] = normalizedValue;
    values[`@${key}`] = normalizedValue;
  }
  return { kind: "named", values };
}

function runQuery(query: PreparedQuery, bindings: QueryBindings): QueryRunResult {
  if (bindings.kind === "none") {
    return query.run();
  }
  if (bindings.kind === "positional") {
    return query.run(...bindings.values);
  }
  return query.run(bindings.values);
}

function readQueryRows(query: PreparedQuery, bindings: QueryBindings): unknown[] {
  if (bindings.kind === "none") {
    return query.all();
  }
  if (bindings.kind === "positional") {
    return query.all(...bindings.values);
  }
  return query.all(bindings.values);
}

function normalizeRowValue(value: unknown): unknown {
  if (value instanceof Uint8Array) {
    return value.buffer.slice(value.byteOffset, value.byteOffset + value.byteLength);
  }
  return value;
}

function toRow(raw: unknown, columns: string[]): Row {
  const source = raw && typeof raw === "object" && !Array.isArray(raw) ? raw : {};
  const sourceRecord = source as Record<string, unknown>;
  const row = { length: columns.length } as Record<string, unknown>;
  for (const [index, column] of columns.entries()) {
    const value = normalizeRowValue(sourceRecord[column]);
    row[index] = value;
    row[column] = value;
  }
  return row as unknown as Row;
}

function createResultSet(params: {
  columns: string[];
  columnTypes: string[];
  rows: Row[];
  rowsAffected: number;
  lastInsertRowid: bigint | undefined;
}): ResultSet {
  const payload = {
    columns: params.columns,
    columnTypes: params.columnTypes,
    rows: params.rows,
    rowsAffected: params.rowsAffected,
    lastInsertRowid: params.lastInsertRowid,
  };
  return {
    ...payload,
    toJSON() {
      return payload;
    },
  };
}

function toLastInsertRowid(value: number | bigint | undefined): bigint | undefined {
  if (typeof value === "bigint") {
    return value;
  }
  if (typeof value === "number" && Number.isInteger(value)) {
    return BigInt(value);
  }
  return undefined;
}

function executeStatement(db: Database, statement: NormalizedStatement): ResultSet {
  const query = db.query(statement.sql) as unknown as PreparedQuery;
  const bindings = normalizeBindings(statement.args);
  const columns = Array.isArray(query.columnNames) ? [...query.columnNames] : [];

  if (columns.length > 0) {
    const columnTypes = Array.isArray(query.columnTypes) ? [...query.columnTypes] : columns.map(() => "");
    const rows = readQueryRows(query, bindings).map((raw) => toRow(raw, columns));
    return createResultSet({
      columns,
      columnTypes,
      rows,
      rowsAffected: 0,
      lastInsertRowid: undefined,
    });
  }

  const run = runQuery(query, bindings);
  return createResultSet({
    columns: [],
    columnTypes: [],
    rows: [],
    rowsAffected: run.changes ?? 0,
    lastInsertRowid: toLastInsertRowid(run.lastInsertRowid),
  });
}

function beginSql(mode: TransactionMode): string {
  switch (mode) {
    case "write":
      return "BEGIN IMMEDIATE";
    case "read":
      return "BEGIN DEFERRED";
    case "deferred":
      return "BEGIN DEFERRED";
  }
}

function openDatabase(path: string): Database {
  const db = new Database(path);
  db.run("PRAGMA foreign_keys=ON");
  return db;
}

class BunFixtureTransaction implements Transaction {
  #db: Database;
  closed = false;

  constructor(db: Database, mode: TransactionMode) {
    this.#db = db;
    this.#db.run(beginSql(mode));
  }

  #assertOpen(): void {
    if (this.closed) {
      throw new Error("Transaction is closed");
    }
  }

  async execute(stmt: InStatement): Promise<ResultSet>;
  async execute(sql: string, args?: InArgs): Promise<ResultSet>;
  async execute(stmtOrSql: InStatement | string, args?: InArgs): Promise<ResultSet> {
    this.#assertOpen();
    return executeStatement(this.#db, normalizeStatement(stmtOrSql, args));
  }

  async batch(stmts: Array<InStatement>): Promise<Array<ResultSet>> {
    this.#assertOpen();
    const results: Array<ResultSet> = [];
    for (const stmt of stmts) {
      results.push(executeStatement(this.#db, normalizeStatement(stmt)));
    }
    return results;
  }

  async executeMultiple(sql: string): Promise<void> {
    this.#assertOpen();
    this.#db.exec(sql);
  }

  async rollback(): Promise<void> {
    if (this.closed) {
      return;
    }
    if (this.#db.inTransaction) {
      this.#db.run("ROLLBACK");
    }
    this.closed = true;
  }

  async commit(): Promise<void> {
    this.#assertOpen();
    if (this.#db.inTransaction) {
      this.#db.run("COMMIT");
    }
    this.closed = true;
  }

  close(): void {
    if (this.closed) {
      return;
    }
    if (this.#db.inTransaction) {
      this.#db.run("ROLLBACK");
    }
    this.closed = true;
  }
}

class BunFixtureClient implements Client {
  #path: string;
  #db: Database;
  closed = false;
  protocol = "http";

  constructor(path: string) {
    this.#path = path;
    this.#db = openDatabase(path);
  }

  #assertOpen(): void {
    if (this.closed) {
      throw new Error("Client is closed");
    }
  }

  async execute(stmt: InStatement): Promise<ResultSet>;
  async execute(sql: string, args?: InArgs): Promise<ResultSet>;
  async execute(stmtOrSql: InStatement | string, args?: InArgs): Promise<ResultSet> {
    this.#assertOpen();
    return executeStatement(this.#db, normalizeStatement(stmtOrSql, args));
  }

  async batch(stmts: Array<InStatement | [string, InArgs?]>, mode: TransactionMode = "deferred"): Promise<Array<ResultSet>> {
    this.#assertOpen();
    this.#db.run(beginSql(mode));
    try {
      const results: Array<ResultSet> = [];
      for (const stmt of stmts) {
        results.push(executeStatement(this.#db, normalizeBatchStatement(stmt)));
      }
      this.#db.run("COMMIT");
      return results;
    } catch (error) {
      if (this.#db.inTransaction) {
        this.#db.run("ROLLBACK");
      }
      throw error;
    }
  }

  async migrate(stmts: Array<InStatement>): Promise<Array<ResultSet>> {
    this.#assertOpen();
    this.#db.run("PRAGMA foreign_keys=OFF");
    try {
      return await this.batch(stmts, "deferred");
    } finally {
      this.#db.run("PRAGMA foreign_keys=ON");
    }
  }

  async transaction(mode: TransactionMode = "write"): Promise<Transaction> {
    this.#assertOpen();
    return new BunFixtureTransaction(this.#db, mode);
  }

  async executeMultiple(sql: string): Promise<void> {
    this.#assertOpen();
    this.#db.exec(sql);
  }

  async sync(): Promise<Replicated> {
    this.#assertOpen();
    return undefined;
  }

  reconnect(): void {
    if (!this.closed) {
      return;
    }
    this.#db = openDatabase(this.#path);
    this.closed = false;
  }

  close(): void {
    if (this.closed) {
      return;
    }
    this.#db.close(false);
    this.closed = true;
  }
}

function remotePathFromUrl(remoteUrl: string): string {
  if (!URL.canParse(remoteUrl)) {
    throw new Error(`Remote URL is invalid: ${remoteUrl}`);
  }
  const normalizedUrl = new URL(remoteUrl).toString();
  const path = registeredRemoteByUrl.get(normalizedUrl);
  if (!path) {
    throw new Error(`HTTP remote fixture is not registered: ${normalizedUrl}`);
  }
  return path;
}

export function registerHttpRemoteFixture(remoteDbPath: string): string {
  const existing = registeredUrlByPath.get(remoteDbPath);
  if (existing) {
    return existing;
  }
  fixtureCounter += 1;
  const url = new URL(`https://fixture-${fixtureCounter}.lore.invalid`).toString();
  registeredRemoteByUrl.set(url, remoteDbPath);
  registeredUrlByPath.set(remoteDbPath, url);
  return url;
}

export function installHttpRemoteFixture(): void {
  if (fixtureInstalled) {
    return;
  }
  fixtureInstalled = true;
  mock.module("@libsql/client/web", () => {
    return {
      createClient(config: { url: string }) {
        return new BunFixtureClient(remotePathFromUrl(config.url));
      },
    };
  });
}
