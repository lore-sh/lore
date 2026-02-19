import type { Database } from "bun:sqlite";
import { canonicalJson, sha256Hex } from "./checksum";
import { listUserTables } from "./db";
import { TossError } from "./errors";
import { quoteIdentifier } from "./sql";
import type { JsonObject, JsonPrimitive } from "./types";

export interface TableInfoRow {
  cid: number;
  name: string;
  type: string;
  notnull: number;
  dflt_value: string | null;
  pk: number;
}

export function serializeValue(value: JsonPrimitive): JsonPrimitive {
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      return null;
    }
    return value;
  }
  return value;
}

export function normalizeRowObject(row: Record<string, unknown>): JsonObject {
  const output: JsonObject = {};
  for (const [key, value] of Object.entries(row)) {
    if (value === null || typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
      output[key] = value;
      continue;
    }
    if (value instanceof Uint8Array) {
      output[key] = Buffer.from(value).toString("base64");
      continue;
    }
    output[key] = JSON.stringify(value);
  }
  return output;
}

export function tableInfo(db: Database, table: string): TableInfoRow[] {
  return db.query(`PRAGMA table_info(${quoteIdentifier(table)})`).all() as TableInfoRow[];
}

export function primaryKeyColumns(db: Database, table: string): string[] {
  return tableInfo(db, table)
    .filter((column) => column.pk > 0)
    .sort((a, b) => a.pk - b.pk)
    .map((column) => column.name);
}

export function tableDDL(db: Database, table: string): string | null {
  const row = db
    .query("SELECT sql FROM sqlite_master WHERE type='table' AND name=? LIMIT 1")
    .get(table) as { sql: string | null } | null;
  return row?.sql ?? null;
}

export function whereClauseFromRecord(
  values: Record<string, JsonPrimitive>,
): { clause: string; bindings: JsonPrimitive[] } {
  const keys = Object.keys(values);
  if (keys.length === 0) {
    throw new TossError("INVALID_OPERATION", "where must not be empty");
  }

  const terms: string[] = [];
  const bindings: JsonPrimitive[] = [];
  for (const key of keys) {
    const value = values[key];
    if (value === undefined) {
      throw new TossError("INVALID_OPERATION", `where value missing for key: ${key}`);
    }
    const quoted = quoteIdentifier(key);
    if (value === null) {
      terms.push(`${quoted} IS NULL`);
      continue;
    }
    terms.push(`${quoted} = ?`);
    bindings.push(serializeValue(value));
  }
  return { clause: terms.join(" AND "), bindings };
}

export function fetchRowsByWhere(
  db: Database,
  table: string,
  where: Record<string, JsonPrimitive>,
): Array<Record<string, unknown>> {
  const { clause, bindings } = whereClauseFromRecord(where);
  const sql = `SELECT rowid AS __toss_rowid, * FROM ${quoteIdentifier(table)} WHERE ${clause}`;
  return db.query(sql).all(...bindings) as Array<Record<string, unknown>>;
}

export function fetchAllRows(db: Database, table: string): JsonObject[] {
  const rows = db.query(`SELECT rowid AS __toss_rowid, * FROM ${quoteIdentifier(table)} ORDER BY rowid ASC`).all() as Array<
    Record<string, unknown>
  >;
  return rows.map((row) => normalizeRowObject(row));
}

export function pkFromRow(db: Database, table: string, row: Record<string, unknown>): Record<string, JsonPrimitive> {
  const pkCols = primaryKeyColumns(db, table);
  if (pkCols.length === 0) {
    const rowid = row.__toss_rowid;
    if (typeof rowid !== "number") {
      throw new TossError("INVALID_OPERATION", `Cannot determine rowid primary key for ${table}`);
    }
    return { __rowid: rowid };
  }

  const pk: Record<string, JsonPrimitive> = {};
  for (const column of pkCols) {
    const value = row[column];
    if (value === undefined) {
      throw new TossError("INVALID_OPERATION", `PK column missing in row: ${table}.${column}`);
    }
    if (value === null || typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
      pk[column] = value;
    } else {
      throw new TossError("INVALID_OPERATION", `Unsupported PK value type in ${table}.${column}`);
    }
  }
  return pk;
}

export function fetchRowByPk(
  db: Database,
  table: string,
  pk: Record<string, JsonPrimitive>,
): Record<string, unknown> | null {
  if (Object.hasOwn(pk, "__rowid")) {
    const rowid = pk.__rowid;
    if (typeof rowid !== "number") {
      throw new TossError("INVALID_OPERATION", "Invalid __rowid key");
    }
    return db
      .query(`SELECT rowid AS __toss_rowid, * FROM ${quoteIdentifier(table)} WHERE rowid = ? LIMIT 1`)
      .get(rowid) as Record<string, unknown> | null;
  }

  const { clause, bindings } = whereClauseFromRecord(pk);
  return db
    .query(`SELECT rowid AS __toss_rowid, * FROM ${quoteIdentifier(table)} WHERE ${clause} LIMIT 1`)
    .get(...bindings) as Record<string, unknown> | null;
}

export function rowHash(row: JsonObject | null): string | null {
  if (!row) {
    return null;
  }
  return sha256Hex(canonicalJson(row));
}

export function schemaHash(db: Database): string {
  const rows = db
    .query(
      "SELECT name, sql FROM sqlite_master WHERE type='table' AND name NOT LIKE '_toss_%' AND name NOT LIKE 'sqlite_%' ORDER BY name",
    )
    .all() as Array<{ name: string; sql: string | null }>;
  return sha256Hex(rows);
}

export function stateHash(db: Database): string {
  const tables = listUserTables(db);
  const state: Record<string, JsonObject[]> = {};
  for (const table of tables) {
    state[table] = fetchAllRows(db, table);
  }
  return sha256Hex(state);
}
