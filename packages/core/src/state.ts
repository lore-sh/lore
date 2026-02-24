import type { Client, Transaction } from "@libsql/client";
import { listUserTables, type Database } from "./db";
import { CodedError } from "./error";
import { sha256Hex } from "./hash";
import { pragmaLiteral, quoteIdentifier } from "./sql";

type StorageClass = "null" | "integer" | "real" | "text" | "blob";

function parseRowRecord(value: unknown, label: string): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error(`${label} must be an object row`);
  }
  const row: Record<string, unknown> = {};
  for (const [key, cell] of Object.entries(value)) {
    row[key] = cell;
  }
  return row;
}

function parseStringLike(value: unknown, label: string): string {
  if (typeof value === "string") {
    return value;
  }
  if (typeof value === "number" || typeof value === "bigint" || typeof value === "boolean") {
    return String(value);
  }
  throw new Error(`${label} must be string-like`);
}

function parseInteger(value: unknown, label: string): number {
  if (typeof value === "number") {
    if (!Number.isInteger(value) || !Number.isSafeInteger(value)) {
      throw new Error(`${label} must be a safe integer`);
    }
    return value;
  }
  if (typeof value === "bigint") {
    const asNumber = Number(value);
    if (!Number.isSafeInteger(asNumber)) {
      throw new Error(`${label} must be a safe integer`);
    }
    return asNumber;
  }
  const normalized = parseStringLike(value, label).trim();
  if (!/^[+-]?\d+$/.test(normalized)) {
    throw new Error(`${label} must be an integer`);
  }
  const asNumber = Number(normalized);
  if (!Number.isInteger(asNumber) || !Number.isSafeInteger(asNumber)) {
    throw new Error(`${label} must be a safe integer`);
  }
  return asNumber;
}

function parseStorageClass(value: unknown, label: string): StorageClass {
  if (value === "null" || value === "integer" || value === "real" || value === "text" || value === "blob") {
    return value;
  }
  throw new Error(`${label} storage class is invalid`);
}

function isVisibleColumn(hidden: number): boolean {
  return hidden === 0 || hidden === 2 || hidden === 3;
}

function columnsFromPragmaRows(rows: Array<Record<string, unknown>>, tableName: string): string[] {
  const columns = rows
    .map((row) => ({
      cid: parseInteger(row.cid, `PRAGMA table_xinfo(${tableName}).cid`),
      name: parseStringLike(row.name, `PRAGMA table_xinfo(${tableName}).name`),
      hidden: parseInteger(row.hidden, `PRAGMA table_xinfo(${tableName}).hidden`),
    }))
    .filter((column) => isVisibleColumn(column.hidden))
    .sort((a, b) => a.cid - b.cid)
    .map((column) => column.name);

  if (columns.length === 0) {
    throw new Error(`Table ${tableName} has no visible columns`);
  }
  return columns;
}

function primaryKeysFromPragmaRows(rows: Array<Record<string, unknown>>, tableName: string): string[] {
  const keys = rows
    .map((row) => ({
      name: parseStringLike(row.name, `PRAGMA table_info(${tableName}).name`),
      pk: parseInteger(row.pk, `PRAGMA table_info(${tableName}).pk`),
    }))
    .filter((column) => column.pk > 0)
    .sort((a, b) => a.pk - b.pk)
    .map((column) => column.name);

  if (keys.length === 0) {
    throw new CodedError("NO_PRIMARY_KEY", `Table ${tableName} must define PRIMARY KEY for tracked operations`);
  }
  return keys;
}

function buildStateSelectSql(tableName: string, columns: string[], pkColumns: string[]): {
  sql: string;
  quoteAliases: string[];
  hexAliases: string[];
  typeAliases: string[];
} {
  const quoteAliases = columns.map((_, index) => `__toss_state_quote_${index}`);
  const hexAliases = columns.map((_, index) => `__toss_state_hex_${index}`);
  const typeAliases = columns.map((_, index) => `__toss_state_type_${index}`);

  const selectParts: string[] = [];
  for (let i = 0; i < columns.length; i += 1) {
    const column = columns[i]!;
    const quoted = quoteIdentifier(column, { unsafe: true });
    selectParts.push(`quote(${quoted}) AS ${quoteIdentifier(quoteAliases[i]!, { unsafe: true })}`);
    selectParts.push(`hex(CAST(${quoted} AS BLOB)) AS ${quoteIdentifier(hexAliases[i]!, { unsafe: true })}`);
    selectParts.push(`typeof(${quoted}) AS ${quoteIdentifier(typeAliases[i]!, { unsafe: true })}`);
  }

  const orderBy = pkColumns.map((column) => `${quoteIdentifier(column, { unsafe: true })} ASC`).join(", ");
  return {
    sql: `SELECT ${selectParts.join(", ")} FROM ${quoteIdentifier(tableName, { unsafe: true })} ORDER BY ${orderBy}`,
    quoteAliases,
    hexAliases,
    typeAliases,
  };
}

function encodeStateRow(
  row: Record<string, unknown>,
  columns: string[],
  quoteAliases: string[],
  hexAliases: string[],
  typeAliases: string[],
): Record<string, string> {
  const encoded: Record<string, string> = {};
  for (let i = 0; i < columns.length; i += 1) {
    const column = columns[i]!;
    const storageClass = parseStorageClass(row[typeAliases[i]!], `${column}.storageClass`);
    if (storageClass === "null") {
      encoded[column] = "NULL";
      continue;
    }
    if (storageClass === "text") {
      const hex = parseStringLike(row[hexAliases[i]!], `${column}.textHex`);
      encoded[column] = `CAST(X'${hex}' AS TEXT)`;
      continue;
    }
    if (storageClass === "blob") {
      const hex = parseStringLike(row[hexAliases[i]!], `${column}.blobHex`);
      encoded[column] = `X'${hex}'`;
      continue;
    }
    encoded[column] = parseStringLike(row[quoteAliases[i]!], `${column}.quoted`);
  }
  return encoded;
}

export function stateHashForDb(db: Database): string {
  const tables = listUserTables(db);
  const state: Record<string, Array<Record<string, string>>> = {};

  for (const tableName of tables) {
    const pkRows = db.$client
      .query<Record<string, unknown>, []>(`PRAGMA table_info(${pragmaLiteral(tableName)})`)
      .all()
      .map((row) => parseRowRecord(row, `PRAGMA table_info(${tableName})`));
    const pkColumns = primaryKeysFromPragmaRows(pkRows, tableName);
    const columnRows = db.$client
      .query<Record<string, unknown>, []>(`PRAGMA table_xinfo(${pragmaLiteral(tableName)})`)
      .all()
      .map((row) => parseRowRecord(row, `PRAGMA table_xinfo(${tableName})`));
    const columns = columnsFromPragmaRows(columnRows, tableName);
    const { sql, quoteAliases, hexAliases, typeAliases } = buildStateSelectSql(tableName, columns, pkColumns);
    const rows = db.$client
      .query<Record<string, unknown>, []>(sql)
      .all()
      .map((row) => parseRowRecord(row, `state row ${tableName}`))
      .map((row) => encodeStateRow(row, columns, quoteAliases, hexAliases, typeAliases));
    state[tableName] = rows;
  }

  return sha256Hex(state);
}

export async function stateHashForRemote(executor: Client | Transaction): Promise<string> {
  const tableResult = await executor.execute({
    sql: "SELECT name FROM sqlite_master WHERE type='table' AND name NOT GLOB '_toss_*' AND name NOT GLOB '__drizzle_*' AND name NOT GLOB 'sqlite_*' ORDER BY name",
  });
  const tables = tableResult.rows.map((row) => parseStringLike(parseRowRecord(row, "sqlite_master").name, "sqlite_master.name"));
  const state: Record<string, Array<Record<string, string>>> = {};

  for (const tableName of tables) {
    const pkResult = await executor.execute(`PRAGMA table_info(${pragmaLiteral(tableName)})`);
    const pkRows = pkResult.rows.map((row) => parseRowRecord(row, `PRAGMA table_info(${tableName})`));
    const pkColumns = primaryKeysFromPragmaRows(pkRows, tableName);
    const columnResult = await executor.execute(`PRAGMA table_xinfo(${pragmaLiteral(tableName)})`);
    const columnRows = columnResult.rows.map((row) => parseRowRecord(row, `PRAGMA table_xinfo(${tableName})`));
    const columns = columnsFromPragmaRows(columnRows, tableName);
    const { sql, quoteAliases, hexAliases, typeAliases } = buildStateSelectSql(tableName, columns, pkColumns);
    const result = await executor.execute(sql);
    const rows = result.rows
      .map((row) => parseRowRecord(row, `state row ${tableName}`))
      .map((row) => encodeStateRow(row, columns, quoteAliases, hexAliases, typeAliases));
    state[tableName] = rows;
  }

  return sha256Hex(state);
}
