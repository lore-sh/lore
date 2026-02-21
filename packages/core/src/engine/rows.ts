import type { Database } from "bun:sqlite";
import { getRow, getRows } from "./db";
import { TossError } from "../errors";
import { assertTableHasPrimaryKey } from "./inspect";
import { quoteIdentifier } from "./sql";
import type { JsonObject, JsonPrimitive } from "../types";

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
      output[key] = serializeValue(value);
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
  const sql = `SELECT * FROM ${quoteIdentifier(table)} WHERE ${clause}`;
  return getRows<Record<string, unknown>>(db, sql, ...bindings);
}

export function fetchAllRows(db: Database, table: string): JsonObject[] {
  const rows = getRows<Record<string, unknown>>(db, `SELECT * FROM ${quoteIdentifier(table)} ORDER BY rowid ASC`);
  return rows.map((row) => normalizeRowObject(row));
}

export function pkFromRow(db: Database, table: string, row: Record<string, unknown>): Record<string, JsonPrimitive> {
  const pkCols = assertTableHasPrimaryKey(db, table);

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
  const { clause, bindings } = whereClauseFromRecord(pk);
  return getRow<Record<string, unknown>>(db, `SELECT * FROM ${quoteIdentifier(table)} WHERE ${clause} LIMIT 1`, ...bindings);
}
