import { listUserTables, type Database } from "./db";
import { CodedError } from "./error";
import { sha256Hex } from "./hash";
import type { JsonObject, JsonPrimitive } from "./schema";
import {
  extractCheckConstraints,
  normalizeSqlNullable,
  parseColumnDefinitionsFromCreateTable,
  pragmaLiteral,
  quoteIdentifier,
} from "./sql";

export interface TableInfoRow {
  cid: number;
  name: string;
  type: string;
  notnull: number;
  dflt_value: string | null;
  pk: number;
}

interface TableListRow {
  schema: string;
  name: string;
  type: string;
  ncol: number;
  wr: number;
  strict: number;
}

interface TableXInfoRow {
  cid: number;
  name: string;
  type: string;
  notnull: number;
  dflt_value: string | null;
  pk: number;
  hidden: number;
}

interface ForeignKeyRow {
  id: number;
  seq: number;
  table: string;
  from: string;
  to: string | null;
  on_update: string;
  on_delete: string;
  match: string;
}

interface IndexListRow {
  seq: number;
  name: string;
  unique: number;
  origin: "c" | "u" | "pk";
  partial: number;
}

interface IndexXInfoRow {
  seqno: number;
  cid: number;
  name: string | null;
  desc: number;
  coll: string | null;
  key: number;
}

export interface SchemaColumnDescriptor {
  definitionSql: string | null;
  cid: number;
  name: string;
  type: string;
  notNull: boolean;
  defaultValue: string | null;
  primaryKey: boolean;
  hidden: number;
}

export interface SchemaForeignKeyDescriptor {
  id: number;
  refTable: string;
  onUpdate: string;
  onDelete: string;
  match: string;
  mappings: Array<{ seq: number; from: string; to: string | null }>;
}

export interface SchemaIndexDescriptor {
  name: string;
  unique: boolean;
  origin: "c" | "u" | "pk";
  partial: boolean;
  sql: string | null;
  columns: Array<{
    seqno: number;
    cid: number;
    name: string | null;
    desc: number;
    coll: string | null;
    key: number;
  }>;
}

export interface SchemaTriggerDescriptor {
  name: string;
  sql: string | null;
}

export interface SchemaTableDescriptor {
  tableSql: string | null;
  table: string;
  options: {
    withoutRowid: boolean;
    strict: boolean;
  };
  columns: SchemaColumnDescriptor[];
  foreignKeys: SchemaForeignKeyDescriptor[];
  indexes: SchemaIndexDescriptor[];
  checks: string[];
  triggers: SchemaTriggerDescriptor[];
}

export interface SchemaDescriptor {
  tables: SchemaTableDescriptor[];
}

const MAX_PAGE_SIZE = 500;
const DEFAULT_PAGE_SIZE = 50;

export function schemaHashFromDescriptor(descriptor: SchemaDescriptor): string {
  return sha256Hex(descriptor.tables);
}

export function serializeScalar(value: JsonPrimitive): JsonPrimitive {
  if (typeof value === "number" && !Number.isFinite(value)) {
    return null;
  }
  return value;
}

export function normalizeRow(row: Record<string, unknown>): JsonObject {
  const output: JsonObject = {};
  for (const [key, value] of Object.entries(row)) {
    if (value === null || typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
      output[key] = serializeScalar(value);
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
  return db.$client.query<TableInfoRow, []>(`PRAGMA table_info(${quoteIdentifier(table, { unsafe: true })})`).all();
}

export function primaryKeyColumns(db: Database, table: string): string[] {
  return tableInfo(db, table)
    .filter((column) => column.pk > 0)
    .sort((a, b) => a.pk - b.pk)
    .map((column) => column.name);
}

export function assertTableHasPrimaryKey(db: Database, table: string): string[] {
  const pkColumns = primaryKeyColumns(db, table);
  if (pkColumns.length === 0) {
    throw new CodedError("NO_PRIMARY_KEY", `Table ${table} must define PRIMARY KEY for tracked operations`);
  }
  return pkColumns;
}

export function tableDDL(db: Database, table: string): string | null {
  const row = db.$client
    .query<{ sql: string | null }, [string]>(
      "SELECT sql FROM sqlite_master WHERE type='table' AND name = ? COLLATE NOCASE LIMIT 1",
    )
    .get(table);
  return row?.sql ?? null;
}

export function describeSchema(db: Database): SchemaDescriptor {
  const tableNames = listUserTables(db);
  const tableList = db.$client.query<TableListRow, []>("PRAGMA table_list").all();
  const tableOptions = new Map(
    tableList
      .filter((row) => row.schema === "main" && row.type === "table")
      .map((row) => [row.name, { withoutRowid: row.wr === 1, strict: row.strict === 1 }] as const),
  );

  const tables = tableNames.map((table) => {
    const tableDdl = tableDDL(db, table);
    const columnDefs = parseColumnDefinitionsFromCreateTable(tableDdl);
    const checks = extractCheckConstraints(tableDdl);
    return {
      tableSql: normalizeSqlNullable(tableDdl),
      table,
      options: tableOptions.get(table) ?? { withoutRowid: false, strict: false },
      columns: db.$client
        .query<TableXInfoRow, []>(`PRAGMA table_xinfo(${pragmaLiteral(table)})`)
        .all()
        .map((column) => ({
          definitionSql: columnDefs.get(column.name.toLowerCase()) ?? null,
          cid: column.cid,
          name: column.name,
          type: column.type.trim().toUpperCase(),
          notNull: column.notnull === 1,
          defaultValue: column.dflt_value,
          primaryKey: column.pk > 0,
          hidden: column.hidden,
        }))
        .sort((a, b) => a.cid - b.cid),
      foreignKeys: (() => {
        const rows = db.$client.query<ForeignKeyRow, []>(`PRAGMA foreign_key_list(${pragmaLiteral(table)})`).all();
        const grouped = new Map<
          number,
          {
            id: number;
            refTable: string;
            onUpdate: string;
            onDelete: string;
            match: string;
            mappings: Array<{ seq: number; from: string; to: string | null }>;
          }
        >();
        for (const row of rows) {
          const entry = grouped.get(row.id);
          if (!entry) {
            grouped.set(row.id, {
              id: row.id,
              refTable: row.table,
              onUpdate: row.on_update,
              onDelete: row.on_delete,
              match: row.match,
              mappings: [{ seq: row.seq, from: row.from, to: row.to }],
            });
            continue;
          }
          entry.mappings.push({ seq: row.seq, from: row.from, to: row.to });
        }
        return Array.from(grouped.values())
          .map((fk) => ({ ...fk, mappings: fk.mappings.sort((a, b) => a.seq - b.seq) }))
          .sort((a, b) => a.id - b.id);
      })(),
      indexes: db.$client
        .query<IndexListRow, []>(`PRAGMA index_list(${pragmaLiteral(table)})`)
        .all()
        .map((index) => {
          const indexSqlRow = db.$client
            .query<{ sql: string | null }, [string]>("SELECT sql FROM sqlite_master WHERE type='index' AND name=? LIMIT 1")
            .get(index.name);
          const indexColumns = db.$client
            .query<IndexXInfoRow, []>(`PRAGMA index_xinfo(${pragmaLiteral(index.name)})`)
            .all()
            .map((entry) => ({
              seqno: entry.seqno,
              cid: entry.cid,
              name: entry.name,
              desc: entry.desc,
              coll: entry.coll,
              key: entry.key,
            }))
            .sort((a, b) => a.seqno - b.seqno);
          return {
            name: index.name,
            unique: index.unique === 1,
            origin: index.origin,
            partial: index.partial === 1,
            sql: normalizeSqlNullable(indexSqlRow?.sql ?? null),
            columns: indexColumns,
          };
        })
        .sort((a, b) => a.name.localeCompare(b.name)),
      checks,
      triggers: db.$client
        .query<{ name: string; sql: string | null }, [string]>(
          "SELECT name, sql FROM sqlite_master WHERE type='trigger' AND tbl_name=? ORDER BY name ASC",
        )
        .all(table)
        .map((trigger) => ({
          name: trigger.name,
          sql: normalizeSqlNullable(trigger.sql),
        })),
    };
  });

  return { tables };
}

export function schemaHash(db: Database): string {
  return schemaHashFromDescriptor(describeSchema(db));
}

export function stateHash(db: Database): string {
  const tables = listUserTables(db);
  const state: Record<string, JsonObject[]> = {};
  for (const table of tables) {
    const pkColumns = assertTableHasPrimaryKey(db, table);
    const orderBy = pkColumns.map((column) => `${quoteIdentifier(column, { unsafe: true })} ASC`).join(", ");
    const rows = db.$client
      .query<Record<string, unknown>, []>(`SELECT * FROM ${quoteIdentifier(table, { unsafe: true })} ORDER BY ${orderBy}`)
      .all();
    state[table] = rows.map((row) => normalizeRow(row));
  }
  return sha256Hex(state);
}

export function whereClause(values: Record<string, JsonPrimitive>): { clause: string; bindings: JsonPrimitive[] } {
  const keys = Object.keys(values);
  if (keys.length === 0) {
    throw new CodedError("INVALID_OPERATION", "where must not be empty");
  }

  const terms: string[] = [];
  const bindings: JsonPrimitive[] = [];
  for (const key of keys) {
    const value = values[key];
    if (value === undefined) {
      throw new CodedError("INVALID_OPERATION", `where value missing for key: ${key}`);
    }
    const quoted = quoteIdentifier(key);
    if (value === null) {
      terms.push(`${quoted} IS NULL`);
      continue;
    }
    terms.push(`${quoted} = ?`);
    bindings.push(serializeScalar(value));
  }

  return { clause: terms.join(" AND "), bindings };
}

export function getRowsByWhere(db: Database, table: string, where: Record<string, JsonPrimitive>): Array<Record<string, unknown>> {
  const predicate = whereClause(where);
  const query = `SELECT * FROM ${quoteIdentifier(table)} WHERE ${predicate.clause}`;
  return db.$client.query<Record<string, unknown>, JsonPrimitive[]>(query).all(...predicate.bindings);
}

export function getAllRows(db: Database, table: string): JsonObject[] {
  const rows = db.$client.query<Record<string, unknown>, []>(`SELECT * FROM ${quoteIdentifier(table)} ORDER BY rowid ASC`).all();
  return rows.map((row) => normalizeRow(row));
}

export function pkFromRow(db: Database, table: string, row: Record<string, unknown>): Record<string, JsonPrimitive> {
  const pkColumns = assertTableHasPrimaryKey(db, table);
  const pk: Record<string, JsonPrimitive> = {};
  for (const column of pkColumns) {
    const value = row[column];
    if (value === undefined) {
      throw new CodedError("INVALID_OPERATION", `PK column missing in row: ${table}.${column}`);
    }
    if (value === null || typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
      pk[column] = value;
      continue;
    }
    throw new CodedError("INVALID_OPERATION", `Unsupported PK value type in ${table}.${column}`);
  }
  return pk;
}

export function getRowByPk(db: Database, table: string, pk: Record<string, JsonPrimitive>): Record<string, unknown> | null {
  const predicate = whereClause(pk);
  return db.$client
    .query<Record<string, unknown>, JsonPrimitive[]>(`SELECT * FROM ${quoteIdentifier(table)} WHERE ${predicate.clause} LIMIT 1`)
    .get(...predicate.bindings);
}

export function countRows(db: Database, tableName: string): number {
  return (
    db.$client
      .query<{ c: number }, []>(`SELECT COUNT(*) AS c FROM ${quoteIdentifier(tableName, { unsafe: true })}`)
      .get()?.c ?? 0
  );
}

export function isVisibleColumn(hidden: number): boolean {
  return hidden === 0 || hidden === 2 || hidden === 3;
}

export function normalizePageSize(input: number | undefined): number {
  if (typeof input !== "number" || !Number.isFinite(input) || input < 1) {
    return DEFAULT_PAGE_SIZE;
  }
  return Math.min(MAX_PAGE_SIZE, Math.floor(input));
}

export function normalizePage(input: number | undefined): number {
  if (typeof input !== "number" || !Number.isFinite(input) || input < 1) {
    return 1;
  }
  return Math.floor(input);
}
