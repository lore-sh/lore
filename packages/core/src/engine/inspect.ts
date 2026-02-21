import type { Database } from "bun:sqlite";
import { sha256Hex } from "./checksum";
import { getRow, getRows, listUserTables } from "./db";
import { TossError } from "../errors";
import { normalizeSql, quoteIdentifier } from "./sql";
import { extractCheckConstraints, parseColumnDefinitionsFromCreateTable } from "./ddl";
import type { JsonObject, JsonPrimitive } from "../types";

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
  notnull: number;
  dfltValue: string | null;
  pk: number;
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

export function schemaHashFromDescriptor(descriptor: SchemaDescriptor): string {
  return sha256Hex(descriptor.tables);
}

function pragmaLiteral(value: string): string {
  return `'${value.replaceAll("'", "''")}'`;
}

function normalizeSqlNullable(sql: string | null): string | null {
  if (sql === null) {
    return null;
  }
  return normalizeSql(sql, { tight: true });
}

function serializeStateValue(value: JsonPrimitive): JsonPrimitive {
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      return null;
    }
    return value;
  }
  return value;
}

function normalizeStateRow(row: Record<string, unknown>): JsonObject {
  const output: JsonObject = {};
  for (const [key, value] of Object.entries(row)) {
    if (value === null || typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
      output[key] = serializeStateValue(value);
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
  return getRows<TableInfoRow>(db, `PRAGMA table_info(${quoteIdentifier(table, { unsafe: true })})`);
}

export function primaryKeyColumns(db: Database, table: string): string[] {
  return tableInfo(db, table)
    .filter((column) => column.pk > 0)
    .sort((a, b) => a.pk - b.pk)
    .map((column) => column.name);
}

export function assertTableHasPrimaryKey(db: Database, table: string): string[] {
  const pkCols = primaryKeyColumns(db, table);
  if (pkCols.length === 0) {
    throw new TossError("TABLE_WITHOUT_PRIMARY_KEY", `Table ${table} must define PRIMARY KEY for tracked operations`);
  }
  return pkCols;
}

export function tableDDL(db: Database, table: string): string | null {
  const row = getRow<{ sql: string | null }>(
    db,
    "SELECT sql FROM sqlite_master WHERE type='table' AND name = ? COLLATE NOCASE LIMIT 1",
    table,
  );
  return row?.sql ?? null;
}

export function describeSchema(db: Database): SchemaDescriptor {
  const tableNames = listUserTables(db);
  const tableList = getRows<TableListRow>(db, "PRAGMA table_list");
  const tableOpts = new Map(
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
      options: tableOpts.get(table) ?? { withoutRowid: false, strict: false },
      columns: getRows<TableXInfoRow>(db, `PRAGMA table_xinfo(${pragmaLiteral(table)})`)
        .map((column) => ({
          definitionSql: columnDefs.get(column.name.toLowerCase()) ?? null,
          cid: column.cid,
          name: column.name,
          type: column.type.trim().toUpperCase(),
          notnull: column.notnull,
          dfltValue: column.dflt_value,
          pk: column.pk,
          hidden: column.hidden,
        }))
        .sort((a, b) => a.cid - b.cid),
      foreignKeys: (() => {
        const rows = getRows<ForeignKeyRow>(db, `PRAGMA foreign_key_list(${pragmaLiteral(table)})`);
        const byId = new Map<
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
          const existing = byId.get(row.id);
          if (!existing) {
            byId.set(row.id, {
              id: row.id,
              refTable: row.table,
              onUpdate: row.on_update,
              onDelete: row.on_delete,
              match: row.match,
              mappings: [{ seq: row.seq, from: row.from, to: row.to }],
            });
            continue;
          }
          existing.mappings.push({ seq: row.seq, from: row.from, to: row.to });
        }
        return Array.from(byId.values())
          .map((fk) => ({
            ...fk,
            mappings: fk.mappings.sort((a, b) => a.seq - b.seq),
          }))
          .sort((a, b) => a.id - b.id);
      })(),
      indexes: getRows<IndexListRow>(db, `PRAGMA index_list(${pragmaLiteral(table)})`)
        .map((index) => {
          const indexSqlRow = getRow<{ sql: string | null }>(
            db,
            "SELECT sql FROM sqlite_master WHERE type='index' AND name=? LIMIT 1",
            index.name,
          );
          const indexColumns = getRows<IndexXInfoRow>(db, `PRAGMA index_xinfo(${pragmaLiteral(index.name)})`)
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
      triggers: (
        getRows<{ name: string; sql: string | null }>(
          db,
          "SELECT name, sql FROM sqlite_master WHERE type='trigger' AND tbl_name=? ORDER BY name ASC",
          table,
        )
      ).map((trigger) => ({
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
    const rows = getRows<Record<string, unknown>>(
      db,
      `SELECT * FROM ${quoteIdentifier(table, { unsafe: true })} ORDER BY ${orderBy}`,
    );
    state[table] = rows.map((row) => normalizeStateRow(row));
  }
  return sha256Hex(state);
}
