import type { Database } from "bun:sqlite";
import { getRow } from "./engine/db";
import { CodedError } from "./error";
import { describeSchema, schemaHashFromDescriptor, type SchemaTableDescriptor } from "./engine/inspect";
import { asciiCaseFold, quoteIdentifier } from "./engine/sql";

export interface GetSchemaOptions {
  table?: string | undefined;
}

export interface SchemaTableView extends Omit<SchemaTableDescriptor, "table"> {
  name: string;
  rowCount: number;
}

export interface SchemaView {
  dbPath: string;
  generatedAt: string;
  schemaHash: string;
  tables: SchemaTableView[];
}

function selectTables(tables: SchemaTableDescriptor[], tableName?: string | undefined): SchemaTableDescriptor[] {
  if (!tableName) {
    return tables;
  }
  const selected = tables.filter((table) => sqliteIdentifierEquals(table.table, tableName));
  if (selected.length === 0) {
    throw new CodedError("NOT_FOUND", `Table not found: ${tableName}`);
  }
  return selected;
}

function sqliteIdentifierEquals(left: string, right: string): boolean {
  return left === right || asciiCaseFold(left) === asciiCaseFold(right);
}

export function getSchema(db: Database, options: GetSchemaOptions = {}): SchemaView {
  const descriptor = describeSchema(db);
  const selected = selectTables(descriptor.tables, options.table);
  const tables = selected.map(({ table: name, ...rest }) => {
    const row = getRow<{ c: number }>(db, `SELECT COUNT(*) AS c FROM ${quoteIdentifier(name, { unsafe: true })}`);
    return { name, ...rest, rowCount: row?.c ?? 0 };
  });

  return {
    dbPath: db.filename,
    generatedAt: new Date().toISOString(),
    schemaHash: schemaHashFromDescriptor(descriptor),
    tables,
  };
}
