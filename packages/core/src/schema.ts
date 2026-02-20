import { withInitializedDatabase, getRow } from "./db";
import { TossError } from "./errors";
import { describeSchema, schemaHashFromDescriptor, type SchemaTableDescriptor } from "./rows";
import { asciiCaseFold, quoteName } from "./sql";
import type { DatabaseOptions } from "./types";

export interface GetSchemaOptions extends DatabaseOptions {
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
    throw new TossError("NOT_FOUND", `Table not found: ${tableName}`);
  }
  return selected;
}

function sqliteIdentifierEquals(left: string, right: string): boolean {
  return left === right || asciiCaseFold(left) === asciiCaseFold(right);
}

export function getSchema(options: GetSchemaOptions = {}): SchemaView {
  return withInitializedDatabase(options, ({ db, dbPath }) => {
    const descriptor = describeSchema(db);
    const selected = selectTables(descriptor.tables, options.table);
    const tables = selected.map(({ table: name, ...rest }) => {
      const row = getRow<{ c: number }>(db, `SELECT COUNT(*) AS c FROM ${quoteName(name)}`);
      return { name, ...rest, rowCount: row?.c ?? 0 };
    });

    return {
      dbPath,
      generatedAt: new Date().toISOString(),
      schemaHash: schemaHashFromDescriptor(descriptor),
      tables,
    };
  });
}
