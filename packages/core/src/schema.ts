import { withInitializedDatabase, getRow } from "./db";
import { TossError } from "./errors";
import { describeSchema, schemaHashFromDescriptor, type SchemaTableDescriptor } from "./rows";
import { quoteName } from "./sql";
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
  if (left === right) {
    return true;
  }
  return asciiCaseFold(left) === asciiCaseFold(right);
}

function asciiCaseFold(value: string): string {
  let out = "";
  for (let i = 0; i < value.length; i += 1) {
    const code = value.charCodeAt(i);
    if (code >= 65 && code <= 90) {
      out += String.fromCharCode(code + 32);
      continue;
    }
    out += value[i];
  }
  return out;
}

export function getSchema(options: GetSchemaOptions = {}): SchemaView {
  return withInitializedDatabase(options, ({ db, dbPath }) => {
    const descriptor = describeSchema(db);
    const selected = selectTables(descriptor.tables, options.table);
    const tables = selected.map((table) => {
      const row = getRow<{ c: number }>(db, `SELECT COUNT(*) AS c FROM ${quoteName(table.table)}`);
      return {
        name: table.table,
        tableSql: table.tableSql,
        rowCount: row?.c ?? 0,
        options: table.options,
        columns: table.columns,
        foreignKeys: table.foreignKeys,
        indexes: table.indexes,
        checks: table.checks,
        triggers: table.triggers,
      };
    });

    return {
      dbPath,
      generatedAt: new Date().toISOString(),
      schemaHash: schemaHashFromDescriptor(descriptor),
      tables,
    };
  });
}
