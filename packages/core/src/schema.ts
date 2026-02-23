import type { Database } from "./engine/db";
import {
  describeSchema,
  schemaHashFromDescriptor,
  type SchemaForeignKeyDescriptor,
  type SchemaIndexDescriptor,
  type SchemaTableDescriptor,
  type SchemaTriggerDescriptor,
} from "./engine/inspect";
import { countTableRows, isVisibleColumn, resolveTableName, uniqueColumnNames } from "./table";

export interface SchemaOptions {
  table?: string | undefined;
}

export interface SchemaColumn {
  definitionSql: string | null;
  cid: number;
  name: string;
  type: string;
  notNull: boolean;
  defaultValue: string | null;
  primaryKey: boolean;
  unique: boolean;
  hidden: boolean;
}

export interface SchemaTable {
  tableSql: string | null;
  name: string;
  options: {
    withoutRowid: boolean;
    strict: boolean;
  };
  columns: SchemaColumn[];
  foreignKeys: SchemaForeignKeyDescriptor[];
  indexes: SchemaIndexDescriptor[];
  checks: string[];
  triggers: SchemaTriggerDescriptor[];
  rowCount: number;
}

export interface Schema {
  dbPath: string;
  generatedAt: string;
  schemaHash: string;
  tables: SchemaTable[];
}

function mapSchemaTable(db: Database, table: SchemaTableDescriptor): SchemaTable {
  const unique = uniqueColumnNames(table);
  return {
    tableSql: table.tableSql,
    name: table.table,
    options: table.options,
    columns: table.columns.map((column) => ({
      definitionSql: column.definitionSql,
      cid: column.cid,
      name: column.name,
      type: column.type,
      notNull: column.notnull === 1,
      defaultValue: column.dfltValue,
      primaryKey: column.pk > 0,
      unique: unique.has(column.name),
      hidden: !isVisibleColumn(column.hidden),
    })),
    foreignKeys: table.foreignKeys,
    indexes: table.indexes,
    checks: table.checks,
    triggers: table.triggers,
    rowCount: countTableRows(db, table.table),
  };
}

export function schema(db: Database, options: SchemaOptions = {}): Schema {
  const descriptor = describeSchema(db);
  const selectedTable = options.table ? resolveTableName(db, options.table) : null;
  const tables = descriptor.tables
    .filter((table) => selectedTable === null || table.table === selectedTable)
    .map((table) => mapSchemaTable(db, table));

  return {
    dbPath: db.$client.filename,
    generatedAt: new Date().toISOString(),
    schemaHash: schemaHashFromDescriptor(descriptor),
    tables,
  };
}
