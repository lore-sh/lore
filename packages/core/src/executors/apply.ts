import type { Database } from "bun:sqlite";
import { TossError } from "../errors";
import type { AddColumnOperation, ColumnDefinition, CreateTableOperation, InsertOperation, Operation } from "../types";

const IDENTIFIER_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/;

function quoteIdentifier(value: string): string {
  if (!IDENTIFIER_PATTERN.test(value)) {
    throw new TossError("INVALID_IDENTIFIER", `Invalid identifier: ${value}`);
  }
  return `"${value.replaceAll('"', '""')}"`;
}

function serializeLiteral(value: string | number | boolean | null): string {
  if (value === null) {
    return "NULL";
  }
  if (typeof value === "string") {
    return `'${value.replaceAll("'", "''")}'`;
  }
  if (typeof value === "boolean") {
    return value ? "1" : "0";
  }
  return String(value);
}

function buildColumnSql(column: ColumnDefinition, forAddColumn = false): string {
  const tokens = [quoteIdentifier(column.name), column.type.toUpperCase()];

  if (column.primaryKey) {
    if (forAddColumn) {
      throw new TossError("UNSUPPORTED_OPERATION", "add_column does not support primaryKey");
    }
    tokens.push("PRIMARY KEY");
  }
  if (column.unique) {
    if (forAddColumn) {
      throw new TossError("UNSUPPORTED_OPERATION", "add_column does not support unique");
    }
    tokens.push("UNIQUE");
  }
  if (column.notNull) {
    tokens.push("NOT NULL");
  }
  if (Object.hasOwn(column, "default")) {
    tokens.push("DEFAULT", serializeLiteral(column.default ?? null));
  }

  return tokens.join(" ");
}

function executeCreateTable(db: Database, operation: CreateTableOperation): void {
  const columns = operation.columns.map((column) => buildColumnSql(column)).join(", ");
  db.exec(`CREATE TABLE ${quoteIdentifier(operation.table)} (${columns})`);
}

function executeAddColumn(db: Database, operation: AddColumnOperation): void {
  const column = buildColumnSql(operation.column, true);
  db.exec(`ALTER TABLE ${quoteIdentifier(operation.table)} ADD COLUMN ${column}`);
}

function executeInsert(db: Database, operation: InsertOperation): void {
  const keys = Object.keys(operation.values);
  if (keys.length === 0) {
    throw new TossError("INVALID_OPERATION", "insert values must not be empty");
  }

  const columns = keys.map((key) => quoteIdentifier(key)).join(", ");
  const placeholders = keys.map(() => "?").join(", ");
  const values = keys.map((key) => {
    const value = operation.values[key];
    if (value === undefined) {
      throw new TossError("INVALID_OPERATION", `insert value is missing for key: ${key}`);
    }
    return value;
  });

  db.query(`INSERT INTO ${quoteIdentifier(operation.table)} (${columns}) VALUES (${placeholders})`).run(...values);
}

export function executeOperation(db: Database, operation: Operation): void {
  switch (operation.type) {
    case "create_table":
      executeCreateTable(db, operation);
      return;
    case "add_column":
      executeAddColumn(db, operation);
      return;
    case "insert":
      executeInsert(db, operation);
      return;
    default:
      throw new TossError("UNSUPPORTED_OPERATION", `Unsupported operation type: ${(operation as Operation).type}`);
  }
}

export function executeOperations(db: Database, operations: Operation[]): void {
  for (const operation of operations) {
    executeOperation(db, operation);
  }
}
