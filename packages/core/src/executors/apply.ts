import type { Database } from "bun:sqlite";
import { TossError } from "../errors";
import { COLUMN_TYPE_PATTERN, quoteIdentifier } from "../sql";
import type {
  AddColumnOperation,
  AlterColumnTypeOperation,
  ColumnDefinition,
  DeleteOperation,
  DropColumnOperation,
  DropTableOperation,
  CreateTableOperation,
  InsertOperation,
  Operation,
  UpdateOperation,
} from "../types";

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

function normalizeColumnType(value: string): string {
  const normalized = value.trim().toUpperCase();
  if (!COLUMN_TYPE_PATTERN.test(normalized)) {
    throw new TossError("INVALID_OPERATION", `Invalid column type: ${value}`);
  }
  return normalized;
}

function buildColumnSql(column: ColumnDefinition, forAddColumn = false): string {
  const tokens = [quoteIdentifier(column.name), normalizeColumnType(column.type)];

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
  db.run(`CREATE TABLE ${quoteIdentifier(operation.table)} (${columns})`);
}

function executeAddColumn(db: Database, operation: AddColumnOperation): void {
  const column = buildColumnSql(operation.column, true);
  db.run(`ALTER TABLE ${quoteIdentifier(operation.table)} ADD COLUMN ${column}`);
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

function buildWhereClause(
  where: Record<string, string | number | boolean | null>,
  label: string,
): { clause: string; bindings: Array<string | number | boolean | null> } {
  const keys = Object.keys(where);
  if (keys.length === 0) {
    throw new TossError("INVALID_OPERATION", `${label} must not be empty`);
  }

  const clauses: string[] = [];
  const bindings: Array<string | number | boolean | null> = [];
  for (const key of keys) {
    const value = where[key];
    if (value === undefined) {
      throw new TossError("INVALID_OPERATION", `${label} value is missing for key: ${key}`);
    }
    const column = quoteIdentifier(key);
    if (value === null) {
      clauses.push(`${column} IS NULL`);
      continue;
    }
    clauses.push(`${column} = ?`);
    bindings.push(value);
  }
  return { clause: clauses.join(" AND "), bindings };
}

function executeUpdate(db: Database, operation: UpdateOperation): void {
  const valueKeys = Object.keys(operation.values);
  if (valueKeys.length === 0) {
    throw new TossError("INVALID_OPERATION", "update values must not be empty");
  }

  const setParts: string[] = [];
  const setBindings: Array<string | number | boolean | null> = [];
  for (const key of valueKeys) {
    const value = operation.values[key];
    if (value === undefined) {
      throw new TossError("INVALID_OPERATION", `update value is missing for key: ${key}`);
    }
    setParts.push(`${quoteIdentifier(key)} = ?`);
    setBindings.push(value);
  }

  const where = buildWhereClause(operation.where, "update.where");
  db.query(`UPDATE ${quoteIdentifier(operation.table)} SET ${setParts.join(", ")} WHERE ${where.clause}`).run(
    ...setBindings,
    ...where.bindings,
  );
}

function executeDelete(db: Database, operation: DeleteOperation): void {
  const where = buildWhereClause(operation.where, "delete.where");
  db.query(`DELETE FROM ${quoteIdentifier(operation.table)} WHERE ${where.clause}`).run(...where.bindings);
}

function executeDropTable(db: Database, operation: DropTableOperation): void {
  db.run(`DROP TABLE ${quoteIdentifier(operation.table)}`);
}

function executeDropColumn(db: Database, operation: DropColumnOperation): void {
  db.run(`ALTER TABLE ${quoteIdentifier(operation.table)} DROP COLUMN ${quoteIdentifier(operation.column)}`);
}

interface TableInfoRow {
  cid: number;
  name: string;
  type: string;
  notnull: number;
  dflt_value: string | null;
  pk: number;
}

function executeAlterColumnType(db: Database, operation: AlterColumnTypeOperation): void {
  const newType = normalizeColumnType(operation.newType);
  const tableName = quoteIdentifier(operation.table);
  const tableInfo = db.query(`PRAGMA table_info(${tableName})`).all() as TableInfoRow[];
  if (tableInfo.length === 0) {
    throw new TossError("INVALID_OPERATION", `Table does not exist: ${operation.table}`);
  }

  const target = tableInfo.find((column) => column.name === operation.column);
  if (!target) {
    throw new TossError("INVALID_OPERATION", `Column does not exist: ${operation.table}.${operation.column}`);
  }

  const secondaryObjects = db
    .query(
      `
      SELECT type, name, sql
      FROM sqlite_master
      WHERE tbl_name = ? AND type IN ('index', 'trigger') AND sql IS NOT NULL
      `,
    )
    .all(operation.table) as Array<{ type: "index" | "trigger"; name: string; sql: string }>;

  const tempTable = `__toss_tmp_${operation.table}_${crypto.randomUUID().replaceAll("-", "")}`;
  const quotedTempTable = quoteIdentifier(tempTable);
  const columnDefinitions = tableInfo.map((column) => {
    const typeToken = column.name === operation.column ? newType : normalizeColumnType(column.type || "TEXT");
    const tokens = [quoteIdentifier(column.name), typeToken];
    if (column.notnull === 1) {
      tokens.push("NOT NULL");
    }
    if (column.dflt_value !== null) {
      tokens.push("DEFAULT", column.dflt_value);
    }
    if (column.pk > 0) {
      tokens.push("PRIMARY KEY");
    }
    return tokens.join(" ");
  });

  const columnList = tableInfo.map((column) => quoteIdentifier(column.name)).join(", ");
  const selectList = tableInfo
    .map((column) => {
      const quoted = quoteIdentifier(column.name);
      if (column.name === operation.column) {
        return `CAST(${quoted} AS ${newType}) AS ${quoted}`;
      }
      return quoted;
    })
    .join(", ");

  db.run(`CREATE TABLE ${quotedTempTable} (${columnDefinitions.join(", ")})`);
  db.run(`INSERT INTO ${quotedTempTable} (${columnList}) SELECT ${selectList} FROM ${tableName}`);
  db.run(`DROP TABLE ${tableName}`);
  db.run(`ALTER TABLE ${quotedTempTable} RENAME TO ${tableName}`);

  for (const object of secondaryObjects) {
    db.run(object.sql);
  }
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
    case "update":
      executeUpdate(db, operation);
      return;
    case "delete":
      executeDelete(db, operation);
      return;
    case "drop_table":
      executeDropTable(db, operation);
      return;
    case "drop_column":
      executeDropColumn(db, operation);
      return;
    case "alter_column_type":
      executeAlterColumnType(db, operation);
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
