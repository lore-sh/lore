import type { Database } from "bun:sqlite";
import { TossError } from "../errors";
import { COLUMN_TYPE_PATTERN, quoteIdentifier } from "../sql";
import type {
  AddColumnOperation,
  AlterColumnTypeOperation,
  ColumnDefinition,
  RestoreTableOperation,
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

function skipWhitespace(sql: string, start: number): number {
  let i = start;
  while (i < sql.length && /\s/.test(sql[i]!)) {
    i += 1;
  }
  return i;
}

function readKeyword(sql: string, start: number): { value: string; end: number } | null {
  let i = start;
  let value = "";
  while (i < sql.length) {
    const ch = sql[i]!;
    const isAlpha = (ch >= "A" && ch <= "Z") || (ch >= "a" && ch <= "z") || ch === "_";
    if (!isAlpha) {
      break;
    }
    value += ch;
    i += 1;
  }
  if (value.length === 0) {
    return null;
  }
  return { value: value.toUpperCase(), end: i };
}

function readIdentifier(sql: string, start: number): { end: number } {
  const ch = sql[start];
  if (!ch) {
    throw new TossError("INVALID_OPERATION", "Malformed CREATE TABLE statement");
  }

  if (ch === '"') {
    let i = start + 1;
    while (i < sql.length) {
      if (sql[i] === '"') {
        if (sql[i + 1] === '"') {
          i += 2;
          continue;
        }
        return { end: i + 1 };
      }
      i += 1;
    }
    throw new TossError("INVALID_OPERATION", "Malformed quoted identifier in CREATE TABLE");
  }

  if (ch === "`") {
    let i = start + 1;
    while (i < sql.length) {
      if (sql[i] === "`") {
        if (sql[i + 1] === "`") {
          i += 2;
          continue;
        }
        return { end: i + 1 };
      }
      i += 1;
    }
    throw new TossError("INVALID_OPERATION", "Malformed backtick identifier in CREATE TABLE");
  }

  if (ch === "[") {
    const end = sql.indexOf("]", start + 1);
    if (end < 0) {
      throw new TossError("INVALID_OPERATION", "Malformed bracket identifier in CREATE TABLE");
    }
    return { end: end + 1 };
  }

  let i = start;
  while (i < sql.length) {
    const c = sql[i]!;
    const isSpace = /\s/.test(c);
    if (c === "." || c === "(" || isSpace) {
      break;
    }
    i += 1;
  }
  if (i === start) {
    throw new TossError("INVALID_OPERATION", "Malformed bare identifier in CREATE TABLE");
  }
  return { end: i };
}

function expectKeyword(sql: string, start: number, expected: string): number {
  const kw = readKeyword(sql, start);
  if (!kw || kw.value !== expected) {
    throw new TossError("INVALID_OPERATION", `Expected ${expected} in CREATE TABLE`);
  }
  return kw.end;
}

function rewriteCreateTableName(ddlSql: string, newTable: string): string {
  let i = skipWhitespace(ddlSql, 0);
  i = expectKeyword(ddlSql, i, "CREATE");
  i = skipWhitespace(ddlSql, i);

  const maybeTemp = readKeyword(ddlSql, i);
  if (maybeTemp && (maybeTemp.value === "TEMP" || maybeTemp.value === "TEMPORARY")) {
    i = maybeTemp.end;
    i = skipWhitespace(ddlSql, i);
  }

  i = expectKeyword(ddlSql, i, "TABLE");
  i = skipWhitespace(ddlSql, i);

  const maybeIf = readKeyword(ddlSql, i);
  if (maybeIf?.value === "IF") {
    i = maybeIf.end;
    i = skipWhitespace(ddlSql, i);
    i = expectKeyword(ddlSql, i, "NOT");
    i = skipWhitespace(ddlSql, i);
    i = expectKeyword(ddlSql, i, "EXISTS");
    i = skipWhitespace(ddlSql, i);
  }

  const nameStart = i;
  const firstIdent = readIdentifier(ddlSql, i);
  i = firstIdent.end;
  i = skipWhitespace(ddlSql, i);
  if (ddlSql[i] === ".") {
    i += 1;
    i = skipWhitespace(ddlSql, i);
    const secondIdent = readIdentifier(ddlSql, i);
    i = secondIdent.end;
  }
  const nameEnd = i;

  return `${ddlSql.slice(0, nameStart)}${quoteIdentifier(newTable)}${ddlSql.slice(nameEnd)}`;
}

function executeRestoreTable(db: Database, operation: RestoreTableOperation): void {
  const tmpTable = `__toss_restore_${operation.table}_${crypto.randomUUID().replaceAll("-", "")}`;
  const quotedTmp = quoteIdentifier(tmpTable);
  const quotedTable = quoteIdentifier(operation.table);
  db.run("SAVEPOINT toss_restore_table");
  try {
    db.run(rewriteCreateTableName(operation.ddlSql, tmpTable));
    if (operation.rows && operation.rows.length > 0) {
      const first = operation.rows[0];
      if (first) {
        const columns = Object.keys(first);
        if (columns.length > 0) {
          const columnSql = columns.map((column) => quoteIdentifier(column)).join(", ");
          const placeholderSql = columns.map(() => "?").join(", ");
          const stmt = db.query(`INSERT INTO ${quotedTmp} (${columnSql}) VALUES (${placeholderSql})`);
          for (const row of operation.rows) {
            const values = columns.map((column) => {
              const value = row[column];
              if (
                value === null ||
                typeof value === "string" ||
                typeof value === "number" ||
                typeof value === "boolean"
              ) {
                return value;
              }
              return JSON.stringify(value);
            });
            stmt.run(...values);
          }
        }
      }
    }

    db.run(`DROP TABLE IF EXISTS ${quotedTable}`);
    db.run(`ALTER TABLE ${quotedTmp} RENAME TO ${quotedTable}`);
    db.run("RELEASE toss_restore_table");
  } catch (error) {
    try {
      db.run(`DROP TABLE IF EXISTS ${quotedTmp}`);
    } catch {
      // no-op
    }
    db.run("ROLLBACK TO toss_restore_table");
    db.run("RELEASE toss_restore_table");
    throw error;
  }
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
    case "restore_table":
      executeRestoreTable(db, operation);
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
