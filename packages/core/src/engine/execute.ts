import type { Database } from "bun:sqlite";
import { getRow, getRows, runInSavepoint, tableExists } from "./db";
import { CodedError } from "../error";
import { whereClauseFromRecord } from "./rows";
import { COLUMN_TYPE_PATTERN, quoteIdentifier } from "./sql";
import {
  rewriteAddCheckInCreateTable,
  rewriteColumnTypeInCreateTable,
  rewriteCreateTableName,
  rewriteDropCheckInCreateTable,
} from "./ddl";
import type { TableInfoRow } from "./inspect";
import type {
  AddCheckOperation,
  AddColumnOperation,
  AlterColumnTypeOperation,
  EncodedCell,
  ColumnDefinition,
  CreateTableOperation,
  DeleteOperation,
  DropColumnOperation,
  DropCheckOperation,
  DropTableOperation,
  InsertOperation,
  Operation,
  RestoreTableOperation,
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
    throw new CodedError("INVALID_OPERATION", `Invalid column type: ${value}`);
  }
  return normalized;
}

function buildColumnSql(column: ColumnDefinition, forAddColumn = false): string {
  const tokens = [quoteIdentifier(column.name), normalizeColumnType(column.type)];

  if (column.primaryKey) {
    if (forAddColumn) {
      throw new CodedError("UNSUPPORTED", "add_column does not support primaryKey");
    }
    tokens.push("PRIMARY KEY");
  }
  if (column.unique) {
    if (forAddColumn) {
      throw new CodedError("UNSUPPORTED", "add_column does not support unique");
    }
    tokens.push("UNIQUE");
  }
  if (column.notNull) {
    tokens.push("NOT NULL");
  }
  if (column.default) {
    const def = column.default;
    tokens.push("DEFAULT", def.kind === "literal" ? serializeLiteral(def.value) : def.expr);
  }

  return tokens.join(" ");
}

function executeCreateTable(db: Database, operation: CreateTableOperation): void {
  const columns = operation.columns.map((column) => buildColumnSql(column)).join(", ");
  db.run(`CREATE TABLE ${quoteIdentifier(operation.table)} (${columns})`);
}

function executeAddColumn(db: Database, operation: AddColumnOperation): void {
  if (operation.column.default?.kind === "sql") {
    const row = getRow<{ found: number }>(db, `SELECT 1 AS found FROM ${quoteIdentifier(operation.table)} LIMIT 1`);
    if (row) {
      throw new CodedError(
        "INVALID_OPERATION",
        "add_column with SQL default is only allowed on empty tables; use staged table rebuild for non-empty tables",
      );
    }
  }
  const column = buildColumnSql(operation.column, true);
  db.run(`ALTER TABLE ${quoteIdentifier(operation.table)} ADD COLUMN ${column}`);
}

function executeInsert(db: Database, operation: InsertOperation): void {
  const keys = Object.keys(operation.values);
  if (keys.length === 0) {
    throw new CodedError("INVALID_OPERATION", "insert values must not be empty");
  }

  const columns = keys.map((key) => quoteIdentifier(key)).join(", ");
  const placeholders = keys.map(() => "?").join(", ");
  const values = keys.map((key) => {
    const value = operation.values[key];
    if (value === undefined) {
      throw new CodedError("INVALID_OPERATION", `insert value is missing for key: ${key}`);
    }
    return value;
  });

  db.query(`INSERT INTO ${quoteIdentifier(operation.table)} (${columns}) VALUES (${placeholders})`).run(...values);
}

function executeUpdate(db: Database, operation: UpdateOperation): void {
  const valueKeys = Object.keys(operation.values);
  if (valueKeys.length === 0) {
    throw new CodedError("INVALID_OPERATION", "update values must not be empty");
  }

  const setParts: string[] = [];
  const setBindings: Array<string | number | boolean | null> = [];
  for (const key of valueKeys) {
    const value = operation.values[key];
    if (value === undefined) {
      throw new CodedError("INVALID_OPERATION", `update value is missing for key: ${key}`);
    }
    setParts.push(`${quoteIdentifier(key)} = ?`);
    setBindings.push(value);
  }

  const where = whereClauseFromRecord(operation.where);
  db.query(`UPDATE ${quoteIdentifier(operation.table)} SET ${setParts.join(", ")} WHERE ${where.clause}`).run(
    ...setBindings,
    ...where.bindings,
  );
}

function executeDelete(db: Database, operation: DeleteOperation): void {
  const where = whereClauseFromRecord(operation.where);
  db.query(`DELETE FROM ${quoteIdentifier(operation.table)} WHERE ${where.clause}`).run(...where.bindings);
}

function executeDropTable(db: Database, operation: DropTableOperation): void {
  db.run(`DROP TABLE ${quoteIdentifier(operation.table)}`);
}

function executeDropColumn(db: Database, operation: DropColumnOperation): void {
  db.run(`ALTER TABLE ${quoteIdentifier(operation.table)} DROP COLUMN ${quoteIdentifier(operation.column)}`);
}

interface SecondaryObjectRow {
  type: "index" | "trigger";
  name: string;
  sql: string;
}

interface MutableTableState {
  tableInfo: TableInfoRow[];
  resolvedTableName: string;
  quotedTableName: string;
  tableDdlSql: string;
  secondaryObjects: SecondaryObjectRow[];
}

interface SqliteSequenceSnapshot {
  seqLiteral: string;
}

function resolveMutableTableState(db: Database, table: string): MutableTableState {
  const requestedTableName = quoteIdentifier(table);
  const tableInfo = getRows<TableInfoRow>(db, `PRAGMA table_info(${requestedTableName})`);
  if (tableInfo.length === 0) {
    throw new CodedError("INVALID_OPERATION", `Table does not exist: ${table}`);
  }

  const tableDdlRow = getRow<{ name: string; sql: string | null }>(
    db,
    "SELECT name, sql FROM sqlite_master WHERE type='table' AND name = ? COLLATE NOCASE LIMIT 1",
    table,
  );
  if (!tableDdlRow?.sql) {
    throw new CodedError("INVALID_OPERATION", `Table DDL is not available: ${table}`);
  }

  const secondaryObjects = getRows<SecondaryObjectRow>(
    db,
    `
      SELECT type, name, sql
      FROM sqlite_master
      WHERE tbl_name = ? AND type IN ('index', 'trigger') AND sql IS NOT NULL
      `,
    tableDdlRow.name,
  );

  return {
    tableInfo,
    resolvedTableName: tableDdlRow.name,
    quotedTableName: quoteIdentifier(tableDdlRow.name),
    tableDdlSql: tableDdlRow.sql,
    secondaryObjects,
  };
}

function captureSqliteSequenceSnapshot(db: Database, tableName: string): SqliteSequenceSnapshot | null {
  if (!tableExists(db, "sqlite_sequence")) {
    return null;
  }
  const row = getRow<{ seqLiteral: string | null }>(
    db,
    "SELECT quote(seq) AS seqLiteral FROM sqlite_sequence WHERE name = ? LIMIT 1",
    tableName,
  );
  if (!row || typeof row.seqLiteral !== "string") {
    return null;
  }
  return { seqLiteral: row.seqLiteral };
}

function restoreSqliteSequenceSnapshot(db: Database, tableName: string, snapshot: SqliteSequenceSnapshot | null): void {
  if (!snapshot || !tableExists(db, "sqlite_sequence")) {
    return;
  }
  db.query("DELETE FROM sqlite_sequence WHERE name = ?").run(tableName);
  db.run(`INSERT INTO sqlite_sequence(name, seq) VALUES (${serializeLiteral(tableName)}, ${snapshot.seqLiteral})`);
}

function rebuildTableWithRewrittenDdl(
  db: Database,
  state: MutableTableState,
  rewrittenDdl: string,
  options: { savepointName: string; selectList?: string | undefined },
): void {
  const tempTable = `__toss_tmp_${state.resolvedTableName}_${crypto.randomUUID().replaceAll("-", "")}`;
  const quotedTempTable = quoteIdentifier(tempTable);
  const columnList = state.tableInfo.map((column) => quoteIdentifier(column.name)).join(", ");
  const selectList = options.selectList ?? columnList;

  runInSavepoint(db, options.savepointName, () => {
    const sequenceSnapshot = captureSqliteSequenceSnapshot(db, state.resolvedTableName);
    db.run(rewriteCreateTableName(rewrittenDdl, tempTable));
    db.run(`INSERT INTO ${quotedTempTable} (${columnList}) SELECT ${selectList} FROM ${state.quotedTableName}`);
    db.run(`DROP TABLE ${state.quotedTableName}`);
    db.run(`ALTER TABLE ${quotedTempTable} RENAME TO ${state.quotedTableName}`);
    restoreSqliteSequenceSnapshot(db, state.resolvedTableName, sequenceSnapshot);

    for (const object of state.secondaryObjects) {
      db.run(object.sql);
    }
  });
}

function executeAddCheck(db: Database, operation: AddCheckOperation): void {
  const state = resolveMutableTableState(db, operation.table);
  const rewrittenDdl = rewriteAddCheckInCreateTable(state.tableDdlSql, operation.expression);
  rebuildTableWithRewrittenDdl(db, state, rewrittenDdl, { savepointName: "toss_add_check" });
}

function executeDropCheck(db: Database, operation: DropCheckOperation): void {
  const state = resolveMutableTableState(db, operation.table);
  const rewrittenDdl = rewriteDropCheckInCreateTable(state.tableDdlSql, operation.expression);
  rebuildTableWithRewrittenDdl(db, state, rewrittenDdl, { savepointName: "toss_drop_check" });
}

function executeRestoreTable(db: Database, operation: RestoreTableOperation): void {
  const tmpTable = `__toss_restore_${operation.table}_${crypto.randomUUID().replaceAll("-", "")}`;
  const quotedTmp = quoteIdentifier(tmpTable);
  const quotedTable = quoteIdentifier(operation.table);
  const isSqlStorageClass = (value: unknown): value is EncodedCell["storageClass"] =>
    value === "null" || value === "integer" || value === "real" || value === "text" || value === "blob";
  const rowForRestore = (value: unknown): Record<string, unknown> => {
    if (!value || typeof value !== "object" || Array.isArray(value)) {
      throw new CodedError("INVALID_OPERATION", "restore_table row must be an object");
    }
    return value as Record<string, unknown>;
  };

  const literalForRestoreCell = (value: unknown): string => {
    if (value === null || typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
      return serializeLiteral(value);
    }
    if (!value || typeof value !== "object" || Array.isArray(value)) {
      throw new CodedError("INVALID_OPERATION", "restore_table row contains unsupported encoded value");
    }
    const storageClass = "storageClass" in value ? value.storageClass : undefined;
    const sqlLiteral = "sqlLiteral" in value ? value.sqlLiteral : undefined;
    if (isSqlStorageClass(storageClass) && typeof sqlLiteral === "string") {
      return sqlLiteral;
    }
    throw new CodedError("INVALID_OPERATION", "restore_table row contains unsupported encoded value");
  };

  runInSavepoint(db, "toss_restore_table", () => {
    const sequenceSnapshot = captureSqliteSequenceSnapshot(db, operation.table);
    db.run(rewriteCreateTableName(operation.ddlSql, tmpTable));
    const first = operation.rows?.[0];
    if (first) {
      const firstRow = rowForRestore(first);
      const columns = Object.keys(firstRow).sort((a, b) => a.localeCompare(b));
      if (columns.length === 0) {
        throw new CodedError("INVALID_OPERATION", "restore_table row must include at least one column");
      }
      const expected = new Set(columns);
      const columnSql = columns.map((column) => quoteIdentifier(column)).join(", ");
      for (const rawRow of operation.rows!) {
        const row = rowForRestore(rawRow);
        const rowColumns = Object.keys(row);
        if (rowColumns.length !== columns.length || rowColumns.some((column) => !expected.has(column))) {
          throw new CodedError("INVALID_OPERATION", "restore_table row column set does not match snapshot");
        }
        const valuesSql = columns.map((column) => literalForRestoreCell(row[column])).join(", ");
        db.run(`INSERT INTO ${quotedTmp} (${columnSql}) VALUES (${valuesSql})`);
      }
    }

    db.run(`DROP TABLE IF EXISTS ${quotedTable}`);
    db.run(`ALTER TABLE ${quotedTmp} RENAME TO ${quotedTable}`);
    restoreSqliteSequenceSnapshot(db, operation.table, sequenceSnapshot);
    for (const object of operation.secondaryObjects ?? []) {
      db.run(object.sql);
    }
  });
}

function executeAlterColumnType(db: Database, operation: AlterColumnTypeOperation): void {
  const newType = normalizeColumnType(operation.newType);
  const state = resolveMutableTableState(db, operation.table);

  const target = state.tableInfo.find((column) => column.name === operation.column);
  if (!target) {
    throw new CodedError("INVALID_OPERATION", `Column does not exist: ${operation.table}.${operation.column}`);
  }

  const rewrittenDdl = rewriteColumnTypeInCreateTable(state.tableDdlSql, operation.column, newType);
  const selectList = state.tableInfo
    .map((column) => {
      const quoted = quoteIdentifier(column.name);
      if (column.name === operation.column) {
        return `CAST(${quoted} AS ${newType}) AS ${quoted}`;
      }
      return quoted;
    })
    .join(", ");

  rebuildTableWithRewrittenDdl(db, state, rewrittenDdl, {
    savepointName: "toss_alter_column_type",
    selectList,
  });
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
    case "add_check":
      executeAddCheck(db, operation);
      return;
    case "drop_check":
      executeDropCheck(db, operation);
      return;
    case "restore_table":
      executeRestoreTable(db, operation);
      return;
    default:
      throw new CodedError("UNSUPPORTED", `Unsupported operation type: ${(operation as Operation).type}`);
  }
}

export function executeReadSql(db: Database, sql: string): Record<string, unknown>[] {
  return db.query<Record<string, unknown>, []>(sql).all();
}
