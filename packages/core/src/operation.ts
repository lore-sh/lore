import { z } from "zod";
import { runInSavepoint, tableExists, type Database } from "./db";
import { CodedError } from "./error";
import { whereClause } from "./inspect";
import { EncodedRow, JsonPrimitive, TableSecondaryObject, isSqlStorageClass } from "./schema";
import {
  asciiCaseFold,
  COLUMN_TYPE_PATTERN,
  IDENTIFIER_PATTERN,
  createScanner,
  parseColumnDefinitionsFromCreateTable,
  quoteIdentifier,
  rewriteAddCheckInCreateTable,
  rewriteColumnTypeInCreateTable,
  rewriteCreateTableName,
  rewriteDropColumnInCreateTable,
  rewriteDropCheckInCreateTable,
  sqlMentionsIdentifier,
} from "./sql";

const whereSchema = z.record(z.string(), JsonPrimitive).refine((value) => Object.keys(value).length > 0, {
  message: "where must not be empty",
});

export const ColumnDef = z
  .object({
    name: z.string().trim().min(1),
    type: z.string().trim().min(1),
    notNull: z.boolean().optional(),
    primaryKey: z.boolean().optional(),
    unique: z.boolean().optional(),
    default: z
      .discriminatedUnion("kind", [
        z
          .object({
            kind: z.literal("literal"),
            value: JsonPrimitive,
          })
          .strict(),
        z
          .object({
            kind: z.literal("sql"),
            expr: z.enum(["CURRENT_TIMESTAMP", "CURRENT_DATE", "CURRENT_TIME"]),
          })
          .strict(),
      ])
      .optional(),
  })
  .strict();
export type ColumnDef = z.infer<typeof ColumnDef>;

const CreateTable = z
  .object({
    type: z.literal("create_table"),
    table: z.string().trim().min(1),
    columns: z.array(ColumnDef).min(1),
  })
  .strict();
type CreateTable = z.infer<typeof CreateTable>;

const AddColumn = z
  .object({
    type: z.literal("add_column"),
    table: z.string().trim().min(1),
    column: ColumnDef,
  })
  .strict();
type AddColumn = z.infer<typeof AddColumn>;

const Insert = z
  .object({
    type: z.literal("insert"),
    table: z.string().trim().min(1),
    values: z.record(z.string(), JsonPrimitive).refine((value) => Object.keys(value).length > 0, {
      message: "insert values must not be empty",
    }),
  })
  .strict();
type Insert = z.infer<typeof Insert>;

const DropTable = z
  .object({
    type: z.literal("drop_table"),
    table: z.string().trim().min(1),
  })
  .strict();
type DropTable = z.infer<typeof DropTable>;

const DropColumn = z
  .object({
    type: z.literal("drop_column"),
    table: z.string().trim().min(1),
    column: z.string().trim().min(1),
  })
  .strict();
type DropColumn = z.infer<typeof DropColumn>;

const AlterColumnType = z
  .object({
    type: z.literal("alter_column_type"),
    table: z.string().trim().min(1),
    column: z.string().trim().min(1),
    newType: z.string().trim().min(1),
  })
  .strict();
type AlterColumnType = z.infer<typeof AlterColumnType>;

const AddCheck = z
  .object({
    type: z.literal("add_check"),
    table: z.string().trim().min(1),
    expression: z.string().trim().min(1),
  })
  .strict();
type AddCheck = z.infer<typeof AddCheck>;

const DropCheck = z
  .object({
    type: z.literal("drop_check"),
    table: z.string().trim().min(1),
    expression: z.string().trim().min(1),
  })
  .strict();
type DropCheck = z.infer<typeof DropCheck>;

const DropIndex = z
  .object({
    type: z.literal("drop_index"),
    table: z.string().trim().min(1),
    name: z.string().trim().min(1),
  })
  .strict();
type DropIndex = z.infer<typeof DropIndex>;

const DropTrigger = z
  .object({
    type: z.literal("drop_trigger"),
    table: z.string().trim().min(1),
    name: z.string().trim().min(1),
  })
  .strict();
type DropTrigger = z.infer<typeof DropTrigger>;

const DropView = z
  .object({
    type: z.literal("drop_view"),
    name: z.string().trim().min(1),
  })
  .strict();
type DropView = z.infer<typeof DropView>;

const RestoreTable = z
  .object({
    type: z.literal("restore_table"),
    table: z.string().trim().min(1),
    ddlSql: z.string().min(1),
    rows: z.array(EncodedRow).nullable(),
    secondaryObjects: z.array(TableSecondaryObject).optional(),
  })
  .strict();
type RestoreTable = z.infer<typeof RestoreTable>;

const Update = z
  .object({
    type: z.literal("update"),
    table: z.string().trim().min(1),
    values: z.record(z.string(), JsonPrimitive).refine((value) => Object.keys(value).length > 0, {
      message: "update values must not be empty",
    }),
    where: whereSchema,
  })
  .strict();
type Update = z.infer<typeof Update>;

const Delete = z
  .object({
    type: z.literal("delete"),
    table: z.string().trim().min(1),
    where: whereSchema,
  })
  .strict();
type Delete = z.infer<typeof Delete>;

export const Operation = z.discriminatedUnion("type", [
  CreateTable,
  AddColumn,
  Insert,
  DropTable,
  DropColumn,
  AlterColumnType,
  AddCheck,
  DropCheck,
  DropIndex,
  DropTrigger,
  DropView,
  RestoreTable,
  Update,
  Delete,
]);
export type Operation = z.infer<typeof Operation>;

type DependencyConflictDetail = {
  operation: "drop_column" | "drop_table";
  table?: string;
  column?: string;
  conflicts: Array<{
    type: "index" | "trigger" | "view" | "fk" | "generated";
    name: string;
    table?: string;
    reason: string;
  }>;
  suggestedOps: Operation[];
};

export const Plan = z
  .object({
    baseSchemaHash: z.string().regex(/^[0-9a-f]{64}$/i, "baseSchemaHash must be a 64-char hex string"),
    message: z.string().trim().min(1),
    operations: z.array(Operation).min(1),
  })
  .strict();
export type Plan = z.infer<typeof Plan>;

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

function buildColumnSql(column: ColumnDef, forAddColumn = false): string {
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

function executeCreateTable(db: Database, operation: CreateTable): void {
  const columns = operation.columns.map((column) => buildColumnSql(column)).join(", ");
  db.$client.run(`CREATE TABLE ${quoteIdentifier(operation.table)} (${columns})`);
}

function executeAddColumn(db: Database, operation: AddColumn): void {
  if (operation.column.default?.kind === "sql") {
    const row = db.$client.query<{ found: number }, []>(`SELECT 1 AS found FROM ${quoteIdentifier(operation.table)} LIMIT 1`).get();
    if (row) {
      throw new CodedError(
        "INVALID_OPERATION",
        "add_column with SQL default is only allowed on empty tables; use staged table rebuild for non-empty tables",
      );
    }
  }
  const column = buildColumnSql(operation.column, true);
  db.$client.run(`ALTER TABLE ${quoteIdentifier(operation.table)} ADD COLUMN ${column}`);
}

function executeInsert(db: Database, operation: Insert): void {
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

  db.$client.query(`INSERT INTO ${quoteIdentifier(operation.table)} (${columns}) VALUES (${placeholders})`).run(...values);
}

function executeUpdate(db: Database, operation: Update): void {
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

  const where = whereClause(operation.where);
  db.$client.query(`UPDATE ${quoteIdentifier(operation.table)} SET ${setParts.join(", ")} WHERE ${where.clause}`).run(
    ...setBindings,
    ...where.bindings,
  );
}

function executeDelete(db: Database, operation: Delete): void {
  const where = whereClause(operation.where);
  db.$client.query(`DELETE FROM ${quoteIdentifier(operation.table)} WHERE ${where.clause}`).run(...where.bindings);
}

function sqliteIdentifierEquals(left: string, right: string): boolean {
  return asciiCaseFold(left) === asciiCaseFold(right);
}

function findColumnBySqliteName<T extends { name: string }>(columns: T[], name: string): T | undefined {
  return columns.find((column) => sqliteIdentifierEquals(column.name, name));
}

type SqliteSchemaObject =
  | { type: "index"; name: string; tableName: string; sql: string }
  | { type: "trigger"; name: string; tableName: string; sql: string }
  | { type: "view"; name: string; tableName: string; sql: string };

function listRewrittenSchemaObjects(db: Database, runProbe: () => void) {
  const querySchemaObjects = () =>
    db.$client
      .query<SqliteSchemaObject, []>(
        "SELECT type, name, tbl_name AS tableName, sql FROM sqlite_master WHERE type IN ('index', 'trigger', 'view') AND sql IS NOT NULL",
      )
      .all();

  return runInSavepoint(
    db,
    "lore_dependency_probe",
    () => {
      const before = querySchemaObjects();
      const beforeByKey = new Map(
        before.map((object) => [`${object.type}:${asciiCaseFold(object.name)}`, object] as const),
      );
      runProbe();
      const after = querySchemaObjects();
      const rewritten: Array<{ before: SqliteSchemaObject; after: SqliteSchemaObject }> = [];
      for (const object of after) {
        const prev = beforeByKey.get(`${object.type}:${asciiCaseFold(object.name)}`);
        if (!prev || prev.sql === object.sql) {
          continue;
        }
        rewritten.push({ before: prev, after: object });
      }
      return rewritten;
    },
    { rollbackOnSuccess: true },
  );
}

function createProbeIdentifier(prefix: string, exists: (candidate: string) => boolean): string {
  while (true) {
    const candidate = `${prefix}${crypto.randomUUID().replaceAll("-", "")}`;
    if (!exists(candidate)) {
      return candidate;
    }
  }
}

function listTableRenameRewrittenSchemaObjects(db: Database, targetTable: string) {
  const probeTableName = createProbeIdentifier("__lore_probe_table_", (candidate) => tableExists(db, candidate));
  return listRewrittenSchemaObjects(db, () => {
    db.$client.run(`ALTER TABLE ${quoteIdentifier(targetTable)} RENAME TO ${quoteIdentifier(probeTableName)}`);
  });
}

function buildDependencyConflictError(detail: Omit<DependencyConflictDetail, "suggestedOps">) {
  const suggestedOps: Operation[] = [];
  const seen = new Set<string>();
  for (const conflict of detail.conflicts) {
    if (conflict.type === "index" && conflict.table) {
      const key = `drop_index:${conflict.table}:${conflict.name}`;
      if (!seen.has(key)) {
        seen.add(key);
        suggestedOps.push({ type: "drop_index", table: conflict.table, name: conflict.name });
      }
      continue;
    }
    if (conflict.type === "trigger" && conflict.table) {
      const key = `drop_trigger:${conflict.table}:${conflict.name}`;
      if (!seen.has(key)) {
        seen.add(key);
        suggestedOps.push({ type: "drop_trigger", table: conflict.table, name: conflict.name });
      }
      continue;
    }
    if (conflict.type === "view") {
      const key = `drop_view:${conflict.name}`;
      if (!seen.has(key)) {
        seen.add(key);
        suggestedOps.push({ type: "drop_view", name: conflict.name });
      }
    }
  }
  const full: DependencyConflictDetail = {
    ...detail,
    suggestedOps,
  };
  return new CodedError("DEPENDENCY_CONFLICT", "Operation causes implicit dependency loss", { detail: full });
}

function listInboundForeignKeyConflicts(
  db: Database,
  targetTable: string,
  options: { column?: string; targetColumnIsPrimaryKey?: boolean },
) {
  const conflicts: DependencyConflictDetail["conflicts"] = [];
  const rows = db.$client
    .query<{ name: string }, []>(
      "SELECT name FROM sqlite_master WHERE type='table' AND name NOT GLOB '_lore_*' AND name NOT GLOB '__drizzle_*' AND name NOT GLOB 'sqlite_*'",
    )
    .all();
  for (const row of rows) {
    const sourceTable = row.name;
    if (sqliteIdentifierEquals(sourceTable, targetTable)) {
      continue;
    }
    const fkRows = db.$client
      .query<{ id: number; table: string; from: string; to: string | null }, []>(
        `PRAGMA foreign_key_list(${quoteIdentifier(sourceTable, { unsafe: true })})`,
      )
      .all();
    for (const fk of fkRows) {
      if (!sqliteIdentifierEquals(fk.table, targetTable)) {
        continue;
      }
      if (options.column) {
        const fkMatchesColumn =
          (fk.to !== null && sqliteIdentifierEquals(fk.to, options.column)) ||
          (fk.to === null && options.targetColumnIsPrimaryKey === true);
        if (!fkMatchesColumn) {
          continue;
        }
      }
      conflicts.push({
        type: "fk",
        name: `fk_${sourceTable}_${fk.id}`,
        table: sourceTable,
        reason: `Inbound foreign key from ${sourceTable}.${fk.from}`,
      });
    }
  }
  return conflicts;
}

function executeDropTable(db: Database, operation: DropTable): void {
  const conflicts = [...listInboundForeignKeyConflicts(db, operation.table, {})];
  const targetLower = asciiCaseFold(operation.table);
  for (const rewritten of listTableRenameRewrittenSchemaObjects(db, operation.table)) {
    const object = rewritten.before;
    if (object.type === "view") {
      conflicts.push({
        type: "view" as const,
        name: object.name,
        reason: `View ${object.name} depends on ${operation.table}`,
      });
    } else if (object.type === "trigger" && asciiCaseFold(object.tableName) !== targetLower) {
      conflicts.push({
        type: "trigger" as const,
        name: object.name,
        table: object.tableName,
        reason: `Trigger ${object.name} depends on ${operation.table}`,
      });
    }
  }
  if (conflicts.length > 0) {
    throw buildDependencyConflictError({
      operation: "drop_table",
      table: operation.table,
      conflicts,
    });
  }
  db.$client.run(`DROP TABLE ${quoteIdentifier(operation.table)}`);
}

function executeDropColumn(db: Database, operation: DropColumn): void {
  const state = resolveMutableTableState(db, operation.table);
  const targetInfo = findColumnBySqliteName(state.tableInfo, operation.column);
  if (!targetInfo) {
    throw new CodedError("INVALID_OPERATION", `Column does not exist: ${operation.table}.${operation.column}`);
  }
  const targetColumn = targetInfo.name;
  const conflicts: DependencyConflictDetail["conflicts"] = [];
  const existingColumns = new Set(state.tableInfo.map((column) => asciiCaseFold(column.name)));
  const probeColumnName = createProbeIdentifier("__lore_probe_column_", (candidate) =>
    existingColumns.has(asciiCaseFold(candidate)),
  );
  for (const rewritten of listRewrittenSchemaObjects(db, () => {
    db.$client.run(
      `ALTER TABLE ${quoteIdentifier(state.resolvedTableName)} RENAME COLUMN ${quoteIdentifier(targetColumn)} TO ${quoteIdentifier(probeColumnName)}`,
    );
  })) {
    const object = rewritten.after;
    if (object.type === "index" && sqliteIdentifierEquals(object.tableName, state.resolvedTableName)) {
      conflicts.push({
        type: "index",
        name: object.name,
        table: state.resolvedTableName,
        reason: `Index ${object.name} depends on ${state.resolvedTableName}.${targetColumn}`,
      });
    } else if (object.type === "trigger") {
      conflicts.push({
        type: "trigger",
        name: object.name,
        table: object.tableName,
        reason: `Trigger ${object.name} depends on ${state.resolvedTableName}.${targetColumn}`,
      });
    } else if (object.type === "view") {
      conflicts.push({
        type: "view",
        name: object.name,
        reason: `View ${object.name} depends on ${state.resolvedTableName}.${targetColumn}`,
      });
    }
  }
  conflicts.push(
    ...listInboundForeignKeyConflicts(db, state.resolvedTableName, {
      column: targetColumn,
      targetColumnIsPrimaryKey: targetInfo.pk > 0,
    }),
  );
  const originalColumnName = new Map(state.tableInfo.map((column) => [column.name.toLowerCase(), column.name]));
  for (const [columnName, def] of parseColumnDefinitionsFromCreateTable(state.tableDdlSql)) {
    if (columnName === targetColumn.toLowerCase()) {
      continue;
    }
    const generated = def.toUpperCase().includes(" GENERATED ") || /(?:^|[^A-Za-z0-9_])AS\s*\(/i.test(def);
    if (!generated || !sqlMentionsIdentifier(def, targetColumn)) {
      continue;
    }
    conflicts.push({
      type: "generated",
      name: originalColumnName.get(columnName) ?? columnName,
      reason: `Generated column ${columnName} references ${targetColumn}`,
    });
  }

  if (conflicts.length > 0) {
    throw buildDependencyConflictError({
      operation: "drop_column",
      table: state.resolvedTableName,
      column: targetColumn,
      conflicts,
    });
  }

  const remainingColumns = state.tableInfo.filter((column) => column.name !== targetColumn);
  if (remainingColumns.length === 0) {
    throw new CodedError("INVALID_OPERATION", "drop_column cannot remove the last column");
  }
  const rewrittenDdl = rewriteDropColumnInCreateTable(state.tableDdlSql, targetColumn);
  const remainingSelectList = remainingColumns.map((column) => quoteIdentifier(column.name)).join(", ");
  const remainingColumnList = remainingColumns.map((column) => quoteIdentifier(column.name)).join(", ");

  rebuildTableWithRewrittenDdl(db, state, rewrittenDdl, {
    savepointName: "lore_drop_column",
    selectList: remainingSelectList,
    columnList: remainingColumnList,
  });
}

function resolveSchemaObjectTarget(
  db: Database,
  type: "index" | "trigger",
  name: string,
  expectedTable: string,
): { name: string; tableName: string } {
  const label = type === "index" ? "Index" : "Trigger";
  const row = db.$client
    .query<{ name: string; tableName: string }, [string]>(
      `SELECT name, tbl_name AS tableName FROM sqlite_master WHERE type='${type}' AND name = ? COLLATE NOCASE LIMIT 1`,
    )
    .get(name);
  if (!row) {
    throw new CodedError("INVALID_OPERATION", `${label} does not exist: ${name}`);
  }
  if (!sqliteIdentifierEquals(row.tableName, expectedTable)) {
    throw new CodedError(
      "INVALID_OPERATION",
      `${label} ${row.name} belongs to table ${row.tableName}, not ${expectedTable}`,
    );
  }
  return row;
}

function executeDropIndex(db: Database, operation: DropIndex): void {
  const target = resolveSchemaObjectTarget(db, "index", operation.name, operation.table);
  db.$client.run(`DROP INDEX ${quoteIdentifier(target.name)}`);
}

function executeDropTrigger(db: Database, operation: DropTrigger): void {
  const target = resolveSchemaObjectTarget(db, "trigger", operation.name, operation.table);
  db.$client.run(`DROP TRIGGER ${quoteIdentifier(target.name)}`);
}

function executeDropView(db: Database, operation: DropView): void {
  db.$client.run(`DROP VIEW ${quoteIdentifier(operation.name)}`);
}

function resolveMutableTableState(db: Database, table: string) {
  const requestedTableName = quoteIdentifier(table);
  const tableInfo = db.$client
    .query<{
      cid: number;
      name: string;
      type: string;
      notnull: number;
      dflt_value: string | null;
      pk: number;
    }, []>(`PRAGMA table_info(${requestedTableName})`)
    .all();
  if (tableInfo.length === 0) {
    throw new CodedError("INVALID_OPERATION", `Table does not exist: ${table}`);
  }

  const tableDdlRow = db.$client
    .query<{ name: string; sql: string | null }, [string]>(
      "SELECT name, sql FROM sqlite_master WHERE type='table' AND name = ? COLLATE NOCASE LIMIT 1",
    )
    .get(table);
  if (!tableDdlRow?.sql) {
    throw new CodedError("INVALID_OPERATION", `Table DDL is not available: ${table}`);
  }

  const secondaryObjects = db.$client
    .query<{ type: "index" | "trigger"; name: string; sql: string }, [string]>(`
      SELECT type, name, sql
      FROM sqlite_master
      WHERE tbl_name = ? AND type IN ('index', 'trigger') AND sql IS NOT NULL
      `)
    .all(tableDdlRow.name);

  return {
    tableInfo,
    resolvedTableName: tableDdlRow.name,
    quotedTableName: quoteIdentifier(tableDdlRow.name),
    tableDdlSql: tableDdlRow.sql,
    secondaryObjects,
  };
}

function captureSqliteSequenceSnapshot(db: Database, tableName: string) {
  if (!tableExists(db, "sqlite_sequence")) {
    return null;
  }
  const row = db.$client
    .query<{ seqLiteral: string | null }, [string]>(
      "SELECT quote(seq) AS seqLiteral FROM sqlite_sequence WHERE name = ? LIMIT 1",
    )
    .get(tableName);
  if (!row || typeof row.seqLiteral !== "string") {
    return null;
  }
  return { seqLiteral: row.seqLiteral };
}

function restoreSqliteSequenceSnapshot(
  db: Database,
  tableName: string,
  snapshot: ReturnType<typeof captureSqliteSequenceSnapshot>,
): void {
  if (!snapshot || !tableExists(db, "sqlite_sequence")) {
    return;
  }
  db.$client.query("DELETE FROM sqlite_sequence WHERE name = ?").run(tableName);
  db.$client.run(`INSERT INTO sqlite_sequence(name, seq) VALUES (${serializeLiteral(tableName)}, ${snapshot.seqLiteral})`);
}

function rebuildTableWithRewrittenDdl(
  db: Database,
  state: ReturnType<typeof resolveMutableTableState>,
  rewrittenDdl: string,
  options: { savepointName: string; selectList?: string | undefined; columnList?: string | undefined },
): void {
  const tempTable = `__lore_tmp_${state.resolvedTableName}_${crypto.randomUUID().replaceAll("-", "")}`;
  const quotedTempTable = quoteIdentifier(tempTable);
  const columnList = options.columnList ?? state.tableInfo.map((column) => quoteIdentifier(column.name)).join(", ");
  const selectList = options.selectList ?? columnList;
  const targetLower = asciiCaseFold(state.resolvedTableName);
  const externalObjects: SqliteSchemaObject[] = [];
  for (const rewritten of listTableRenameRewrittenSchemaObjects(db, state.resolvedTableName)) {
    const object = rewritten.before;
    if (object.type === "view" || (object.type === "trigger" && asciiCaseFold(object.tableName) !== targetLower)) {
      externalObjects.push(object);
    }
  }

  runInSavepoint(db, options.savepointName, () => {
    for (const object of externalObjects) {
      if (object.type === "trigger") {
        db.$client.run(`DROP TRIGGER ${quoteIdentifier(object.name)}`);
      }
    }
    for (const object of externalObjects) {
      if (object.type === "view") {
        db.$client.run(`DROP VIEW ${quoteIdentifier(object.name)}`);
      }
    }

    const sequenceSnapshot = captureSqliteSequenceSnapshot(db, state.resolvedTableName);
    db.$client.run(rewriteCreateTableName(rewrittenDdl, tempTable));
    db.$client.run(`INSERT INTO ${quotedTempTable} (${columnList}) SELECT ${selectList} FROM ${state.quotedTableName}`);
    db.$client.run(`DROP TABLE ${state.quotedTableName}`);
    db.$client.run(`ALTER TABLE ${quotedTempTable} RENAME TO ${state.quotedTableName}`);
    restoreSqliteSequenceSnapshot(db, state.resolvedTableName, sequenceSnapshot);

    for (const object of state.secondaryObjects) {
      db.$client.run(object.sql);
    }
    for (const object of externalObjects) {
      if (object.type === "view") {
        db.$client.run(object.sql);
      }
    }
    for (const object of externalObjects) {
      if (object.type === "trigger") {
        db.$client.run(object.sql);
      }
    }
  });
}

function executeAddCheck(db: Database, operation: AddCheck): void {
  const state = resolveMutableTableState(db, operation.table);
  const rewrittenDdl = rewriteAddCheckInCreateTable(state.tableDdlSql, operation.expression);
  rebuildTableWithRewrittenDdl(db, state, rewrittenDdl, { savepointName: "lore_add_check" });
}

function executeDropCheck(db: Database, operation: DropCheck): void {
  const state = resolveMutableTableState(db, operation.table);
  const rewrittenDdl = rewriteDropCheckInCreateTable(state.tableDdlSql, operation.expression);
  rebuildTableWithRewrittenDdl(db, state, rewrittenDdl, { savepointName: "lore_drop_check" });
}

function executeRestoreTable(db: Database, operation: RestoreTable): void {
  const tmpTable = `__lore_restore_${operation.table}_${crypto.randomUUID().replaceAll("-", "")}`;
  const quotedTmp = quoteIdentifier(tmpTable);
  const quotedTable = quoteIdentifier(operation.table);
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

  runInSavepoint(db, "lore_restore_table", () => {
    const sequenceSnapshot = captureSqliteSequenceSnapshot(db, operation.table);
    db.$client.run(rewriteCreateTableName(operation.ddlSql, tmpTable));
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
        db.$client.run(`INSERT INTO ${quotedTmp} (${columnSql}) VALUES (${valuesSql})`);
      }
    }

    db.$client.run(`DROP TABLE IF EXISTS ${quotedTable}`);
    db.$client.run(`ALTER TABLE ${quotedTmp} RENAME TO ${quotedTable}`);
    restoreSqliteSequenceSnapshot(db, operation.table, sequenceSnapshot);
    for (const object of operation.secondaryObjects ?? []) {
      db.$client.run(object.sql);
    }
  });
}

function executeAlterColumnType(db: Database, operation: AlterColumnType): void {
  const newType = normalizeColumnType(operation.newType);
  const state = resolveMutableTableState(db, operation.table);

  const target = findColumnBySqliteName(state.tableInfo, operation.column);
  if (!target) {
    throw new CodedError("INVALID_OPERATION", `Column does not exist: ${operation.table}.${operation.column}`);
  }
  const targetColumn = target.name;

  const rewrittenDdl = rewriteColumnTypeInCreateTable(state.tableDdlSql, targetColumn, newType);
  const selectList = state.tableInfo
    .map((column) => {
      const quoted = quoteIdentifier(column.name);
      if (column.name === targetColumn) {
        return `CAST(${quoted} AS ${newType}) AS ${quoted}`;
      }
      return quoted;
    })
    .join(", ");

  rebuildTableWithRewrittenDdl(db, state, rewrittenDdl, {
    savepointName: "lore_alter_column_type",
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
    case "drop_index":
      executeDropIndex(db, operation);
      return;
    case "drop_trigger":
      executeDropTrigger(db, operation);
      return;
    case "drop_view":
      executeDropView(db, operation);
      return;
    case "restore_table":
      executeRestoreTable(db, operation);
      return;
    default:
      throw new CodedError("UNSUPPORTED", `Unsupported operation type: ${(operation as Operation).type}`);
  }
}

export function executeReadSql(db: Database, sql: string): Record<string, unknown>[] {
  return db.$client.query<Record<string, unknown>, []>(sql).all();
}

function assertIdentifier(value: string, label: string): void {
  if (!IDENTIFIER_PATTERN.test(value)) {
    throw new CodedError("INVALID_OPERATION", `${label} must match ${IDENTIFIER_PATTERN.source}: ${value}`);
  }
  if (value.startsWith("_lore_") || value.startsWith("sqlite_")) {
    throw new CodedError("INVALID_OPERATION", `${label} cannot use reserved prefix: ${value}`);
  }
}

function validateColumn(
  column: {
    name: string;
    type: string;
    primaryKey?: boolean | undefined;
    unique?: boolean | undefined;
    notNull?: boolean | undefined;
    default?: unknown;
  },
  operationLabel: string,
): void {
  assertIdentifier(column.name, `${operationLabel}.column`);
  if (!COLUMN_TYPE_PATTERN.test(column.type)) {
    throw new CodedError("INVALID_OPERATION", `${operationLabel}.column type is invalid: ${column.type}`);
  }
}

function assertColumnType(value: string, label: string): void {
  if (!COLUMN_TYPE_PATTERN.test(value)) {
    throw new CodedError("INVALID_OPERATION", `${label} is invalid: ${value}`);
  }
}

function scanSqlControlTokens(sql: string): { hasCommentToken: boolean; hasSemicolon: boolean } {
  const scanner = createScanner(sql);
  let hasCommentToken = false;
  let hasSemicolon = false;

  while (scanner.pos < sql.length) {
    const ch = sql[scanner.pos]!;
    const next = sql[scanner.pos + 1];

    // Detect comment tokens before skipInterior consumes them
    if (!scanner.insideLiteral) {
      if ((ch === "-" && next === "-") || (ch === "/" && next === "*") || (ch === "*" && next === "/")) {
        hasCommentToken = true;
      }
    }

    if (scanner.skipInterior()) {
      continue;
    }

    if (ch === ";") {
      hasSemicolon = true;
    }
    scanner.advance();
  }

  return { hasCommentToken, hasSemicolon };
}

function analyzeSqlExpressionShape(sql: string): {
  hasTopLevelComma: boolean;
  hasUnbalancedParen: boolean;
  hasUnterminatedLiteral: boolean;
} {
  const scanner = createScanner(sql);
  let depth = 0;
  let hasTopLevelComma = false;

  while (scanner.pos < sql.length) {
    if (scanner.skipInterior()) {
      continue;
    }
    const ch = sql[scanner.pos]!;
    if (ch === "(") {
      depth += 1;
    } else if (ch === ")") {
      depth -= 1;
      if (depth < 0) {
        return { hasTopLevelComma, hasUnbalancedParen: true, hasUnterminatedLiteral: false };
      }
    } else if (ch === "," && depth === 0) {
      hasTopLevelComma = true;
    }
    scanner.advance();
  }

  return {
    hasTopLevelComma,
    hasUnbalancedParen: depth !== 0,
    hasUnterminatedLiteral: scanner.insideLiteral,
  };
}

function assertCheckExpression(value: string, label: string): void {
  const expression = value.trim();
  if (expression.length === 0) {
    throw new CodedError("INVALID_OPERATION", `${label} must not be empty`);
  }
  if (expression.includes("\0")) {
    throw new CodedError("INVALID_OPERATION", `${label} must not contain NUL`);
  }
  const scan = scanSqlControlTokens(expression);
  if (scan.hasCommentToken) {
    throw new CodedError("INVALID_OPERATION", `${label} must not contain SQL comments`);
  }
  if (scan.hasSemicolon) {
    throw new CodedError("INVALID_OPERATION", `${label} must be a single SQL expression`);
  }
  const shape = analyzeSqlExpressionShape(expression);
  if (shape.hasTopLevelComma) {
    throw new CodedError("INVALID_OPERATION", `${label} must not contain top-level commas`);
  }
  if (shape.hasUnbalancedParen || shape.hasUnterminatedLiteral) {
    throw new CodedError("INVALID_OPERATION", `${label} must be a valid single SQL expression`);
  }
}

function assertPredicate(
  values: Record<string, string | number | boolean | null>,
  label: string,
): void {
  const keys = Object.keys(values);
  if (keys.length === 0) {
    throw new CodedError("INVALID_OPERATION", `${label} must not be empty`);
  }
  for (const key of keys) {
    assertIdentifier(key, `${label} key`);
  }
}

function semanticValidation(plan: Plan): void {
  for (const operation of plan.operations) {
    if ("table" in operation) {
      assertIdentifier(operation.table, `${operation.type}.table`);
    }

    if (operation.type === "create_table") {
      const seen = new Set<string>();
      let primaryKeyCount = 0;
      for (const column of operation.columns) {
        validateColumn(column, operation.type);
        if (seen.has(column.name)) {
          throw new CodedError("INVALID_OPERATION", `duplicate column name: ${column.name}`);
        }
        seen.add(column.name);
        if (column.primaryKey) {
          primaryKeyCount += 1;
        }
      }
      if (primaryKeyCount > 1) {
        throw new CodedError("INVALID_OPERATION", "create_table cannot contain multiple primary keys");
      }
      if (primaryKeyCount === 0) {
        throw new CodedError("INVALID_OPERATION", "create_table must define a PRIMARY KEY");
      }
      continue;
    }

    if (operation.type === "add_column") {
      validateColumn(operation.column, operation.type);
      if (operation.column.primaryKey || operation.column.unique) {
        throw new CodedError(
          "INVALID_OPERATION",
          "add_column does not allow primaryKey/unique in MVP because SQLite ALTER restrictions vary",
        );
      }
      if (operation.column.notNull && !Object.hasOwn(operation.column, "default")) {
        throw new CodedError("INVALID_OPERATION", "add_column with NOT NULL requires default");
      }
      continue;
    }

    if (operation.type === "insert") {
      assertPredicate(operation.values, "insert.values");
      continue;
    }

    if (operation.type === "drop_column") {
      assertIdentifier(operation.column, "drop_column.column");
      continue;
    }

    if (operation.type === "alter_column_type") {
      assertIdentifier(operation.column, "alter_column_type.column");
      assertColumnType(operation.newType, "alter_column_type.newType");
      continue;
    }

    if (operation.type === "add_check") {
      assertCheckExpression(operation.expression, "add_check.expression");
      continue;
    }

    if (operation.type === "drop_check") {
      assertCheckExpression(operation.expression, "drop_check.expression");
      continue;
    }

    if (operation.type === "drop_index") {
      assertIdentifier(operation.name, "drop_index.name");
      continue;
    }

    if (operation.type === "drop_trigger") {
      assertIdentifier(operation.name, "drop_trigger.name");
      continue;
    }

    if (operation.type === "drop_view") {
      assertIdentifier(operation.name, "drop_view.name");
      continue;
    }

    if (operation.type === "update") {
      assertPredicate(operation.values, "update.values");
      assertPredicate(operation.where, "update.where");
      continue;
    }

    if (operation.type === "delete") {
      assertPredicate(operation.where, "delete.where");
      continue;
    }

    if (operation.type === "restore_table") {
      throw new CodedError("INVALID_OPERATION", "restore_table is an internal operation and cannot be used in plans");
    }
  }
}

export function parsePlan(input: string): Plan {
  let parsed: unknown;
  try {
    parsed = JSON.parse(input);
  } catch (error) {
    throw new CodedError("INVALID_JSON", `Plan must be valid JSON: ${(error as Error).message}`);
  }

  const result = Plan.safeParse(parsed);
  if (!result.success) {
    throw new CodedError("INVALID_PLAN", result.error.issues.map((issue) => issue.message).join("; "));
  }

  semanticValidation(result.data);
  return result.data;
}
