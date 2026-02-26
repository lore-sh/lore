import { z } from "zod";
import { listUserTables, listUserViews, tableExists, type Database } from "./db";
import { CodedError, type ErrorCode } from "./error";
import { canonicalJson, sha256Hex } from "./hash";
import { primaryKeys, tableDDL, tableInfo } from "./inspect";
import { executeOperation, type Operation } from "./operation";
import { buildPkWhereClause, buildRowSelectSql } from "./replay-sql";
import { EncodedRow, TableSecondaryObject, isSqlStorageClass, type EncodedCell } from "./schema";
import { quoteIdentifier, sqlMentionsIdentifier } from "./sql";

export const RowEffect = z.object({
  tableName: z.string(),
  pk: z.record(z.string(), z.string()),
  opKind: z.enum(["insert", "update", "delete"]),
  beforeRow: EncodedRow.nullable(),
  afterRow: EncodedRow.nullable(),
  beforeHash: z.string().nullable(),
  afterHash: z.string().nullable(),
});
export type RowEffect = z.infer<typeof RowEffect>;

export const TableSnapshot = z.object({
  kind: z.enum(["table", "view"]).optional().default("table"),
  tableName: z.string(),
  ddlSql: z.string(),
  rows: z.array(EncodedRow),
  secondaryObjects: z.array(TableSecondaryObject),
  references: z.array(z.string()),
});
export type TableSnapshot = z.infer<typeof TableSnapshot>;

export const SchemaEffect = z.object({
  tableName: z.string(),
  beforeTable: TableSnapshot.nullable(),
  afterTable: TableSnapshot.nullable(),
});
export type SchemaEffect = z.infer<typeof SchemaEffect>;

export function pkWhere(pk: Record<string, string>): string {
  return buildPkWhereClause(pk, (message) => {
    throw new CodedError("REVERT_FAILED", message);
  });
}

function encodeRowFromResult(
  row: Record<string, unknown>,
  columns: string[],
  quoteAliases: string[],
  hexAliases: string[],
  typeAliases: string[],
): EncodedRow {
  const encoded: EncodedRow = {};
  for (let i = 0; i < columns.length; i++) {
    const column = columns[i]!;
    const quoteAlias = quoteAliases[i]!;
    const hexAlias = hexAliases[i]!;
    const typeAlias = typeAliases[i]!;
    const quoteValue = row[quoteAlias];
    const hexValue = row[hexAlias];
    const typeValue = row[typeAlias];
    if (!isSqlStorageClass(typeValue)) {
      throw new CodedError("APPLY_FAILED", `Unsupported sqlite storage class for ${column}: ${String(typeValue)}`);
    }
    let sqlLiteral: string;
    if (typeValue === "null") {
      sqlLiteral = "NULL";
    } else if (typeValue === "text") {
      if (typeof hexValue !== "string") {
        throw new CodedError("APPLY_FAILED", `Failed to encode text bytes for ${column}`);
      }
      sqlLiteral = `CAST(X'${hexValue}' AS TEXT)`;
    } else if (typeValue === "blob") {
      if (typeof hexValue !== "string") {
        throw new CodedError("APPLY_FAILED", `Failed to encode blob bytes for ${column}`);
      }
      sqlLiteral = `X'${hexValue}'`;
    } else {
      if (typeof quoteValue !== "string") {
        throw new CodedError("APPLY_FAILED", `Failed to encode numeric literal for ${column}`);
      }
      sqlLiteral = quoteValue;
    }
    const cell: EncodedCell = {
      storageClass: typeValue,
      sqlLiteral,
    };
    encoded[column] = cell;
  }
  return encoded;
}

function tableColumns(db: Database, table: string): string[] {
  const info = tableInfo(db, table);
  if (info.length === 0) {
    throw new CodedError("APPLY_FAILED", `Unable to inspect table columns: ${table}`);
  }
  return info.map((column) => column.name);
}

export function isSystemTable(table: string): boolean {
  return table === "sqlite_sequence";
}

function captureTableState(db: Database, table: string) {
  const keyColumns = primaryKeys(db, table);
  if (keyColumns.length === 0) {
    if (isSystemTable(table)) {
      keyColumns.push("name");
    } else {
      throw new CodedError("NO_PRIMARY_KEY", `Table ${table} must define PRIMARY KEY for tracked operations`);
    }
  }

  const ddlSql = tableDDL(db, table) ?? (isSystemTable(table) ? "CREATE TABLE sqlite_sequence(name,seq)" : null);
  if (!ddlSql) {
    throw new CodedError("APPLY_FAILED", `Unable to read CREATE TABLE SQL for ${table}`);
  }

  const secondaryObjects = db.$client
    .query<{ type: "index" | "trigger"; name: string; sql: string }, [string]>(`
      SELECT type, name, sql
      FROM sqlite_master
      WHERE tbl_name = ? AND type IN ('index', 'trigger') AND sql IS NOT NULL
      ORDER BY type ASC, name ASC
      `)
    .all(table);
  const references = Array.from(
    new Set(
      db.$client
        .query<{ table: string }, []>(`PRAGMA foreign_key_list(${quoteIdentifier(table, { unsafe: true })})`)
        .all()
        .map((row) => row.table),
    ),
  ).sort((a, b) => a.localeCompare(b));

  const columns = tableColumns(db, table);
  const quoteAliases = columns.map((_, i) => `__lore_quote_${i}`);
  const hexAliases = columns.map((_, i) => `__lore_hex_${i}`);
  const typeAliases = columns.map((_, i) => `__lore_type_${i}`);
  const rowsRaw = db.$client.query<Record<string, unknown>, []>(buildRowSelectSql(table, columns, keyColumns, null)).all();

  const rows: EncodedRow[] = [];
  const rowsByPk = new Map<string, { pk: Record<string, string>; row: EncodedRow; rowHash: string }>();
  for (const raw of rowsRaw) {
    const row = encodeRowFromResult(raw, columns, quoteAliases, hexAliases, typeAliases);
    const pk: Record<string, string> = {};
    for (const pkColumn of keyColumns) {
      const cell = row[pkColumn];
      if (!cell) {
        throw new CodedError("APPLY_FAILED", `PK column missing in encoded row: ${table}.${pkColumn}`);
      }
      if (cell.sqlLiteral.toUpperCase() === "NULL") {
        throw new CodedError(
          "APPLY_FAILED",
          `Tracked table ${table} has NULL primary key value at column ${pkColumn}; nullable PK values are not supported.`,
        );
      }
      pk[pkColumn] = cell.sqlLiteral;
    }
    const key = canonicalJson(pk);
    if (rowsByPk.has(key)) {
      throw new CodedError(
        "APPLY_FAILED",
        `Tracked table ${table} has duplicate primary-key identity in observed capture: ${canonicalJson(pk)}`,
      );
    }
    const hash = rowHash(row);
    rows.push(row);
    rowsByPk.set(key, { pk, row, rowHash: hash ?? "" });
  }

  const snapshot: TableSnapshot = {
    kind: "table",
    tableName: table,
    ddlSql,
    rows,
    secondaryObjects,
    references,
  };
  const schemaSignature = sha256Hex({
    kind: snapshot.kind,
    ddlSql: snapshot.ddlSql,
    secondaryObjects: snapshot.secondaryObjects,
    references: snapshot.references,
  });
  return { snapshot, schemaSignature, rowsByPk, keyColumns };
}

function captureViewState(db: Database, view: string, referenceCandidates: string[]) {
  const row = db.$client
    .query<{ name: string; sql: string | null }, [string]>(
      "SELECT name, sql FROM sqlite_master WHERE type='view' AND name = ? COLLATE NOCASE LIMIT 1",
    )
    .get(view);
  if (!row?.sql) {
    throw new CodedError("APPLY_FAILED", `Unable to read CREATE VIEW SQL for ${view}`);
  }
  const viewSql = row.sql;

  const references = referenceCandidates
    .filter((candidate) => candidate !== row.name && sqlMentionsIdentifier(viewSql, candidate))
    .sort((a, b) => a.localeCompare(b));

  const snapshot: TableSnapshot = {
    kind: "view",
    tableName: row.name,
    ddlSql: viewSql,
    rows: [],
    secondaryObjects: [],
    references,
  };
  const schemaSignature = sha256Hex({
    kind: snapshot.kind,
    ddlSql: snapshot.ddlSql,
  });
  return {
    snapshot,
    schemaSignature,
    rowsByPk: new Map<string, { pk: Record<string, string>; row: EncodedRow; rowHash: string }>(),
    keyColumns: [] as string[],
  };
}

export function captureState(db: Database) {
  const tableNames = listUserTables(db);
  if (tableExists(db, "sqlite_sequence")) {
    tableNames.push("sqlite_sequence");
  }
  tableNames.sort((a, b) => a.localeCompare(b));
  const viewNames = listUserViews(db);
  viewNames.sort((a, b) => a.localeCompare(b));
  const referenceCandidates = [...tableNames.filter((name) => !isSystemTable(name)), ...viewNames].sort((a, b) =>
    a.localeCompare(b),
  );

  const captured = new Map<string, ReturnType<typeof captureTableState>>();
  for (const table of tableNames) {
    captured.set(table, captureTableState(db, table));
  }
  for (const view of viewNames) {
    captured.set(view, captureViewState(db, view, referenceCandidates));
  }
  return { tables: captured };
}

export function diffState(
  before: ReturnType<typeof captureState>,
  after: ReturnType<typeof captureState>,
) {
  const names = Array.from(new Set([...before.tables.keys(), ...after.tables.keys()])).sort((a, b) =>
    a.localeCompare(b),
  );

  const schemaEffects: SchemaEffect[] = [];
  type RowBucket = { inserts: RowEffect[]; updates: RowEffect[]; deletes: RowEffect[] };
  const rowEffectsByTable = new Map<string, RowBucket>();
  const tableRefs = new Map<string, string[]>();

  for (const tableName of names) {
    const beforeTable = before.tables.get(tableName);
    const afterTable = after.tables.get(tableName);
    const hasSchemaChange = !beforeTable || !afterTable
      ? beforeTable !== afterTable
      : beforeTable.schemaSignature !== afterTable.schemaSignature;

    if (hasSchemaChange && !isSystemTable(tableName)) {
      schemaEffects.push({
        tableName,
        beforeTable: beforeTable ? beforeTable.snapshot : null,
        afterTable: afterTable ? afterTable.snapshot : null,
      });
      continue;
    }

    const kind = afterTable?.snapshot.kind ?? beforeTable?.snapshot.kind ?? "table";
    if (kind === "view") {
      continue;
    }

    const refs = afterTable?.snapshot.references ?? beforeTable?.snapshot.references ?? [];
    tableRefs.set(tableName, refs);

    const beforeRows = beforeTable?.rowsByPk ?? new Map<string, { pk: Record<string, string>; row: EncodedRow; rowHash: string }>();
    const afterRows = afterTable?.rowsByPk ?? new Map<string, { pk: Record<string, string>; row: EncodedRow; rowHash: string }>();
    const pkKeys = Array.from(new Set([...beforeRows.keys(), ...afterRows.keys()])).sort((a, b) => a.localeCompare(b));
    const bucket: RowBucket = { inserts: [], updates: [], deletes: [] };

    for (const key of pkKeys) {
      const beforeEntry = beforeRows.get(key);
      const afterEntry = afterRows.get(key);

      if (!beforeEntry && afterEntry) {
        bucket.inserts.push({
          tableName,
          pk: afterEntry.pk,
          opKind: "insert",
          beforeRow: null,
          afterRow: afterEntry.row,
          beforeHash: null,
          afterHash: afterEntry.rowHash,
        });
        continue;
      }

      if (beforeEntry && !afterEntry) {
        bucket.deletes.push({
          tableName,
          pk: beforeEntry.pk,
          opKind: "delete",
          beforeRow: beforeEntry.row,
          afterRow: null,
          beforeHash: beforeEntry.rowHash,
          afterHash: null,
        });
        continue;
      }

      if (beforeEntry && afterEntry && beforeEntry.rowHash !== afterEntry.rowHash) {
        bucket.updates.push({
          tableName,
          pk: beforeEntry.pk,
          opKind: "update",
          beforeRow: beforeEntry.row,
          afterRow: afterEntry.row,
          beforeHash: beforeEntry.rowHash,
          afterHash: afterEntry.rowHash,
        });
      }
    }
    rowEffectsByTable.set(tableName, bucket);
  }

  const rowEffects: RowEffect[] = [];
  const rowTables = Array.from(rowEffectsByTable.keys());
  const parentFirst = dependencyOrder(rowTables, tableRefs, "parent-first");
  const childFirst = dependencyOrder(rowTables, tableRefs, "child-first");
  for (const table of childFirst) {
    rowEffects.push(...(rowEffectsByTable.get(table)?.deletes ?? []));
  }
  for (const table of parentFirst) {
    rowEffects.push(...(rowEffectsByTable.get(table)?.updates ?? []));
  }
  for (const table of parentFirst) {
    rowEffects.push(...(rowEffectsByTable.get(table)?.inserts ?? []));
  }

  return { rowEffects, schemaEffects };
}

export function dependencyOrder(
  tables: string[],
  tableRefs: Map<string, string[]>,
  mode: "parent-first" | "child-first",
): string[] {
  const compareTableNames = (a: string, b: string): number => {
    const aSystem = isSystemTable(a);
    const bSystem = isSystemTable(b);
    if (aSystem !== bSystem) {
      return aSystem ? -1 : 1;
    }
    return a.localeCompare(b);
  };
  const tableSet = new Set(tables);
  const outgoing = new Map<string, string[]>();
  for (const table of tables) {
    const refs = (tableRefs.get(table) ?? []).filter((ref) => tableSet.has(ref));
    outgoing.set(table, refs.sort(compareTableNames));
  }

  const temp = new Set<string>();
  const perm = new Set<string>();
  const parentFirst: string[] = [];
  const visit = (node: string): void => {
    if (perm.has(node) || temp.has(node)) {
      return;
    }
    temp.add(node);
    const refs = outgoing.get(node) ?? [];
    for (const ref of refs) {
      visit(ref);
    }
    temp.delete(node);
    perm.add(node);
    parentFirst.push(node);
  };

  for (const table of [...tables].sort(compareTableNames)) {
    visit(table);
  }
  return mode === "parent-first" ? parentFirst : [...parentFirst].reverse();
}

export function rowHash(row: EncodedRow | null): string | null {
  if (!row) {
    return null;
  }
  return sha256Hex(row);
}

export function readRow(db: Database, table: string, pk: Record<string, string>): EncodedRow | null {
  if (!tableExists(db, table)) {
    if (isSystemTable(table)) {
      return null;
    }
    throw new CodedError("REVERT_FAILED", `Table does not exist while reading observed row: ${table}`);
  }
  const columns = tableColumns(db, table);
  const keyColumns = Object.keys(pk).sort((a, b) => a.localeCompare(b));
  if (keyColumns.length === 0) {
    throw new CodedError("REVERT_FAILED", `Cannot fetch row without key columns: ${table}`);
  }
  const quoteAliases = columns.map((_, i) => `__lore_quote_${i}`);
  const hexAliases = columns.map((_, i) => `__lore_hex_${i}`);
  const typeAliases = columns.map((_, i) => `__lore_type_${i}`);
  const whereClause = pkWhere(pk);
  const sql = `${buildRowSelectSql(table, columns, keyColumns, whereClause)} LIMIT 1`;
  const row = db.$client.query<Record<string, unknown>, []>(sql).get();
  if (!row) {
    return null;
  }
  return encodeRowFromResult(row, columns, quoteAliases, hexAliases, typeAliases);
}

function insertEncodedRow(db: Database, table: string, row: EncodedRow): void {
  const columns = Object.keys(row).sort((a, b) => a.localeCompare(b));
  if (columns.length === 0) {
    throw new CodedError("REVERT_FAILED", `Cannot insert empty encoded row for ${table}`);
  }
  const colSql = columns.map((column) => quoteIdentifier(column, { unsafe: true })).join(", ");
  const valuesSql = columns
    .map((column) => {
      const cell = row[column];
      if (!cell) {
        throw new CodedError("REVERT_FAILED", `Missing encoded cell for ${table}.${column}`);
      }
      return cell.sqlLiteral;
    })
    .join(", ");
  db.$client.run(`INSERT INTO ${quoteIdentifier(table, { unsafe: true })} (${colSql}) VALUES (${valuesSql})`);
}

function updateEncodedRow(db: Database, table: string, pk: Record<string, string>, row: EncodedRow): void {
  const columns = Object.keys(row).sort((a, b) => a.localeCompare(b));
  if (columns.length === 0) {
    throw new CodedError("REVERT_FAILED", `Cannot update empty encoded row for ${table}`);
  }
  const setSql = columns
    .map((column) => {
      const cell = row[column];
      if (!cell) {
        throw new CodedError("REVERT_FAILED", `Missing encoded cell for ${table}.${column}`);
      }
      return `${quoteIdentifier(column, { unsafe: true })} = ${cell.sqlLiteral}`;
    })
    .join(", ");
  db.$client.run(`UPDATE ${quoteIdentifier(table, { unsafe: true })} SET ${setSql} WHERE ${pkWhere(pk)}`);
}

function deleteByPk(db: Database, table: string, pk: Record<string, string>): void {
  db.$client.run(`DELETE FROM ${quoteIdentifier(table, { unsafe: true })} WHERE ${pkWhere(pk)}`);
}

function referencedTables(db: Database, table: string): string[] {
  if (!tableExists(db, table)) {
    return [];
  }
  const rows = db.$client.query<{ table: string }, []>(`PRAGMA foreign_key_list(${quoteIdentifier(table, { unsafe: true })})`).all();
  return Array.from(new Set(rows.map((row) => row.table))).sort((a, b) => a.localeCompare(b));
}

function missingReferencedTables(db: Database, table: string): string[] {
  return referencedTables(db, table).filter((refTable) => !tableExists(db, refTable));
}

export function applyRowEffects(
  db: Database,
  effects: RowEffect[],
  direction: "forward" | "inverse",
  options: {
    disableTableTriggers: boolean;
    includeSystemEffects?: boolean;
    includeUserEffects?: boolean;
    systemPolicy?: "strict" | "reconcile";
  },
): void {
  const includeSystemEffects = options.includeSystemEffects ?? true;
  const includeUserEffects = options.includeUserEffects ?? true;
  const systemPolicy = options.systemPolicy ?? "strict";
  const filtered = effects.filter((effect) =>
    isSystemTable(effect.tableName) ? includeSystemEffects : includeUserEffects,
  );
  const droppedTriggers = options.disableTableTriggers ? dropTriggersForTables(db, filtered) : null;
  try {
    const ordered = direction === "forward" ? filtered : filtered.toReversed();
    for (const effect of ordered) {
      const forward = direction === "forward";
      const { beforeRow, afterRow } = effect;
      let expectedCurrent: EncodedRow | null;
      let target: EncodedRow | null;
      let opLabel: string;
      if (effect.opKind === "insert") {
        expectedCurrent = forward ? null : afterRow;
        target = forward ? afterRow : null;
        opLabel = forward ? "insert" : "inverse-delete";
      } else if (effect.opKind === "update") {
        expectedCurrent = forward ? beforeRow : afterRow;
        target = forward ? afterRow : beforeRow;
        opLabel = forward ? "update" : "inverse-update";
      } else {
        expectedCurrent = forward ? beforeRow : null;
        target = forward ? null : beforeRow;
        opLabel = forward ? "delete" : "inverse-insert";
      }
      const isSystem = isSystemTable(effect.tableName);
      if (isSystem && systemPolicy === "reconcile") {
        applySystemRowEffectReconciled(db, effect.tableName, effect.pk, target);
        continue;
      }
      const current = readRow(db, effect.tableName, effect.pk);
      const currentHash = rowHash(current);
      const expectedHash = rowHash(expectedCurrent);
      if (currentHash !== expectedHash) {
        throw new CodedError(
          "REVERT_FAILED",
          `Observed row mismatch during ${opLabel} on ${effect.tableName} (pk=${canonicalJson(effect.pk)})`,
        );
      }
      if (!target) {
        deleteByPk(db, effect.tableName, effect.pk);
        continue;
      }
      if (!current) {
        insertEncodedRow(db, effect.tableName, target);
        continue;
      }
      updateEncodedRow(db, effect.tableName, effect.pk, target);
    }
  } finally {
    if (droppedTriggers) {
      restoreDroppedTriggers(db, droppedTriggers);
    }
  }
}

function applySystemRowEffectReconciled(
  db: Database,
  table: string,
  pk: Record<string, string>,
  target: EncodedRow | null,
): void {
  const exists = tableExists(db, table);
  if (!target) {
    if (!exists) {
      return;
    }
    deleteByPk(db, table, pk);
    return;
  }
  if (!exists) {
    throw new CodedError("REVERT_FAILED", `System table does not exist for reconciled effect: ${table}`);
  }
  const current = readRow(db, table, pk);
  if (!current) {
    insertEncodedRow(db, table, target);
    return;
  }
  updateEncodedRow(db, table, pk, target);
}

function dropTriggersForTables(db: Database, effects: RowEffect[]) {
  const touched = Array.from(new Set(effects.map((effect) => effect.tableName))).sort((a, b) => a.localeCompare(b));
  const dropped: Array<{ name: string; sql: string }> = [];
  for (const table of touched) {
    const rows = db.$client
      .query<{ name: string; sql: string }, [string]>(
        "SELECT name, sql FROM sqlite_master WHERE type='trigger' AND tbl_name=? AND sql IS NOT NULL ORDER BY name ASC",
      )
      .all(table);
    for (const row of rows) {
      db.$client.run(`DROP TRIGGER IF EXISTS ${quoteIdentifier(row.name, { unsafe: true })}`);
      dropped.push(row);
    }
  }
  return dropped;
}

function restoreDroppedTriggers(db: Database, dropped: Array<{ name: string; sql: string }>): void {
  for (const trigger of dropped) {
    db.$client.run(trigger.sql);
  }
}

function applySingleSchemaEffect(db: Database, effect: SchemaEffect, direction: "forward" | "inverse"): void {
  const snapshot = direction === "forward" ? effect.afterTable : effect.beforeTable;
  const current = direction === "forward" ? effect.beforeTable : effect.afterTable;
  if (!snapshot) {
    if ((current?.kind ?? "table") === "view") {
      executeOperation(db, { type: "drop_view", name: effect.tableName });
      return;
    }
    executeOperation(db, { type: "drop_table", table: effect.tableName });
    return;
  }
  if (snapshot.kind === "view") {
    db.$client.run(snapshot.ddlSql);
    return;
  }
  const restore: Operation = {
    type: "restore_table",
    table: effect.tableName,
    ddlSql: snapshot.ddlSql,
    rows: snapshot.rows,
    secondaryObjects: snapshot.secondaryObjects,
  };
  executeOperation(db, restore);
}

function orderSchemaEffectsForReplay(effects: SchemaEffect[], direction: "forward" | "inverse"): SchemaEffect[] {
  if (effects.length <= 1) {
    return effects;
  }

  const byTable = new Map<string, SchemaEffect>();
  for (const effect of effects) {
    byTable.set(effect.tableName, effect);
  }

  const restoreRefs = new Map<string, string[]>();
  const restoreTables: string[] = [];
  const dropRefs = new Map<string, string[]>();
  const dropTables: string[] = [];

  for (const effect of effects) {
    const target = direction === "forward" ? effect.afterTable : effect.beforeTable;
    if (target) {
      restoreTables.push(effect.tableName);
      restoreRefs.set(effect.tableName, target.references);
      continue;
    }
    const current = direction === "forward" ? effect.beforeTable : effect.afterTable;
    dropTables.push(effect.tableName);
    dropRefs.set(effect.tableName, current?.references ?? []);
  }

  const restoreOrdered = dependencyOrder(restoreTables, restoreRefs, "parent-first");
  const dropOrdered = dependencyOrder(dropTables, dropRefs, "child-first");
  const orderedTables = [...restoreOrdered, ...dropOrdered];
  return orderedTables.map((table) => byTable.get(table)).filter((effect): effect is SchemaEffect => effect !== undefined);
}

function canApplyUserRowEffectNow(db: Database, effect: RowEffect): boolean {
  if (isSystemTable(effect.tableName)) {
    return false;
  }
  if (!tableExists(db, effect.tableName)) {
    return false;
  }
  return missingReferencedTables(db, effect.tableName).length === 0;
}

export function applyEffects(
  db: Database,
  rowEffects: RowEffect[],
  schemaEffects: SchemaEffect[],
  direction: "forward" | "inverse",
  options: {
    disableTableTriggers: boolean;
  },
): void {
  const pendingRows = (direction === "forward" ? rowEffects : rowEffects.toReversed()).filter(
    (effect) => !isSystemTable(effect.tableName),
  );
  const orderedSchemas = orderSchemaEffectsForReplay(schemaEffects, direction);
  let schemaIndex = 0;

  while (pendingRows.length > 0 || schemaIndex < orderedSchemas.length) {
    while (pendingRows.length > 0 && canApplyUserRowEffectNow(db, pendingRows[0]!)) {
      applyRowEffects(db, [pendingRows.shift()!], direction, {
        disableTableTriggers: options.disableTableTriggers,
        includeUserEffects: true,
        includeSystemEffects: false,
      });
    }

    if (pendingRows.length === 0) {
      if (schemaIndex < orderedSchemas.length) {
        applySingleSchemaEffect(db, orderedSchemas[schemaIndex]!, direction);
        schemaIndex += 1;
        continue;
      }
      break;
    }

    if (schemaIndex < orderedSchemas.length) {
      applySingleSchemaEffect(db, orderedSchemas[schemaIndex]!, direction);
      schemaIndex += 1;
      continue;
    }

    const blocked = pendingRows[0]!;
    if (!tableExists(db, blocked.tableName)) {
      throw new CodedError(
        "REVERT_FAILED",
        `Observed row effect blocked because target table does not exist: ${blocked.tableName}`,
      );
    }
    const missingRefs = missingReferencedTables(db, blocked.tableName);
    if (missingRefs.length > 0) {
      throw new CodedError(
        "REVERT_FAILED",
        `Observed row effect blocked by missing referenced table(s): ${blocked.tableName} -> ${missingRefs.join(", ")}`,
      );
    }
    applyRowEffects(db, [blocked], direction, {
      disableTableTriggers: options.disableTableTriggers,
      includeUserEffects: true,
      includeSystemEffects: false,
    });
    pendingRows.shift();
  }
}

export function assertForeignKeys(db: Database, errorCode: ErrorCode, context: string): void {
  const rows = db.$client.query<{
    table: string;
    rowid: number;
    parent: string;
    fkid: number;
  }, []>("PRAGMA foreign_key_check").all();
  if (rows.length === 0) {
    return;
  }

  const first = rows[0]!;
  throw new CodedError(
    errorCode,
    `${context}: foreign_key_check failed at ${first.table} rowid=${first.rowid} parent=${first.parent} fk=${first.fkid}`,
  );
}
