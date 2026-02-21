import type { Database } from "bun:sqlite";
import { canonicalJson } from "./checksum";
import { getRows, tableExists } from "./db";
import { TossError } from "../errors";
import { executeOperation } from "./execute";
import {
  dependencyOrder,
  fetchObservedRowByPk,
  isSystemSideEffectTable,
  rowHash,
  toPkWhereClause,
  type RowEffect,
  type SchemaEffect,
} from "./diff";
import { quoteIdentifier } from "./sql";
import type { EncodedRow, RestoreTableOperation } from "../types";

function insertEncodedRow(db: Database, table: string, row: EncodedRow): void {
  const columns = Object.keys(row).sort((a, b) => a.localeCompare(b));
  if (columns.length === 0) {
    throw new TossError("REVERT_FAILED", `Cannot insert empty encoded row for ${table}`);
  }
  const colSql = columns.map((column) => quoteIdentifier(column, { unsafe: true })).join(", ");
  const valuesSql = columns
    .map((column) => {
      const cell = row[column];
      if (!cell) {
        throw new TossError("REVERT_FAILED", `Missing encoded cell for ${table}.${column}`);
      }
      return cell.sqlLiteral;
    })
    .join(", ");
  db.run(`INSERT INTO ${quoteIdentifier(table, { unsafe: true })} (${colSql}) VALUES (${valuesSql})`);
}

function updateEncodedRow(db: Database, table: string, pk: Record<string, string>, row: EncodedRow): void {
  const columns = Object.keys(row).sort((a, b) => a.localeCompare(b));
  if (columns.length === 0) {
    throw new TossError("REVERT_FAILED", `Cannot update empty encoded row for ${table}`);
  }
  const setSql = columns
    .map((column) => {
      const cell = row[column];
      if (!cell) {
        throw new TossError("REVERT_FAILED", `Missing encoded cell for ${table}.${column}`);
      }
      return `${quoteIdentifier(column, { unsafe: true })} = ${cell.sqlLiteral}`;
    })
    .join(", ");
  db.run(`UPDATE ${quoteIdentifier(table, { unsafe: true })} SET ${setSql} WHERE ${toPkWhereClause(pk)}`);
}

function deleteByPk(db: Database, table: string, pk: Record<string, string>): void {
  db.run(`DELETE FROM ${quoteIdentifier(table, { unsafe: true })} WHERE ${toPkWhereClause(pk)}`);
}

function referencedTables(db: Database, table: string): string[] {
  if (!tableExists(db, table)) {
    return [];
  }
  const rows = getRows<{ table: string }>(db, `PRAGMA foreign_key_list(${quoteIdentifier(table, { unsafe: true })})`);
  return Array.from(new Set(rows.map((row) => row.table))).sort((a, b) => a.localeCompare(b));
}

function missingReferencedTables(db: Database, table: string): string[] {
  return referencedTables(db, table).filter((refTable) => !tableExists(db, refTable));
}

function effectRowMode(
  effect: RowEffect,
  direction: "forward" | "inverse",
): { expectedCurrent: EncodedRow | null; target: EncodedRow | null; opLabel: string } {
  if (direction === "forward") {
    if (effect.opKind === "insert") {
      return { expectedCurrent: null, target: effect.afterRow, opLabel: "insert" };
    }
    if (effect.opKind === "update") {
      return { expectedCurrent: effect.beforeRow, target: effect.afterRow, opLabel: "update" };
    }
    return { expectedCurrent: effect.beforeRow, target: null, opLabel: "delete" };
  }

  if (effect.opKind === "insert") {
    return { expectedCurrent: effect.afterRow, target: null, opLabel: "inverse-delete" };
  }
  if (effect.opKind === "update") {
    return { expectedCurrent: effect.afterRow, target: effect.beforeRow, opLabel: "inverse-update" };
  }
  return { expectedCurrent: null, target: effect.beforeRow, opLabel: "inverse-insert" };
}

export function applyRowEffectsWithOptions(
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
    isSystemSideEffectTable(effect.tableName) ? includeSystemEffects : includeUserEffects,
  );
  const droppedTriggers = options.disableTableTriggers ? dropTriggersForTables(db, filtered) : null;
  const ordered = direction === "forward" ? filtered : filtered.toReversed();
  for (const effect of ordered) {
    const { expectedCurrent, target, opLabel } = effectRowMode(effect, direction);
    const isSystem = isSystemSideEffectTable(effect.tableName);
    if (isSystem && systemPolicy === "reconcile") {
      applySystemRowEffectReconciled(db, effect.tableName, effect.pk, target);
      continue;
    }
    const current = fetchObservedRowByPk(db, effect.tableName, effect.pk);
    const currentHash = rowHash(current);
    const expectedHash = rowHash(expectedCurrent);
    if (currentHash !== expectedHash) {
      throw new TossError(
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
  if (droppedTriggers) {
    restoreDroppedTriggers(db, droppedTriggers);
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
    throw new TossError("REVERT_FAILED", `System table does not exist for reconciled effect: ${table}`);
  }
  const current = fetchObservedRowByPk(db, table, pk);
  if (!current) {
    insertEncodedRow(db, table, target);
    return;
  }
  updateEncodedRow(db, table, pk, target);
}

interface DroppedTrigger {
  name: string;
  sql: string;
}

function dropTriggersForTables(db: Database, effects: RowEffect[]): DroppedTrigger[] {
  const touched = Array.from(new Set(effects.map((effect) => effect.tableName))).sort((a, b) => a.localeCompare(b));
  const dropped: DroppedTrigger[] = [];
  for (const table of touched) {
    const rows = getRows<DroppedTrigger>(
      db,
      "SELECT name, sql FROM sqlite_master WHERE type='trigger' AND tbl_name=? AND sql IS NOT NULL ORDER BY name ASC",
      table,
    );
    for (const row of rows) {
      db.run(`DROP TRIGGER IF EXISTS ${quoteIdentifier(row.name, { unsafe: true })}`);
      dropped.push(row);
    }
  }
  return dropped;
}

function restoreDroppedTriggers(db: Database, dropped: DroppedTrigger[]): void {
  for (const trigger of dropped) {
    db.run(trigger.sql);
  }
}

function applySingleSchemaEffect(db: Database, effect: SchemaEffect, direction: "forward" | "inverse"): void {
  const snapshot = direction === "forward" ? effect.afterTable : effect.beforeTable;
  if (!snapshot) {
    executeOperation(db, { type: "drop_table", table: effect.tableName });
    return;
  }
  const restore: RestoreTableOperation = {
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
  if (isSystemSideEffectTable(effect.tableName)) {
    return false;
  }
  if (!tableExists(db, effect.tableName)) {
    return false;
  }
  return missingReferencedTables(db, effect.tableName).length === 0;
}

export function applyUserRowAndSchemaEffects(
  db: Database,
  rowEffects: RowEffect[],
  schemaEffects: SchemaEffect[],
  direction: "forward" | "inverse",
  options: {
    disableTableTriggers: boolean;
  },
): void {
  const pendingRows = (direction === "forward" ? rowEffects : rowEffects.toReversed()).filter(
    (effect) => !isSystemSideEffectTable(effect.tableName),
  );
  const orderedSchemas = orderSchemaEffectsForReplay(schemaEffects, direction);
  let schemaIndex = 0;

  while (pendingRows.length > 0 || schemaIndex < orderedSchemas.length) {
    while (pendingRows.length > 0 && canApplyUserRowEffectNow(db, pendingRows[0]!)) {
      applyRowEffectsWithOptions(db, [pendingRows.shift()!], direction, {
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
      throw new TossError(
        "REVERT_FAILED",
        `Observed row effect blocked because target table does not exist: ${blocked.tableName}`,
      );
    }
    const missingRefs = missingReferencedTables(db, blocked.tableName);
    if (missingRefs.length > 0) {
      throw new TossError(
        "REVERT_FAILED",
        `Observed row effect blocked by missing referenced table(s): ${blocked.tableName} -> ${missingRefs.join(", ")}`,
      );
    }
    applyRowEffectsWithOptions(db, [blocked], direction, {
      disableTableTriggers: options.disableTableTriggers,
      includeUserEffects: true,
      includeSystemEffects: false,
    });
    pendingRows.shift();
  }
}

export function applySchemaEffects(db: Database, effects: SchemaEffect[], direction: "forward" | "inverse"): void {
  const ordered = orderSchemaEffectsForReplay(effects, direction);
  for (const effect of ordered) {
    applySingleSchemaEffect(db, effect, direction);
  }
}

export function assertNoForeignKeyViolations(db: Database, errorCode: string, context: string): void {
  const rows = getRows<{
    table: string;
    rowid: number;
    parent: string;
    fkid: number;
  }>(db, "PRAGMA foreign_key_check");
  if (rows.length === 0) {
    return;
  }

  const first = rows[0]!;
  throw new TossError(
    errorCode,
    `${context}: foreign_key_check failed at ${first.table} rowid=${first.rowid} parent=${first.parent} fk=${first.fkid}`,
  );
}
