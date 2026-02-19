import { readFile } from "node:fs/promises";
import type { Database } from "bun:sqlite";
import { sha256Hex } from "./checksum";
import {
  assertInitialized,
  closeDatabase,
  openDatabase,
  runInTransaction,
} from "./db";
import { TossError } from "./errors";
import { executeOperation } from "./executors/apply";
import {
  appendCommit,
  getHeadCommit,
  getNextCommitSeq,
  type RowEffect,
  type SchemaEffect,
} from "./log";
import {
  assertTableHasPrimaryKey,
  fetchAllRows,
  fetchRowByPk,
  fetchRowsByWhere,
  normalizeRowObject,
  pkFromRow,
  primaryKeyColumns,
  schemaHash,
  stateHash,
  tableDDL,
} from "./rows";
import { quoteIdentifier } from "./sql";
import type { CommitEntry, DatabaseOptions, JsonPrimitive, Operation } from "./types";
import { parseAndValidateOperationPlan } from "./validators/operation";

export function readPlanInput(planRef: string): Promise<string> {
  if (planRef === "-") {
    return Bun.stdin.text();
  }
  return readFile(planRef, "utf8");
}

export function buildRowEffectsForUpdateDelete(
  db: Database,
  table: string,
  opKind: "update" | "delete",
  beforeRows: Array<Record<string, unknown>>,
): RowEffect[] {
  const effects: RowEffect[] = [];
  for (const beforeRow of beforeRows) {
    const pk = pkFromRow(db, table, beforeRow);
    const afterRow = opKind === "delete" ? null : fetchRowByPk(db, table, pk);
    effects.push({
      tableName: table,
      pk,
      opKind,
      beforeRow: normalizeRowObject(beforeRow),
      afterRow: afterRow ? normalizeRowObject(afterRow) : null,
    });
  }
  return effects;
}

export function applySchemaOperationWithEffects(
  db: Database,
  operation: Operation & { table: string },
  opKind: SchemaEffect["opKind"],
  columnName: string | null,
): { rowEffects: RowEffect[]; schemaEffects: SchemaEffect[] } {
  const capturesBefore = opKind !== "create_table";
  if (capturesBefore && opKind !== "restore_table") {
    assertTableHasPrimaryKey(db, operation.table);
  }
  const ddlBeforeSql = capturesBefore ? tableDDL(db, operation.table) : null;
  if (capturesBefore && opKind !== "restore_table" && !ddlBeforeSql) {
    throw new TossError("APPLY_FAILED", `Unable to read current table DDL for ${operation.table} before ${opKind}`);
  }
  const tableRowsBefore = capturesBefore && ddlBeforeSql ? fetchAllRows(db, operation.table) : null;

  executeOperation(db, operation);

  const capturesAfter = opKind !== "drop_table";
  const ddlAfterSql = capturesAfter ? tableDDL(db, operation.table) : null;

  return {
    rowEffects: [],
    schemaEffects: [{ tableName: operation.table, columnName, opKind, ddlBeforeSql, ddlAfterSql, tableRowsBefore }],
  };
}

export function applyOperationWithEffects(db: Database, operation: Operation): { rowEffects: RowEffect[]; schemaEffects: SchemaEffect[] } {
  switch (operation.type) {
    case "insert": {
      assertTableHasPrimaryKey(db, operation.table);
      executeOperation(db, operation);
      const pkCols = primaryKeyColumns(db, operation.table);
      let insertedRow: Record<string, unknown> | null;
      if (pkCols.length > 0 && pkCols.every((column) => Object.hasOwn(operation.values, column))) {
        const pkWhere = Object.fromEntries(pkCols.map((column) => [column, operation.values[column] ?? null])) as Record<
          string,
          JsonPrimitive
        >;
        insertedRow = fetchRowByPk(db, operation.table, pkWhere);
      } else {
        insertedRow = db
          .query(`SELECT * FROM ${quoteIdentifier(operation.table)} WHERE rowid = last_insert_rowid()`)
          .get() as Record<string, unknown> | null;
      }

      if (!insertedRow) {
        throw new TossError("APPLY_FAILED", `Unable to capture inserted row for table ${operation.table}`);
      }

      return {
        rowEffects: [
          {
            tableName: operation.table,
            pk: pkFromRow(db, operation.table, insertedRow),
            opKind: "insert",
            beforeRow: null,
            afterRow: normalizeRowObject(insertedRow),
          },
        ],
        schemaEffects: [],
      };
    }

    case "update": {
      assertTableHasPrimaryKey(db, operation.table);
      const beforeRows = fetchRowsByWhere(db, operation.table, operation.where);
      executeOperation(db, operation);
      return { rowEffects: buildRowEffectsForUpdateDelete(db, operation.table, "update", beforeRows), schemaEffects: [] };
    }

    case "delete": {
      assertTableHasPrimaryKey(db, operation.table);
      const beforeRows = fetchRowsByWhere(db, operation.table, operation.where);
      executeOperation(db, operation);
      return { rowEffects: buildRowEffectsForUpdateDelete(db, operation.table, "delete", beforeRows), schemaEffects: [] };
    }

    case "create_table":
      return applySchemaOperationWithEffects(db, operation, "create_table", null);

    case "add_column":
      return applySchemaOperationWithEffects(db, operation, "add_column", operation.column.name);

    case "drop_table":
      return applySchemaOperationWithEffects(db, operation, "drop_table", null);

    case "drop_column":
      return applySchemaOperationWithEffects(db, operation, "drop_column", operation.column);

    case "alter_column_type":
      return applySchemaOperationWithEffects(db, operation, "alter_column_type", operation.column);

    case "restore_table":
      return applySchemaOperationWithEffects(db, operation, "restore_table", null);

    default:
      throw new TossError("UNSUPPORTED_OPERATION", `Unsupported operation type: ${(operation as Operation).type}`);
  }
}

export function applyOperationsWithEffects(
  db: Database,
  operations: Operation[],
): { rowEffects: RowEffect[]; schemaEffects: SchemaEffect[] } {
  const rowEffects: RowEffect[] = [];
  const schemaEffects: SchemaEffect[] = [];
  for (const operation of operations) {
    const captured = applyOperationWithEffects(db, operation);
    rowEffects.push(...captured.rowEffects);
    schemaEffects.push(...captured.schemaEffects);
  }
  return { rowEffects, schemaEffects };
}

export function buildCommitOperationsResult(db: Database, planOperations: Operation[], kind: "apply" | "revert", message: string, revertedTargetId: string | null): CommitEntry {
  const parent = getHeadCommit(db);
  const parentIds = parent ? [parent.commitId] : [];
  const seq = getNextCommitSeq(db);
  const createdAt = new Date().toISOString();
  const beforeSchemaHash = schemaHash(db);

  const captured = applyOperationsWithEffects(db, planOperations);
  const afterSchemaHash = schemaHash(db);
  const afterStateHash = stateHash(db);
  const planHash = sha256Hex(planOperations);

  return appendCommit(db, {
    seq,
    kind,
    message,
    createdAt,
    parentIds,
    schemaHashBefore: beforeSchemaHash,
    schemaHashAfter: afterSchemaHash,
    stateHashAfter: afterStateHash,
    planHash,
    inverseReady: true,
    revertedTargetId,
    operations: planOperations,
    rowEffects: captured.rowEffects,
    schemaEffects: captured.schemaEffects,
  });
}

export async function applyPlan(planRef: string, options: DatabaseOptions = {}): Promise<CommitEntry> {
  const payload = await readPlanInput(planRef);
  const plan = parseAndValidateOperationPlan(payload);

  const { db, dbPath } = openDatabase(options.dbPath);
  let commit: CommitEntry;
  try {
    assertInitialized(db, dbPath);
    commit = runInTransaction(db, () => buildCommitOperationsResult(db, plan.operations, "apply", plan.message, null));
  } finally {
    closeDatabase(db);
  }

  const { maybeCreateSnapshot } = await import("./snapshot");
  await maybeCreateSnapshot(dbPath, commit);
  return commit;
}
