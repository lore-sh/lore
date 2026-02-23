import type { Database } from "bun:sqlite";
import type { ColumnDefinition, EncodedRow, JsonPrimitive, TableSecondaryObject } from "./engine/primitives";
import { runInSavepoint, runInTransaction } from "./engine/db";
import { CodedError } from "./error";
import { executeOperation } from "./engine/execute";
import { appendCommitObserved } from "./engine/log";
import { captureObservedState, diffObservedState } from "./engine/diff";
import { schemaHash } from "./engine/inspect";
import type { Commit } from "./history";

export interface CreateTableOperation {
  type: "create_table";
  table: string;
  columns: ColumnDefinition[];
}

export interface AddColumnOperation {
  type: "add_column";
  table: string;
  column: ColumnDefinition;
}

export interface InsertOperation {
  type: "insert";
  table: string;
  values: Record<string, JsonPrimitive>;
}

export interface DropTableOperation {
  type: "drop_table";
  table: string;
}

export interface DropColumnOperation {
  type: "drop_column";
  table: string;
  column: string;
}

export interface AlterColumnTypeOperation {
  type: "alter_column_type";
  table: string;
  column: string;
  newType: string;
}

export interface AddCheckOperation {
  type: "add_check";
  table: string;
  expression: string;
}

export interface DropCheckOperation {
  type: "drop_check";
  table: string;
  expression: string;
}

export interface RestoreTableOperation {
  type: "restore_table";
  table: string;
  ddlSql: string;
  rows: EncodedRow[] | null;
  secondaryObjects?: TableSecondaryObject[] | undefined;
}

export interface UpdateOperation {
  type: "update";
  table: string;
  values: Record<string, JsonPrimitive>;
  where: Record<string, JsonPrimitive>;
}

export interface DeleteOperation {
  type: "delete";
  table: string;
  where: Record<string, JsonPrimitive>;
}

export type Operation =
  | CreateTableOperation
  | AddColumnOperation
  | InsertOperation
  | DropTableOperation
  | DropColumnOperation
  | AlterColumnTypeOperation
  | AddCheckOperation
  | DropCheckOperation
  | RestoreTableOperation
  | UpdateOperation
  | DeleteOperation;

export interface OperationPlan {
  message: string;
  operations: Operation[];
}

export interface CheckIssue {
  code: string;
  message: string;
  operationIndex?: number | undefined;
  operationType?: Operation["type"] | undefined;
  table?: string | undefined;
}

export interface CheckSummary {
  operations: number;
  schemaOperations: number;
  dataOperations: number;
  destructiveOperations: number;
  touchedTables: string[];
  predicted: {
    rowEffects: number;
    schemaEffects: number;
    tables: string[];
  };
}

export interface CheckResult {
  ok: boolean;
  risk: "low" | "medium" | "high";
  errors: CheckIssue[];
  warnings: CheckIssue[];
  summary: CheckSummary;
  checkedAt: string;
}

export async function apply(db: Database, plan: OperationPlan): Promise<Commit> {
  const commit = runInTransaction(db, () => {
    const beforeSchemaHash = schemaHash(db);
    const beforeObservedState = captureObservedState(db);
    for (const operation of plan.operations) {
      executeOperation(db, operation);
    }
    return appendCommitObserved(db, {
      operations: plan.operations,
      kind: "apply",
      message: plan.message,
      revertTargetId: null,
      beforeSchemaHash,
      beforeObservedState,
    });
  });

  const { maybeCreateSnapshot } = await import("./snapshot");
  await maybeCreateSnapshot(db, commit);
  return commit;
}

const SCHEMA_OPERATION_TYPES = new Set<Operation["type"]>([
  "create_table",
  "add_column",
  "drop_table",
  "drop_column",
  "alter_column_type",
  "add_check",
  "drop_check",
]);

const DESTRUCTIVE_OPERATION_TYPES = new Set<Operation["type"]>([
  "drop_table",
  "drop_column",
  "alter_column_type",
  "drop_check",
  "update",
  "delete",
]);

function issueFromError(error: unknown): CheckIssue {
  if (CodedError.is(error)) {
    return { code: error.code, message: error.message };
  }
  if (error instanceof Error) {
    return { code: "PLAN_CHECK_FAILED", message: error.message };
  }
  return { code: "PLAN_CHECK_FAILED", message: String(error) };
}

function classifyRisk(
  errors: CheckIssue[],
  summary: CheckSummary,
): CheckResult["risk"] {
  if (errors.length > 0 || summary.destructiveOperations > 0) {
    return "high";
  }
  if (summary.schemaOperations > 0 || summary.predicted.schemaEffects > 0) {
    return "medium";
  }
  return "low";
}

function uniqueSorted(values: string[]): string[] {
  return Array.from(new Set(values)).sort((a, b) => a.localeCompare(b));
}

function summarizeOperations(operations: Operation[]): Omit<CheckSummary, "predicted"> {
  const touchedTables = uniqueSorted(operations.map((op) => op.table));
  const schemaOps = operations.filter((op) => SCHEMA_OPERATION_TYPES.has(op.type)).length;
  const destructiveOps = operations.filter((op) => DESTRUCTIVE_OPERATION_TYPES.has(op.type)).length;
  return {
    operations: operations.length,
    schemaOperations: schemaOps,
    dataOperations: operations.length - schemaOps,
    destructiveOperations: destructiveOps,
    touchedTables,
  };
}

function destructiveWarnings(operations: Operation[]): CheckIssue[] {
  return operations.flatMap((op, i) =>
    DESTRUCTIVE_OPERATION_TYPES.has(op.type)
      ? [{
          code: "DESTRUCTIVE_OPERATION",
          message: `Operation ${i} (${op.type}) changes or removes existing data/schema.`,
          operationIndex: i,
          operationType: op.type,
          table: op.table,
        }]
      : [],
  );
}

interface DryRunResult {
  errors: CheckIssue[];
  predicted: CheckSummary["predicted"];
}

function dryRunWithDb(db: Database, operations: Operation[]): DryRunResult {
  try {
    const before = captureObservedState(db);
    const predicted = runInSavepoint(
      db,
      "toss_plan_check",
      () => {
        for (const op of operations) {
          executeOperation(db, op);
        }
        const after = captureObservedState(db);
        const diff = diffObservedState(before, after);
        return {
          rowEffects: diff.rowEffects.length,
          schemaEffects: diff.schemaEffects.length,
          tables: uniqueSorted([
            ...diff.rowEffects.map((e) => e.tableName),
            ...diff.schemaEffects.map((e) => e.tableName),
          ]),
        };
      },
      { rollbackOnSuccess: true },
    );
    return { errors: [], predicted };
  } catch (error) {
    return {
      errors: [issueFromError(error)],
      predicted: { rowEffects: 0, schemaEffects: 0, tables: [] },
    };
  }
}

export function check(db: Database, plan: OperationPlan): CheckResult {
  const checkedAt = new Date().toISOString();

  const warnings = destructiveWarnings(plan.operations);
  const summaryBase = summarizeOperations(plan.operations);
  const { errors, predicted } = dryRunWithDb(db, plan.operations);

  const summary: CheckSummary = { ...summaryBase, predicted };

  return {
    ok: errors.length === 0,
    risk: classifyRisk(errors, summary),
    errors,
    warnings,
    summary,
    checkedAt,
  };
}
