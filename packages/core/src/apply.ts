import { appendCommitObserved } from "./commit";
import { runInSavepoint, type Database } from "./db";
import { captureObservedState, diffObservedState } from "./effect";
import { CodedError } from "./error";
import { schemaHash } from "./inspect";
import { executeOperation, type Operation, type OperationPlan } from "./operation";
import type { Commit } from "./schema";

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
  const commit = db.transaction(() => {
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
  }, { behavior: "immediate" });

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
  let schemaOps = 0;
  let destructiveOps = 0;
  const tableSet = new Set<string>();
  for (const op of operations) {
    tableSet.add(op.table);
    if (SCHEMA_OPERATION_TYPES.has(op.type)) schemaOps += 1;
    if (DESTRUCTIVE_OPERATION_TYPES.has(op.type)) destructiveOps += 1;
  }
  return {
    operations: operations.length,
    schemaOperations: schemaOps,
    dataOperations: operations.length - schemaOps,
    destructiveOperations: destructiveOps,
    touchedTables: Array.from(tableSet).sort((a, b) => a.localeCompare(b)),
  };
}

function destructiveWarnings(operations: Operation[]): CheckIssue[] {
  const warnings: CheckIssue[] = [];
  for (let i = 0; i < operations.length; i++) {
    const op = operations[i]!;
    if (DESTRUCTIVE_OPERATION_TYPES.has(op.type)) {
      warnings.push({
        code: "DESTRUCTIVE_OPERATION",
        message: `Operation ${i} (${op.type}) changes or removes existing data/schema.`,
        operationIndex: i,
        operationType: op.type,
        table: op.table,
      });
    }
  }
  return warnings;
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
