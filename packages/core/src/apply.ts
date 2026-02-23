import type { Database } from "bun:sqlite";
import { runInSavepoint, runInTransaction } from "./engine/db";
import { CodedError } from "./error";
import { executeOperation } from "./engine/execute";
import { appendCommitFromObservedChange } from "./engine/log";
import { captureObservedState, diffObservedState } from "./engine/diff";
import { schemaHash } from "./engine/inspect";
import type { CommitEntry, Operation } from "./types";
import { parseAndValidateOperationPlan } from "./engine/validate";

export function readPlanInput(planRef: string): Promise<string> {
  if (planRef === "-") {
    return Bun.stdin.text();
  }
  return Bun.file(planRef).text();
}

export async function applyPlan(db: Database, planRef: string): Promise<CommitEntry> {
  const payload = await readPlanInput(planRef);
  const plan = parseAndValidateOperationPlan(payload);

  const commit = runInTransaction(db, () => {
    const beforeSchemaHash = schemaHash(db);
    const beforeObservedState = captureObservedState(db);
    for (const operation of plan.operations) {
      executeOperation(db, operation);
    }
    return appendCommitFromObservedChange(db, {
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

export interface PlanCheckIssue {
  code: string;
  message: string;
  operationIndex?: number | undefined;
  operationType?: Operation["type"] | undefined;
  table?: string | undefined;
}

export interface PlanCheckSummary {
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

export interface PlanCheckResult {
  ok: boolean;
  risk: "low" | "medium" | "high";
  errors: PlanCheckIssue[];
  warnings: PlanCheckIssue[];
  summary: PlanCheckSummary;
  checkedAt: string;
}

const EMPTY_SUMMARY: PlanCheckSummary = {
  operations: 0,
  schemaOperations: 0,
  dataOperations: 0,
  destructiveOperations: 0,
  touchedTables: [],
  predicted: { rowEffects: 0, schemaEffects: 0, tables: [] },
};

function issueFromError(error: unknown): PlanCheckIssue {
  if (CodedError.is(error)) {
    return { code: error.code, message: error.message };
  }
  if (error instanceof Error) {
    return { code: "PLAN_CHECK_FAILED", message: error.message };
  }
  return { code: "PLAN_CHECK_FAILED", message: String(error) };
}

function classifyRisk(
  errors: PlanCheckIssue[],
  summary: PlanCheckSummary,
): PlanCheckResult["risk"] {
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

function summarizeOperations(operations: Operation[]): Omit<PlanCheckSummary, "predicted"> {
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

function destructiveWarnings(operations: Operation[]): PlanCheckIssue[] {
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

function failedResult(checkedAt: string, error: unknown): PlanCheckResult {
  return {
    ok: false,
    risk: "high",
    errors: [issueFromError(error)],
    warnings: [],
    summary: EMPTY_SUMMARY,
    checkedAt,
  };
}

interface DryRunResult {
  errors: PlanCheckIssue[];
  predicted: PlanCheckSummary["predicted"];
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

export async function planCheck(db: Database, planRef: string): Promise<PlanCheckResult> {
  const checkedAt = new Date().toISOString();
  let payload: string;
  try {
    payload = await readPlanInput(planRef);
  } catch (error) {
    return failedResult(checkedAt, error);
  }

  let plan: ReturnType<typeof parseAndValidateOperationPlan>;
  try {
    plan = parseAndValidateOperationPlan(payload);
  } catch (error) {
    return failedResult(checkedAt, error);
  }

  const warnings = destructiveWarnings(plan.operations);
  const summaryBase = summarizeOperations(plan.operations);
  const { errors, predicted } = dryRunWithDb(db, plan.operations);

  const summary: PlanCheckSummary = { ...summaryBase, predicted };

  return {
    ok: errors.length === 0,
    risk: classifyRisk(errors, summary),
    errors,
    warnings,
    summary,
    checkedAt,
  };
}
