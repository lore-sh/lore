import { readPlanInput } from "./commit";
import { runInSavepoint, withInitializedDatabase } from "./db";
import { isTossError } from "./errors";
import { executeOperation } from "./executors/apply";
import { captureObservedState, diffObservedState } from "./observed";
import type { Operation } from "./types";
import { parseAndValidateOperationPlan } from "./validators/operation";

const SCHEMA_OPERATION_TYPES = new Set<Operation["type"]>([
  "create_table",
  "add_column",
  "drop_table",
  "drop_column",
  "alter_column_type",
]);

const DESTRUCTIVE_OPERATION_TYPES = new Set<Operation["type"]>([
  "drop_table",
  "drop_column",
  "alter_column_type",
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
  if (isTossError(error)) {
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

function dryRun(operations: Operation[]): DryRunResult {
  try {
    const predicted = withInitializedDatabase(({ db }) => {
      const before = captureObservedState(db);
      return runInSavepoint(
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
    });
    return { errors: [], predicted };
  } catch (error) {
    return {
      errors: [issueFromError(error)],
      predicted: { rowEffects: 0, schemaEffects: 0, tables: [] },
    };
  }
}

export async function planCheck(planRef: string): Promise<PlanCheckResult> {
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
  const { errors, predicted } = dryRun(plan.operations);

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
