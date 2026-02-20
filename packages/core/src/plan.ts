import { readPlanInput } from "./commit";
import { runInSavepoint, withInitializedDatabase } from "./db";
import { isTossError } from "./errors";
import { executeOperation } from "./executors/apply";
import { captureObservedState, diffObservedState } from "./observed";
import type { DatabaseOptions, Operation } from "./types";
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

function emptySummary(): PlanCheckSummary {
  return {
    operations: 0,
    schemaOperations: 0,
    dataOperations: 0,
    destructiveOperations: 0,
    touchedTables: [],
    predicted: {
      rowEffects: 0,
      schemaEffects: 0,
      tables: [],
    },
  };
}

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

function summarizeOperations(operations: Operation[]): Omit<PlanCheckSummary, "predicted"> {
  const touchedTables = Array.from(new Set(operations.map((operation) => operation.table))).sort((a, b) => a.localeCompare(b));
  const schemaOperations = operations.reduce(
    (count, operation) => count + (SCHEMA_OPERATION_TYPES.has(operation.type) ? 1 : 0),
    0,
  );
  const destructiveOperations = operations.reduce(
    (count, operation) => count + (DESTRUCTIVE_OPERATION_TYPES.has(operation.type) ? 1 : 0),
    0,
  );
  return {
    operations: operations.length,
    schemaOperations,
    dataOperations: operations.length - schemaOperations,
    destructiveOperations,
    touchedTables,
  };
}

function destructiveWarnings(operations: Operation[]): PlanCheckIssue[] {
  const warnings: PlanCheckIssue[] = [];
  for (let i = 0; i < operations.length; i += 1) {
    const operation = operations[i]!;
    if (!DESTRUCTIVE_OPERATION_TYPES.has(operation.type)) {
      continue;
    }
    warnings.push({
      code: "DESTRUCTIVE_OPERATION",
      message: `Operation ${i} (${operation.type}) changes or removes existing data/schema.`,
      operationIndex: i,
      operationType: operation.type,
      table: operation.table,
    });
  }
  return warnings;
}

function failedResult(checkedAt: string, error: unknown): PlanCheckResult {
  return {
    ok: false,
    risk: "high",
    errors: [issueFromError(error)],
    warnings: [],
    summary: emptySummary(),
    checkedAt,
  };
}

export async function planCheck(planRef: string, options: DatabaseOptions = {}): Promise<PlanCheckResult> {
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
  const errors: PlanCheckIssue[] = [];
  let predictedRowEffects = 0;
  let predictedSchemaEffects = 0;
  let predictedTables: string[] = [];

  try {
    withInitializedDatabase(options, ({ db }) => {
      const before = captureObservedState(db);
      runInSavepoint(
        db,
        "toss_plan_check",
        () => {
          for (const operation of plan.operations) {
            executeOperation(db, operation);
          }
          const after = captureObservedState(db);
          const diff = diffObservedState(before, after);
          predictedRowEffects = diff.rowEffects.length;
          predictedSchemaEffects = diff.schemaEffects.length;
          predictedTables = Array.from(
            new Set([
              ...diff.rowEffects.map((effect) => effect.tableName),
              ...diff.schemaEffects.map((effect) => effect.tableName),
            ]),
          ).sort((a, b) => a.localeCompare(b));
        },
        { rollbackOnSuccess: true },
      );
    });
  } catch (error) {
    errors.push(issueFromError(error));
  }

  const summary: PlanCheckSummary = {
    ...summaryBase,
    predicted: {
      rowEffects: predictedRowEffects,
      schemaEffects: predictedSchemaEffects,
      tables: predictedTables,
    },
  };

  return {
    ok: errors.length === 0,
    risk: classifyRisk(errors, summary),
    errors,
    warnings,
    summary,
    checkedAt,
  };
}
