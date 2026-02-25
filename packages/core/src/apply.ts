import { createCommit, type Commit } from "./commit";
import { runInSavepoint, type Database } from "./db";
import { captureState, diffState } from "./effect";
import { CodedError } from "./error";
import { schemaHash } from "./inspect";
import { executeOperation, type Operation, type Plan } from "./operation";
import { maybeCreateSnapshot } from "./snapshot";

export async function apply(db: Database, plan: Plan): Promise<Commit> {
  const commit = db.transaction(() => {
    const beforeSchemaHash = schemaHash(db);
    const beforeState = captureState(db);
    for (const operation of plan.operations) {
      executeOperation(db, operation);
    }
    return createCommit(db, {
      operations: plan.operations,
      kind: "apply",
      message: plan.message,
      revertTargetId: null,
      beforeSchemaHash,
      beforeState,
    });
  }, { behavior: "immediate" });

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

function dryRunWithDb(db: Database, operations: Operation[]) {
  try {
    const before = captureState(db);
    const predicted = runInSavepoint(
      db,
      "lore_plan_check",
      () => {
        for (const op of operations) {
          executeOperation(db, op);
        }
        const after = captureState(db);
        const diff = diffState(before, after);
        return {
          rowEffects: diff.rowEffects.length,
          schemaEffects: diff.schemaEffects.length,
          tables: Array.from(
            new Set([
            ...diff.rowEffects.map((e) => e.tableName),
            ...diff.schemaEffects.map((e) => e.tableName),
            ]),
          ).sort((a, b) => a.localeCompare(b)),
        };
      },
      { rollbackOnSuccess: true },
    );
    return { errors: [], predicted };
  } catch (error) {
    const issue = CodedError.is(error)
      ? { code: error.code, message: error.message }
      : error instanceof Error
      ? { code: "PLAN_CHECK_FAILED", message: error.message }
      : { code: "PLAN_CHECK_FAILED", message: String(error) };
    return {
      errors: [issue],
      predicted: { rowEffects: 0, schemaEffects: 0, tables: [] },
    };
  }
}

export function check(db: Database, plan: Plan) {
  const checkedAt = new Date().toISOString();
  let schemaOperations = 0;
  let destructiveOperations = 0;
  const touchedTables = new Set<string>();
  const warnings: Array<{
    code: string;
    message: string;
    operationIndex?: number | undefined;
    operationType?: Operation["type"] | undefined;
    table?: string | undefined;
  }> = [];
  for (let i = 0; i < plan.operations.length; i += 1) {
    const operation = plan.operations[i]!;
    touchedTables.add(operation.table);
    if (SCHEMA_OPERATION_TYPES.has(operation.type)) {
      schemaOperations += 1;
    }
    if (DESTRUCTIVE_OPERATION_TYPES.has(operation.type)) {
      destructiveOperations += 1;
      warnings.push({
        code: "DESTRUCTIVE_OPERATION",
        message: `Operation ${i} (${operation.type}) changes or removes existing data/schema.`,
        operationIndex: i,
        operationType: operation.type,
        table: operation.table,
      });
    }
  }
  const { errors, predicted } = dryRunWithDb(db, plan.operations);
  const summary = {
    operations: plan.operations.length,
    schemaOperations,
    dataOperations: plan.operations.length - schemaOperations,
    destructiveOperations,
    touchedTables: Array.from(touchedTables).sort((a, b) => a.localeCompare(b)),
    predicted,
  };
  let risk: "low" | "medium" | "high" = "low";
  if (errors.length > 0 || destructiveOperations > 0) {
    risk = "high";
  } else if (schemaOperations > 0 || predicted.schemaEffects > 0) {
    risk = "medium";
  }

  return {
    ok: errors.length === 0,
    risk,
    errors,
    warnings,
    summary,
    checkedAt,
  };
}
