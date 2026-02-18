import { z } from "zod";
import { TossError } from "../errors";
import type { OperationPlan } from "../types";

const IDENTIFIER_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/;
const COLUMN_TYPE_PATTERN = /^[A-Za-z][A-Za-z0-9_]*(\s*\(\s*\d+\s*(,\s*\d+\s*)?\))?$/;

const scalarValueSchema = z.union([z.string(), z.number(), z.boolean(), z.null()]);

const sourceSchema = z
  .object({
    planner: z.string().trim().min(1).optional(),
    skill: z.string().trim().min(1).optional(),
  })
  .strict();

const columnSchema = z
  .object({
    name: z.string().trim().min(1),
    type: z.string().trim().min(1),
    notNull: z.boolean().optional(),
    primaryKey: z.boolean().optional(),
    unique: z.boolean().optional(),
    default: scalarValueSchema.optional(),
  })
  .strict();

const createTableSchema = z
  .object({
    type: z.literal("create_table"),
    table: z.string().trim().min(1),
    columns: z.array(columnSchema).min(1),
  })
  .strict();

const addColumnSchema = z
  .object({
    type: z.literal("add_column"),
    table: z.string().trim().min(1),
    column: columnSchema,
  })
  .strict();

const insertSchema = z
  .object({
    type: z.literal("insert"),
    table: z.string().trim().min(1),
    values: z.record(z.string(), scalarValueSchema).refine((value) => Object.keys(value).length > 0, {
      message: "insert values must not be empty",
    }),
  })
  .strict();

export const operationPlanSchema = z
  .object({
    message: z.string().trim().min(1),
    operations: z.array(z.discriminatedUnion("type", [createTableSchema, addColumnSchema, insertSchema])).min(1),
    source: sourceSchema.optional(),
  })
  .strict();

function assertIdentifier(value: string, label: string): void {
  if (!IDENTIFIER_PATTERN.test(value)) {
    throw new TossError("INVALID_OPERATION", `${label} must match ${IDENTIFIER_PATTERN.source}: ${value}`);
  }
  if (value.startsWith("_toss_") || value.startsWith("sqlite_")) {
    throw new TossError("INVALID_OPERATION", `${label} cannot use reserved prefix: ${value}`);
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
    throw new TossError("INVALID_OPERATION", `${operationLabel}.column type is invalid: ${column.type}`);
  }
}

function semanticValidation(plan: OperationPlan): void {
  for (const operation of plan.operations) {
    assertIdentifier(operation.table, `${operation.type}.table`);

    if (operation.type === "create_table") {
      const seen = new Set<string>();
      let primaryKeyCount = 0;
      for (const column of operation.columns) {
        validateColumn(column, operation.type);
        if (seen.has(column.name)) {
          throw new TossError("INVALID_OPERATION", `duplicate column name: ${column.name}`);
        }
        seen.add(column.name);
        if (column.primaryKey) {
          primaryKeyCount += 1;
        }
      }
      if (primaryKeyCount > 1) {
        throw new TossError("INVALID_OPERATION", "create_table cannot contain multiple primary keys");
      }
      continue;
    }

    if (operation.type === "add_column") {
      validateColumn(operation.column, operation.type);
      if (operation.column.primaryKey || operation.column.unique) {
        throw new TossError(
          "INVALID_OPERATION",
          "add_column does not allow primaryKey/unique in MVP because SQLite ALTER restrictions vary",
        );
      }
      if (operation.column.notNull && !Object.hasOwn(operation.column, "default")) {
        throw new TossError("INVALID_OPERATION", "add_column with NOT NULL requires default");
      }
      continue;
    }

    if (operation.type === "insert") {
      const keys = Object.keys(operation.values);
      if (keys.length === 0) {
        throw new TossError("INVALID_OPERATION", "insert values must not be empty");
      }
      for (const key of keys) {
        assertIdentifier(key, "insert.values key");
      }
    }
  }
}

export function parseAndValidateOperationPlan(input: string): OperationPlan {
  let parsed: unknown;
  try {
    parsed = JSON.parse(input);
  } catch (error) {
    throw new TossError("INVALID_JSON", `Plan must be valid JSON: ${(error as Error).message}`);
  }

  const result = operationPlanSchema.safeParse(parsed);
  if (!result.success) {
    throw new TossError("INVALID_PLAN", result.error.issues.map((issue) => issue.message).join("; "));
  }

  semanticValidation(result.data);
  return result.data;
}
