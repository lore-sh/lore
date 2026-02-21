import { z } from "zod";
import { TossError } from "../errors";
import { COLUMN_TYPE_PATTERN, IDENTIFIER_PATTERN, createScanner } from "./sql";
import type { OperationPlan } from "../types";

const scalarValueSchema = z.union([z.string(), z.number(), z.boolean(), z.null()]);
const columnDefaultSchema = z.discriminatedUnion("kind", [
  z
    .object({
      kind: z.literal("literal"),
      value: scalarValueSchema,
    })
    .strict(),
  z
    .object({
      kind: z.literal("sql"),
      expr: z.enum(["CURRENT_TIMESTAMP", "CURRENT_DATE", "CURRENT_TIME"]),
    })
    .strict(),
]);

const columnSchema = z
  .object({
    name: z.string().trim().min(1),
    type: z.string().trim().min(1),
    notNull: z.boolean().optional(),
    primaryKey: z.boolean().optional(),
    unique: z.boolean().optional(),
    default: columnDefaultSchema.optional(),
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

const whereSchema = z.record(z.string(), scalarValueSchema).refine((value) => Object.keys(value).length > 0, {
  message: "where must not be empty",
});

const dropTableSchema = z
  .object({
    type: z.literal("drop_table"),
    table: z.string().trim().min(1),
  })
  .strict();

const dropColumnSchema = z
  .object({
    type: z.literal("drop_column"),
    table: z.string().trim().min(1),
    column: z.string().trim().min(1),
  })
  .strict();

const alterColumnTypeSchema = z
  .object({
    type: z.literal("alter_column_type"),
    table: z.string().trim().min(1),
    column: z.string().trim().min(1),
    newType: z.string().trim().min(1),
  })
  .strict();

const addCheckSchema = z
  .object({
    type: z.literal("add_check"),
    table: z.string().trim().min(1),
    expression: z.string().trim().min(1),
  })
  .strict();

const dropCheckSchema = z
  .object({
    type: z.literal("drop_check"),
    table: z.string().trim().min(1),
    expression: z.string().trim().min(1),
  })
  .strict();

const updateSchema = z
  .object({
    type: z.literal("update"),
    table: z.string().trim().min(1),
    values: z.record(z.string(), scalarValueSchema).refine((value) => Object.keys(value).length > 0, {
      message: "update values must not be empty",
    }),
    where: whereSchema,
  })
  .strict();

const deleteSchema = z
  .object({
    type: z.literal("delete"),
    table: z.string().trim().min(1),
    where: whereSchema,
  })
  .strict();

export const operationPlanSchema = z
  .object({
    message: z.string().trim().min(1),
    operations: z
      .array(
        z.discriminatedUnion("type", [
          createTableSchema,
          addColumnSchema,
          insertSchema,
          dropTableSchema,
          dropColumnSchema,
          alterColumnTypeSchema,
          addCheckSchema,
          dropCheckSchema,
          updateSchema,
          deleteSchema,
        ]),
      )
      .min(1),
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

function assertColumnType(value: string, label: string): void {
  if (!COLUMN_TYPE_PATTERN.test(value)) {
    throw new TossError("INVALID_OPERATION", `${label} is invalid: ${value}`);
  }
}

function scanSqlControlTokens(sql: string): { hasCommentToken: boolean; hasSemicolon: boolean } {
  const scanner = createScanner(sql);
  let hasCommentToken = false;
  let hasSemicolon = false;

  while (scanner.pos < sql.length) {
    const ch = sql[scanner.pos]!;
    const next = sql[scanner.pos + 1];

    // Detect comment tokens before skipInterior consumes them
    if (!scanner.insideLiteral) {
      if ((ch === "-" && next === "-") || (ch === "/" && next === "*") || (ch === "*" && next === "/")) {
        hasCommentToken = true;
      }
    }

    if (scanner.skipInterior()) {
      continue;
    }

    if (ch === ";") {
      hasSemicolon = true;
    }
    scanner.advance();
  }

  return { hasCommentToken, hasSemicolon };
}

function analyzeSqlExpressionShape(sql: string): {
  hasTopLevelComma: boolean;
  hasUnbalancedParen: boolean;
  hasUnterminatedLiteral: boolean;
} {
  const scanner = createScanner(sql);
  let depth = 0;
  let hasTopLevelComma = false;

  while (scanner.pos < sql.length) {
    if (scanner.skipInterior()) {
      continue;
    }
    const ch = sql[scanner.pos]!;
    if (ch === "(") {
      depth += 1;
    } else if (ch === ")") {
      depth -= 1;
      if (depth < 0) {
        return { hasTopLevelComma, hasUnbalancedParen: true, hasUnterminatedLiteral: false };
      }
    } else if (ch === "," && depth === 0) {
      hasTopLevelComma = true;
    }
    scanner.advance();
  }

  return {
    hasTopLevelComma,
    hasUnbalancedParen: depth !== 0,
    hasUnterminatedLiteral: scanner.insideLiteral,
  };
}

function assertCheckExpression(value: string, label: string): void {
  const expression = value.trim();
  if (expression.length === 0) {
    throw new TossError("INVALID_OPERATION", `${label} must not be empty`);
  }
  if (expression.includes("\0")) {
    throw new TossError("INVALID_OPERATION", `${label} must not contain NUL`);
  }
  const scan = scanSqlControlTokens(expression);
  if (scan.hasCommentToken) {
    throw new TossError("INVALID_OPERATION", `${label} must not contain SQL comments`);
  }
  if (scan.hasSemicolon) {
    throw new TossError("INVALID_OPERATION", `${label} must be a single SQL expression`);
  }
  const shape = analyzeSqlExpressionShape(expression);
  if (shape.hasTopLevelComma) {
    throw new TossError("INVALID_OPERATION", `${label} must not contain top-level commas`);
  }
  if (shape.hasUnbalancedParen || shape.hasUnterminatedLiteral) {
    throw new TossError("INVALID_OPERATION", `${label} must be a valid single SQL expression`);
  }
}

function assertPredicate(
  values: Record<string, string | number | boolean | null>,
  label: string,
): void {
  const keys = Object.keys(values);
  if (keys.length === 0) {
    throw new TossError("INVALID_OPERATION", `${label} must not be empty`);
  }
  for (const key of keys) {
    assertIdentifier(key, `${label} key`);
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
      if (primaryKeyCount === 0) {
        throw new TossError("INVALID_OPERATION", "create_table must define a PRIMARY KEY");
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
      assertPredicate(operation.values, "insert.values");
      continue;
    }

    if (operation.type === "drop_column") {
      assertIdentifier(operation.column, "drop_column.column");
      continue;
    }

    if (operation.type === "alter_column_type") {
      assertIdentifier(operation.column, "alter_column_type.column");
      assertColumnType(operation.newType, "alter_column_type.newType");
      continue;
    }

    if (operation.type === "add_check") {
      assertCheckExpression(operation.expression, "add_check.expression");
      continue;
    }

    if (operation.type === "drop_check") {
      assertCheckExpression(operation.expression, "drop_check.expression");
      continue;
    }

    if (operation.type === "update") {
      assertPredicate(operation.values, "update.values");
      assertPredicate(operation.where, "update.where");
      continue;
    }

    if (operation.type === "delete") {
      assertPredicate(operation.where, "delete.where");
      continue;
    }

    if (operation.type === "restore_table") {
      throw new TossError("INVALID_OPERATION", "restore_table is an internal operation and cannot be used in plans");
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

const sqlInputSchema = z.string().trim().min(1, "SQL must not be empty");

const FORBIDDEN_KEYWORDS = [
  "INSERT",
  "UPDATE",
  "DELETE",
  "DROP",
  "ALTER",
  "CREATE",
  "REPLACE",
  "TRUNCATE",
  "ATTACH",
  "DETACH",
  "PRAGMA",
  "VACUUM",
  "BEGIN",
  "COMMIT",
  "ROLLBACK",
] as const;

function stripStringLiterals(sql: string): string {
  return sql
    .replace(/'([^']|'')*'/g, "''")
    .replace(/\"([^\"\\]|\\.)*\"/g, '""')
    .replace(/`([^`]|``)*`/g, "``")
    .replace(/--.*$/gm, "")
    .replace(/\/\*[\s\S]*?\*\//g, "");
}

export function validateReadSql(inputSql: string): string {
  const parsed = sqlInputSchema.safeParse(inputSql);
  if (!parsed.success) {
    throw new TossError("INVALID_SQL", parsed.error.issues.map((issue) => issue.message).join("; "));
  }

  let sql = parsed.data.trim();
  if (sql.endsWith(";")) {
    sql = sql.slice(0, -1).trim();
  }

  const stripped = stripStringLiterals(sql);
  if (stripped.includes(";")) {
    throw new TossError("INVALID_SQL", "Multiple SQL statements are not allowed");
  }

  const upper = stripped.trim().toUpperCase();
  if (!(upper.startsWith("SELECT") || upper.startsWith("WITH"))) {
    throw new TossError("INVALID_SQL", "Only SELECT / WITH ... SELECT queries are allowed");
  }

  for (const keyword of FORBIDDEN_KEYWORDS) {
    const regex = new RegExp(`\\b${keyword}\\b`, "i");
    if (regex.test(stripped)) {
      throw new TossError("INVALID_SQL", `Forbidden keyword in read-only query: ${keyword}`);
    }
  }

  return sql;
}
