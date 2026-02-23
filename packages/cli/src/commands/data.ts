import type { Database } from "bun:sqlite";
import {
  CodedError,
  apply,
  autoSyncAfterApply,
  check,
  commitSizeWarning,
  type CheckIssue,
  type CheckResult,
  type CheckSummary,
  getSchema,
  parsePlan,
  readQuery,
} from "@toss/core";
import { printTable, toJson, summarizeCommit } from "../format";

export function parseSinglePlanRef(command: "plan" | "apply", args: string[]): string {
  if (args.length === 0) {
    throw new Error(`${command} requires <file|->`);
  }
  if (args.length > 1) {
    throw new Error(`${command} accepts exactly one <file|-> argument`);
  }
  const planRef = args[0]!;
  if (planRef.startsWith("--")) {
    throw new Error(`${command} does not accept option arguments. Use: toss ${command} <file|->`);
  }
  return planRef;
}

function readPlanInput(planRef: string): Promise<string> {
  if (planRef === "-") {
    return Bun.stdin.text();
  }
  return Bun.file(planRef).text();
}

const EMPTY_CHECK_SUMMARY: CheckSummary = {
  operations: 0,
  schemaOperations: 0,
  dataOperations: 0,
  destructiveOperations: 0,
  touchedTables: [],
  predicted: { rowEffects: 0, schemaEffects: 0, tables: [] },
};

function checkIssueFromError(error: unknown): CheckIssue {
  if (CodedError.is(error)) {
    return { code: error.code, message: error.message };
  }
  if (error instanceof Error) {
    return { code: "PLAN_CHECK_FAILED", message: error.message };
  }
  return { code: "PLAN_CHECK_FAILED", message: String(error) };
}

function failedCheckResult(error: unknown): CheckResult {
  return {
    ok: false,
    risk: "high",
    errors: [checkIssueFromError(error)],
    warnings: [],
    summary: EMPTY_CHECK_SUMMARY,
    checkedAt: new Date().toISOString(),
  };
}

export function parseSchemaArgs(args: string[]): { table?: string | undefined } {
  if (args.length > 1) {
    throw new Error("schema accepts at most one <table> argument");
  }
  const table = args[0];
  if (table?.startsWith("--")) {
    throw new Error(`schema does not accept argument: ${table}`);
  }
  return { table };
}

export function runSchema(db: Database, args: string[]): void {
  const { table } = parseSchemaArgs(args);
  console.log(toJson(getSchema(db, { table })));
}

export function validateSchemaArgs(args: string[]): void {
  parseSchemaArgs(args);
}

export async function runPlan(db: Database, args: string[]): Promise<void> {
  const planRef = parseSinglePlanRef("plan", args);
  let plan: ReturnType<typeof parsePlan>;
  try {
    const payload = await readPlanInput(planRef);
    plan = parsePlan(payload);
  } catch (error) {
    const result = failedCheckResult(error);
    console.log(toJson(result));
    process.exit(1);
    return;
  }
  const result = check(db, plan);
  console.log(toJson(result));
  if (!result.ok) {
    process.exit(1);
  }
}

export function validatePlanArgs(args: string[]): void {
  parseSinglePlanRef("plan", args);
}

export async function runApply(db: Database, args: string[]): Promise<void> {
  const planRef = parseSinglePlanRef("apply", args);
  const payload = await readPlanInput(planRef);
  const plan = parsePlan(payload);
  const commit = await apply(db, plan);
  const sync = await autoSyncAfterApply(db);
  const warning = commitSizeWarning(db, commit.commitId);
  console.log(
    toJson({
      status: "ok",
      commit: summarizeCommit(commit),
      operations: commit.operations.length,
      sync,
      warnings: warning ? [warning] : [],
    }),
  );
}

export function validateApplyArgs(args: string[]): void {
  parseSinglePlanRef("apply", args);
}

function parseReadArgs(args: string[]): { sql: string; json: boolean } {
  let sql: string | null = null;
  let json = false;
  for (let i = 0; i < args.length; i += 1) {
    const arg = args[i]!;
    if (arg === "--sql") {
      sql = args[i + 1] ?? null;
      i += 1;
      continue;
    }
    if (arg === "--json") {
      json = true;
      continue;
    }
    throw new Error(`read does not accept argument: ${arg}`);
  }
  if (!sql) {
    throw new Error('read requires --sql "<SELECT...>"');
  }
  return { sql, json };
}

export function validateReadArgs(args: string[]): void {
  parseReadArgs(args);
}

export function runRead(db: Database, args: string[]): void {
  const { sql, json } = parseReadArgs(args);
  const rows = readQuery(db, sql);
  if (json) {
    console.log(toJson(rows));
    return;
  }
  console.log(printTable(rows));
}
