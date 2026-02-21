import {
  applyPlan,
  autoSyncAfterApply,
  commitSizeWarning,
  getSchema,
  planCheck,
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

export function runSchema(args: string[]): void {
  const { table } = parseSchemaArgs(args);
  console.log(toJson(getSchema({ table })));
}

export async function runPlan(args: string[]): Promise<void> {
  const planRef = parseSinglePlanRef("plan", args);
  const result = await planCheck(planRef);
  console.log(toJson(result));
  if (!result.ok) {
    process.exit(1);
  }
}

export async function runApply(args: string[]): Promise<void> {
  const plan = parseSinglePlanRef("apply", args);
  const commit = await applyPlan(plan);
  const sync = await autoSyncAfterApply();
  const warning = commitSizeWarning(commit.commitId);
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

export function runRead(args: string[]): void {
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
  const rows = readQuery(sql);
  if (json) {
    console.log(toJson(rows));
    return;
  }
  console.log(printTable(rows));
}
