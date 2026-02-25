import { CodedError, check, parsePlan, type Database } from "@lore/core";
import { parseArgs } from "node:util";
import { z } from "zod";
import { toJson } from "../format";

type CheckResult = ReturnType<typeof check>;
type CheckIssue = CheckResult["errors"][number];
type CheckSummary = CheckResult["summary"];

const EMPTY_CHECK_SUMMARY: CheckSummary = {
  operations: 0,
  schemaOperations: 0,
  dataOperations: 0,
  destructiveOperations: 0,
  touchedTables: [],
  predicted: { rowEffects: 0, schemaEffects: 0, tables: [] },
};

const PlanRefSchema = z.string().min(1);

export function parsePlanArgs(args: string[]): string {
  const parsed = parseArgs({
    strict: true,
    args,
    allowPositionals: true,
    options: {},
  });
  const [planRef] = z.tuple([PlanRefSchema]).parse(parsed.positionals);
  return planRef;
}

export function readPlanInput(planRef: string): Promise<string> {
  if (planRef === "-") {
    return Bun.stdin.text();
  }
  return Bun.file(planRef).text();
}

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

export async function runPlan(db: Database, planRef: string): Promise<void> {
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
