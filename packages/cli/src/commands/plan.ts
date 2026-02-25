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

export const PlanInputSchema = z.discriminatedUnion("kind", [
  z.object({ kind: z.literal("file"), path: z.string().min(1) }),
  z.object({ kind: z.literal("stdin") }),
]);

export type PlanInput = z.infer<typeof PlanInputSchema>;

export function parsePlanInput(args: string[]): PlanInput {
  const parsed = parseArgs({
    strict: true,
    args,
    allowPositionals: true,
    options: {
      file: { type: "string", short: "f" },
    },
  });

  const file = parsed.values.file;
  const positionals = parsed.positionals;

  if (positionals.length > 0) {
    throw new Error("Positional arguments are not allowed. Use -f <file|->");
  }

  if (file !== undefined) {
    return PlanInputSchema.parse(
      file === "-" ? { kind: "stdin" } : { kind: "file", path: file },
    );
  }

  throw new Error("Missing required option: -f <file|->");
}

export function parsePlanArgs(args: string[]): PlanInput {
  return parsePlanInput(args);
}

export function readPlanInput(input: PlanInput): Promise<string> {
  switch (input.kind) {
    case "stdin":
      return Bun.stdin.text();
    case "file":
      return Bun.file(input.path).text();
  }
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

export async function runPlan(db: Database, input: PlanInput): Promise<void> {
  let plan: ReturnType<typeof parsePlan>;
  try {
    const payload = await readPlanInput(input);
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
