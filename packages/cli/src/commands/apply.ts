import { apply, autoSync, parsePlan, sizeWarning, type Database } from "@toss/core";
import { parseArgs } from "node:util";
import { z } from "zod";
import { summarizeCommit, toJson } from "../format";
import { readPlanInput } from "./plan";

const PlanRefSchema = z.string().min(1);

export function parseApplyArgs(args: string[]): string {
  const parsed = parseArgs({
    strict: true,
    args,
    allowPositionals: true,
    options: {},
  });
  const [planRef] = z.tuple([PlanRefSchema]).parse(parsed.positionals);
  return planRef;
}

export async function runApply(db: Database, planRef: string): Promise<void> {
  const payload = await readPlanInput(planRef);
  const plan = parsePlan(payload);
  const commit = await apply(db, plan);
  const sync = await autoSync(db);
  const warning = sizeWarning(db, commit.commitId);
  console.log(
    toJson({
      status: "ok",
      commit: summarizeCommit(commit),
      operations: plan.operations.length,
      sync,
      warnings: warning ? [warning] : [],
    }),
  );
}
