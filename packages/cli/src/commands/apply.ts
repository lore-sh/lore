import { apply, autoSync, parsePlan, sizeWarning, type Database } from "@lore/core";
import { summarizeCommit, toJson } from "../format";
import { parsePlanInput, readPlanInput, type PlanInput } from "./plan";

export function parseApplyArgs(args: string[]): PlanInput {
  return parsePlanInput(args);
}

export async function runApply(db: Database, input: PlanInput): Promise<void> {
  const payload = await readPlanInput(input);
  const plan = parsePlan(payload);
  const commit = await apply(db, plan);
  const sync = await autoSync(db);
  const warning = sizeWarning(db, commit.commitId);
  console.log(
    toJson({
      status: "ok",
      commit: summarizeCommit(commit),
      schema_hash_after: commit.schemaHashAfter,
      state_hash_after: commit.stateHashAfter,
      operations: plan.operations.length,
      sync,
      warnings: warning ? [warning] : [],
    }),
  );
}
