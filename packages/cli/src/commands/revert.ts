import { revert, type Database } from "@lore/core";
import { z } from "zod";
import { summarizeCommit, toJson } from "../format";
import { parseRequiredPositional } from "../parse";

export const RevertArgsSchema = z.object({ commitId: z.string().min(1) });

export function parseRevertArgs(args: string[]): z.infer<typeof RevertArgsSchema> {
  const commitId = parseRequiredPositional(args);
  return RevertArgsSchema.parse({ commitId });
}

export function runRevert(db: Database, args: z.infer<typeof RevertArgsSchema>): void {
  const { commitId } = args;
  const result = revert(db, commitId);
  if (!result.ok) {
    console.log(toJson({ status: "conflict", conflicts: result.conflicts }));
    process.exit(1);
  }
  console.log(toJson({ status: "ok", revert_commit: summarizeCommit(result.revertCommit) }));
}
