import { revert, type Database } from "@toss/core";
import { parseArgs } from "node:util";
import { z } from "zod";
import { summarizeCommit, toJson } from "../format";

export const RevertArgsSchema = z.object({ commitId: z.string().min(1) });

export function parseRevertArgs(args: string[]): z.infer<typeof RevertArgsSchema> {
  const parsed = parseArgs({
    strict: true,
    args,
    allowPositionals: true,
    options: {},
  });
  const [commitId] = z.tuple([z.string().trim().min(1)]).parse(parsed.positionals);
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
