import { recover, resolveDbPath } from "@toss/core";
import { parseArgs } from "node:util";
import { z } from "zod";
import { toJson } from "../format";

export const RecoverArgsSchema = z.object({ snapshotCommitId: z.string().min(1) });

export function parseRecoverArgs(args: string[]): z.infer<typeof RecoverArgsSchema> {
  const parsed = parseArgs({
    strict: true,
    args,
    allowPositionals: true,
    options: {},
  });
  const [snapshotCommitId] = z.tuple([z.string().trim().min(1)]).parse(parsed.positionals);
  return RecoverArgsSchema.parse({ snapshotCommitId });
}

export async function runRecover(args: z.infer<typeof RecoverArgsSchema>): Promise<void> {
  const { snapshotCommitId } = args;
  const result = await recover(resolveDbPath(), snapshotCommitId);
  console.log(toJson({ status: "ok", ...result }));
}
