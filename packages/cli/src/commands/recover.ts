import { recover, resolveDbPath } from "@lore/core";
import { z } from "zod";
import { toJson } from "../format";
import { parseRequiredPositional } from "../parse";

export const RecoverArgsSchema = z.object({ snapshotCommitId: z.string().min(1) });

export function parseRecoverArgs(args: string[]): z.infer<typeof RecoverArgsSchema> {
  const snapshotCommitId = parseRequiredPositional(args);
  return RecoverArgsSchema.parse({ snapshotCommitId });
}

export async function runRecover(args: z.infer<typeof RecoverArgsSchema>): Promise<void> {
  const { snapshotCommitId } = args;
  const result = await recover(resolveDbPath(), snapshotCommitId);
  console.log(toJson({ status: "ok", ...result }));
}
