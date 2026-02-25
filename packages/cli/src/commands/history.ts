import { listCommits, type Database } from "@lore/core";
import { parseArgs } from "node:util";
import { z } from "zod";
import { formatTimestamp, printTable, toJson } from "../format";

export const HistoryArgsSchema = z.object({ verbose: z.boolean(), json: z.boolean() });

export function parseHistoryArgs(args: string[]): z.infer<typeof HistoryArgsSchema> {
  const parsed = parseArgs({
    strict: true,
    args,
    allowPositionals: false,
    options: {
      verbose: { type: "boolean" },
      json: { type: "boolean" },
    },
  });
  return HistoryArgsSchema.parse({
    verbose: parsed.values.verbose ?? false,
    json: parsed.values.json ?? false,
  });
}

export function runHistory(db: Database, args: z.infer<typeof HistoryArgsSchema>): void {
  const { verbose, json } = args;
  const entries = listCommits(db, true);
  if (json) {
    console.log(toJson(entries));
    return;
  }
  const rows = entries.map((entry) =>
    verbose
      ? {
          seq: entry.seq,
          commit_id: entry.commitId,
          created_at: formatTimestamp(entry.createdAt),
          created_at_unix_ms: entry.createdAt,
          kind: entry.kind,
          parent_count: entry.parentCount,
          revert_target: entry.revertTargetId ?? "",
          state_hash: entry.stateHashAfter,
          revertible: entry.revertible,
          message: entry.message,
        }
      : {
          seq: entry.seq,
          commit_id: entry.commitId,
          created_at: formatTimestamp(entry.createdAt),
          created_at_unix_ms: entry.createdAt,
          kind: entry.kind,
          message: entry.message.length > 80 ? `${entry.message.slice(0, 77)}...` : entry.message,
        },
  );
  console.log(rows.length === 0 ? "(no commits)" : printTable(rows));
}
