import { status, type Database } from "@lore/core";
import { parseArgs } from "node:util";
import { z } from "zod";
import { printTable, toJson } from "../format";

export const StatusArgsSchema = z.object({ json: z.boolean() });

export function parseStatusArgs(args: string[]): z.infer<typeof StatusArgsSchema> {
  const parsed = parseArgs({
    strict: true,
    args,
    allowPositionals: false,
    options: {
      json: { type: "boolean" },
    },
  });
  return StatusArgsSchema.parse({ json: parsed.values.json ?? false });
}

export function runStatus(db: Database, args: z.infer<typeof StatusArgsSchema>): void {
  const { json } = args;
  const currentStatus = status(db);
  if (json) {
    console.log(toJson(currentStatus));
    return;
  }
  const rows = currentStatus.tables.map((table) => ({ table: table.name, rows: table.count }));
  console.log(`DB: ${currentStatus.dbPath}`);
  console.log(`User Tables: ${currentStatus.tableCount}`);
  console.log(`Snapshots: ${currentStatus.snapshotCount}`);
  console.log(`Last Verified At: ${currentStatus.lastVerifiedAt ?? "never"}`);
  let verifiedLabel = "unknown";
  if (currentStatus.lastVerifiedOk === true) {
    verifiedLabel = "yes";
  } else if (currentStatus.lastVerifiedOk === false) {
    verifiedLabel = "no";
  }
  console.log(`Last Verified OK: ${verifiedLabel}`);
  console.log(`Sync Configured: ${currentStatus.sync.configured ? "yes" : "no"}`);
  console.log(`Sync State: ${currentStatus.sync.state}`);
  console.log(`Pending Commits: ${currentStatus.sync.pendingCommits}`);
  console.log(`Remote Platform: ${currentStatus.sync.remotePlatform ?? "not configured"}`);
  console.log(`Remote: ${currentStatus.sync.remoteUrl ?? "not configured"}`);
  console.log(`History Commits: ${currentStatus.storage.commitCount}`);
  console.log(`History Size Estimate: ${currentStatus.storage.estimatedHistoryBytes} bytes`);
  console.log(
    `Latest Commit Size Estimate: ${currentStatus.storage.latestCommitEstimatedBytes === null ? "n/a" : `${currentStatus.storage.latestCommitEstimatedBytes} bytes`}`,
  );
  console.log(
    currentStatus.headCommit
      ? `HEAD: ${currentStatus.headCommit.commitId} (seq=${currentStatus.headCommit.seq}, kind=${currentStatus.headCommit.kind})`
      : "HEAD: none",
  );
  console.log(rows.length === 0 ? "(no user tables)" : printTable(rows));
}
