import {
  getHistory,
  getStatus,
  recoverFromSnapshot,
  revertCommit,
  verifyDatabase,
} from "@toss/core";
import { formatTimestamp, printTable, summarizeCommit, toJson } from "../format";

export function runStatus(args: string[]): void {
  let json = false;
  for (const arg of args) {
    if (arg === "--json") {
      json = true;
      continue;
    }
    throw new Error(`status does not accept argument: ${arg}`);
  }
  const status = getStatus();
  if (json) {
    console.log(toJson(status));
    return;
  }
  const rows = status.tables.map((table) => ({ table: table.name, rows: table.count }));
  console.log(`DB: ${status.dbPath}`);
  console.log(`User Tables: ${status.tableCount}`);
  console.log(`Snapshots: ${status.snapshotCount}`);
  console.log(`Last Verified At: ${status.lastVerifiedAt ?? "never"}`);
  const verifiedLabel = status.lastVerifiedOk === null ? "unknown" : status.lastVerifiedOk ? "yes" : "no";
  console.log(`Last Verified OK: ${verifiedLabel}`);
  console.log(`Sync Configured: ${status.sync.configured ? "yes" : "no"}`);
  console.log(`Sync State: ${status.sync.state}`);
  console.log(`Pending Commits: ${status.sync.pendingCommits}`);
  console.log(`Remote Platform: ${status.sync.remotePlatform ?? "not configured"}`);
  console.log(`Remote: ${status.sync.remoteUrl ?? "not configured"}`);
  console.log(`History Commits: ${status.storage.commitCount}`);
  console.log(`History Size Estimate: ${status.storage.estimatedHistoryBytes} bytes`);
  console.log(
    `Latest Commit Size Estimate: ${status.storage.latestCommitEstimatedBytes === null ? "n/a" : `${status.storage.latestCommitEstimatedBytes} bytes`}`,
  );
  console.log(
    status.headCommit
      ? `HEAD: ${status.headCommit.commitId} (seq=${status.headCommit.seq}, kind=${status.headCommit.kind})`
      : "HEAD: none",
  );
  console.log(rows.length === 0 ? "(no user tables)" : printTable(rows));
}

export function runHistory(args: string[]): void {
  let verbose = false;
  let json = false;
  for (const arg of args) {
    if (arg === "--verbose") {
      verbose = true;
      continue;
    }
    if (arg === "--json") {
      json = true;
      continue;
    }
    throw new Error(`history does not accept argument: ${arg}`);
  }
  const history = getHistory();
  if (json) {
    console.log(toJson(history));
    return;
  }
  const rows = history.map((entry) =>
    verbose
      ? {
          seq: entry.seq,
          commit_id: entry.commitId,
          created_at: formatTimestamp(entry.createdAt),
          created_at_unix_ms: entry.createdAt,
          kind: entry.kind,
          parent: entry.parentIds.join(","),
          reverted_target: entry.revertedTargetId ?? "",
          state_hash: entry.stateHashAfter,
          inverse_ready: entry.inverseReady,
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

export function runRevert(args: string[]): void {
  const commitId = args[0];
  if (!commitId) {
    throw new Error("revert requires <commit_id>");
  }
  if (args.length > 1) {
    throw new Error("revert accepts exactly one <commit_id>");
  }
  const result = revertCommit(commitId);
  if (!result.ok) {
    console.log(toJson({ status: "conflict", conflicts: result.conflicts }));
    process.exit(1);
  }
  console.log(toJson({ status: "ok", revert_commit: summarizeCommit(result.revertCommit) }));
}

export function runVerify(args: string[]): void {
  let full = false;
  for (const arg of args) {
    if (arg === "--full") {
      full = true;
      continue;
    }
    throw new Error("verify accepts only --full");
  }
  const result = verifyDatabase({ full });
  console.log(toJson(result));
  if (!result.ok) {
    process.exit(1);
  }
}

export async function runRecover(args: string[]): Promise<void> {
  const snapshotCommitId = args[0];
  if (!snapshotCommitId) {
    throw new Error("recover requires <commit_id>");
  }
  if (args.length > 1) {
    throw new Error("recover accepts exactly one <commit_id>");
  }
  const result = await recoverFromSnapshot(snapshotCommitId);
  console.log(toJson({ status: "ok", ...result }));
}
