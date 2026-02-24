import {
  listCommits,
  status,
  recover,
  resolveDbPath,
  revert,
  verify,
  type Database,
} from "@toss/core";
import { formatTimestamp, printTable, summarizeCommit, toJson } from "../format";

function parseStatusArgs(args: string[]): { json: boolean } {
  let json = false;
  for (const arg of args) {
    if (arg === "--json") {
      json = true;
      continue;
    }
    throw new Error(`status does not accept argument: ${arg}`);
  }
  return { json };
}

function parseHistoryArgs(args: string[]): { verbose: boolean; json: boolean } {
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
  return { verbose, json };
}

function parseRevertArgs(args: string[]): { commitId: string } {
  const commitId = args[0];
  if (!commitId) {
    throw new Error("revert requires <commit_id>");
  }
  if (args.length > 1) {
    throw new Error("revert accepts exactly one <commit_id>");
  }
  return { commitId };
}

function parseVerifyArgs(args: string[]): { full: boolean } {
  let full = false;
  for (const arg of args) {
    if (arg === "--full") {
      full = true;
      continue;
    }
    throw new Error("verify accepts only --full");
  }
  return { full };
}

function parseRecoverArgs(args: string[]): { snapshotCommitId: string } {
  const snapshotCommitId = args[0];
  if (!snapshotCommitId) {
    throw new Error("recover requires <commit_id>");
  }
  if (args.length > 1) {
    throw new Error("recover accepts exactly one <commit_id>");
  }
  return { snapshotCommitId };
}

export function validateStatusArgs(args: string[]): void {
  parseStatusArgs(args);
}

export function validateHistoryArgs(args: string[]): void {
  parseHistoryArgs(args);
}

export function validateRevertArgs(args: string[]): void {
  parseRevertArgs(args);
}

export function validateVerifyArgs(args: string[]): void {
  parseVerifyArgs(args);
}

export function validateRecoverArgs(args: string[]): void {
  parseRecoverArgs(args);
}

export function runStatus(db: Database, args: string[]): void {
  const { json } = parseStatusArgs(args);
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

export function runHistory(db: Database, args: string[]): void {
  const { verbose, json } = parseHistoryArgs(args);
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

export function runRevert(db: Database, args: string[]): void {
  const { commitId } = parseRevertArgs(args);
  const result = revert(db, commitId);
  if (!result.ok) {
    console.log(toJson({ status: "conflict", conflicts: result.conflicts }));
    process.exit(1);
  }
  console.log(toJson({ status: "ok", revert_commit: summarizeCommit(result.revertCommit) }));
}

export function runVerify(db: Database, args: string[]): void {
  const { full } = parseVerifyArgs(args);
  const result = verify(db, { full });
  console.log(toJson(result));
  if (!result.ok) {
    process.exit(1);
  }
}

export async function runRecover(args: string[]): Promise<void> {
  const { snapshotCommitId } = parseRecoverArgs(args);
  const result = await recover(resolveDbPath(), snapshotCommitId);
  console.log(toJson({ status: "ok", ...result }));
}
