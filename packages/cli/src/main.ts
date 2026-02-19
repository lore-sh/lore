#!/usr/bin/env bun
import {
  applyPlan,
  getHistory,
  getStatus,
  initDatabase,
  isTossError,
  readQuery,
  recoverFromSnapshot,
  revertCommit,
  verifyDatabase,
} from "@toss/core";
import type { CommitEntry } from "@toss/core";
import { printTable, toJson } from "./format";

function usage(): string {
  return [
    "toss CLI",
    "",
    "Commands:",
    "  toss init [--no-skills] [--force-new]",
    "  toss apply --plan <file|->",
    "  toss read --sql \"<SELECT...>\" [--json]",
    "  toss status",
    "  toss history [--verbose]",
    "  toss revert <commit_id>",
    "  toss verify [--full]",
    "  toss recover --from-snapshot <commit_id>",
    "",
    "Environment:",
    "  TOSS_DB_PATH   Override default database path (default: ./toss.db)",
  ].join("\n");
}

function getOptionValue(args: string[], name: string): string | null {
  const index = args.indexOf(name);
  if (index < 0) {
    return null;
  }
  return args[index + 1] ?? null;
}

function hasFlag(args: string[], name: string): boolean {
  return args.includes(name);
}

function summarizeCommit(entry: CommitEntry): Record<string, unknown> {
  return {
    commit_id: entry.commitId,
    seq: entry.seq,
    created_at: entry.createdAt,
    kind: entry.kind,
    message: entry.message,
    parent_ids: entry.parentIds,
    state_hash_after: entry.stateHashAfter,
    schema_hash_after: entry.schemaHashAfter,
    inverse_ready: entry.inverseReady,
    reverted_target_id: entry.revertedTargetId,
  };
}

async function runInit(args: string[]): Promise<void> {
  const noSkills = hasFlag(args, "--no-skills");
  const forceNew = hasFlag(args, "--force-new");
  const invalidArgs = args.filter((arg) => arg !== "--no-skills" && arg !== "--force-new");
  if (invalidArgs.length > 0) {
    throw new Error("init accepts only --no-skills and --force-new");
  }

  const result = await initDatabase({ generateSkills: !noSkills, workspacePath: process.cwd(), forceNew });
  console.log(`Initialized toss database at ${result.dbPath}`);
  if (forceNew) {
    console.log("Reinitialized database with clean-break history format.");
  }
  if (result.generatedSkills) {
    console.log(`Generated skills at ${result.generatedSkills.skillsRoot}`);
    console.log(`Updated agents file at ${result.generatedSkills.agentsPath}`);
  } else {
    console.log("Skipped skill generation (--no-skills)");
  }
}

async function runApply(args: string[]): Promise<void> {
  const plan = getOptionValue(args, "--plan");
  if (!plan) {
    throw new Error("apply requires --plan <file|->");
  }
  const commit = await applyPlan(plan);
  console.log(toJson({ status: "ok", commit: summarizeCommit(commit), operations: commit.operations.length }));
}

function runRead(args: string[]): void {
  const sql = getOptionValue(args, "--sql");
  if (!sql) {
    throw new Error('read requires --sql "<SELECT...>"');
  }
  const rows = readQuery(sql);
  if (hasFlag(args, "--json")) {
    console.log(toJson(rows));
    return;
  }
  console.log(printTable(rows));
}

function runStatus(args: string[]): void {
  if (args.length > 0) {
    throw new Error("status does not accept positional arguments");
  }
  const status = getStatus();
  const rows = status.tables.map((table) => ({ table: table.name, rows: table.count }));
  console.log(`DB: ${status.dbPath}`);
  console.log(`History Engine: ${status.historyEngine}`);
  console.log(`Format Generation: ${status.formatGeneration}`);
  console.log(`SQLite Min Version: ${status.sqliteMinVersion}`);
  console.log(`User Tables: ${status.tableCount}`);
  console.log(`Snapshots: ${status.snapshotCount}`);
  console.log(`Last Verified At: ${status.lastVerifiedAt ?? "never"}`);
  console.log(
    status.headCommit
      ? `HEAD: ${status.headCommit.commitId} (seq=${status.headCommit.seq}, kind=${status.headCommit.kind})`
      : "HEAD: none",
  );
  console.log(rows.length === 0 ? "(no user tables)" : printTable(rows));
}

function runHistory(args: string[]): void {
  const verbose = hasFlag(args, "--verbose");
  const invalidArgs = args.filter((arg) => arg !== "--verbose");
  if (invalidArgs.length > 0) {
    throw new Error("history accepts only --verbose");
  }
  const history = getHistory({ verbose });
  const rows = history.map((entry) =>
    verbose
      ? {
          seq: entry.seq,
          commit_id: entry.commitId,
          created_at: entry.createdAt,
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
          created_at: entry.createdAt,
          kind: entry.kind,
          message: entry.message.length > 80 ? `${entry.message.slice(0, 77)}...` : entry.message,
        },
  );
  console.log(rows.length === 0 ? "(no commits)" : printTable(rows));
}

function runRevert(args: string[]): never | void {
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

function runVerify(args: string[]): never | void {
  const full = hasFlag(args, "--full");
  const invalidArgs = args.filter((arg) => arg !== "--full");
  if (invalidArgs.length > 0) {
    throw new Error("verify accepts only --full");
  }
  const result = verifyDatabase({ full });
  console.log(toJson(result));
  if (!result.ok) {
    process.exit(1);
  }
}

async function runRecover(args: string[]): Promise<void> {
  const snapshotCommitId = getOptionValue(args, "--from-snapshot");
  if (!snapshotCommitId) {
    throw new Error("recover requires --from-snapshot <commit_id>");
  }
  const result = await recoverFromSnapshot(snapshotCommitId);
  console.log(toJson({ status: "ok", ...result }));
}

async function main(): Promise<void> {
  const args = Bun.argv.slice(2);
  const command = args[0];
  if (!command || command === "--help" || command === "-h") {
    console.log(usage());
    return;
  }
  const rest = args.slice(1);
  switch (command) {
    case "init":
      await runInit(rest);
      return;
    case "apply":
      await runApply(rest);
      return;
    case "read":
      runRead(rest);
      return;
    case "status":
      runStatus(rest);
      return;
    case "history":
      runHistory(rest);
      return;
    case "revert":
      runRevert(rest);
      return;
    case "verify":
      runVerify(rest);
      return;
    case "recover":
      await runRecover(rest);
      return;
    default:
      throw new Error(`Unknown command: ${command}`);
  }
}

main().catch((error: unknown) => {
  if (isTossError(error)) {
    console.error(`Error [${error.code}]: ${error.message}`);
    process.exit(1);
  }
  const message = error instanceof Error ? error.message : String(error);
  console.error(`Error: ${message}`);
  process.exit(1);
});

