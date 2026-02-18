#!/usr/bin/env bun
import {
  applyPlan,
  getHistory,
  getStatus,
  initDatabase,
  isTossError,
  readQuery,
  revertCommit,
} from "@toss/core";
import type { LogEntry } from "@toss/core";
import { printTable, toJson } from "./format";

function usage(): string {
  return [
    "toss CLI (MVP)",
    "",
    "Commands:",
    "  toss init [--no-skills]",
    "  toss apply --plan <file|->",
    "  toss read --sql \"<SELECT...>\" [--json]",
    "  toss status",
    "  toss history",
    "  toss revert <commit_id>",
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

function summarize(entry: LogEntry): Record<string, unknown> {
  return {
    id: entry.id,
    timestamp: entry.timestamp,
    kind: entry.kind,
    message: entry.message,
  };
}

async function runInit(args: string[]): Promise<void> {
  const noSkills = hasFlag(args, "--no-skills");
  const invalidArgs = args.filter((arg) => arg !== "--no-skills");
  if (invalidArgs.length > 0) {
    throw new Error("init accepts only --no-skills");
  }
  const result = await initDatabase({ generateSkills: !noSkills, workspacePath: process.cwd() });
  console.log(`Initialized toss database at ${result.dbPath}`);
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
  console.log(
    toJson({
      status: "ok",
      commit: summarize(commit),
      operations: commit.operations.length,
    }),
  );
}

async function runRead(args: string[]): Promise<void> {
  const sql = getOptionValue(args, "--sql");
  if (!sql) {
    throw new Error("read requires --sql \"<SELECT...>\"");
  }

  const rows = readQuery(sql);
  if (hasFlag(args, "--json")) {
    console.log(toJson(rows));
    return;
  }

  console.log(printTable(rows));
}

async function runStatus(args: string[]): Promise<void> {
  if (args.length > 0) {
    throw new Error("status does not accept positional arguments");
  }

  const status = getStatus();
  const rows = status.tables.map((table) => ({ table: table.name, rows: table.count }));
  console.log(`DB: ${status.dbPath}`);
  console.log(`Schema Version: ${status.schemaVersion}`);
  console.log(`User Tables: ${status.tableCount}`);
  console.log(status.latestCommit ? `Latest Commit: ${status.latestCommit.id} (${status.latestCommit.kind})` : "Latest Commit: none");
  console.log(rows.length === 0 ? "(no user tables)" : printTable(rows));
}

async function runHistory(args: string[]): Promise<void> {
  if (args.length > 0) {
    throw new Error("history does not accept positional arguments");
  }

  const history = getHistory();
  const rows = history.map((entry) => ({
    id: entry.id,
    timestamp: entry.timestamp,
    kind: entry.kind,
    message: entry.message.length > 80 ? `${entry.message.slice(0, 77)}...` : entry.message,
  }));

  console.log(rows.length === 0 ? "(no commits)" : printTable(rows));
}

async function runRevert(args: string[]): Promise<void> {
  const commitId = args[0];
  if (!commitId) {
    throw new Error("revert requires <commit_id>");
  }
  if (args.length > 1) {
    throw new Error("revert accepts exactly one <commit_id>");
  }

  const result = revertCommit(commitId);
  console.log(
    toJson({
      status: "ok",
      revert_commit: summarize(result.revertCommit),
      replayed_apply_count: result.replayedApplyCount,
    }),
  );
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
      await runRead(rest);
      return;
    case "status":
      await runStatus(rest);
      return;
    case "history":
      await runHistory(rest);
      return;
    case "revert":
      await runRevert(rest);
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
