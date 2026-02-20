#!/usr/bin/env bun
import {
  applyPlan,
  cleanSkills,
  getHistory,
  getSchema,
  getStatus,
  initDatabase,
  isTossError,
  planCheck,
  readQuery,
  recoverFromSnapshot,
  revertCommit,
  verifyDatabase,
} from "@toss/core";
import type { CommitEntry } from "@toss/core";
import { parseStudioPortArg, startStudioServer } from "@toss/studio";
import { createInterface } from "node:readline/promises";
import { stdin, stdout } from "node:process";
import { printTable, toJson } from "./format";
import {
  canUseInteractivePrompt,
  parseInitArgs,
  promptPlatformSelection,
  renderCleanResult,
  renderInitResult,
  resolveInitSelection,
} from "./init-ui";

function usage(): string {
  return [
    "toss CLI",
    "",
    "Commands:",
    "  toss init [--no-skills] [--force-new] [--platforms <list>] [--yes]",
    "  toss clean [--yes]",
    "  toss schema [--table <name>]",
    "  toss plan <file|->",
    "  toss apply <file|->",
    "  toss read --sql \"<SELECT...>\" [--json]",
    "  toss status",
    "  toss history [--verbose]",
    "  toss revert <commit_id>",
    "  toss verify [--full]",
    "  toss recover --from-snapshot <commit_id>",
    "  toss studio [--port <n>] [--no-open]",
    "",
    "Environment:",
    "  TOSS_DB_PATH   Override default database path (default: ~/.toss/toss.db)",
    "",
    "Init Platforms:",
    "  claude,cursor,codex,opencode,openclaw",
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

function formatTimestamp(unixMs: number): string {
  const date = new Date(unixMs);
  return Number.isNaN(date.getTime()) ? String(unixMs) : date.toISOString();
}

function summarizeCommit(entry: CommitEntry): Record<string, unknown> {
  return {
    commit_id: entry.commitId,
    seq: entry.seq,
    created_at: formatTimestamp(entry.createdAt),
    created_at_unix_ms: entry.createdAt,
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
  const parsed = parseInitArgs(args);
  const isInteractiveTty = canUseInteractivePrompt(stdin.isTTY === true, stdout.isTTY === true);
  const resolved = resolveInitSelection(parsed, isInteractiveTty);
  const skillPlatforms = resolved.interactive ? await promptPlatformSelection() : resolved.platforms;
  const result = await initDatabase({
    generateSkills: !parsed.noSkills,
    forceNew: parsed.forceNew,
    skillPlatforms,
  });
  console.log(
    renderInitResult({
      dbPath: result.dbPath,
      forceNew: parsed.forceNew,
      selectedPlatforms: skillPlatforms,
      generatedSkills: result.generatedSkills,
      noSkills: parsed.noSkills,
      useColor: stdout.isTTY === true,
    }),
  );
}

function parseCleanArgs(args: string[]): { yes: boolean } {
  let yes = false;
  for (const arg of args) {
    if (arg === "--yes") {
      yes = true;
      continue;
    }
    throw new Error(`clean does not accept argument: ${arg}`);
  }
  return { yes };
}

async function confirmClean(): Promise<boolean> {
  const prompt = createInterface({ input: stdin, output: stdout });
  try {
    const answer = await prompt.question("This will remove global toss integrations. Continue? [y/N] ");
    const normalized = answer.trim().toLowerCase();
    return normalized === "y" || normalized === "yes";
  } finally {
    prompt.close();
  }
}

async function runClean(args: string[]): Promise<void> {
  const parsed = parseCleanArgs(args);
  const isInteractiveTty = canUseInteractivePrompt(stdin.isTTY === true, stdout.isTTY === true);
  if (!isInteractiveTty && !parsed.yes) {
    throw new Error("clean requires --yes in non-interactive mode.");
  }
  if (!parsed.yes) {
    const confirmed = await confirmClean();
    if (!confirmed) {
      console.log("clean cancelled");
      return;
    }
  }
  const result = await cleanSkills();
  console.log(
    renderCleanResult({
      files: result.files,
      useColor: stdout.isTTY === true,
    }),
  );
}

function parseSinglePlanRef(command: "plan" | "apply", args: string[]): string {
  if (args.length === 0) {
    throw new Error(`${command} requires <file|->`);
  }
  if (args.length > 1) {
    throw new Error(`${command} accepts exactly one <file|-> argument`);
  }
  const planRef = args[0]!;
  if (planRef.startsWith("--")) {
    throw new Error(`${command} does not accept option arguments. Use: toss ${command} <file|->`);
  }
  return planRef;
}

async function runApply(args: string[]): Promise<void> {
  const plan = parseSinglePlanRef("apply", args);
  const commit = await applyPlan(plan);
  console.log(toJson({ status: "ok", commit: summarizeCommit(commit), operations: commit.operations.length }));
}

function parseSchemaArgs(args: string[]): { table?: string | undefined } {
  let table: string | undefined;
  for (let i = 0; i < args.length; i += 1) {
    const arg = args[i]!;
    if (arg === "--table") {
      table = args[i + 1];
      if (!table) {
        throw new Error("schema requires a value for --table");
      }
      i += 1;
      continue;
    }
    throw new Error(`schema does not accept argument: ${arg}`);
  }
  return { table };
}

function runSchema(args: string[]): void {
  const { table } = parseSchemaArgs(args);
  console.log(toJson(getSchema({ table })));
}

async function runPlan(args: string[]): Promise<void> {
  const planRef = parseSinglePlanRef("plan", args);
  const result = await planCheck(planRef);
  console.log(toJson(result));
  if (!result.ok) {
    process.exit(1);
  }
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
  console.log(`User Tables: ${status.tableCount}`);
  console.log(`Snapshots: ${status.snapshotCount}`);
  console.log(`Last Verified At: ${status.lastVerifiedAt ?? "never"}`);
  console.log(`Last Verified OK: ${status.lastVerifiedOk === null ? "unknown" : status.lastVerifiedOk ? "yes" : "no"}`);
  console.log(`Last Verified OK At: ${status.lastVerifiedOkAt ?? "never"}`);
  console.log(
    status.headCommit
      ? `HEAD: ${status.headCommit.commitId} (seq=${status.headCommit.seq}, kind=${status.headCommit.kind})`
      : "HEAD: none",
  );
  console.log(rows.length === 0 ? "(no user tables)" : printTable(rows));
}

function parseStudioArgs(args: string[]): { port?: number | undefined; open: boolean } {
  let port: number | undefined;
  let open = true;
  for (let i = 0; i < args.length; i += 1) {
    const arg = args[i]!;
    if (arg === "--port") {
      port = parseStudioPortArg(args[i + 1]);
      i += 1;
      continue;
    }
    if (arg === "--no-open") {
      open = false;
      continue;
    }
    throw new Error(`studio does not accept argument: ${arg}`);
  }
  return { port, open };
}

function runStudio(args: string[]): void {
  const parsed = parseStudioArgs(args);
  const started = startStudioServer({
    port: parsed.port,
    open: parsed.open,
  });
  console.log(`Studio: ${started.url}`);
  console.log("Press Ctrl+C to stop.");
}

function runHistory(args: string[]): void {
  const verbose = hasFlag(args, "--verbose");
  const invalidArgs = args.filter((arg) => arg !== "--verbose");
  if (invalidArgs.length > 0) {
    throw new Error("history accepts only --verbose");
  }
  const history = getHistory();
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
    case "clean":
      await runClean(rest);
      return;
    case "schema":
      runSchema(rest);
      return;
    case "plan":
      await runPlan(rest);
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
    case "studio":
      runStudio(rest);
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
