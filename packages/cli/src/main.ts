#!/usr/bin/env bun
import {
  autoSyncAfterApply,
  applyPlan,
  cloneFromRemote,
  cleanSkills,
  commitSizeWarning,
  connectRemote,
  getHistory,
  getRemoteStatus,
  getSchema,
  getStatus,
  initDatabase,
  isTossError,
  planCheck,
  pullFromRemote,
  pushToRemote,
  readQuery,
  recoverFromSnapshot,
  revertCommit,
  syncWithRemote,
  verifyDatabase,
} from "@toss/core";
import type { CommitEntry, RemotePlatform } from "@toss/core";
import { parseStudioPortArg, startStudioServer } from "@toss/studio";
import { stdin, stdout } from "node:process";
import { printTable, toJson } from "./format";
import {
  canUseConnectPrompt,
  platformName,
  promptRemoteConnect,
} from "./connect-ui";
import { parseRemoteConnectArgs } from "./remote-connect-args";
import {
  canUseInteractivePrompt,
  parseInitArgs,
  promptHeartbeat,
  promptPlatformSelection,
  renderCleanResult,
  renderInitResult,
  resolveInitSelection,
} from "./init-ui";
import { promptConfirm } from "./prompt-ui";

function usage(): string {
  return [
    "toss CLI",
    "",
    "Commands:",
    "  toss init [--platforms <list>] [--no-skills] [--no-heartbeat] [--force-new] [--yes] [--json]",
    "  toss clean [--yes] [--json]",
    "  toss schema [<table>]",
    "  toss plan <file|->",
    "  toss apply <file|->",
    "  toss read --sql \"<SELECT...>\" [--json]",
    "  toss status [--json]",
    "  toss history [--verbose] [--json]",
    "  toss revert <commit_id>",
    "  toss verify [--full]",
    "  toss recover <commit_id>",
    "  toss remote connect",
    "  toss remote connect --platform <turso|libsql> --url <url> [--token <token>|--clear-token]",
    "  toss remote status",
    "  toss push",
    "  toss pull",
    "  toss sync",
    "  toss clone <url> --platform <turso|libsql> [--force-new]",
    "  toss studio [--port <n>] [--no-open]",
    "",
    "Config Files:",
    "  ~/.toss/config.json        Remote connection settings",
    "  ~/.toss/credentials.json   Auth tokens (chmod 600)",
    "",
    "Environment:",
    "  TURSO_AUTH_TOKEN  Auth token fallback (CI)",
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

  let openclawHeartbeat = false;
  if (!parsed.noSkills && skillPlatforms.includes("openclaw")) {
    if (parsed.noHeartbeat) {
      openclawHeartbeat = false;
    } else {
      openclawHeartbeat = resolved.interactive ? await promptHeartbeat() : true;
    }
  }

  const result = await initDatabase({
    generateSkills: !parsed.noSkills,
    forceNew: parsed.forceNew,
    skillPlatforms,
    openclawHeartbeat,
  });
  if (parsed.json) {
    console.log(
      toJson({
        dbPath: result.dbPath,
        platforms: skillPlatforms,
        files: result.generatedSkills?.files.map((file) => file.path) ?? [],
      }),
    );
    return;
  }
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

function parseCleanArgs(args: string[]): { yes: boolean; json: boolean } {
  let yes = false;
  let json = false;
  for (const arg of args) {
    if (arg === "--yes") {
      yes = true;
      continue;
    }
    if (arg === "--json") {
      json = true;
      continue;
    }
    throw new Error(`clean does not accept argument: ${arg}`);
  }
  return { yes, json };
}

async function confirmClean(): Promise<boolean> {
  try {
    return await promptConfirm({
      title: "toss clean",
      message: "This will remove global toss integrations. Continue?",
      defaultValue: false,
      yesLabel: "Continue",
      noLabel: "Cancel",
      yesHint: "Remove shared skills and platform integration files.",
      noHint: "Keep current global toss integration state.",
      cancelMessage: "clean cancelled",
    });
  } catch (error) {
    if (error instanceof Error && error.message === "clean cancelled") {
      return false;
    }
    throw error;
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
  if (parsed.json) {
    console.log(
      toJson({
        removed: result.files.filter((file) => file.removed).length,
        files: result.files.map((file) => file.path),
      }),
    );
    return;
  }
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
  const sync = await autoSyncAfterApply();
  const warning = commitSizeWarning(commit.commitId);
  console.log(
    toJson({
      status: "ok",
      commit: summarizeCommit(commit),
      operations: commit.operations.length,
      sync,
      warnings: warning ? [warning] : [],
    }),
  );
}

function parseSchemaArgs(args: string[]): { table?: string | undefined } {
  if (args.length > 1) {
    throw new Error("schema accepts at most one <table> argument");
  }
  const table = args[0];
  if (table?.startsWith("--")) {
    throw new Error(`schema does not accept argument: ${table}`);
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
  console.log(`Last Verified OK: ${status.lastVerifiedOk === null ? "unknown" : status.lastVerifiedOk ? "yes" : "no"}`);
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

function parseRemoteStatusArgs(args: string[]): void {
  if (args.length > 0) {
    throw new Error("remote status does not accept arguments");
  }
}

async function runRemote(args: string[]): Promise<void> {
  const sub = args[0];
  const rest = args.slice(1);
  if (!sub) {
    throw new Error("remote requires subcommand: connect | status");
  }
  if (sub === "connect") {
    const parsed = parseRemoteConnectArgs(rest);
    let config: Awaited<ReturnType<typeof connectRemote>>;
    let authToken: string | null | undefined;
    if (parsed.interactive) {
      if (!canUseConnectPrompt(stdin.isTTY === true, stdout.isTTY === true)) {
        throw new Error("interactive terminal required. Use --platform --url [--token|--clear-token] for non-interactive mode.");
      }
      const input = await promptRemoteConnect();
      authToken = input.authToken;
      config = await connectRemote(input);
    } else {
      authToken = parsed.authToken;
      config = await connectRemote({
        platform: parsed.platform,
        url: parsed.url,
        authToken: parsed.authToken,
      });
    }
    console.log(`Connected to ${platformName(config.platform)} (${config.remoteDbName ?? "unknown"}).`);
    console.log("Config saved to ~/.toss/config.json");
    if (authToken !== undefined) {
      console.log("Credentials saved to ~/.toss/credentials.json");
    }
    return;
  }
  if (sub === "status") {
    parseRemoteStatusArgs(rest);
    const status = await getRemoteStatus();
    console.log(toJson(status));
    return;
  }
  throw new Error(`Unknown remote subcommand: ${sub}`);
}

async function runPush(args: string[]): Promise<void> {
  if (args.length > 0) {
    throw new Error("push does not accept arguments");
  }
  const result = await pushToRemote();
  console.log(toJson({ status: "ok", ...result }));
}

async function runPull(args: string[]): Promise<void> {
  if (args.length > 0) {
    throw new Error("pull does not accept arguments");
  }
  const result = await pullFromRemote();
  console.log(toJson({ status: "ok", ...result }));
}

async function runSync(args: string[]): Promise<void> {
  if (args.length > 0) {
    throw new Error("sync does not accept arguments");
  }
  const result = await syncWithRemote();
  console.log(toJson({ status: "ok", ...result }));
}

function parseCloneArgs(args: string[]): {
  platform: RemotePlatform;
  url: string;
  forceNew: boolean;
} {
  let platform: RemotePlatform | undefined;
  let url: string | undefined;
  let forceNew = false;
  for (let i = 0; i < args.length; i += 1) {
    const arg = args[i]!;
    if (arg === "--platform") {
      const value = args[i + 1];
      if (!value) {
        throw new Error("clone requires value for --platform");
      }
      if (value !== "turso" && value !== "libsql") {
        throw new Error(`clone does not accept --platform=${value}. Use turso or libsql.`);
      }
      platform = value;
      i += 1;
      continue;
    }
    if (arg.startsWith("--platform=")) {
      const value = arg.slice("--platform=".length);
      if (value !== "turso" && value !== "libsql") {
        throw new Error(`clone does not accept --platform=${value}. Use turso or libsql.`);
      }
      platform = value;
      continue;
    }
    if (arg === "--force-new") {
      forceNew = true;
      continue;
    }
    if (arg.startsWith("--")) {
      throw new Error(`clone does not accept argument: ${arg}`);
    }
    if (!url) {
      url = arg;
      continue;
    }
    throw new Error(`clone does not accept argument: ${arg}`);
  }
  if (!url) {
    throw new Error("clone requires <url>");
  }
  if (!platform) {
    throw new Error("clone requires --platform <turso|libsql>");
  }
  return { platform, url, forceNew };
}

async function runClone(args: string[]): Promise<void> {
  const parsed = parseCloneArgs(args);
  const result = await cloneFromRemote(parsed);
  console.log(toJson({ status: "ok", db_path: result.dbPath, sync: result.sync }));
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
    case "remote":
      await runRemote(rest);
      return;
    case "push":
      await runPush(rest);
      return;
    case "pull":
      await runPull(rest);
      return;
    case "sync":
      await runSync(rest);
      return;
    case "clone":
      await runClone(rest);
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
