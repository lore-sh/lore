#!/usr/bin/env bun
import { CodedError, openDb, type ErrorCode, type Database } from "@lore/core";
import { parseInitArgs, parseCleanArgs, runInit, runClean } from "./commands/init";
import { parseRemoteArgs, runRemote } from "./commands/remote";
import { parsePushArgs, runPush } from "./commands/push";
import { parsePullArgs, runPull } from "./commands/pull";
import { parseSyncArgs, runSync } from "./commands/sync";
import { parseCloneArgs, runClone } from "./commands/clone";
import { parseSchemaArgs, runSchema } from "./commands/schema";
import { parsePlanArgs, runPlan } from "./commands/plan";
import { parseApplyArgs, runApply } from "./commands/apply";
import { parseReadArgs, runRead } from "./commands/read";
import { parseStatusArgs, runStatus } from "./commands/status";
import { parseHistoryArgs, runHistory } from "./commands/history";
import { parseRevertArgs, runRevert } from "./commands/revert";
import { parseVerifyArgs, runVerify } from "./commands/verify";
import { parseRecoverArgs, runRecover } from "./commands/recover";
import { parseStudioArgs, runStudio } from "./commands/studio";

type DbCommandExecutor = (db: Database) => void | Promise<void>;
type CommandExecutor = () => void | Promise<void>;
type CommandDefinition = {
  requiresDb: boolean;
  prepareDb?: ((args: string[]) => DbCommandExecutor) | undefined;
  prepare?: ((args: string[]) => CommandExecutor) | undefined;
};

const CLI_HINTS: Partial<Record<ErrorCode, string>> = {
  SYNC_NOT_CONFIGURED: "Run `lore remote connect` to configure remote.",
  NOT_INITIALIZED: "Run `lore init` to initialize the database.",
};

export function usage(): string {
  return [
    "Lore CLI",
    "",
    "Commands:",
    "  lore init [--platforms <list>] [--no-skills] [--no-heartbeat] [--force-new] [--yes] [--json]",
    "  lore clean [--yes] [--json]",
    "  lore schema [<table>]",
    "  lore plan -f <file|->",
    "  lore apply -f <file|->",
    "  lore read --sql \"<SELECT...>\" [--json]",
    "  lore status [--json]",
    "  lore history [--verbose] [--json]",
    "  lore revert <commit_id>",
    "  lore verify [--full]",
    "  lore recover <commit_id>",
    "  lore remote connect",
    "  lore remote connect --platform <turso|libsql> --url <url> [--token <token>|--clear-token]",
    "  lore remote status",
    "  lore push",
    "  lore pull",
    "  lore sync",
    "  lore clone <url> --platform <turso|libsql> [--force-new]",
    "  lore studio [--port <n>] [--no-open]",
    "",
    "Config Files:",
    "  ~/.lore/config.json        Remote connection settings",
    "  ~/.lore/credentials.json   Auth tokens (chmod 600)",
    "",
    "Environment:",
    "  TURSO_AUTH_TOKEN  Auth token fallback (CI)",
    "",
    "Init Platforms:",
    "  claude,cursor,codex,opencode,openclaw",
  ].join("\n");
}

const commands = new Map<string, CommandDefinition>([
  ["init", { requiresDb: false, prepare: (args) => { const parsed = parseInitArgs(args); return () => runInit(parsed); } }],
  ["clean", { requiresDb: false, prepare: (args) => { const parsed = parseCleanArgs(args); return () => runClean(parsed); } }],
  ["schema", { requiresDb: true, prepareDb: (args) => { const parsed = parseSchemaArgs(args); return (db) => runSchema(db, parsed); } }],
  ["plan", { requiresDb: true, prepareDb: (args) => { const parsed = parsePlanArgs(args); return (db) => runPlan(db, parsed); } }],
  ["apply", { requiresDb: true, prepareDb: (args) => { const parsed = parseApplyArgs(args); return (db) => runApply(db, parsed); } }],
  ["read", { requiresDb: true, prepareDb: (args) => { const parsed = parseReadArgs(args); return (db) => runRead(db, parsed); } }],
  ["status", { requiresDb: true, prepareDb: (args) => { const parsed = parseStatusArgs(args); return (db) => runStatus(db, parsed); } }],
  ["history", { requiresDb: true, prepareDb: (args) => { const parsed = parseHistoryArgs(args); return (db) => runHistory(db, parsed); } }],
  ["revert", { requiresDb: true, prepareDb: (args) => { const parsed = parseRevertArgs(args); return (db) => runRevert(db, parsed); } }],
  ["verify", { requiresDb: true, prepareDb: (args) => { const parsed = parseVerifyArgs(args); return (db) => runVerify(db, parsed); } }],
  ["recover", { requiresDb: false, prepare: (args) => { const parsed = parseRecoverArgs(args); return () => runRecover(parsed); } }],
  ["remote", { requiresDb: true, prepareDb: (args) => { const parsed = parseRemoteArgs(args); return (db) => runRemote(db, parsed); } }],
  ["push", { requiresDb: true, prepareDb: (args) => { parsePushArgs(args); return (db) => runPush(db); } }],
  ["pull", { requiresDb: true, prepareDb: (args) => { parsePullArgs(args); return (db) => runPull(db); } }],
  ["sync", { requiresDb: true, prepareDb: (args) => { parseSyncArgs(args); return (db) => runSync(db); } }],
  ["clone", { requiresDb: false, prepare: (args) => { const parsed = parseCloneArgs(args); return () => runClone(parsed); } }],
  ["studio", { requiresDb: false, prepare: (args) => { const parsed = parseStudioArgs(args); return () => runStudio(parsed); } }],
]);

export function getCommandHandler(command: string): CommandDefinition | null {
  return commands.get(command) ?? null;
}

export async function runCli(args: string[]): Promise<void> {
  const command = args[0];
  if (!command || command === "--help" || command === "-h") {
    console.log(usage());
    return;
  }
  const handler = getCommandHandler(command);
  if (!handler) {
    throw new Error(`Unknown command: ${command}`);
  }
  const commandArgs = args.slice(1);
  if (!handler.requiresDb) {
    if (!handler.prepare) {
      throw new Error(`Missing handler for command: ${command}`);
    }
    const execute = handler.prepare(commandArgs);
    return execute();
  }
  if (!handler.prepareDb) {
    throw new Error(`Missing database handler for command: ${command}`);
  }
  const executeDb = handler.prepareDb(commandArgs);
  const db = openDb();
  try {
    return await executeDb(db);
  } finally {
    db.$client.close(false);
  }
}

export function formatError(error: unknown): string {
  if (CodedError.is(error)) {
    const base = `Error [${error.code}]: ${error.message}`;
    const hint = CLI_HINTS[error.code];
    return hint ? `${base}\n  ${hint}` : base;
  }
  const message = error instanceof Error ? error.message : String(error);
  return `Error: ${message}`;
}

if (import.meta.main) {
  runCli(Bun.argv.slice(2)).catch((error: unknown) => {
    console.error(formatError(error));
    process.exit(1);
  });
}
