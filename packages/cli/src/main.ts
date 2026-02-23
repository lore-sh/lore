#!/usr/bin/env bun
import { CodedError, openDb, type ErrorCode, type Database } from "@toss/core";
import { runInit, runClean } from "./commands/init";
import {
  runRemote,
  runPush,
  runPull,
  runSync,
  runClone,
  validatePullArgs,
  validatePushArgs,
  validateRemoteArgs,
  validateSyncArgs,
} from "./commands/remote";
import {
  runSchema,
  runPlan,
  runApply,
  runRead,
  validateApplyArgs,
  validatePlanArgs,
  validateReadArgs,
  validateSchemaArgs,
} from "./commands/data";
import {
  runStatus,
  runHistory,
  runRevert,
  runVerify,
  runRecover,
  validateHistoryArgs,
  validateRecoverArgs,
  validateRevertArgs,
  validateStatusArgs,
  validateVerifyArgs,
} from "./commands/local";
import { runStudio } from "./commands/studio";

type DbCommandHandler = (db: Database, args: string[]) => void | Promise<void>;
type CommandHandler = (args: string[]) => void | Promise<void>;
type ArgsValidator = (args: string[]) => void;
type CommandDefinition = {
  requiresDb: boolean;
  dbHandler?: DbCommandHandler | undefined;
  handler?: CommandHandler | undefined;
  validateArgs?: ArgsValidator | undefined;
};

const CLI_HINTS: Partial<Record<ErrorCode, string>> = {
  SYNC_NOT_CONFIGURED: "Run `toss remote connect` to configure remote.",
  NOT_INITIALIZED: "Run `toss init` to initialize the database.",
};

export function usage(): string {
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

const commands = new Map<string, CommandDefinition>([
  ["init", { requiresDb: false, handler: runInit }],
  ["clean", { requiresDb: false, handler: runClean }],
  ["schema", { requiresDb: true, dbHandler: runSchema, validateArgs: validateSchemaArgs }],
  ["plan", { requiresDb: true, dbHandler: runPlan, validateArgs: validatePlanArgs }],
  ["apply", { requiresDb: true, dbHandler: runApply, validateArgs: validateApplyArgs }],
  ["read", { requiresDb: true, dbHandler: runRead, validateArgs: validateReadArgs }],
  ["status", { requiresDb: true, dbHandler: runStatus, validateArgs: validateStatusArgs }],
  ["history", { requiresDb: true, dbHandler: runHistory, validateArgs: validateHistoryArgs }],
  ["revert", { requiresDb: true, dbHandler: runRevert, validateArgs: validateRevertArgs }],
  ["verify", { requiresDb: true, dbHandler: runVerify, validateArgs: validateVerifyArgs }],
  ["recover", { requiresDb: false, handler: runRecover, validateArgs: validateRecoverArgs }],
  ["remote", { requiresDb: true, dbHandler: runRemote, validateArgs: validateRemoteArgs }],
  ["push", { requiresDb: true, dbHandler: runPush, validateArgs: validatePushArgs }],
  ["pull", { requiresDb: true, dbHandler: runPull, validateArgs: validatePullArgs }],
  ["sync", { requiresDb: true, dbHandler: runSync, validateArgs: validateSyncArgs }],
  ["clone", { requiresDb: false, handler: runClone }],
  ["studio", { requiresDb: false, handler: runStudio }],
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
  handler.validateArgs?.(commandArgs);
  if (!handler.requiresDb) {
    if (!handler.handler) {
      throw new Error(`Missing handler for command: ${command}`);
    }
    return handler.handler(commandArgs);
  }
  if (!handler.dbHandler) {
    throw new Error(`Missing database handler for command: ${command}`);
  }
  const db = openDb();
  try {
    return await handler.dbHandler(db, commandArgs);
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
