#!/usr/bin/env bun
import { CodedError, type ErrorCode } from "@toss/core";
import { runInit, runClean } from "./commands/init";
import { runRemote, runPush, runPull, runSync, runClone } from "./commands/remote";
import { runSchema, runPlan, runApply, runRead } from "./commands/data";
import { runStatus, runHistory, runRevert, runVerify, runRecover } from "./commands/local";
import { runStudio } from "./commands/studio";

type CommandHandler = (args: string[]) => void | Promise<void>;

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

const commands = new Map<string, CommandHandler>([
  ["init", runInit],
  ["clean", runClean],
  ["schema", runSchema],
  ["plan", runPlan],
  ["apply", runApply],
  ["read", runRead],
  ["status", runStatus],
  ["history", runHistory],
  ["revert", runRevert],
  ["verify", runVerify],
  ["recover", runRecover],
  ["remote", runRemote],
  ["push", runPush],
  ["pull", runPull],
  ["sync", runSync],
  ["clone", runClone],
  ["studio", runStudio],
]);

export function getCommandHandler(command: string): CommandHandler | null {
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
  return handler(args.slice(1));
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
