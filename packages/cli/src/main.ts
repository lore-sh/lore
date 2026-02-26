#!/usr/bin/env bun
import { CodedError, openDb, type ErrorCode } from "@lore/core";
import {
  getCommandHandler as getCommandHandlerFromManifest,
  usage as usageFromManifest,
} from "./commands/manifest";
import { VERSION } from "./version";

const CLI_HINTS: Partial<Record<ErrorCode, string>> = {
  SYNC_NOT_CONFIGURED: "Run `lore remote connect` to configure remote.",
  NOT_INITIALIZED: "Run `lore init` to initialize the database.",
};

export function usage(): string {
  return usageFromManifest();
}

export async function runCli(args: string[]): Promise<void> {
  const command = args[0];
  if (!command || command === "--help" || command === "-h") {
    console.log(usage());
    return;
  }
  if (command === "--version" || command === "-v") {
    console.log(VERSION);
    return;
  }
  const handler = getCommandHandler(command);
  if (!handler) {
    throw new Error(`Unknown command: ${command}`);
  }
  const commandArgs = args.slice(1);
  if (handler.requiresDb) {
    const execute = handler.prepare(commandArgs);
    const db = openDb();
    try {
      return await execute(db);
    } finally {
      db.$client.close(false);
    }
  }
  const execute = handler.prepare(commandArgs);
  return execute();
}

export const getCommandHandler = getCommandHandlerFromManifest;

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
