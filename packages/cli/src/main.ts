#!/usr/bin/env bun
import { isTossError } from "@toss/core";
import { runInit, runClean } from "./commands/init";
import { runRemote, runPush, runPull, runSync, runClone } from "./commands/remote";
import { runSchema, runPlan, runApply, runRead } from "./commands/data";
import { runStatus, runHistory, runRevert, runVerify, runRecover } from "./commands/local";
import { runStudio } from "./commands/studio";

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
