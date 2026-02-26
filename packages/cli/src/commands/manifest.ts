import type { Database } from "@lore/core";
import { parseApplyArgs, runApply } from "./apply";
import { parseCloneArgs, runClone } from "./clone";
import { parseHistoryArgs, runHistory } from "./history";
import { parseCleanArgs, parseInitArgs, runClean, runInit } from "./init";
import { parsePlanArgs, runPlan } from "./plan";
import { parsePullArgs, runPull } from "./pull";
import { parsePushArgs, runPush } from "./push";
import { parseReadArgs, runRead } from "./read";
import { parseRecoverArgs, runRecover } from "./recover";
import { parseRemoteArgs, runRemote } from "./remote";
import { parseRevertArgs, runRevert } from "./revert";
import { parseSchemaArgs, runSchema } from "./schema";
import { parseStatusArgs, runStatus } from "./status";
import { parseStudioArgs, runStudio } from "./studio";
import { parseSyncArgs, runSync } from "./sync";
import { parseVerifyArgs, runVerify } from "./verify";

type DbCommandExecutor = (db: Database) => void | Promise<void>;
type CommandExecutor = () => void | Promise<void>;

interface BaseCommandDefinition {
  name: string;
  usage: string[];
}

interface NonDbCommandDefinition extends BaseCommandDefinition {
  requiresDb: false;
  prepare: (args: string[]) => CommandExecutor;
}

interface DbCommandDefinition extends BaseCommandDefinition {
  requiresDb: true;
  prepare: (args: string[]) => DbCommandExecutor;
}

export type CommandDefinition = NonDbCommandDefinition | DbCommandDefinition;

const COMMAND_MANIFEST: CommandDefinition[] = [
  {
    name: "init",
    usage: ["lore init [--platforms <list>] [--no-skills] [--no-heartbeat] [--force-new] [--yes] [--json]"],
    requiresDb: false,
    prepare: (args) => {
      const parsed = parseInitArgs(args);
      return () => runInit(parsed);
    },
  },
  {
    name: "clean",
    usage: ["lore clean [--yes] [--json]"],
    requiresDb: false,
    prepare: (args) => {
      const parsed = parseCleanArgs(args);
      return () => runClean(parsed);
    },
  },
  {
    name: "schema",
    usage: ["lore schema [<table>]"],
    requiresDb: true,
    prepare: (args) => {
      const parsed = parseSchemaArgs(args);
      return (db) => runSchema(db, parsed);
    },
  },
  {
    name: "plan",
    usage: ["lore plan -f <file|->"],
    requiresDb: true,
    prepare: (args) => {
      const parsed = parsePlanArgs(args);
      return (db) => runPlan(db, parsed);
    },
  },
  {
    name: "apply",
    usage: ["lore apply -f <file|->"],
    requiresDb: true,
    prepare: (args) => {
      const parsed = parseApplyArgs(args);
      return (db) => runApply(db, parsed);
    },
  },
  {
    name: "read",
    usage: ['lore read --sql "<SELECT...>" [--json]'],
    requiresDb: true,
    prepare: (args) => {
      const parsed = parseReadArgs(args);
      return (db) => runRead(db, parsed);
    },
  },
  {
    name: "status",
    usage: ["lore status [--json]"],
    requiresDb: true,
    prepare: (args) => {
      const parsed = parseStatusArgs(args);
      return (db) => runStatus(db, parsed);
    },
  },
  {
    name: "history",
    usage: ["lore history [--verbose] [--json]"],
    requiresDb: true,
    prepare: (args) => {
      const parsed = parseHistoryArgs(args);
      return (db) => runHistory(db, parsed);
    },
  },
  {
    name: "revert",
    usage: ["lore revert <commit_id>"],
    requiresDb: true,
    prepare: (args) => {
      const parsed = parseRevertArgs(args);
      return (db) => runRevert(db, parsed);
    },
  },
  {
    name: "verify",
    usage: ["lore verify [--full]"],
    requiresDb: true,
    prepare: (args) => {
      const parsed = parseVerifyArgs(args);
      return (db) => runVerify(db, parsed);
    },
  },
  {
    name: "recover",
    usage: ["lore recover <commit_id>"],
    requiresDb: false,
    prepare: (args) => {
      const parsed = parseRecoverArgs(args);
      return () => runRecover(parsed);
    },
  },
  {
    name: "remote",
    usage: [
      "lore remote connect",
      "lore remote connect --platform <turso|libsql> --url <url> [--token <token>|--clear-token]",
      "lore remote status",
    ],
    requiresDb: true,
    prepare: (args) => {
      const parsed = parseRemoteArgs(args);
      return (db) => runRemote(db, parsed);
    },
  },
  {
    name: "push",
    usage: ["lore push"],
    requiresDb: true,
    prepare: (args) => {
      parsePushArgs(args);
      return (db) => runPush(db);
    },
  },
  {
    name: "pull",
    usage: ["lore pull"],
    requiresDb: true,
    prepare: (args) => {
      parsePullArgs(args);
      return (db) => runPull(db);
    },
  },
  {
    name: "sync",
    usage: ["lore sync"],
    requiresDb: true,
    prepare: (args) => {
      parseSyncArgs(args);
      return (db) => runSync(db);
    },
  },
  {
    name: "clone",
    usage: ["lore clone <url> --platform <turso|libsql> [--force-new]"],
    requiresDb: false,
    prepare: (args) => {
      const parsed = parseCloneArgs(args);
      return () => runClone(parsed);
    },
  },
  {
    name: "studio",
    usage: ["lore studio [--port <n>] [--no-open]"],
    requiresDb: false,
    prepare: (args) => {
      const parsed = parseStudioArgs(args);
      return () => runStudio(parsed);
    },
  },
];

const COMMANDS = new Map(COMMAND_MANIFEST.map((command) => [command.name, command]));

export function getCommandHandler(command: string): CommandDefinition | null {
  return COMMANDS.get(command) ?? null;
}

export function usage(): string {
  return [
    "Lore CLI",
    "",
    "Commands:",
    ...COMMAND_MANIFEST.flatMap((command) => command.usage.map((line) => `  ${line}`)),
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
