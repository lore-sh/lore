import { stdin, stdout } from "node:process";
import { createInterface } from "node:readline/promises";
import type { Database } from "bun:sqlite";
import type { RemotePlatform } from "@toss/core";
import {
  cloneFromRemote,
  connectRemote,
  getRemoteStatus,
  pullFromRemote,
  pushToRemote,
  syncWithRemote,
} from "@toss/core";
import { promptRadioSelection } from "../prompts/radio";
import type { RadioOption } from "../prompts/radio";
import { toJson } from "../format";

export interface ConnectInput {
  platform: RemotePlatform;
  url: string;
  authToken?: string | null | undefined;
}

export type ParsedRemoteConnectArgs = {
  interactive: true;
} | {
  interactive: false;
  platform: RemotePlatform;
  url: string;
  authToken?: string | null | undefined;
};

const PLATFORM_OPTIONS: Array<RadioOption<RemotePlatform>> = [
  { id: "turso", label: "Turso (libSQL)", hint: "Managed Turso database" },
  { id: "libsql", label: "Other libSQL endpoint", hint: "Self-hosted or non-Turso libSQL URL" },
];

function isPlatform(value: string): value is RemotePlatform {
  return value === "turso" || value === "libsql";
}

function parsePlatformValue(value: string): RemotePlatform {
  if (!isPlatform(value)) {
    throw new Error(`remote connect does not accept --platform=${value}. Use turso or libsql.`);
  }
  return value;
}

function consumeOption(
  args: string[],
  index: number,
  arg: string,
  name: string,
): { value: string; skip: number } | null {
  if (arg === name) {
    const value = args[index + 1];
    if (!value || value.startsWith("--")) {
      throw new Error(`remote connect requires a value for ${name}`);
    }
    return { value, skip: 1 };
  }
  const prefix = `${name}=`;
  if (arg.startsWith(prefix)) {
    const value = arg.slice(prefix.length);
    if (value.length === 0) {
      throw new Error(`remote connect requires a value for ${name}`);
    }
    return { value, skip: 0 };
  }
  return null;
}

export function parseRemoteConnectArgs(args: string[]): ParsedRemoteConnectArgs {
  let platform: RemotePlatform | undefined;
  let url: string | undefined;
  let token: string | undefined;
  let clearToken = false;

  for (let i = 0; i < args.length; i += 1) {
    const arg = args[i]!;
    const platformOpt = consumeOption(args, i, arg, "--platform");
    if (platformOpt) {
      platform = parsePlatformValue(platformOpt.value);
      i += platformOpt.skip;
      continue;
    }
    const urlOpt = consumeOption(args, i, arg, "--url");
    if (urlOpt) {
      url = urlOpt.value;
      i += urlOpt.skip;
      continue;
    }
    const tokenOpt = consumeOption(args, i, arg, "--token");
    if (tokenOpt) {
      token = tokenOpt.value;
      i += tokenOpt.skip;
      continue;
    }
    if (arg === "--clear-token") {
      clearToken = true;
      continue;
    }
    throw new Error(`remote connect does not accept argument: ${arg}`);
  }

  if (token !== undefined && clearToken) {
    throw new Error("remote connect does not allow --token with --clear-token.");
  }

  const hasNonInteractiveArg = platform !== undefined || url !== undefined || token !== undefined || clearToken;
  if (!hasNonInteractiveArg) {
    return { interactive: true };
  }
  if (!platform || !url) {
    throw new Error("remote connect non-interactive mode requires --platform <turso|libsql> and --url <url>.");
  }
  return { interactive: false, platform, url, authToken: clearToken ? null : token };
}

export function canUseConnectPrompt(stdinIsTty: boolean, stdoutIsTty: boolean): boolean {
  return stdinIsTty && stdoutIsTty;
}

export function platformName(platform: RemotePlatform): string {
  return platform === "turso" ? "Turso" : "libSQL";
}

function normalizeRequired(input: string, label: string): string {
  const normalized = input.trim();
  if (normalized.length === 0) {
    throw new Error(`${label} is required.`);
  }
  return normalized;
}

function promptPlatformSelection(): Promise<RemotePlatform> {
  return promptRadioSelection({
    title: "toss remote connect",
    subtitle: "Select platform.",
    options: PLATFORM_OPTIONS,
    cancelMessage: "remote connect cancelled",
  });
}

export async function promptRemoteConnect(): Promise<ConnectInput> {
  const platform = await promptPlatformSelection();
  const prompt = createInterface({ input: stdin, output: stdout });
  try {
    const urlLabel = platform === "turso" ? "? Turso database URL: " : "? libSQL endpoint URL: ";
    const url = normalizeRequired(await prompt.question(urlLabel), "Remote URL");
    const tokenActionRaw = (await prompt.question("? Auth token action [keep|set|clear] (default: keep): "))
      .trim()
      .toLowerCase();
    let authToken: string | null | undefined;
    if (tokenActionRaw.length === 0 || tokenActionRaw === "keep") {
      authToken = undefined;
    } else if (tokenActionRaw === "clear") {
      authToken = null;
    } else if (tokenActionRaw === "set") {
      authToken = normalizeRequired(await prompt.question("? Auth token: "), "Auth token");
    } else {
      throw new Error("Auth token action must be one of: keep, set, clear.");
    }
    return {
      platform,
      url,
      authToken,
    };
  } finally {
    prompt.close();
  }
}

function parseRemoteStatusArgs(args: string[]): void {
  if (args.length > 0) {
    throw new Error("remote status does not accept arguments");
  }
}

function parseNoArgs(command: "push" | "pull" | "sync", args: string[]): void {
  if (args.length > 0) {
    throw new Error(`${command} does not accept arguments`);
  }
}

export function parseClonePlatform(value: string): RemotePlatform {
  if (!isPlatform(value)) {
    throw new Error(`clone does not accept --platform=${value}. Use turso or libsql.`);
  }
  return value;
}

export function parseCloneArgs(args: string[]): {
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
      platform = parseClonePlatform(value);
      i += 1;
      continue;
    }
    if (arg.startsWith("--platform=")) {
      platform = parseClonePlatform(arg.slice("--platform=".length));
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

export function validateRemoteArgs(args: string[]): void {
  const sub = args[0];
  const rest = args.slice(1);
  if (!sub) {
    throw new Error("remote requires subcommand: connect | status");
  }
  if (sub === "connect") {
    parseRemoteConnectArgs(rest);
    return;
  }
  if (sub === "status") {
    parseRemoteStatusArgs(rest);
    return;
  }
  throw new Error(`Unknown remote subcommand: ${sub}`);
}

export function validatePushArgs(args: string[]): void {
  parseNoArgs("push", args);
}

export function validatePullArgs(args: string[]): void {
  parseNoArgs("pull", args);
}

export function validateSyncArgs(args: string[]): void {
  parseNoArgs("sync", args);
}

export async function runRemote(db: Database, args: string[]): Promise<void> {
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
      config = await connectRemote(db, input);
    } else {
      authToken = parsed.authToken;
      config = await connectRemote(db, {
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
    const status = await getRemoteStatus(db);
    console.log(toJson(status));
    return;
  }
  throw new Error(`Unknown remote subcommand: ${sub}`);
}

export async function runPush(db: Database, args: string[]): Promise<void> {
  parseNoArgs("push", args);
  const result = await pushToRemote(db);
  console.log(toJson({ status: "ok", ...result }));
}

export async function runPull(db: Database, args: string[]): Promise<void> {
  parseNoArgs("pull", args);
  const result = await pullFromRemote(db);
  console.log(toJson({ status: "ok", ...result }));
}

export async function runSync(db: Database, args: string[]): Promise<void> {
  parseNoArgs("sync", args);
  const result = await syncWithRemote(db);
  console.log(toJson({ status: "ok", ...result }));
}

export async function runClone(args: string[]): Promise<void> {
  const parsed = parseCloneArgs(args);
  const result = await cloneFromRemote(parsed);
  console.log(toJson({ status: "ok", db_path: result.dbPath, sync: result.sync }));
}
