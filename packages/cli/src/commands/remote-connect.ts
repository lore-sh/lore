import { stdin, stdout } from "node:process";
import { createInterface } from "node:readline/promises";
import { CodedError, connect, type Database, validateRemoteUrl } from "@lore/core";
import { z } from "zod";
import { parseCliArgs } from "../parse";
import { promptRadioSelection, type RadioOption } from "../prompts/radio";

export const RemotePlatformSchema = z.enum(["turso", "libsql"]);
export const ParsedRemoteConnectArgsSchema = z.discriminatedUnion("interactive", [
  z.object({ interactive: z.literal(true) }),
  z.object({
    interactive: z.literal(false),
    platform: RemotePlatformSchema,
    url: z.string().trim().min(1),
    authToken: z.string().trim().min(1).nullable().optional(),
  }),
]);

const PLATFORM_OPTIONS: Array<RadioOption<z.infer<typeof RemotePlatformSchema>>> = [
  { id: "turso", label: "Turso (libSQL)", hint: "Managed Turso database" },
  { id: "libsql", label: "Other libSQL endpoint", hint: "Self-hosted or non-Turso libSQL URL" },
];

function parsePlatformValue(value: string): z.infer<typeof RemotePlatformSchema> {
  const parsed = RemotePlatformSchema.safeParse(value);
  if (!parsed.success) {
    throw new Error(`remote connect does not accept --platform=${value}. Use turso or libsql.`);
  }
  return parsed.data;
}

export function parseRemoteConnectArgs(args: string[]): z.infer<typeof ParsedRemoteConnectArgsSchema> {
  const parsed = parseCliArgs(args, {
    options: {
      platform: { type: "string" },
      url: { type: "string" },
      token: { type: "string" },
      "clear-token": { type: "boolean" },
    },
  });
  const platformRawValue = parsed.values.platform;
  const urlValue = parsed.values.url;
  const tokenValue = parsed.values.token;
  const platformRaw = platformRawValue === undefined ? undefined : z.string().parse(platformRawValue);
  const url = urlValue === undefined ? undefined : z.string().parse(urlValue);
  const token = tokenValue === undefined ? undefined : z.string().parse(tokenValue);
  const clearToken = parsed.values["clear-token"] ?? false;

  if (token !== undefined && clearToken) {
    throw new Error("remote connect does not allow --token with --clear-token.");
  }

  const hasNonInteractiveArg = platformRaw !== undefined || url !== undefined || token !== undefined || clearToken;
  if (!hasNonInteractiveArg) {
    return ParsedRemoteConnectArgsSchema.parse({ interactive: true });
  }
  if (platformRaw === undefined || url === undefined) {
    throw new Error("remote connect non-interactive mode requires --platform <turso|libsql> and --url <url>.");
  }
  const platform = parsePlatformValue(platformRaw);
  return ParsedRemoteConnectArgsSchema.parse({
    interactive: false,
    platform,
    url: normalizeRemoteUrl(url),
    authToken: clearToken ? null : token,
  });
}

export function canUseConnectPrompt(stdinIsTty: boolean, stdoutIsTty: boolean): boolean {
  return stdinIsTty && stdoutIsTty;
}

export function platformName(platform: z.infer<typeof RemotePlatformSchema>): string {
  return platform === "turso" ? "Turso" : "libSQL";
}

function normalizeRequired(input: string, label: string): string {
  const normalized = input.trim();
  if (normalized.length === 0) {
    throw new Error(`${label} is required.`);
  }
  return normalized;
}

function normalizeRemoteUrl(url: string): string {
  try {
    return validateRemoteUrl(url);
  } catch (error) {
    if (CodedError.is(error)) {
      throw new Error(error.message);
    }
    throw error;
  }
}

function promptPlatformSelection(): Promise<z.infer<typeof RemotePlatformSchema>> {
  return promptRadioSelection({
    title: "Lore remote connect",
    subtitle: "Select platform.",
    options: PLATFORM_OPTIONS,
    cancelMessage: "remote connect cancelled",
  });
}

type EscapeMode = "none" | "esc" | "csi" | "ss3";

export function reduceMaskedInput(
  value: string,
  chunk: string,
  escapeMode: EscapeMode,
): {
  value: string;
  escapeMode: EscapeMode;
  submit: boolean;
  cancel: boolean;
} {
  let nextValue = value;
  let mode: EscapeMode = escapeMode;

  for (const char of chunk) {
    if (mode === "esc") {
      if (char === "[") {
        mode = "csi";
      } else if (char === "O") {
        mode = "ss3";
      } else {
        mode = "none";
      }
      continue;
    }
    if (mode === "csi") {
      const code = char.charCodeAt(0);
      if (code >= 0x40 && code <= 0x7e) {
        mode = "none";
      }
      continue;
    }
    if (mode === "ss3") {
      mode = "none";
      continue;
    }

    if (char === "\u001b") {
      mode = "esc";
      continue;
    }
    if (char === "\u0003") {
      return { value: nextValue, escapeMode: mode, submit: false, cancel: true };
    }
    if (char === "\r" || char === "\n") {
      return { value: nextValue, escapeMode: mode, submit: true, cancel: false };
    }
    if (char === "\u007F" || char === "\b") {
      nextValue = nextValue.slice(0, -1);
      continue;
    }
    if (char >= " ") {
      nextValue += char;
    }
  }

  return { value: nextValue, escapeMode: mode, submit: false, cancel: false };
}

async function promptMaskedInput(question: string): Promise<string> {
  if (!stdin.isTTY || !stdout.isTTY || typeof stdin.setRawMode !== "function") {
    throw new Error("interactive terminal required for masked input");
  }

  stdout.write(question);
  const wasRaw = stdin.isRaw === true;
  stdin.resume();
  if (!wasRaw) {
    stdin.setRawMode(true);
  }

  return await new Promise<string>((resolve, reject) => {
    let value = "";
    let escapeMode: EscapeMode = "none";

    const finish = (result: string): void => {
      stdin.off("data", onData);
      if (!wasRaw && stdin.isRaw) {
        stdin.setRawMode(false);
      }
      stdin.pause();
      stdout.write("\n");
      resolve(result);
    };

    const fail = (error: Error): void => {
      stdin.off("data", onData);
      if (!wasRaw && stdin.isRaw) {
        stdin.setRawMode(false);
      }
      stdin.pause();
      stdout.write("\n");
      reject(error);
    };

    const onData = (chunk: Buffer | string): void => {
      const text = typeof chunk === "string" ? chunk : chunk.toString("utf8");
      const reduced = reduceMaskedInput(value, text, escapeMode);
      value = reduced.value;
      escapeMode = reduced.escapeMode;
      if (reduced.cancel) {
        fail(new Error("remote connect cancelled"));
        return;
      }
      if (reduced.submit) {
        finish(value);
        return;
      }
    };

    stdin.on("data", onData);
  });
}

export async function promptRemoteConnect() {
  const platform = await promptPlatformSelection();
  const prompt = createInterface({ input: stdin, output: stdout });
  let url = "";
  let tokenActionRaw = "";
  try {
    const urlLabel = platform === "turso" ? "? Turso database URL: " : "? libSQL endpoint URL: ";
    url = normalizeRemoteUrl(normalizeRequired(await prompt.question(urlLabel), "Remote URL"));
    tokenActionRaw = (await prompt.question("? Auth token action [keep|set|clear] (default: keep): "))
      .trim()
      .toLowerCase();
  } finally {
    prompt.close();
  }

  let authToken: string | null | undefined;
  if (tokenActionRaw.length === 0 || tokenActionRaw === "keep") {
    authToken = undefined;
  } else if (tokenActionRaw === "clear") {
    authToken = null;
  } else if (tokenActionRaw === "set") {
    authToken = normalizeRequired(await promptMaskedInput("? Auth token: "), "Auth token");
  } else {
    throw new Error("Auth token action must be one of: keep, set, clear.");
  }

  return {
    platform,
    url,
    authToken,
  };
}

export async function runRemoteConnect(db: Database, parsed: z.infer<typeof ParsedRemoteConnectArgsSchema>): Promise<void> {
  let config: Awaited<ReturnType<typeof connect>>;
  let authToken: string | null | undefined;
  if (parsed.interactive) {
    if (!canUseConnectPrompt(stdin.isTTY === true, stdout.isTTY === true)) {
      throw new Error("interactive terminal required. Use --platform --url [--token|--clear-token] for non-interactive mode.");
    }
    const input = await promptRemoteConnect();
    authToken = input.authToken;
    config = await connect(db, input);
  } else {
    authToken = parsed.authToken;
    config = await connect(db, {
      platform: parsed.platform,
      url: parsed.url,
      authToken: parsed.authToken,
    });
  }
  console.log(`Connected to ${platformName(config.platform)} (${config.remoteDbName ?? "unknown"}).`);
  console.log("Config saved to ~/.lore/config.json");
  if (authToken !== undefined) {
    console.log("Credentials saved to ~/.lore/credentials.json");
  }
}
