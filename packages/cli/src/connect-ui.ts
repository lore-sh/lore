import type { RemotePlatform } from "@toss/core";
import { clearLine, clearScreenDown, cursorTo, emitKeypressEvents, moveCursor } from "node:readline";
import { createInterface } from "node:readline/promises";
import { stdin, stdout } from "node:process";
import { promptConfirm } from "./prompt-ui";

export interface ConnectInput {
  platform: RemotePlatform;
  url: string;
  authToken?: string | null | undefined;
  autoSync: boolean;
}

interface RadioOption<T extends string> {
  id: T;
  label: string;
  hint: string;
}

const PLATFORM_OPTIONS: Array<RadioOption<RemotePlatform>> = [
  { id: "turso", label: "Turso (libSQL)", hint: "Managed Turso database" },
  { id: "libsql", label: "Other libSQL endpoint", hint: "Self-hosted or non-Turso libSQL URL" },
];

export type LibsqlAuthMode = "keep" | "set" | "clear";

const LIBSQL_AUTH_OPTIONS: Array<RadioOption<LibsqlAuthMode>> = [
  { id: "keep", label: "Keep saved token", hint: "Use currently saved libSQL token, if any" },
  { id: "set", label: "Set new token", hint: "Provide and store a new libSQL token" },
  { id: "clear", label: "No token", hint: "Clear saved libSQL token and connect without auth" },
];

export type RadioKey = "up" | "down" | "enter" | "cancel";
export interface RadioState {
  cursor: number;
}

export function canUseConnectPrompt(stdinIsTty: boolean, stdoutIsTty: boolean): boolean {
  return stdinIsTty && stdoutIsTty;
}

function colorEnabled(): boolean {
  return stdout.isTTY && process.env.NO_COLOR !== "1" && process.env.TERM !== "dumb";
}

function style(text: string, code: string, enabled: boolean): string {
  if (!enabled) {
    return text;
  }
  return `\x1B[${code}m${text}\x1B[0m`;
}

function radioMark(selected: boolean, withColor: boolean): string {
  if (selected) {
    return style("(*)", "32;1", withColor);
  }
  return style("( )", "37;2", withColor);
}

function renderRadioPrompt<T extends string>(
  title: string,
  subtitle: string,
  state: RadioState,
  options: ReadonlyArray<RadioOption<T>>,
  withColor: boolean,
): string {
  const lines = [
    style(title, "1;36", withColor),
    subtitle,
    "Keys: Up/Down move | Enter confirm | Ctrl+C cancel",
    "",
  ];

  for (let index = 0; index < options.length; index += 1) {
    const option = options[index];
    if (!option) {
      continue;
    }
    const pointer = index === state.cursor ? style(">>", "1;36", withColor) : "  ";
    const label = index === state.cursor ? style(option.label, "1", withColor) : option.label;
    const hint = style(option.hint, "2", withColor);
    lines.push(`${pointer} ${radioMark(index === state.cursor, withColor)} ${label}`);
    lines.push(`   ${hint}`);
  }

  return lines.join("\n");
}

export function createRadioState(cursor = 0, optionCount = PLATFORM_OPTIONS.length): RadioState {
  if (optionCount <= 0) {
    return { cursor: 0 };
  }
  if (cursor < 0 || cursor >= optionCount) {
    return { cursor: 0 };
  }
  return { cursor };
}

export function reduceRadioState(state: RadioState, key: RadioKey, optionCount = PLATFORM_OPTIONS.length): RadioState {
  if (optionCount <= 0) {
    return state;
  }
  switch (key) {
    case "up":
      return { cursor: state.cursor === 0 ? optionCount - 1 : state.cursor - 1 };
    case "down":
      return { cursor: state.cursor === optionCount - 1 ? 0 : state.cursor + 1 };
    case "enter":
    case "cancel":
      return state;
  }
}

async function promptRadioSelection<T extends string>(options: {
  title: string;
  subtitle: string;
  options: ReadonlyArray<RadioOption<T>>;
  cancelMessage: string;
}): Promise<T> {
  if (!stdin.isTTY || !stdout.isTTY) {
    throw new Error("interactive selection requires a TTY");
  }

  let state = createRadioState(0, options.options.length);
  let renderedLines = 0;
  const withColor = colorEnabled();

  const redraw = (): void => {
    if (renderedLines > 0) {
      if (renderedLines > 1) {
        moveCursor(stdout, 0, -(renderedLines - 1));
      }
      cursorTo(stdout, 0);
    }

    const lines = renderRadioPrompt(options.title, options.subtitle, state, options.options, withColor).split("\n");
    for (let index = 0; index < lines.length; index += 1) {
      const line = lines[index] ?? "";
      clearLine(stdout, 0);
      cursorTo(stdout, 0);
      stdout.write(line);
      if (index < lines.length - 1) {
        stdout.write("\n");
      }
    }
    renderedLines = lines.length;
    clearScreenDown(stdout);
  };

  emitKeypressEvents(stdin);
  stdin.setRawMode(true);
  stdin.resume();
  stdout.write("\x1B[?25l");
  stdout.write("\n");
  redraw();

  return await new Promise<T>((resolve, reject) => {
    const cleanup = (): void => {
      stdin.off("keypress", onKeypress);
      if (stdin.isRaw) {
        stdin.setRawMode(false);
      }
      stdin.pause();
      stdout.write("\x1B[?25h");
      stdout.write("\x1B[0m");
      stdout.write("\n");
    };

    const onKeypress = (_input: string, key: { name?: string; ctrl?: boolean }): void => {
      let action: RadioKey | null = null;
      if (key.ctrl && key.name === "c") {
        action = "cancel";
      } else if (key.name === "up") {
        action = "up";
      } else if (key.name === "down") {
        action = "down";
      } else if (key.name === "return") {
        action = "enter";
      }
      if (!action) {
        return;
      }

      if (action === "cancel") {
        cleanup();
        reject(new Error(options.cancelMessage));
        return;
      }

      if (action === "enter") {
        const selected = options.options[state.cursor];
        if (!selected) {
          stdout.write("\x07");
          return;
        }
        cleanup();
        resolve(selected.id);
        return;
      }

      state = reduceRadioState(state, action, options.options.length);
      redraw();
    };

    stdin.on("keypress", onKeypress);
  });
}

async function promptPlatformSelection(): Promise<RemotePlatform> {
  return await promptRadioSelection({
    title: "toss remote connect",
    subtitle: "Select platform.",
    options: PLATFORM_OPTIONS,
    cancelMessage: "remote connect cancelled",
  });
}

async function promptLibsqlAuthMode(): Promise<LibsqlAuthMode> {
  return await promptRadioSelection({
    title: "toss remote connect",
    subtitle: "Choose auth token handling for this libSQL endpoint.",
    options: LIBSQL_AUTH_OPTIONS,
    cancelMessage: "remote connect cancelled",
  });
}

function normalizeRequired(input: string, label: string): string {
  const normalized = input.trim();
  if (normalized.length === 0) {
    throw new Error(`${label} is required.`);
  }
  return normalized;
}

export function resolveLibsqlAuthToken(mode: LibsqlAuthMode, rawToken?: string): string | null | undefined {
  if (mode === "keep") {
    return undefined;
  }
  if (mode === "clear") {
    return null;
  }
  return normalizeRequired(rawToken ?? "", "Auth token");
}

export function platformName(platform: RemotePlatform): string {
  return platform === "turso" ? "Turso" : "libSQL";
}

export async function promptRemoteConnect(): Promise<ConnectInput> {
  const platform = await promptPlatformSelection();
  const prompt = createInterface({ input: stdin, output: stdout });
  let url = "";
  let authToken: string | null | undefined;
  try {
    const urlLabel = platform === "turso" ? "? Turso database URL: " : "? libSQL endpoint URL: ";
    url = normalizeRequired(await prompt.question(urlLabel), "Remote URL");

    if (platform === "turso") {
      const rawToken = await prompt.question("? Auth token (paste, then Enter): ");
      authToken = normalizeRequired(rawToken, "Auth token");
    }
  } finally {
    prompt.close();
  }

  if (platform === "libsql") {
    const mode = await promptLibsqlAuthMode();
    if (mode === "set") {
      const tokenPrompt = createInterface({ input: stdin, output: stdout });
      try {
        const rawToken = await tokenPrompt.question("? Auth token (paste, then Enter): ");
        authToken = resolveLibsqlAuthToken(mode, rawToken);
      } finally {
        tokenPrompt.close();
      }
    } else {
      authToken = resolveLibsqlAuthToken(mode);
    }
  }

  const autoSync = await promptConfirm({
    title: "toss remote connect",
    message: "Enable auto-sync after apply?",
    defaultValue: true,
    yesLabel: "Enable",
    noLabel: "Disable",
    yesHint: "Run pull+push automatically after each apply.",
    noHint: "Sync only when you run push/pull/sync manually.",
    cancelMessage: "remote connect cancelled",
  });

  return {
    platform,
    url,
    authToken,
    autoSync,
  };
}
