import { clearLine, clearScreenDown, cursorTo, emitKeypressEvents, moveCursor } from "node:readline";
import { stdin, stdout } from "node:process";
import { relative, resolve } from "node:path";
import type { SkillPlatform } from "@toss/core";

export const DEFAULT_INIT_PLATFORMS: SkillPlatform[] = ["claude", "cursor", "codex", "opencode", "openclaw"];

export const PLATFORM_OPTIONS: Array<{ id: SkillPlatform; label: string; hint: string }> = [
  { id: "claude", label: "Claude Code", hint: "~/.claude/skills + ~/.claude/CLAUDE.md" },
  { id: "cursor", label: "Cursor", hint: "~/.cursor/rules/toss.mdc" },
  { id: "codex", label: "Codex CLI", hint: "~/.agents/skills + ~/.codex/AGENTS.md" },
  { id: "opencode", label: "OpenCode", hint: "~/.agents/skills + ~/.config/opencode/AGENTS.md" },
  { id: "openclaw", label: "OpenClaw", hint: "~/.openclaw/workspace/skills + ~/.openclaw/workspace/AGENTS.md" },
];

const PLATFORM_ALIASES: Record<string, SkillPlatform> = {
  claude: "claude",
  claudecode: "claude",
  cursor: "cursor",
  codex: "codex",
  codexcli: "codex",
  opencode: "opencode",
  openclaw: "openclaw",
};

export interface ParsedInitArgs {
  noSkills: boolean;
  forceNew: boolean;
  yes: boolean;
  platforms: SkillPlatform[] | null;
}

export interface ResolvedInitSelection {
  interactive: boolean;
  platforms: SkillPlatform[];
}

type GeneratedPlatform = SkillPlatform | "shared";

export interface InitGeneratedFile {
  platform: GeneratedPlatform;
  path: string;
}

export interface InitGeneratedSkills {
  canonicalSkillPath: string;
  files: InitGeneratedFile[];
}

export interface CleanedSkillFile {
  platform: GeneratedPlatform;
  path: string;
  removed: boolean;
}

export interface CleanResultView {
  files: CleanedSkillFile[];
  useColor?: boolean | undefined;
}

export interface InitResultView {
  dbPath: string;
  forceNew: boolean;
  selectedPlatforms: SkillPlatform[];
  generatedSkills: InitGeneratedSkills | null;
  noSkills: boolean;
  useColor?: boolean | undefined;
}

export type MultiSelectKey = "up" | "down" | "space" | "toggle_all" | "enter" | "cancel";
export type MultiSelectAction = "none" | "submit" | "cancel";

export interface MultiSelectState {
  cursor: number;
  selected: boolean[];
}

function normalizePlatformToken(input: string): string {
  return input.trim().toLowerCase().replace(/[\s_-]+/g, "");
}

export function canUseInteractivePrompt(stdinIsTTY: boolean, stdoutIsTTY: boolean): boolean {
  return stdinIsTTY && stdoutIsTTY;
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

function platformLabel(platform: GeneratedPlatform): string {
  if (platform === "shared") {
    return "Shared";
  }
  const option = PLATFORM_OPTIONS.find((item) => item.id === platform);
  return option?.label ?? platform;
}

function checkbox(selected: boolean, enabled: boolean): string {
  if (selected) {
    return style("[✓]", "32;1", enabled);
  }
  return style("[ ]", "37;2", enabled);
}

function displayPath(path: string): string {
  const cwd = process.cwd();
  const rel = relative(cwd, resolve(path));
  if (rel.length > 0 && !rel.startsWith("..") && !rel.startsWith("/")) {
    return `./${rel}`;
  }
  return path;
}

function joinPlatformLabels(platforms: SkillPlatform[]): string {
  return platforms.map(platformLabel).join(", ");
}

function renderFileGroup(
  files: InitGeneratedFile[],
  platform: GeneratedPlatform,
  label: string,
  withColor: boolean,
): string[] {
  const filtered = files.filter((file) => file.platform === platform);
  if (filtered.length === 0) {
    return [];
  }
  const lines = [`  ${style(label, "1", withColor)}`];
  for (const file of filtered) {
    lines.push(`    ${style("[✓]", "32;1", withColor)} ${displayPath(file.path)}`);
  }
  return lines;
}

export function renderInitResult(view: InitResultView): string {
  const withColor = view.useColor ?? colorEnabled();
  const ok = style("[✓]", "32;1", withColor);
  const info = style("[i]", "36;1", withColor);
  const lines = [style("toss init complete", "1;36", withColor)];
  lines.push(`${ok} Database: ${displayPath(view.dbPath)}`);

  if (view.forceNew) {
    lines.push(`${info} Reinitialized with clean-break history format.`);
  }

  if (view.noSkills || !view.generatedSkills) {
    lines.push(`${info} Skill generation skipped.`);
    return lines.join("\n");
  }

  lines.push(`${ok} Platforms: ${joinPlatformLabels(view.selectedPlatforms)}`);
  lines.push(`${ok} Canonical skill: ${displayPath(view.generatedSkills.canonicalSkillPath)}`);
  lines.push(style("Generated files", "1", withColor));
  lines.push(...renderFileGroup(view.generatedSkills.files, "shared", "Shared", withColor));
  lines.push(...renderFileGroup(view.generatedSkills.files, "claude", "Claude Code", withColor));
  lines.push(...renderFileGroup(view.generatedSkills.files, "cursor", "Cursor", withColor));
  lines.push(...renderFileGroup(view.generatedSkills.files, "codex", "Codex CLI", withColor));
  lines.push(...renderFileGroup(view.generatedSkills.files, "opencode", "OpenCode", withColor));
  lines.push(...renderFileGroup(view.generatedSkills.files, "openclaw", "OpenClaw", withColor));
  return lines.join("\n");
}

export function renderCleanResult(view: CleanResultView): string {
  const withColor = view.useColor ?? colorEnabled();
  const ok = style("[✓]", "32;1", withColor);
  const info = style("[i]", "36;1", withColor);
  const muted = style("[ ]", "37;2", withColor);
  const removedCount = view.files.filter((file) => file.removed).length;
  const lines = [style("toss clean complete", "1;36", withColor)];
  lines.push(`${ok} Removed: ${removedCount}/${view.files.length}`);
  lines.push(style("Cleanup targets", "1", withColor));
  for (const file of view.files) {
    const mark = file.removed ? ok : muted;
    const label = platformLabel(file.platform);
    const note = file.removed ? "removed" : "not found";
    lines.push(`  ${mark} ${label}: ${displayPath(file.path)} ${style(`(${note})`, "2", withColor)}`);
  }
  lines.push(`${info} Global toss integration state has been reset.`);
  return lines.join("\n");
}

export function parsePlatformList(value: string): SkillPlatform[] {
  const parts = value.split(",");
  const result: SkillPlatform[] = [];
  const seen = new Set<SkillPlatform>();

  for (const rawPart of parts) {
    const token = normalizePlatformToken(rawPart);
    if (token.length === 0) {
      throw new Error("platform list contains an empty value");
    }
    const platform = PLATFORM_ALIASES[token];
    if (!platform) {
      throw new Error(`unknown platform: ${rawPart.trim()}`);
    }
    if (!seen.has(platform)) {
      seen.add(platform);
      result.push(platform);
    }
  }

  if (result.length === 0) {
    throw new Error("at least one platform is required");
  }

  return result;
}

export function parseInitArgs(args: string[]): ParsedInitArgs {
  let noSkills = false;
  let forceNew = false;
  let yes = false;
  let platforms: SkillPlatform[] | null = null;

  for (let i = 0; i < args.length; i += 1) {
    const arg = args[i] ?? "";
    if (arg === "--no-skills") {
      noSkills = true;
      continue;
    }
    if (arg === "--force-new") {
      forceNew = true;
      continue;
    }
    if (arg === "--yes") {
      yes = true;
      continue;
    }
    if (arg === "--platforms") {
      const value = args[i + 1];
      if (!value) {
        throw new Error("init requires a value for --platforms");
      }
      platforms = parsePlatformList(value);
      i += 1;
      continue;
    }
    if (arg.startsWith("--platforms=")) {
      const value = arg.slice("--platforms=".length);
      platforms = parsePlatformList(value);
      continue;
    }
    throw new Error(`init does not accept argument: ${arg}`);
  }

  return { noSkills, forceNew, yes, platforms };
}

export function resolveInitSelection(parsed: ParsedInitArgs, isTty: boolean): ResolvedInitSelection {
  if (parsed.noSkills) {
    return { interactive: false, platforms: [] };
  }
  if (!isTty && !parsed.yes) {
    throw new Error("init requires --yes in non-interactive mode. Use --yes --platforms <list>.");
  }
  if (parsed.platforms) {
    return { interactive: false, platforms: parsed.platforms };
  }
  if (parsed.yes) {
    return { interactive: false, platforms: [...DEFAULT_INIT_PLATFORMS] };
  }
  return { interactive: true, platforms: [...DEFAULT_INIT_PLATFORMS] };
}

export function createMultiSelectState(count: number, selectedByDefault = true): MultiSelectState {
  return { cursor: 0, selected: Array.from({ length: count }, () => selectedByDefault) };
}

export function hasAnySelected(state: MultiSelectState): boolean {
  return state.selected.some(Boolean);
}

export function reduceMultiSelectState(
  state: MultiSelectState,
  key: MultiSelectKey,
): { state: MultiSelectState; action: MultiSelectAction } {
  if (state.selected.length === 0) {
    return { state, action: key === "cancel" ? "cancel" : "none" };
  }
  switch (key) {
    case "up": {
      const cursor = state.cursor === 0 ? state.selected.length - 1 : state.cursor - 1;
      return { state: { ...state, cursor }, action: "none" };
    }
    case "down": {
      const cursor = state.cursor === state.selected.length - 1 ? 0 : state.cursor + 1;
      return { state: { ...state, cursor }, action: "none" };
    }
    case "space": {
      const selected = [...state.selected];
      selected[state.cursor] = !selected[state.cursor];
      return { state: { ...state, selected }, action: "none" };
    }
    case "toggle_all": {
      const shouldSelectAll = state.selected.some((value) => !value);
      const selected = state.selected.map(() => shouldSelectAll);
      return { state: { ...state, selected }, action: "none" };
    }
    case "enter":
      return { state, action: "submit" };
    case "cancel":
      return { state, action: "cancel" };
  }
}

function renderPrompt(state: MultiSelectState, withColor: boolean): string {
  const selectedCount = state.selected.filter(Boolean).length;
  const lines = [
    style("toss init - Platform Installer", "1;36", withColor),
    "Select target platforms and confirm to generate skill integrations.",
    "Keys: Up/Down move | Space toggle checkbox | a all/none | Enter confirm | Ctrl+C cancel",
    style(`Selected: ${selectedCount}/${PLATFORM_OPTIONS.length}`, "1;33", withColor),
    "",
  ];

  for (let index = 0; index < PLATFORM_OPTIONS.length; index += 1) {
    const option = PLATFORM_OPTIONS[index];
    if (!option) {
      continue;
    }
    const stateBadge = checkbox(state.selected[index] === true, withColor);
    const pointer = index === state.cursor ? style(">>", "1;36", withColor) : "  ";
    const label = index === state.cursor ? style(option.label, "1", withColor) : option.label;
    const hint = style(option.hint, "2", withColor);
    lines.push(`${pointer} ${stateBadge} ${label}`);
    lines.push(`   ${hint}`);
  }

  return lines.join("\n");
}

function selectedPlatformsFromState(state: MultiSelectState): SkillPlatform[] {
  const selected: SkillPlatform[] = [];
  for (let index = 0; index < state.selected.length; index += 1) {
    if (!state.selected[index]) {
      continue;
    }
    const option = PLATFORM_OPTIONS[index];
    if (option) {
      selected.push(option.id);
    }
  }
  return selected;
}

export async function promptPlatformSelection(): Promise<SkillPlatform[]> {
  if (!stdin.isTTY || !stdout.isTTY) {
    throw new Error("interactive platform selection requires a TTY");
  }

  let state = createMultiSelectState(PLATFORM_OPTIONS.length, true);
  let renderedLines = 0;
  const withColor = colorEnabled();

  const redraw = (): void => {
    if (renderedLines > 0) {
      if (renderedLines > 1) {
        moveCursor(stdout, 0, -(renderedLines - 1));
      }
      cursorTo(stdout, 0);
    }

    const lines = renderPrompt(state, withColor).split("\n");
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

  return await new Promise<SkillPlatform[]>((resolve, reject) => {
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
      let keyAction: MultiSelectKey | null = null;
      if (key.ctrl && key.name === "c") {
        keyAction = "cancel";
      } else if (key.name === "up") {
        keyAction = "up";
      } else if (key.name === "down") {
        keyAction = "down";
      } else if (key.name === "space") {
        keyAction = "space";
      } else if (key.name === "return") {
        keyAction = "enter";
      } else if (key.name === "a") {
        keyAction = "toggle_all";
      }
      if (!keyAction) {
        return;
      }

      const reduced = reduceMultiSelectState(state, keyAction);
      state = reduced.state;
      if (reduced.action === "cancel") {
        cleanup();
        reject(new Error("init cancelled"));
        return;
      }
      if (reduced.action === "submit") {
        if (!hasAnySelected(state)) {
          stdout.write("\x07");
        } else {
          const selected = selectedPlatformsFromState(state);
          cleanup();
          resolve(selected);
          return;
        }
      }
      redraw();
    };

    stdin.on("keypress", onKeypress);
  });
}
