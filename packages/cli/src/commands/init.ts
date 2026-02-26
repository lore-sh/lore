import { relative, resolve } from "node:path";
import { stdin, stdout } from "node:process";
import type { SkillPlatform } from "@lore/core";
import { initDb } from "@lore/core";
import { z } from "zod";
import { cleanSkills, generateSkills } from "../skills";
import type { GeneratedPlatform } from "../skills";
import { parseCliArgs } from "../parse";
import { promptConfirm } from "../prompts/confirm";
import { promptMultiSelect } from "../prompts/multiselect";
import type { MultiSelectOption } from "../prompts/multiselect";
import { colorEnabled, style } from "../terminal";
import { toJson } from "../format";

export const DEFAULT_INIT_PLATFORMS: SkillPlatform[] = ["claude", "cursor", "codex", "opencode", "openclaw"];

export const PLATFORM_OPTIONS: Array<MultiSelectOption<SkillPlatform>> = [
  { id: "claude", label: "Claude Code", hint: "~/.claude/skills + ~/.claude/CLAUDE.md" },
  { id: "cursor", label: "Cursor", hint: "~/.cursor/rules/lore.mdc" },
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

const SkillPlatformSchema = z.enum(["claude", "cursor", "codex", "opencode", "openclaw"]);
const SkillPlatformListSchema = z.array(SkillPlatformSchema).min(1);
export const ParsedInitArgsSchema = z.object({
  noSkills: z.boolean(),
  forceNew: z.boolean(),
  yes: z.boolean(),
  noHeartbeat: z.boolean(),
  json: z.boolean(),
  platforms: SkillPlatformListSchema.nullable(),
});
export const ParsedCleanArgsSchema = z.object({
  yes: z.boolean(),
  json: z.boolean(),
});

export type ParsedInitArgs = z.infer<typeof ParsedInitArgsSchema>;

export interface ResolvedInitSelection {
  interactive: boolean;
  platforms: SkillPlatform[];
}

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

function normalizePlatformToken(input: string): string {
  return input.trim().toLowerCase().replace(/[\s_-]+/g, "");
}

export function canUseInteractivePrompt(stdinIsTTY: boolean, stdoutIsTTY: boolean): boolean {
  return stdinIsTTY && stdoutIsTTY;
}

function platformLabel(platform: GeneratedPlatform): string {
  if (platform === "shared") {
    return "Shared";
  }
  const option = PLATFORM_OPTIONS.find((item) => item.id === platform);
  return option?.label ?? platform;
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
  withColor: boolean,
): string[] {
  const filtered = files.filter((file) => file.platform === platform);
  if (filtered.length === 0) {
    return [];
  }
  const lines = [`  ${style(platformLabel(platform), "1", withColor)}`];
  for (const file of filtered) {
    lines.push(`    ${style("[✓]", "32;1", withColor)} ${displayPath(file.path)}`);
  }
  return lines;
}

export function renderInitResult(view: InitResultView): string {
  const withColor = view.useColor ?? colorEnabled();
  const ok = style("[✓]", "32;1", withColor);
  const info = style("[i]", "36;1", withColor);
  const lines = [style("Lore init complete", "1;36", withColor)];
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
  const groups: GeneratedPlatform[] = ["shared", ...PLATFORM_OPTIONS.map((opt) => opt.id)];
  for (const group of groups) {
    lines.push(...renderFileGroup(view.generatedSkills.files, group, withColor));
  }
  return lines.join("\n");
}

export function renderCleanResult(view: CleanResultView): string {
  const withColor = view.useColor ?? colorEnabled();
  const ok = style("[✓]", "32;1", withColor);
  const info = style("[i]", "36;1", withColor);
  const muted = style("[ ]", "37;2", withColor);
  const removedCount = view.files.filter((file) => file.removed).length;
  const lines = [style("Lore clean complete", "1;36", withColor)];
  lines.push(`${ok} Removed: ${removedCount}/${view.files.length}`);
  lines.push(style("Cleanup targets", "1", withColor));
  for (const file of view.files) {
    const mark = file.removed ? ok : muted;
    const label = platformLabel(file.platform);
    const note = file.removed ? "removed" : "not found";
    lines.push(`  ${mark} ${label}: ${displayPath(file.path)} ${style(`(${note})`, "2", withColor)}`);
  }
  lines.push(`${info} Global Lore integration state has been reset.`);
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

  return SkillPlatformListSchema.parse(result);
}

export function parseInitArgs(args: string[]): ParsedInitArgs {
  const parsed = parseCliArgs(args, {
    options: {
      "no-skills": { type: "boolean" },
      "force-new": { type: "boolean" },
      yes: { type: "boolean" },
      "no-heartbeat": { type: "boolean" },
      json: { type: "boolean" },
      platforms: { type: "string" },
    },
  });
  const platformsValue = parsed.values.platforms;
  const platforms = platformsValue === undefined ? null : parsePlatformList(z.string().parse(platformsValue));
  return ParsedInitArgsSchema.parse({
    noSkills: parsed.values["no-skills"] ?? false,
    forceNew: parsed.values["force-new"] ?? false,
    yes: parsed.values.yes ?? false,
    noHeartbeat: parsed.values["no-heartbeat"] ?? false,
    json: parsed.values.json ?? false,
    platforms,
  });
}

export function resolveInitSelection(parsed: ParsedInitArgs, isTty: boolean): ResolvedInitSelection {
  if (parsed.noSkills) {
    return { interactive: false, platforms: [] };
  }
  if (parsed.platforms) {
    return { interactive: false, platforms: parsed.platforms };
  }
  if (!isTty && !parsed.yes) {
    throw new Error("init requires --yes in non-interactive mode. Use --yes --platforms <list>.");
  }
  if (parsed.yes) {
    return { interactive: false, platforms: [...DEFAULT_INIT_PLATFORMS] };
  }
  return { interactive: true, platforms: [...DEFAULT_INIT_PLATFORMS] };
}

export function promptPlatformSelection(): Promise<SkillPlatform[]> {
  return promptMultiSelect({
    title: "Lore init - Platform Installer",
    subtitle: "Select target platforms and confirm to generate skill integrations.",
    keyHint: "Keys: Up/Down move | Space toggle checkbox | a all/none | Enter confirm | Ctrl+C cancel",
    options: PLATFORM_OPTIONS,
    selectedByDefault: true,
    cancelMessage: "init cancelled",
  });
}

export function promptHeartbeat(): Promise<boolean> {
  return promptConfirm({
    title: "Lore init - OpenClaw",
    message: "Enable OpenClaw heartbeat patrol for Lore data?",
    defaultValue: true,
    yesLabel: "Enable",
    noLabel: "Disable",
    yesHint: "Creates a heartbeat check script and scheduler scaffold.",
    noHint: "Skips heartbeat setup and keeps standard Lore skill output only.",
    cancelMessage: "init cancelled",
  });
}

export function parseCleanArgs(args: string[]): z.infer<typeof ParsedCleanArgsSchema> {
  const parsed = parseCliArgs(args, {
    options: {
      yes: { type: "boolean" },
      json: { type: "boolean" },
    },
  });
  return ParsedCleanArgsSchema.parse({
    yes: parsed.values.yes ?? false,
    json: parsed.values.json ?? false,
  });
}

async function confirmClean(): Promise<boolean> {
  try {
    return await promptConfirm({
      title: "Lore clean",
      message: "This will remove global Lore integrations. Continue?",
      defaultValue: false,
      yesLabel: "Continue",
      noLabel: "Cancel",
      yesHint: "Remove shared skills and platform integration files.",
      noHint: "Keep current global Lore integration state.",
      cancelMessage: "clean cancelled",
    });
  } catch (error) {
    if (error instanceof Error && error.message === "clean cancelled") {
      return false;
    }
    throw error;
  }
}

export async function runInit(parsed: ParsedInitArgs): Promise<void> {
  const isInteractiveTty = canUseInteractivePrompt(stdin.isTTY === true, stdout.isTTY === true);
  const resolved = resolveInitSelection(parsed, isInteractiveTty);
  const skillPlatforms = resolved.interactive ? await promptPlatformSelection() : resolved.platforms;

  let openclawHeartbeat = false;
  if (!parsed.noSkills && !parsed.noHeartbeat && skillPlatforms.includes("openclaw")) {
    openclawHeartbeat = resolved.interactive ? await promptHeartbeat() : true;
  }

  const { path } = await initDb({ forceNew: parsed.forceNew });
  const generatedSkills = !parsed.noSkills
    ? await generateSkills({ platforms: skillPlatforms, openclawHeartbeat })
    : null;
  if (parsed.json) {
    console.log(
      toJson({
        dbPath: path,
        platforms: skillPlatforms,
        files: generatedSkills?.files.map((file) => file.path) ?? [],
      }),
    );
    return;
  }
  console.log(
    renderInitResult({
      dbPath: path,
      forceNew: parsed.forceNew,
      selectedPlatforms: skillPlatforms,
      generatedSkills,
      noSkills: parsed.noSkills,
      useColor: stdout.isTTY === true,
    }),
  );
}

export async function runClean(parsed: z.infer<typeof ParsedCleanArgsSchema>): Promise<void> {
  const isInteractiveTty = canUseInteractivePrompt(stdin.isTTY === true, stdout.isTTY === true);
  if (!isInteractiveTty && !parsed.yes) {
    throw new Error("clean requires --yes in non-interactive mode.");
  }
  if (!parsed.yes) {
    const confirmed = await confirmClean();
    if (!confirmed) {
      console.log("clean cancelled");
      return;
    }
  }
  const result = await cleanSkills();
  if (parsed.json) {
    console.log(
      toJson({
        removed: result.files.filter((file) => file.removed).length,
        files: result.files.map((file) => file.path),
      }),
    );
    return;
  }
  console.log(
    renderCleanResult({
      files: result.files,
      useColor: stdout.isTTY === true,
    }),
  );
}
