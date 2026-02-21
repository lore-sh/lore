import { describe, expect, test } from "bun:test";
import {
  canUseInteractivePrompt,
  DEFAULT_INIT_PLATFORMS,
  parseCleanArgs,
  parseInitArgs,
  parsePlatformList,
  renderCleanResult,
  renderInitResult,
  resolveInitSelection,
} from "../../src/commands/init";

describe("init command", () => {
  test("canUseInteractivePrompt requires both stdin and stdout TTY", () => {
    expect(canUseInteractivePrompt(true, true)).toBe(true);
    expect(canUseInteractivePrompt(true, false)).toBe(false);
    expect(canUseInteractivePrompt(false, true)).toBe(false);
  });

  test("parsePlatformList supports aliases and de-duplicates", () => {
    const parsed = parsePlatformList("claude code,codex cli,cursor,codex");
    expect(parsed).toEqual(["claude", "codex", "cursor"]);
  });

  test("parseInitArgs parses init flags", () => {
    const parsed = parseInitArgs(["--force-new", "--yes", "--no-heartbeat", "--json", "--platforms", "opencode,openclaw"]);
    expect(parsed.forceNew).toBe(true);
    expect(parsed.yes).toBe(true);
    expect(parsed.noHeartbeat).toBe(true);
    expect(parsed.json).toBe(true);
    expect(parsed.platforms).toEqual(["opencode", "openclaw"]);
  });

  test("resolveInitSelection requires --yes in non-tty mode", () => {
    const parsed = parseInitArgs([]);
    expect(() => resolveInitSelection(parsed, false)).toThrow(
      "init requires --yes in non-interactive mode. Use --yes --platforms <list>.",
    );
  });

  test("resolveInitSelection accepts --platforms in non-tty mode without --yes", () => {
    const parsed = parseInitArgs(["--platforms", "codex,cursor"]);
    expect(resolveInitSelection(parsed, false)).toEqual({
      interactive: false,
      platforms: ["codex", "cursor"],
    });
  });

  test("resolveInitSelection defaults to all platforms with --yes", () => {
    const parsed = parseInitArgs(["--yes"]);
    const resolved = resolveInitSelection(parsed, false);
    expect(resolved.interactive).toBe(false);
    expect(resolved.platforms).toEqual(DEFAULT_INIT_PLATFORMS);
  });

  test("resolveInitSelection allows --no-skills in non-tty mode without --yes", () => {
    const parsed = parseInitArgs(["--no-skills"]);
    const resolved = resolveInitSelection(parsed, false);
    expect(resolved.interactive).toBe(false);
    expect(resolved.platforms).toEqual([]);
  });

  test("renderInitResult prints generated summary", () => {
    const text = renderInitResult({
      dbPath: "/tmp/work/toss.db",
      forceNew: true,
      selectedPlatforms: ["codex", "cursor"],
      noSkills: false,
      useColor: false,
      generatedSkills: {
        canonicalSkillPath: "/tmp/work/.agents/skills/toss/SKILL.md",
        files: [
          { platform: "shared", path: "/tmp/work/AGENTS.md" },
          { platform: "cursor", path: "/tmp/work/.cursor/rules/toss.mdc" },
        ],
      },
    });
    expect(text.includes("toss init complete")).toBe(true);
    expect(text.includes("Platforms: Codex CLI, Cursor")).toBe(true);
    expect(text.includes("Generated files")).toBe(true);
    expect(text.includes("Shared")).toBe(true);
    expect(text.includes("Cursor")).toBe(true);
  });

  test("renderInitResult prints skipped summary", () => {
    const text = renderInitResult({
      dbPath: "/tmp/work/toss.db",
      forceNew: false,
      selectedPlatforms: [],
      noSkills: true,
      useColor: false,
      generatedSkills: null,
    });
    expect(text.includes("Skill generation skipped")).toBe(true);
  });

  test("renderCleanResult prints cleanup summary", () => {
    const text = renderCleanResult({
      useColor: false,
      files: [
        { platform: "shared", path: "/tmp/home/.agents/skills/toss", removed: true },
        { platform: "cursor", path: "/tmp/home/.cursor/rules/toss.mdc", removed: false },
      ],
    });
    expect(text.includes("toss clean complete")).toBe(true);
    expect(text.includes("Removed: 1/2")).toBe(true);
    expect(text.includes("Shared")).toBe(true);
    expect(text.includes("Cursor")).toBe(true);
  });

  test("parseCleanArgs rejects unknown arguments", () => {
    expect(() => parseCleanArgs(["--unknown"])).toThrow("clean does not accept argument: --unknown");
  });

  test("parseCleanArgs accepts --yes and --json", () => {
    expect(parseCleanArgs(["--yes", "--json"])).toEqual({ yes: true, json: true });
  });
});
