import { describe, expect, test } from "bun:test";
import {
  canUseInteractivePrompt,
  DEFAULT_INIT_PLATFORMS,
  createMultiSelectState,
  hasAnySelected,
  parseInitArgs,
  parsePlatformList,
  renderCleanResult,
  renderInitResult,
  reduceMultiSelectState,
  resolveInitSelection,
} from "../src/init-ui";

describe("init-ui", () => {
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
    const parsed = parseInitArgs(["--force-new", "--yes", "--platforms", "opencode,openclaw"]);
    expect(parsed.forceNew).toBe(true);
    expect(parsed.yes).toBe(true);
    expect(parsed.platforms).toEqual(["opencode", "openclaw"]);
  });

  test("resolveInitSelection requires --yes in non-tty mode", () => {
    const parsed = parseInitArgs([]);
    expect(() => resolveInitSelection(parsed, false)).toThrow(
      "init requires --yes in non-interactive mode. Use --yes --platforms <list>.",
    );
  });

  test("resolveInitSelection still requires --yes when platforms are provided in non-tty mode", () => {
    const parsed = parseInitArgs(["--platforms", "codex,cursor"]);
    expect(() => resolveInitSelection(parsed, false)).toThrow(
      "init requires --yes in non-interactive mode. Use --yes --platforms <list>.",
    );
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

  test("multi-select reducer supports cursor move and toggles", () => {
    let state = createMultiSelectState(3, true);

    state = reduceMultiSelectState(state, "down").state;
    expect(state.cursor).toBe(1);

    state = reduceMultiSelectState(state, "space").state;
    expect(state.selected).toEqual([true, false, true]);

    state = reduceMultiSelectState(state, "toggle_all").state;
    expect(state.selected).toEqual([true, true, true]);

    state = reduceMultiSelectState(state, "toggle_all").state;
    expect(hasAnySelected(state)).toBe(false);

    const submit = reduceMultiSelectState(state, "enter");
    expect(submit.action).toBe("submit");
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
});
