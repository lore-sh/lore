import { describe, expect, test } from "bun:test";
import { chmod, mkdir } from "node:fs/promises";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { initDb } from "@lore/core";
import { cleanSkills, generateSkills } from "../src/skills";

interface GlobalEnvSnapshot {
  HOME?: string | undefined;
  CODEX_HOME?: string | undefined;
  XDG_CONFIG_HOME?: string | undefined;
}

function captureGlobalEnv(): GlobalEnvSnapshot {
  return {
    HOME: process.env.HOME,
    CODEX_HOME: process.env.CODEX_HOME,
    XDG_CONFIG_HOME: process.env.XDG_CONFIG_HOME,
  };
}

function restoreGlobalEnv(snapshot: GlobalEnvSnapshot): void {
  for (const key of ["HOME", "CODEX_HOME", "XDG_CONFIG_HOME"] as const) {
    if (snapshot[key] === undefined) {
      delete process.env[key];
    } else {
      process.env[key] = snapshot[key];
    }
  }
}

interface GlobalEnvPaths {
  home: string;
  codexHome: string;
  xdgConfigHome: string;
  opencodeHome: string;
  openclawWorkspace: string;
}

function createTmpDir(): string {
  return mkdtempSync(join(tmpdir(), "lore-cli-skill-test-"));
}

function withSkillEnv(fn: (paths: GlobalEnvPaths, dbPath: string) => Promise<void>) {
  return async () => {
    const dir = createTmpDir();
    const snapshot = captureGlobalEnv();
    const paths: GlobalEnvPaths = {
      home: join(dir, "home"),
      codexHome: join(dir, "codex-home"),
      xdgConfigHome: join(dir, "xdg-config"),
      opencodeHome: join(dir, "xdg-config", "opencode"),
      openclawWorkspace: join(dir, "home", ".openclaw", "workspace"),
    };
    process.env.HOME = paths.home;
    process.env.CODEX_HOME = paths.codexHome;
    process.env.XDG_CONFIG_HOME = paths.xdgConfigHome;
    const dbPath = join(dir, "lore.db");
    try {
      await initDb({ dbPath });
      await fn(paths, dbPath);
    } finally {
      restoreGlobalEnv(snapshot);
      rmSync(dir, { recursive: true, force: true });
    }
  };
}

function countOccurrences(text: string, pattern: string): number {
  let index = 0;
  let count = 0;
  while (index < text.length) {
    const found = text.indexOf(pattern, index);
    if (found < 0) break;
    count += 1;
    index = found + pattern.length;
  }
  return count;
}

describe("generateSkills", () => {
  test("generates only selected global platforms", withSkillEnv(async (paths) => {
    const result = await generateSkills({ platforms: ["codex", "cursor"] });
    expect(result.files.length).toBeGreaterThan(0);

    const sharedSkillPath = join(paths.home, ".agents", "skills", "lore", "SKILL.md");
    const codexAgentsPath = join(paths.codexHome, "AGENTS.md");
    const cursorRulePath = join(paths.home, ".cursor", "rules", "lore.mdc");
    const claudeSkillPath = join(paths.home, ".claude", "skills", "lore", "SKILL.md");

    expect(await Bun.file(sharedSkillPath).exists()).toBe(true);
    expect(await Bun.file(codexAgentsPath).exists()).toBe(true);
    expect(await Bun.file(cursorRulePath).exists()).toBe(true);
    expect(await Bun.file(claudeSkillPath).exists()).toBe(false);

    const sharedSkillText = await Bun.file(sharedSkillPath).text();
    expect(sharedSkillText.includes("lore plan -f - <<'JSON'")).toBe(true);
    expect(sharedSkillText.includes("lore apply -f - <<'JSON'")).toBe(true);
    expect(sharedSkillText.includes("lore plan '<plan JSON>'")).toBe(false);
    expect(sharedSkillText.includes("lore apply '<plan JSON>'")).toBe(false);

    const cursorRuleText = await Bun.file(cursorRulePath).text();
    expect(cursorRuleText.includes("lore plan -f <file|->")).toBe(true);
    expect(cursorRuleText.includes("lore apply -f <file|->")).toBe(true);
    expect(cursorRuleText.includes("lore plan '<json>'")).toBe(false);
    expect(cursorRuleText.includes("lore apply '<json>'")).toBe(false);
  }));

  test("managed AGENTS block is replaced without duplication", withSkillEnv(async (paths) => {
    await generateSkills({ platforms: ["codex"] });
    await generateSkills({ platforms: ["codex", "opencode"] });

    const codexAgentsPath = join(paths.codexHome, "AGENTS.md");
    const opencodeAgentsPath = join(paths.opencodeHome, "AGENTS.md");
    const codexAgentsText = await Bun.file(codexAgentsPath).text();
    const opencodeAgentsText = await Bun.file(opencodeAgentsPath).text();

    expect(countOccurrences(codexAgentsText, "<!-- lore:init:agents:start -->")).toBe(1);
    expect(countOccurrences(codexAgentsText, "<!-- lore:init:agents:end -->")).toBe(1);
    expect(countOccurrences(opencodeAgentsText, "<!-- lore:init:agents:start -->")).toBe(1);
    expect(countOccurrences(opencodeAgentsText, "<!-- lore:init:agents:end -->")).toBe(1);
  }));

  test("fails safely on malformed managed AGENTS block", withSkillEnv(async (paths) => {
    const codexAgentsPath = join(paths.codexHome, "AGENTS.md");
    await mkdir(paths.codexHome, { recursive: true });
    const malformed = [
      "# AGENTS.md",
      "",
      "before",
      "<!-- lore:init:agents:start -->",
      "user content that must never be truncated",
    ].join("\n");
    await Bun.write(codexAgentsPath, malformed);

    await expect(generateSkills({ platforms: ["codex"] })).rejects.toThrow("Malformed managed block");

    const after = await Bun.file(codexAgentsPath).text();
    expect(after).toBe(malformed);
  }));

  test("managed AGENTS block is removed when deselected", withSkillEnv(async (paths) => {
    await generateSkills({ platforms: ["codex", "opencode"] });
    await generateSkills({ platforms: ["cursor"] });

    const codexAgentsPath = join(paths.codexHome, "AGENTS.md");
    const opencodeAgentsPath = join(paths.opencodeHome, "AGENTS.md");
    const codexAgentsText = await Bun.file(codexAgentsPath).text();
    const opencodeAgentsText = await Bun.file(opencodeAgentsPath).text();
    expect(codexAgentsText.includes("<!-- lore:init:agents:start -->")).toBe(false);
    expect(opencodeAgentsText.includes("<!-- lore:init:agents:start -->")).toBe(false);
  }));

  test("openclaw output uses default workspace", withSkillEnv(async (paths) => {
    const result = await generateSkills({ platforms: ["openclaw"] });
    expect(result.files.length).toBeGreaterThan(0);

    const openclawSkillPath = join(paths.openclawWorkspace, "skills", "lore", "SKILL.md");
    const openclawAgentsPath = join(paths.openclawWorkspace, "AGENTS.md");
    expect(await Bun.file(openclawSkillPath).exists()).toBe(true);
    expect(await Bun.file(openclawAgentsPath).exists()).toBe(true);
  }));

  test("openclaw files are removed when deselected", withSkillEnv(async (paths) => {
    await generateSkills({ platforms: ["openclaw"] });
    await generateSkills({ platforms: ["cursor"] });

    const openclawSkillPath = join(paths.openclawWorkspace, "skills", "lore", "SKILL.md");
    expect(await Bun.file(openclawSkillPath).exists()).toBe(false);
  }));

  test("claude files are removed when deselected", withSkillEnv(async (paths) => {
    await generateSkills({ platforms: ["claude"] });
    await generateSkills({ platforms: ["cursor"] });

    const claudeSkillPath = join(paths.home, ".claude", "skills", "lore", "SKILL.md");
    expect(await Bun.file(claudeSkillPath).exists()).toBe(false);
    const claudePath = join(paths.home, ".claude", "CLAUDE.md");
    if (await Bun.file(claudePath).exists()) {
      const text = await Bun.file(claudePath).text();
      expect(text.includes("<!-- lore:init:claude:start -->")).toBe(false);
    }
  }));

  test("cursor rule is removed when deselected", withSkillEnv(async (paths) => {
    await generateSkills({ platforms: ["cursor"] });
    await generateSkills({ platforms: ["codex"] });

    const cursorRulePath = join(paths.home, ".cursor", "rules", "lore.mdc");
    expect(await Bun.file(cursorRulePath).exists()).toBe(false);
  }));
});

describe("cleanSkills", () => {
  test("removes global integrations", withSkillEnv(async (paths) => {
    await generateSkills({ platforms: ["claude", "cursor", "codex", "opencode", "openclaw"] });

    const cleaned = await cleanSkills();
    expect(cleaned.files.some((file) => file.removed)).toBe(true);

    const sharedSkillPath = join(paths.home, ".agents", "skills", "lore", "SKILL.md");
    const cursorRulePath = join(paths.home, ".cursor", "rules", "lore.mdc");
    const claudeSkillPath = join(paths.home, ".claude", "skills", "lore", "SKILL.md");
    const openclawSkillPath = join(paths.openclawWorkspace, "skills", "lore", "SKILL.md");
    expect(await Bun.file(sharedSkillPath).exists()).toBe(false);
    expect(await Bun.file(cursorRulePath).exists()).toBe(false);
    expect(await Bun.file(claudeSkillPath).exists()).toBe(false);
    expect(await Bun.file(openclawSkillPath).exists()).toBe(false);
  }));

  test("propagates stat permission errors", withSkillEnv(async (paths) => {
    const agentsDir = join(paths.home, ".agents");
    await mkdir(agentsDir, { recursive: true });
    await chmod(agentsDir, 0o000);

    try {
      let thrown: unknown = null;
      try {
        await cleanSkills();
      } catch (error) {
        thrown = error;
      }

      expect(thrown).not.toBeNull();
      if (typeof thrown !== "object" || thrown === null || !("code" in thrown)) {
        throw new Error("cleanSkills should reject with a filesystem error code");
      }
      expect(thrown.code === "EACCES" || thrown.code === "EPERM").toBe(true);
    } finally {
      await chmod(agentsDir, 0o755);
    }
  }));
});
