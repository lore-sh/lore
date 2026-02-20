import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { chmod, mkdir } from "node:fs/promises";
import { join } from "node:path";
import { cleanSkills, getStatus, initDatabase } from "../src";
import { createTestContext, withTmpDirCleanup } from "./helpers";

const testWithTmp = (name: string, fn: () => void | Promise<void>) => test(name, withTmpDirCleanup(fn));

function countOccurrences(text: string, pattern: string): number {
  let index = 0;
  let count = 0;
  while (index < text.length) {
    const found = text.indexOf(pattern, index);
    if (found < 0) {
      break;
    }
    count += 1;
    index = found + pattern.length;
  }
  return count;
}

interface GlobalEnvPaths {
  home: string;
  codexHome: string;
  xdgConfigHome: string;
  opencodeHome: string;
  openclawWorkspace: string;
}

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
  if (snapshot.HOME === undefined) {
    delete process.env.HOME;
  } else {
    process.env.HOME = snapshot.HOME;
  }
  if (snapshot.CODEX_HOME === undefined) {
    delete process.env.CODEX_HOME;
  } else {
    process.env.CODEX_HOME = snapshot.CODEX_HOME;
  }
  if (snapshot.XDG_CONFIG_HOME === undefined) {
    delete process.env.XDG_CONFIG_HOME;
  } else {
    process.env.XDG_CONFIG_HOME = snapshot.XDG_CONFIG_HOME;
  }
}

async function withGlobalSkillEnv<T>(
  root: string,
  fn: (paths: GlobalEnvPaths) => Promise<T>,
): Promise<T> {
  const snapshot = captureGlobalEnv();
  const paths: GlobalEnvPaths = {
    home: join(root, "home"),
    codexHome: join(root, "codex-home"),
    xdgConfigHome: join(root, "xdg-config"),
    opencodeHome: join(root, "xdg-config", "opencode"),
    openclawWorkspace: join(root, "home", ".openclaw", "workspace"),
  };
  process.env.HOME = paths.home;
  process.env.CODEX_HOME = paths.codexHome;
  process.env.XDG_CONFIG_HOME = paths.xdgConfigHome;
  try {
    return await fn(paths);
  } finally {
    restoreGlobalEnv(snapshot);
  }
}

describe("initDatabase", () => {
  testWithTmp("force-new reinitializes database", async () => {
    const { dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const direct = new Database(dbPath);
    direct.run("CREATE TABLE foo (id INTEGER PRIMARY KEY, v TEXT)");
    direct.run("INSERT INTO foo(id, v) VALUES(1, 'x')");
    direct.close(false);

    const reinit = await initDatabase({ dbPath, forceNew: true });
    expect(reinit.dbPath).toBe(dbPath);
    const status = getStatus({ dbPath });
    expect(status.tableCount).toBe(0);
    expect(status.headCommit).toBeNull();
  });

  testWithTmp("init generates only selected global platforms", async () => {
    const { dir, dbPath } = createTestContext();
    await withGlobalSkillEnv(dir, async (paths) => {
      const result = await initDatabase({
        dbPath,
        generateSkills: true,
        skillPlatforms: ["codex", "cursor"],
      });
      expect(result.generatedSkills).not.toBeNull();
      if (!result.generatedSkills) {
        throw new Error("generatedSkills should exist");
      }

      const sharedSkillPath = join(paths.home, ".agents", "skills", "toss", "SKILL.md");
      const codexAgentsPath = join(paths.codexHome, "AGENTS.md");
      const cursorRulePath = join(paths.home, ".cursor", "rules", "toss.mdc");
      const claudeSkillPath = join(paths.home, ".claude", "skills", "toss", "SKILL.md");
      const legacyLocalSkillPath = join(dir, ".toss", "skills", "toss", "SKILL.md");

      expect(await Bun.file(sharedSkillPath).exists()).toBe(true);
      expect(await Bun.file(codexAgentsPath).exists()).toBe(true);
      expect(await Bun.file(cursorRulePath).exists()).toBe(true);
      expect(await Bun.file(claudeSkillPath).exists()).toBe(false);
      expect(await Bun.file(legacyLocalSkillPath).exists()).toBe(false);
    });
  });

  testWithTmp("managed AGENTS block is replaced without duplication", async () => {
    const { dir, dbPath } = createTestContext();
    await withGlobalSkillEnv(dir, async (paths) => {
      await initDatabase({
        dbPath,
        generateSkills: true,
        skillPlatforms: ["codex"],
      });
      await initDatabase({
        dbPath,
        generateSkills: true,
        skillPlatforms: ["codex", "opencode"],
      });

      const codexAgentsPath = join(paths.codexHome, "AGENTS.md");
      const opencodeAgentsPath = join(paths.opencodeHome, "AGENTS.md");
      const codexAgentsText = await Bun.file(codexAgentsPath).text();
      const opencodeAgentsText = await Bun.file(opencodeAgentsPath).text();

      expect(countOccurrences(codexAgentsText, "<!-- toss:init:agents:start -->")).toBe(1);
      expect(countOccurrences(codexAgentsText, "<!-- toss:init:agents:end -->")).toBe(1);
      expect(countOccurrences(opencodeAgentsText, "<!-- toss:init:agents:start -->")).toBe(1);
      expect(countOccurrences(opencodeAgentsText, "<!-- toss:init:agents:end -->")).toBe(1);
    });
  });

  testWithTmp("init fails safely on malformed managed AGENTS block without truncating content", async () => {
    const { dir, dbPath } = createTestContext();
    await withGlobalSkillEnv(dir, async (paths) => {
      const codexAgentsPath = join(paths.codexHome, "AGENTS.md");
      await mkdir(paths.codexHome, { recursive: true });
      const malformed = [
        "# AGENTS.md",
        "",
        "before",
        "<!-- toss:init:agents:start -->",
        "user content that must never be truncated",
      ].join("\n");
      await Bun.write(codexAgentsPath, malformed);

      await expect(
        initDatabase({
          dbPath,
          generateSkills: true,
          skillPlatforms: ["codex"],
        }),
      ).rejects.toThrow("Malformed managed block");

      const after = await Bun.file(codexAgentsPath).text();
      expect(after).toBe(malformed);
    });
  });

  testWithTmp("managed AGENTS block is removed when codex/opencode are deselected", async () => {
    const { dir, dbPath } = createTestContext();
    await withGlobalSkillEnv(dir, async (paths) => {
      await initDatabase({
        dbPath,
        generateSkills: true,
        skillPlatforms: ["codex", "opencode"],
      });
      await initDatabase({
        dbPath,
        generateSkills: true,
        skillPlatforms: ["cursor"],
      });

      const codexAgentsPath = join(paths.codexHome, "AGENTS.md");
      const opencodeAgentsPath = join(paths.opencodeHome, "AGENTS.md");
      const codexAgentsText = await Bun.file(codexAgentsPath).text();
      const opencodeAgentsText = await Bun.file(opencodeAgentsPath).text();
      expect(codexAgentsText.includes("<!-- toss:init:agents:start -->")).toBe(false);
      expect(codexAgentsText.includes("<!-- toss:init:agents:end -->")).toBe(false);
      expect(opencodeAgentsText.includes("<!-- toss:init:agents:start -->")).toBe(false);
      expect(opencodeAgentsText.includes("<!-- toss:init:agents:end -->")).toBe(false);
    });
  });

  testWithTmp("openclaw output uses default workspace under HOME", async () => {
    const { dir, dbPath } = createTestContext();
    await withGlobalSkillEnv(dir, async (paths) => {
      const result = await initDatabase({
        dbPath,
        generateSkills: true,
        skillPlatforms: ["openclaw"],
      });
      expect(result.generatedSkills).not.toBeNull();

      const openclawSkillPath = join(paths.openclawWorkspace, "skills", "toss", "SKILL.md");
      const openclawAgentsPath = join(paths.openclawWorkspace, "AGENTS.md");
      expect(await Bun.file(openclawSkillPath).exists()).toBe(true);
      expect(await Bun.file(openclawAgentsPath).exists()).toBe(true);
      const skillText = await Bun.file(openclawSkillPath).text();
      expect(skillText.includes('bun run --cwd "$PWD" toss apply --plan -')).toBe(true);
      expect(skillText.includes(`bun run --cwd "${dir}" toss apply --plan -`)).toBe(false);
    });
  });

  testWithTmp("openclaw files are removed when openclaw is deselected", async () => {
    const { dir, dbPath } = createTestContext();
    await withGlobalSkillEnv(dir, async (paths) => {
      await initDatabase({
        dbPath,
        generateSkills: true,
        skillPlatforms: ["openclaw"],
      });
      await initDatabase({
        dbPath,
        generateSkills: true,
        skillPlatforms: ["cursor"],
      });

      const openclawSkillPath = join(paths.openclawWorkspace, "skills", "toss", "SKILL.md");
      const openclawAgentsPath = join(paths.openclawWorkspace, "AGENTS.md");
      expect(await Bun.file(openclawSkillPath).exists()).toBe(false);
      if (await Bun.file(openclawAgentsPath).exists()) {
        const openclawAgentsText = await Bun.file(openclawAgentsPath).text();
        expect(openclawAgentsText.includes("<!-- toss:init:agents:start -->")).toBe(false);
        expect(openclawAgentsText.includes("<!-- toss:init:agents:end -->")).toBe(false);
      }
    });
  });

  testWithTmp("claude files are removed when claude is deselected", async () => {
    const { dir, dbPath } = createTestContext();
    await withGlobalSkillEnv(dir, async (paths) => {
      await initDatabase({
        dbPath,
        generateSkills: true,
        skillPlatforms: ["claude"],
      });
      await initDatabase({
        dbPath,
        generateSkills: true,
        skillPlatforms: ["cursor"],
      });

      const claudePath = join(paths.home, ".claude", "CLAUDE.md");
      const claudeSkillPath = join(paths.home, ".claude", "skills", "toss", "SKILL.md");
      if (await Bun.file(claudePath).exists()) {
        const claudeText = await Bun.file(claudePath).text();
        expect(claudeText.includes("<!-- toss:init:claude:start -->")).toBe(false);
        expect(claudeText.includes("<!-- toss:init:claude:end -->")).toBe(false);
      }
      expect(await Bun.file(claudeSkillPath).exists()).toBe(false);
    });
  });

  testWithTmp("cursor rule is removed when cursor is deselected", async () => {
    const { dir, dbPath } = createTestContext();
    await withGlobalSkillEnv(dir, async (paths) => {
      await initDatabase({
        dbPath,
        generateSkills: true,
        skillPlatforms: ["cursor"],
      });
      await initDatabase({
        dbPath,
        generateSkills: true,
        skillPlatforms: ["codex"],
      });

      const cursorRulePath = join(paths.home, ".cursor", "rules", "toss.mdc");
      expect(await Bun.file(cursorRulePath).exists()).toBe(false);
    });
  });

  testWithTmp("cleanSkills removes global integrations", async () => {
    const { dir, dbPath } = createTestContext();
    await withGlobalSkillEnv(dir, async (paths) => {
      await initDatabase({
        dbPath,
        generateSkills: true,
        skillPlatforms: ["claude", "cursor", "codex", "opencode", "openclaw"],
      });

      const cleaned = await cleanSkills();
      expect(cleaned.files.some((file) => file.removed)).toBe(true);

      const sharedSkillPath = join(paths.home, ".agents", "skills", "toss", "SKILL.md");
      const cursorRulePath = join(paths.home, ".cursor", "rules", "toss.mdc");
      const claudeSkillPath = join(paths.home, ".claude", "skills", "toss", "SKILL.md");
      const openclawSkillPath = join(paths.openclawWorkspace, "skills", "toss", "SKILL.md");
      expect(await Bun.file(sharedSkillPath).exists()).toBe(false);
      expect(await Bun.file(cursorRulePath).exists()).toBe(false);
      expect(await Bun.file(claudeSkillPath).exists()).toBe(false);
      expect(await Bun.file(openclawSkillPath).exists()).toBe(false);

      const codexAgentsPath = join(paths.codexHome, "AGENTS.md");
      if (await Bun.file(codexAgentsPath).exists()) {
        const text = await Bun.file(codexAgentsPath).text();
        expect(text.includes("<!-- toss:init:agents:start -->")).toBe(false);
        expect(text.includes("<!-- toss:init:agents:end -->")).toBe(false);
      }
    });
  });

  testWithTmp("cleanSkills propagates stat permission errors", async () => {
    const { dir } = createTestContext();
    await withGlobalSkillEnv(dir, async (paths) => {
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
    });
  });
});
