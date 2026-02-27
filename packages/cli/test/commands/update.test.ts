import { describe, expect, test } from "bun:test";
import {
  createInstallerEnv,
  inferInstallDir,
  parseUpdateArgs,
  runUpdate,
} from "../../src/commands/update";

describe("update command", () => {
  test("parseUpdateArgs parses optional --version", () => {
    expect(parseUpdateArgs([])).toEqual({ version: undefined });
    expect(parseUpdateArgs(["--version", "1.2.3"])).toEqual({ version: "1.2.3" });
  });

  test("parseUpdateArgs rejects unknown arguments", () => {
    expect(() => parseUpdateArgs(["--unknown"])).toThrow("Unknown option '--unknown'");
  });

  test("inferInstallDir skips dev builds", () => {
    expect(inferInstallDir("dev", "/Users/me/.local/bin/lore")).toBeNull();
  });

  test("inferInstallDir skips bun executable", () => {
    expect(inferInstallDir("1.0.0", "/opt/homebrew/bin/bun")).toBeNull();
  });

  test("inferInstallDir returns lore binary directory for release builds", () => {
    expect(inferInstallDir("1.0.0", "/Users/me/.local/bin/lore")).toBe("/Users/me/.local/bin");
  });

  test("createInstallerEnv sets requested version and inferred install dir", () => {
    const env = createInstallerEnv({
      env: { PATH: "/usr/bin" },
      currentVersion: "1.0.0",
      execPath: "/Users/me/.local/bin/lore",
      requestedVersion: "2.0.0",
    });
    expect(env.LORE_VERSION).toBe("2.0.0");
    expect(env.LORE_INSTALL_DIR).toBe("/Users/me/.local/bin");
  });

  test("createInstallerEnv respects existing install dir", () => {
    const env = createInstallerEnv({
      env: { PATH: "/usr/bin", LORE_INSTALL_DIR: "/custom/bin" },
      currentVersion: "1.0.0",
      execPath: "/Users/me/.local/bin/lore",
    });
    expect(env.LORE_INSTALL_DIR).toBe("/custom/bin");
  });

  test("runUpdate fetches installer and executes with prepared env", async () => {
    let fetchedUrl = "";
    let scriptValue = "";
    const capture: { env?: NodeJS.ProcessEnv | undefined } = {};
    const logs: string[] = [];
    const originalLog = console.log;
    console.log = (...args: unknown[]) => {
      logs.push(args.join(" "));
    };
    try {
      await runUpdate(
        { version: "3.0.0" },
        {
          installUrl: "https://example.com/install",
          env: { PATH: "/usr/bin" },
          currentVersion: "1.0.0",
          execPath: "/Users/me/.local/bin/lore",
          fetchInstallScript: async (url) => {
            fetchedUrl = url;
            return "#!/usr/bin/env bash\necho install";
          },
          runInstallerScript: async (script, env) => {
            scriptValue = script;
            capture.env = env;
          },
        },
      );
    } finally {
      console.log = originalLog;
    }
    expect(fetchedUrl).toBe("https://example.com/install");
    expect(scriptValue).toContain("echo install");
    if (!capture.env) {
      throw new Error("Expected installer env to be captured");
    }
    expect(capture.env.LORE_VERSION).toBe("3.0.0");
    expect(capture.env.LORE_INSTALL_DIR).toBe("/Users/me/.local/bin");
    expect(logs.some((line) => line.includes("Update complete"))).toBe(true);
  });
});
