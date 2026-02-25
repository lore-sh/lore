import { describe, expect, test } from "bun:test";
import { CodedError } from "@lore/core";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { formatError, getCommandHandler, runCli } from "../src/main";

async function expectUnknownCommand(command: string): Promise<void> {
  try {
    await runCli([command]);
    throw new Error(`Expected unknown command error for: ${command}`);
  } catch (error) {
    if (!(error instanceof Error)) {
      throw error;
    }
    expect(error.message).toBe(`Unknown command: ${command}`);
  }
}

describe("main command dispatch", () => {
  test("rejects prototype property names as commands", async () => {
    expect(getCommandHandler("toString")).toBeNull();
    expect(getCommandHandler("hasOwnProperty")).toBeNull();
    expect(getCommandHandler("__proto__")).toBeNull();

    await expectUnknownCommand("toString");
    await expectUnknownCommand("hasOwnProperty");
  });

  test("formatError adds actionable hints for known error codes", () => {
    const message = formatError(new CodedError("SYNC_NOT_CONFIGURED", "Remote is not configured"));
    expect(message).toContain("Error [SYNC_NOT_CONFIGURED]: Remote is not configured");
    expect(message).toContain("lore remote connect");
  });

  test("formatError does not add remote hint for generic CONFIG errors", () => {
    const message = formatError(new CodedError("CONFIG", "Config file is not valid JSON"));
    expect(message).toBe("Error [CONFIG]: Config file is not valid JSON");
  });

  test("formatError falls back to generic error output", () => {
    expect(formatError(new Error("boom"))).toBe("Error: boom");
  });

  test("validates db command args before opening database", async () => {
    const home = mkdtempSync(join(tmpdir(), "lore-cli-home-"));
    const snapshot = {
      HOME: process.env.HOME,
      USERPROFILE: process.env.USERPROFILE,
    };
    process.env.HOME = home;
    process.env.USERPROFILE = home;
    try {
      await expect(runCli(["status", "--unknown"])).rejects.toThrow("Unknown option '--unknown'");
      await expect(runCli(["remote"])).rejects.toThrow("remote requires subcommand: connect | status");
    } finally {
      if (snapshot.HOME === undefined) {
        delete process.env.HOME;
      } else {
        process.env.HOME = snapshot.HOME;
      }
      if (snapshot.USERPROFILE === undefined) {
        delete process.env.USERPROFILE;
      } else {
        process.env.USERPROFILE = snapshot.USERPROFILE;
      }
      rmSync(home, { recursive: true, force: true });
    }
  });
});
