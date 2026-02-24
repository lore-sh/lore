import { describe, expect, test } from "bun:test";
import { initDb, openDb, type Database } from "@toss/core";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { parsePlanArgs, runPlan } from "../../src/commands/plan";

async function withTempDb<T>(dir: string, run: (db: Database) => Promise<T>): Promise<T> {
  const dbPath = join(dir, "toss.db");
  await initDb({ dbPath });
  const db = openDb(dbPath);
  try {
    return await run(db);
  } finally {
    db.$client.close(false);
  }
}

describe("plan command", () => {
  test("parsePlanArgs requires exactly one argument", () => {
    expect(() => parsePlanArgs([])).toThrow();
    expect(() => parsePlanArgs(["a", "b"])).toThrow();
  });

  test("parsePlanArgs rejects option arguments", () => {
    expect(() => parsePlanArgs(["--verbose"])).toThrow("Unknown option '--verbose'");
  });

  test("parsePlanArgs returns the plan ref", () => {
    expect(parsePlanArgs(["schema.sql"])).toBe("schema.sql");
    expect(parsePlanArgs(["-"])).toBe("-");
  });

  test("runPlan prints JSON check result when plan file is missing", async () => {
    const dir = mkdtempSync(join(tmpdir(), "toss-cli-plan-missing-"));
    const originalLog = console.log;
    const originalExit = process.exit;
    const logs: string[] = [];
    try {
      console.log = ((value?: unknown) => {
        logs.push(String(value));
      }) as typeof console.log;
      Object.defineProperty(process, "exit", {
        configurable: true,
        value: ((code?: number) => {
          throw new Error(`EXIT:${code ?? 0}`);
        }) as typeof process.exit,
      });
      await withTempDb(dir, async (db) => {
        await expect(runPlan(db, join(dir, "missing-plan.json"))).rejects.toThrow("EXIT:1");
      });
      const result = JSON.parse(logs[0] ?? "{}");
      expect(result.ok).toBe(false);
      expect(result.risk).toBe("high");
      expect(Array.isArray(result.errors)).toBe(true);
      expect(result.errors.length).toBeGreaterThan(0);
      expect(typeof result.checkedAt).toBe("string");
    } finally {
      console.log = originalLog;
      Object.defineProperty(process, "exit", { configurable: true, value: originalExit });
      rmSync(dir, { recursive: true, force: true });
    }
  });

  test("runPlan preserves structured INVALID_JSON output", async () => {
    const dir = mkdtempSync(join(tmpdir(), "toss-cli-plan-invalid-"));
    const originalLog = console.log;
    const originalExit = process.exit;
    const logs: string[] = [];
    try {
      const invalidPath = join(dir, "invalid.json");
      await Bun.write(invalidPath, "{invalid");
      console.log = ((value?: unknown) => {
        logs.push(String(value));
      }) as typeof console.log;
      Object.defineProperty(process, "exit", {
        configurable: true,
        value: ((code?: number) => {
          throw new Error(`EXIT:${code ?? 0}`);
        }) as typeof process.exit,
      });
      await withTempDb(dir, async (db) => {
        await expect(runPlan(db, invalidPath)).rejects.toThrow("EXIT:1");
      });
      const result = JSON.parse(logs[0] ?? "{}");
      expect(result.ok).toBe(false);
      expect(result.errors?.[0]?.code).toBe("INVALID_JSON");
    } finally {
      console.log = originalLog;
      Object.defineProperty(process, "exit", { configurable: true, value: originalExit });
      rmSync(dir, { recursive: true, force: true });
    }
  });
});
