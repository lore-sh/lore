import { describe, expect, test } from "bun:test";
import { initDb, openDb, type Database } from "@lore/core";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { parsePlanArgs, runPlan } from "../../src/commands/plan";

async function withTempDb<T>(dir: string, run: (db: Database) => Promise<T>): Promise<T> {
  const dbPath = join(dir, "lore.db");
  await initDb({ dbPath });
  const db = openDb(dbPath);
  try {
    return await run(db);
  } finally {
    db.$client.close(false);
  }
}

describe("plan command", () => {
  test("parsePlanArgs requires -f input", () => {
    expect(() => parsePlanArgs([])).toThrow("Missing required option: -f <file|->");
  });

  test("parsePlanArgs rejects unknown options", () => {
    expect(() => parsePlanArgs(["--verbose"])).toThrow("Unknown option '--verbose'");
  });

  test("parsePlanArgs rejects positional arguments", () => {
    expect(() => parsePlanArgs(["plan.json"])).toThrow(
      "Positional arguments are not allowed. Use -f <file|->",
    );
  });

  test("parsePlanArgs returns file input from -f", () => {
    expect(parsePlanArgs(["-f", "plan.json"])).toEqual({
      kind: "file",
      path: "plan.json",
    });
    expect(parsePlanArgs(["--file", "plan.json"])).toEqual({
      kind: "file",
      path: "plan.json",
    });
  });

  test("parsePlanArgs returns stdin input from -f -", () => {
    expect(parsePlanArgs(["-f", "-"])).toEqual({ kind: "stdin" });
  });

  test("parsePlanArgs rejects extra positional arguments", () => {
    expect(() => parsePlanArgs(["plan.json", "extra"])).toThrow(
      "Positional arguments are not allowed. Use -f <file|->",
    );
  });

  test("parsePlanArgs rejects combining positional with --file", () => {
    expect(() => parsePlanArgs(["plan.json", "-f", "other.json"])).toThrow(
      "Positional arguments are not allowed. Use -f <file|->",
    );
  });

  test("runPlan prints JSON check result when plan file is missing", async () => {
    const dir = mkdtempSync(join(tmpdir(), "lore-cli-plan-missing-"));
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
        await expect(
          runPlan(db, { kind: "file", path: join(dir, "missing-plan.json") }),
        ).rejects.toThrow("EXIT:1");
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
    const dir = mkdtempSync(join(tmpdir(), "lore-cli-plan-invalid-"));
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
        await expect(runPlan(db, { kind: "file", path: invalidPath })).rejects.toThrow("EXIT:1");
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
