import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { initDatabase } from "@toss/core";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { parseSchemaArgs, parseSinglePlanRef, runPlan } from "../../src/commands/data";

describe("data command", () => {
  test("parseSinglePlanRef requires exactly one argument", () => {
    expect(() => parseSinglePlanRef("plan", [])).toThrow("plan requires <file|->");
    expect(() => parseSinglePlanRef("plan", ["a", "b"])).toThrow("plan accepts exactly one");
  });

  test("parseSinglePlanRef rejects option arguments", () => {
    expect(() => parseSinglePlanRef("apply", ["--verbose"])).toThrow("apply does not accept option arguments");
  });

  test("parseSinglePlanRef returns the plan ref", () => {
    expect(parseSinglePlanRef("plan", ["schema.sql"])).toBe("schema.sql");
    expect(parseSinglePlanRef("apply", ["-"])).toBe("-");
  });

  test("parseSchemaArgs accepts zero or one argument", () => {
    expect(parseSchemaArgs([])).toEqual({ table: undefined });
    expect(parseSchemaArgs(["users"])).toEqual({ table: "users" });
  });

  test("parseSchemaArgs rejects multiple arguments", () => {
    expect(() => parseSchemaArgs(["a", "b"])).toThrow("schema accepts at most one");
  });

  test("parseSchemaArgs rejects option arguments", () => {
    expect(() => parseSchemaArgs(["--verbose"])).toThrow("schema does not accept argument");
  });

  test("runPlan prints JSON check result when plan file is missing", async () => {
    const dir = mkdtempSync(join(tmpdir(), "toss-cli-plan-missing-"));
    const dbPath = join(dir, "toss.db");
    const db = new Database(dbPath, { strict: true });
    const originalLog = console.log;
    const originalExit = process.exit;
    const logs: string[] = [];
    try {
      await initDatabase({ dbPath });
      console.log = ((value?: unknown) => {
        logs.push(String(value));
      }) as typeof console.log;
      Object.defineProperty(process, "exit", {
        configurable: true,
        value: ((code?: number) => {
          throw new Error(`EXIT:${code ?? 0}`);
        }) as typeof process.exit,
      });
      await expect(runPlan(db, [join(dir, "missing-plan.json")])).rejects.toThrow("EXIT:1");
      const result = JSON.parse(logs[0] ?? "{}");
      expect(result.ok).toBe(false);
      expect(result.risk).toBe("high");
      expect(Array.isArray(result.errors)).toBe(true);
      expect(result.errors.length).toBeGreaterThan(0);
      expect(typeof result.checkedAt).toBe("string");
    } finally {
      console.log = originalLog;
      Object.defineProperty(process, "exit", { configurable: true, value: originalExit });
      db.close(false);
      rmSync(dir, { recursive: true, force: true });
    }
  });

  test("runPlan preserves structured INVALID_JSON output", async () => {
    const dir = mkdtempSync(join(tmpdir(), "toss-cli-plan-invalid-"));
    const dbPath = join(dir, "toss.db");
    const db = new Database(dbPath, { strict: true });
    const originalLog = console.log;
    const originalExit = process.exit;
    const logs: string[] = [];
    try {
      await initDatabase({ dbPath });
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
      await expect(runPlan(db, [invalidPath])).rejects.toThrow("EXIT:1");
      const result = JSON.parse(logs[0] ?? "{}");
      expect(result.ok).toBe(false);
      expect(result.errors?.[0]?.code).toBe("INVALID_JSON");
    } finally {
      console.log = originalLog;
      Object.defineProperty(process, "exit", { configurable: true, value: originalExit });
      db.close(false);
      rmSync(dir, { recursive: true, force: true });
    }
  });
});
