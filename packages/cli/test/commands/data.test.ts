import { describe, expect, test } from "bun:test";
import { parseSchemaArgs, parseSinglePlanRef } from "../../src/commands/data";

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
});
