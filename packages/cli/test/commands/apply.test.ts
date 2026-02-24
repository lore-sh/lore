import { describe, expect, test } from "bun:test";
import { parseApplyArgs } from "../../src/commands/apply";

describe("apply command", () => {
  test("parseApplyArgs requires exactly one argument", () => {
    expect(() => parseApplyArgs([])).toThrow();
    expect(() => parseApplyArgs(["a", "b"])).toThrow();
  });

  test("parseApplyArgs rejects option arguments", () => {
    expect(() => parseApplyArgs(["--verbose"])).toThrow("Unknown option '--verbose'");
  });

  test("parseApplyArgs returns the plan ref", () => {
    expect(parseApplyArgs(["schema.sql"])).toBe("schema.sql");
    expect(parseApplyArgs(["-"])).toBe("-");
  });
});
