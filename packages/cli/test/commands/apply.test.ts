import { describe, expect, test } from "bun:test";
import { parseApplyArgs } from "../../src/commands/apply";

describe("apply command", () => {
  test("parseApplyArgs requires -f input", () => {
    expect(() => parseApplyArgs([])).toThrow("Missing required option: -f <file|->");
  });

  test("parseApplyArgs rejects unknown options", () => {
    expect(() => parseApplyArgs(["--verbose"])).toThrow("Unknown option '--verbose'");
  });

  test("parseApplyArgs rejects positional arguments", () => {
    expect(() => parseApplyArgs(["plan.json"])).toThrow(
      "Positional arguments are not allowed. Use -f <file|->",
    );
  });

  test("parseApplyArgs returns file input from -f", () => {
    expect(parseApplyArgs(["-f", "plan.json"])).toEqual({
      kind: "file",
      path: "plan.json",
    });
    expect(parseApplyArgs(["--file", "plan.json"])).toEqual({
      kind: "file",
      path: "plan.json",
    });
  });

  test("parseApplyArgs returns stdin input from -f -", () => {
    expect(parseApplyArgs(["-f", "-"])).toEqual({ kind: "stdin" });
  });

  test("parseApplyArgs rejects extra positional arguments", () => {
    expect(() => parseApplyArgs(["plan.json", "extra"])).toThrow(
      "Positional arguments are not allowed. Use -f <file|->",
    );
  });

  test("parseApplyArgs rejects combining positional with --file", () => {
    expect(() => parseApplyArgs(["plan.json", "-f", "other.json"])).toThrow(
      "Positional arguments are not allowed. Use -f <file|->",
    );
  });
});
