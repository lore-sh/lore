import { describe, expect, test } from "bun:test";
import { parseSchemaArgs } from "../../src/commands/schema";

describe("schema command", () => {
  test("parseSchemaArgs accepts zero or one argument", () => {
    expect(parseSchemaArgs([])).toEqual({ table: undefined });
    expect(parseSchemaArgs(["users"])).toEqual({ table: "users" });
  });

  test("parseSchemaArgs rejects multiple arguments", () => {
    expect(() => parseSchemaArgs(["a", "b"])).toThrow("Array must contain at most 1 element");
  });

  test("parseSchemaArgs rejects option arguments", () => {
    expect(() => parseSchemaArgs(["--verbose"])).toThrow("Unknown option '--verbose'");
  });
});
