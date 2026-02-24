import { describe, expect, test } from "bun:test";
import { parseReadArgs } from "../../src/commands/read";

describe("read command", () => {
  test("parseReadArgs parses --sql and --json", () => {
    expect(parseReadArgs(["--sql", "SELECT 1", "--json"])).toEqual({ sql: "SELECT 1", json: true });
  });

  test("parseReadArgs requires --sql", () => {
    expect(() => parseReadArgs([])).toThrow();
  });

  test("parseReadArgs rejects unknown arguments", () => {
    expect(() => parseReadArgs(["--unknown"])).toThrow("Unknown option '--unknown'");
  });
});
