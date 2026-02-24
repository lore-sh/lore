import { describe, expect, test } from "bun:test";
import { parseStudioArgs } from "../../src/commands/studio";

describe("studio command", () => {
  test("parseStudioArgs defaults to open=true", () => {
    expect(parseStudioArgs([])).toEqual({ port: undefined, open: true });
  });

  test("parseStudioArgs parses --no-open", () => {
    expect(parseStudioArgs(["--no-open"]).open).toBe(false);
  });

  test("parseStudioArgs rejects unknown arguments", () => {
    expect(() => parseStudioArgs(["--unknown"])).toThrow("Unknown option '--unknown'");
  });
});
