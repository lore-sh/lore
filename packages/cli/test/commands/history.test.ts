import { describe, expect, test } from "bun:test";
import { parseHistoryArgs } from "../../src/commands/history";

describe("history command", () => {
  test("parseHistoryArgs rejects unknown arguments", () => {
    expect(() => parseHistoryArgs(["--unknown"])).toThrow("Unknown option '--unknown'");
  });
});
