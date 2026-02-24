import { describe, expect, test } from "bun:test";
import { parseStatusArgs } from "../../src/commands/status";

describe("status command", () => {
  test("parseStatusArgs rejects unknown arguments", () => {
    expect(() => parseStatusArgs(["--unknown"])).toThrow("Unknown option '--unknown'");
  });
});
