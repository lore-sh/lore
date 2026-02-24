import { describe, expect, test } from "bun:test";
import { parseVerifyArgs } from "../../src/commands/verify";

describe("verify command", () => {
  test("parseVerifyArgs rejects unknown arguments", () => {
    expect(() => parseVerifyArgs(["--unknown"])).toThrow("Unknown option '--unknown'");
  });
});
