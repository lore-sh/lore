import { describe, expect, test } from "bun:test";
import { parseRecoverArgs } from "../../src/commands/recover";

describe("recover command", () => {
  test("parseRecoverArgs requires commit_id", () => {
    expect(() => parseRecoverArgs([])).toThrow();
  });
});
