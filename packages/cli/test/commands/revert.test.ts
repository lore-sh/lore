import { describe, expect, test } from "bun:test";
import { parseRevertArgs } from "../../src/commands/revert";

describe("revert command", () => {
  test("parseRevertArgs requires commit_id", () => {
    expect(() => parseRevertArgs([])).toThrow();
  });
});
