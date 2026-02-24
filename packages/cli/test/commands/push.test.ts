import { describe, expect, test } from "bun:test";
import { parsePushArgs } from "../../src/commands/push";

describe("push command", () => {
  test("parsePushArgs rejects extras", () => {
    expect(() => parsePushArgs(["--x"])).toThrow("push does not accept arguments");
  });

  test("parsePushArgs accepts no arguments", () => {
    expect(() => parsePushArgs([])).not.toThrow();
  });
});
