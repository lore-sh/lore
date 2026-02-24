import { describe, expect, test } from "bun:test";
import { parsePullArgs } from "../../src/commands/pull";

describe("pull command", () => {
  test("parsePullArgs rejects extras", () => {
    expect(() => parsePullArgs(["--x"])).toThrow("pull does not accept arguments");
  });

  test("parsePullArgs accepts no arguments", () => {
    expect(() => parsePullArgs([])).not.toThrow();
  });
});
