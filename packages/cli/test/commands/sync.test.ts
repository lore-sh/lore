import { describe, expect, test } from "bun:test";
import { parseSyncArgs } from "../../src/commands/sync";

describe("sync command", () => {
  test("parseSyncArgs rejects extras", () => {
    expect(() => parseSyncArgs(["--x"])).toThrow("sync does not accept arguments");
  });

  test("parseSyncArgs accepts no arguments", () => {
    expect(() => parseSyncArgs([])).not.toThrow();
  });
});
