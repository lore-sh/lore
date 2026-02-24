import { describe, expect, test } from "bun:test";
import { parseRemoteStatusArgs } from "../../src/commands/remote-status";

describe("remote-status command", () => {
  test("parseRemoteStatusArgs rejects extras", () => {
    expect(() => parseRemoteStatusArgs(["--x"])).toThrow("remote status does not accept arguments");
  });

  test("parseRemoteStatusArgs accepts no arguments", () => {
    expect(() => parseRemoteStatusArgs([])).not.toThrow();
  });
});
