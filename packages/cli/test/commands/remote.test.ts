import { describe, expect, test } from "bun:test";
import { parseRemoteArgs } from "../../src/commands/remote";

describe("remote command", () => {
  test("parseRemoteArgs checks subcommand shape", () => {
    expect(() => parseRemoteArgs([])).toThrow("remote requires subcommand");
    expect(() => parseRemoteArgs(["status", "--unknown"])).toThrow("remote status does not accept arguments");
    expect(() => parseRemoteArgs(["unknown"])).toThrow("Invalid enum value");
  });
});
