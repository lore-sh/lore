import { describe, expect, test } from "bun:test";
import { parseRemoteConnectArgs } from "../src/remote-connect-args";

describe("remote-connect-args", () => {
  test("returns interactive mode when no args are passed", () => {
    expect(parseRemoteConnectArgs([])).toEqual({ interactive: true });
  });

  test("parses non-interactive args with explicit token", () => {
    expect(
      parseRemoteConnectArgs([
        "--platform",
        "turso",
        "--url",
        "libsql://db-xxx.turso.io",
        "--token",
        "abc",
      ]),
    ).toEqual({
      interactive: false,
      platform: "turso",
      url: "libsql://db-xxx.turso.io",
      authToken: "abc",
    });
  });

  test("parses clear token mode", () => {
    expect(
      parseRemoteConnectArgs([
        "--platform",
        "libsql",
        "--url",
        "file:/tmp/remote.db",
        "--clear-token",
      ]),
    ).toEqual({
      interactive: false,
      platform: "libsql",
      url: "file:/tmp/remote.db",
      authToken: null,
    });
  });

  test("rejects --url missing value when next arg is a flag", () => {
    expect(() =>
      parseRemoteConnectArgs(["--platform", "turso", "--url", "--clear-token"]))
      .toThrow("remote connect requires a value for --url");
  });

  test("rejects --token missing value when next arg is a flag", () => {
    expect(() =>
      parseRemoteConnectArgs(["--platform", "turso", "--url", "libsql://db-xxx.turso.io", "--token", "--clear-token"]))
      .toThrow("remote connect requires a value for --token");
  });

  test("rejects --token with --clear-token", () => {
    expect(() =>
      parseRemoteConnectArgs(["--platform", "turso", "--url", "libsql://db-xxx.turso.io", "--token=abc", "--clear-token"]))
      .toThrow("remote connect does not allow --token with --clear-token.");
  });

  test("allows token values that start with -- via inline form", () => {
    expect(
      parseRemoteConnectArgs([
        "--platform",
        "turso",
        "--url",
        "libsql://db-xxx.turso.io",
        "--token=--clear-token",
      ]),
    ).toEqual({
      interactive: false,
      platform: "turso",
      url: "libsql://db-xxx.turso.io",
      authToken: "--clear-token",
    });
  });
});
