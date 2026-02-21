import { describe, expect, test } from "bun:test";
import {
  canUseConnectPrompt,
  parseCloneArgs,
  parseClonePlatform,
  parseRemoteConnectArgs,
  platformName,
} from "../../src/commands/remote";

describe("remote command", () => {
  test("canUseConnectPrompt requires stdin/stdout TTY", () => {
    expect(canUseConnectPrompt(true, true)).toBe(true);
    expect(canUseConnectPrompt(true, false)).toBe(false);
    expect(canUseConnectPrompt(false, true)).toBe(false);
  });

  test("platformName renders labels", () => {
    expect(platformName("turso")).toBe("Turso");
    expect(platformName("libsql")).toBe("libSQL");
  });

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

  test("parseClonePlatform rejects invalid platforms", () => {
    expect(() => parseClonePlatform("mysql")).toThrow("clone does not accept --platform=mysql");
  });

  test("parseCloneArgs parses url and platform", () => {
    const parsed = parseCloneArgs(["libsql://db.turso.io", "--platform", "turso"]);
    expect(parsed).toEqual({ platform: "turso", url: "libsql://db.turso.io", forceNew: false });
  });

  test("parseCloneArgs supports --force-new", () => {
    const parsed = parseCloneArgs(["libsql://db.turso.io", "--platform", "libsql", "--force-new"]);
    expect(parsed.forceNew).toBe(true);
  });

  test("parseCloneArgs requires url and platform", () => {
    expect(() => parseCloneArgs([])).toThrow("clone requires <url>");
    expect(() => parseCloneArgs(["libsql://db.turso.io"])).toThrow("clone requires --platform");
  });
});
