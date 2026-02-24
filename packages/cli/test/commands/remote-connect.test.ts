import { describe, expect, test } from "bun:test";
import {
  canUseConnectPrompt,
  parseRemoteConnectArgs,
  platformName,
  reduceMaskedInput,
} from "../../src/commands/remote-connect";

describe("remote-connect command", () => {
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
    expect(() => parseRemoteConnectArgs(["--platform", "turso", "--url", "--clear-token"])).toThrow(
      "Option '--url' argument is ambiguous",
    );
  });

  test("rejects --token missing value when next arg is a flag", () => {
    expect(() =>
      parseRemoteConnectArgs(["--platform", "turso", "--url", "libsql://db-xxx.turso.io", "--token", "--clear-token"]),
    ).toThrow("Option '--token' argument is ambiguous");
  });

  test("rejects --token with --clear-token", () => {
    expect(() =>
      parseRemoteConnectArgs(["--platform", "turso", "--url", "libsql://db-xxx.turso.io", "--token=abc", "--clear-token"]),
    ).toThrow("remote connect does not allow --token with --clear-token.");
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

  test("reduceMaskedInput consumes full CSI escape sequences", () => {
    const reduced = reduceMaskedInput("token", "\u001b[D", "none");
    expect(reduced.value).toBe("token");
    expect(reduced.escapeMode).toBe("none");
    expect(reduced.submit).toBe(false);
    expect(reduced.cancel).toBe(false);
  });

  test("reduceMaskedInput consumes split escape sequences across chunks", () => {
    const first = reduceMaskedInput("ab", "\u001b[", "none");
    expect(first.value).toBe("ab");
    expect(first.escapeMode).toBe("csi");

    const second = reduceMaskedInput(first.value, "D3", first.escapeMode);
    expect(second.value).toBe("ab3");
    expect(second.escapeMode).toBe("none");
  });

  test("reduceMaskedInput ignores delete key sequence and keeps backspace behavior", () => {
    const deleted = reduceMaskedInput("abc", "\u001b[3~", "none");
    expect(deleted.value).toBe("abc");
    expect(deleted.escapeMode).toBe("none");

    const backspaced = reduceMaskedInput("abc", "\u007F", "none");
    expect(backspaced.value).toBe("ab");
  });
});
