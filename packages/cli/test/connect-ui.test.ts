import { describe, expect, test } from "bun:test";
import { canUseConnectPrompt, createRadioState, platformName, reduceRadioState, resolveLibsqlAuthToken } from "../src/connect-ui";

describe("connect-ui", () => {
  test("canUseConnectPrompt requires stdin/stdout TTY", () => {
    expect(canUseConnectPrompt(true, true)).toBe(true);
    expect(canUseConnectPrompt(true, false)).toBe(false);
    expect(canUseConnectPrompt(false, true)).toBe(false);
  });

  test("platformName renders labels", () => {
    expect(platformName("turso")).toBe("Turso");
    expect(platformName("libsql")).toBe("libSQL");
  });

  test("radio reducer wraps cursor", () => {
    let state = createRadioState(0);
    state = reduceRadioState(state, "down");
    expect(state.cursor).toBe(1);
    state = reduceRadioState(state, "down");
    expect(state.cursor).toBe(0);
    state = reduceRadioState(state, "up");
    expect(state.cursor).toBe(1);
  });

  test("resolveLibsqlAuthToken keeps/sets/clears explicitly", () => {
    expect(resolveLibsqlAuthToken("keep")).toBeUndefined();
    expect(resolveLibsqlAuthToken("clear")).toBeNull();
    expect(resolveLibsqlAuthToken("set", "abc")).toBe("abc");
    expect(() => resolveLibsqlAuthToken("set", "  ")).toThrow("Auth token is required.");
  });
});
