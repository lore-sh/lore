import { describe, expect, test } from "bun:test";
import { DEFAULT_STUDIO_PORT, normalizeStudioPort, parseStudioPort, parseStudioPortArg } from "../../src/server/port";

describe("studio port parsing", () => {
  test("parseStudioPort accepts decimal integers in range", () => {
    expect(parseStudioPort("7055")).toBe(7055);
    expect(parseStudioPort("00080")).toBe(80);
  });

  test("parseStudioPort rejects non-numeric and partial numeric values", () => {
    expect(() => parseStudioPort("7061abc")).toThrow("Invalid port: 7061abc");
    expect(() => parseStudioPort("70.5")).toThrow("Invalid port: 70.5");
    expect(() => parseStudioPort("-1")).toThrow("Invalid port: -1");
    expect(() => parseStudioPort("abc")).toThrow("Invalid port: abc");
  });

  test("parseStudioPort rejects out-of-range values", () => {
    expect(() => parseStudioPort("0")).toThrow("Expected an integer between 1 and 65535");
    expect(() => parseStudioPort("65536")).toThrow("Expected an integer between 1 and 65535");
  });

  test("parseStudioPortArg requires --port value", () => {
    expect(() => parseStudioPortArg(undefined)).toThrow("studio requires a value for --port");
  });

  test("normalizeStudioPort validates numeric input and applies default", () => {
    expect(normalizeStudioPort(undefined)).toBe(DEFAULT_STUDIO_PORT);
    expect(normalizeStudioPort(7055)).toBe(7055);
    expect(() => normalizeStudioPort(7000.5)).toThrow("Invalid port: 7000.5");
    expect(() => normalizeStudioPort(0)).toThrow("Invalid port: 0");
  });
});
