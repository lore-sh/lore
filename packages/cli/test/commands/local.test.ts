import { describe, expect, test } from "bun:test";
import { runHistory, runStatus, runVerify } from "../../src/commands/local";

describe("local command", () => {
  test("runStatus rejects unknown arguments", () => {
    expect(() => runStatus(["--unknown"])).toThrow("status does not accept argument: --unknown");
  });

  test("runHistory rejects unknown arguments", () => {
    expect(() => runHistory(["--unknown"])).toThrow("history does not accept argument: --unknown");
  });

  test("runVerify rejects unknown arguments", () => {
    expect(() => runVerify(["--unknown"])).toThrow("verify accepts only --full");
  });
});
