import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import {
  runHistory,
  runStatus,
  runVerify,
  validateHistoryArgs,
  validateRecoverArgs,
  validateRevertArgs,
  validateStatusArgs,
  validateVerifyArgs,
} from "../../src/commands/local";

describe("local command", () => {
  test("runStatus rejects unknown arguments", () => {
    const db = new Database(":memory:");
    try {
      expect(() => runStatus(db, ["--unknown"])).toThrow("status does not accept argument: --unknown");
    } finally {
      db.close(false);
    }
  });

  test("runHistory rejects unknown arguments", () => {
    const db = new Database(":memory:");
    try {
      expect(() => runHistory(db, ["--unknown"])).toThrow("history does not accept argument: --unknown");
    } finally {
      db.close(false);
    }
  });

  test("runVerify rejects unknown arguments", () => {
    const db = new Database(":memory:");
    try {
      expect(() => runVerify(db, ["--unknown"])).toThrow("verify accepts only --full");
    } finally {
      db.close(false);
    }
  });

  test("validate local args", () => {
    expect(() => validateStatusArgs(["--unknown"])).toThrow("status does not accept argument: --unknown");
    expect(() => validateHistoryArgs(["--unknown"])).toThrow("history does not accept argument: --unknown");
    expect(() => validateVerifyArgs(["--unknown"])).toThrow("verify accepts only --full");
    expect(() => validateRevertArgs([])).toThrow("revert requires <commit_id>");
    expect(() => validateRecoverArgs([])).toThrow("recover requires <commit_id>");
  });
});
