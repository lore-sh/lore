import { describe, expect, test } from "bun:test";
import { initDb, openDb, type Database } from "@toss/core";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
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

async function withTempDb<T>(run: (db: Database) => T | Promise<T>): Promise<T> {
  const dir = mkdtempSync(join(tmpdir(), "toss-cli-local-test-"));
  const dbPath = join(dir, "toss.db");
  await initDb({ dbPath });
  const db = openDb(dbPath);
  try {
    return await run(db);
  } finally {
    db.$client.close(false);
    rmSync(dir, { recursive: true, force: true });
  }
}

describe("local command", () => {
  test("runStatus rejects unknown arguments", async () => {
    await withTempDb((db) => {
      expect(() => runStatus(db, ["--unknown"])).toThrow("status does not accept argument: --unknown");
    });
  });

  test("runHistory rejects unknown arguments", async () => {
    await withTempDb((db) => {
      expect(() => runHistory(db, ["--unknown"])).toThrow("history does not accept argument: --unknown");
    });
  });

  test("runVerify rejects unknown arguments", async () => {
    await withTempDb((db) => {
      expect(() => runVerify(db, ["--unknown"])).toThrow("verify accepts only --full");
    });
  });

  test("validate local args", () => {
    expect(() => validateStatusArgs(["--unknown"])).toThrow("status does not accept argument: --unknown");
    expect(() => validateHistoryArgs(["--unknown"])).toThrow("history does not accept argument: --unknown");
    expect(() => validateVerifyArgs(["--unknown"])).toThrow("verify accepts only --full");
    expect(() => validateRevertArgs([])).toThrow("revert requires <commit_id>");
    expect(() => validateRecoverArgs([])).toThrow("recover requires <commit_id>");
  });
});
