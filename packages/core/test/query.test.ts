import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { initDatabase, CodedError, readQuery } from "../src";
import { createTestContext, withTmpDirCleanup, currentDb } from "./helpers";

const testWithTmp = (name: string, fn: () => void | Promise<void>) => test(name, withTmpDirCleanup(fn));

describe("readQuery", () => {
  testWithTmp("executes read-only SELECT", async () => {
    const { dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const db = new Database(dbPath);
    try {
      db.run("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
      db.run("INSERT INTO users(id, name) VALUES (1, 'alice'), (2, 'bob')");
    } finally {
      db.close(false);
    }

    const rows = readQuery(currentDb(), "SELECT id, name FROM users ORDER BY id ASC");
    expect(rows).toEqual([
      { id: 1, name: "alice" },
      { id: 2, name: "bob" },
    ]);
  });

  testWithTmp("rejects non-read SQL", async () => {
    const { dbPath } = createTestContext();
    await initDatabase({ dbPath });

    expect(() => readQuery(currentDb(), "DELETE FROM users")).toThrow();
    try {
      readQuery(currentDb(), "DELETE FROM users");
    } catch (error) {
      expect(CodedError.is(error)).toBe(true);
    }
  });
});
