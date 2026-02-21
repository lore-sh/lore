import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { getStatus, initDatabase } from "../src";
import { createTestContext, withTmpDirCleanup } from "./helpers";

const testWithTmp = (name: string, fn: () => void | Promise<void>) => test(name, withTmpDirCleanup(fn));

describe("getStatus", () => {
  testWithTmp("returns table counts and offline sync state", async () => {
    const { dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const db = new Database(dbPath);
    try {
      db.run("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
      db.run("INSERT INTO users(id, name) VALUES (1, 'alice'), (2, 'bob')");
    } finally {
      db.close(false);
    }

    const status = getStatus();
    expect(status.tableCount).toBe(1);
    expect(status.tables).toEqual([{ name: "users", count: 2 }]);
    expect(status.sync.state).toBe("offline");
    expect(status.storage.commitCount).toBe(0);
  });
});
