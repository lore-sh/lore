import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { status, initDb } from "../src";
import { writeRemoteConfig } from "../src/config";
import { createTestContext, withTestHome, withTmpDirCleanup, currentDb } from "./helpers";

const testWithTmp = (name: string, fn: () => void | Promise<void>) => test(name, withTmpDirCleanup(fn));

describe("status", () => {
  testWithTmp("returns table counts and offline sync state", async () => {
    const { dbPath } = createTestContext();
    await initDb({ dbPath });

    const db = new Database(dbPath);
    try {
      db.run("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
      db.run("INSERT INTO users(id, name) VALUES (1, 'alice'), (2, 'bob')");
    } finally {
      db.close(false);
    }

    const currentStatus = status(currentDb());
    expect(currentStatus.tableCount).toBe(1);
    expect(currentStatus.tables).toEqual([{ name: "users", count: 2 }]);
    expect(currentStatus.sync.state).toBe("offline");
    expect(currentStatus.storage.commitCount).toBe(0);
  });

  testWithTmp("keeps offline state before first sync even when remote is configured", async () => {
    const { dir, dbPath } = createTestContext();
    await initDb({ dbPath });

    withTestHome(dir, () => {
      writeRemoteConfig({ platform: "libsql", url: "libsql://status-test.turso.io" });
      const currentStatus = status(currentDb());
      expect(currentStatus.sync.configured).toBe(true);
      expect(currentStatus.sync.state).toBe("offline");
      expect(currentStatus.sync.pendingCommits).toBe(0);
    });
  });
});
