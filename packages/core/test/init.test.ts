import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { join } from "node:path";
import { status, initDb, openDb } from "../src";
import { createTestContext, withTmpDirCleanup, currentDb } from "./helpers";

const testWithTmp = (name: string, fn: () => void | Promise<void>) => test(name, withTmpDirCleanup(fn));

interface GlobalEnvSnapshot {
  HOME?: string | undefined;
  CODEX_HOME?: string | undefined;
  XDG_CONFIG_HOME?: string | undefined;
}

function captureGlobalEnv(): GlobalEnvSnapshot {
  return {
    HOME: process.env.HOME,
    CODEX_HOME: process.env.CODEX_HOME,
    XDG_CONFIG_HOME: process.env.XDG_CONFIG_HOME,
  };
}

function restoreGlobalEnv(snapshot: GlobalEnvSnapshot): void {
  if (snapshot.HOME === undefined) {
    delete process.env.HOME;
  } else {
    process.env.HOME = snapshot.HOME;
  }
  if (snapshot.CODEX_HOME === undefined) {
    delete process.env.CODEX_HOME;
  } else {
    process.env.CODEX_HOME = snapshot.CODEX_HOME;
  }
  if (snapshot.XDG_CONFIG_HOME === undefined) {
    delete process.env.XDG_CONFIG_HOME;
  } else {
    process.env.XDG_CONFIG_HOME = snapshot.XDG_CONFIG_HOME;
  }
}

describe("initDb", () => {
  testWithTmp("force-new reinitializes database", async () => {
    const { dbPath } = createTestContext();
    await initDb({ dbPath });

    const direct = new Database(dbPath);
    direct.run("CREATE TABLE foo (id INTEGER PRIMARY KEY, v TEXT)");
    direct.run("INSERT INTO foo(id, v) VALUES(1, 'x')");
    direct.close(false);

    const reinit = await initDb({ dbPath, forceNew: true });
    expect(reinit.path).toBe(dbPath);
    const currentStatus = status(currentDb());
    expect(currentStatus.tableCount).toBe(0);
    expect(currentStatus.headCommit).toBeNull();
  });

  testWithTmp("init applies drizzle migrations metadata", async () => {
    const { dbPath } = createTestContext();
    await initDb({ dbPath });

    const db = new Database(dbPath);
    try {
      const migrationTable = db
        .query<{ ok: number }, []>("SELECT 1 AS ok FROM sqlite_master WHERE type='table' AND name='__drizzle_migrations' LIMIT 1")
        .get();
      expect(migrationTable).toEqual({ ok: 1 });
      const row = db.query<{ c: number }, []>("SELECT COUNT(*) AS c FROM __drizzle_migrations").get();
      expect((row?.c ?? 0) > 0).toBe(true);
    } finally {
      db.close(false);
    }
  });

  testWithTmp("default database path is ~/.toss/toss.db", async () => {
    const { dir } = createTestContext();
    const snapshot = captureGlobalEnv();
    process.env.HOME = join(dir, "home");
    process.env.CODEX_HOME = join(dir, "codex-home");
    process.env.XDG_CONFIG_HOME = join(dir, "xdg-config");
    try {
      const result = await initDb();
      const expected = join(dir, "home", ".toss", "toss.db");
      expect(result.path).toBe(expected);
      expect(await Bun.file(expected).exists()).toBe(true);
    } finally {
      restoreGlobalEnv(snapshot);
    }
  });

  testWithTmp("status on missing default database does not create a new file", async () => {
    const { dir } = createTestContext();
    const snapshot = captureGlobalEnv();
    process.env.HOME = join(dir, "home");
    process.env.CODEX_HOME = join(dir, "codex-home");
    process.env.XDG_CONFIG_HOME = join(dir, "xdg-config");
    try {
      const expected = join(dir, "home", ".toss", "toss.db");
      expect(await Bun.file(expected).exists()).toBe(false);
      expect(() => openDb()).toThrow("Database is not initialized");
      expect(await Bun.file(expected).exists()).toBe(false);
    } finally {
      restoreGlobalEnv(snapshot);
    }
  });
});
