import { describe, expect, test } from "bun:test";
import { listUserTables, resolveDbPath } from "../../src/engine/db";
import { CodedError } from "../../src/error";
import { initDb, openDb } from "../../src";
import { createTestContext, currentDb, withTmpDirCleanup } from "../helpers";
import { MetaTable } from "../../src/engine/schema.sql";

const testWithTmp = (name: string, fn: () => void | Promise<void>) => test(name, withTmpDirCleanup(fn));

describe("db path resolution", () => {
  test("resolveDbPath returns CONFIG when no home env is available", () => {
    const env = {
      HOME: process.env.HOME,
      USERPROFILE: process.env.USERPROFILE,
    };

    delete process.env.HOME;
    delete process.env.USERPROFILE;

    try {
      try {
        resolveDbPath();
        throw new Error("resolveDbPath should fail without HOME/USERPROFILE");
      } catch (error) {
        expect(CodedError.is(error)).toBe(true);
        if (CodedError.is(error)) {
          expect(error.code).toBe("CONFIG");
        }
      }
    } finally {
      if (env.HOME === undefined) {
        delete process.env.HOME;
      } else {
        process.env.HOME = env.HOME;
      }
      if (env.USERPROFILE === undefined) {
        delete process.env.USERPROFILE;
      } else {
        process.env.USERPROFILE = env.USERPROFILE;
      }
    }
  });

  testWithTmp("listUserTables excludes only real internal prefixes", async () => {
    const { dbPath } = createTestContext();
    await initDb({ dbPath });
    const db = currentDb();
    db.$client.run("CREATE TABLE abdrizzle_logs(id INTEGER PRIMARY KEY)");
    db.$client.run("CREATE TABLE atoss_table(id INTEGER PRIMARY KEY)");
    db.$client.run("CREATE TABLE __drizzle_custom(id INTEGER PRIMARY KEY)");
    db.$client.run("CREATE TABLE _toss_custom(id INTEGER PRIMARY KEY)");

    const names = listUserTables(db);
    expect(names.includes("abdrizzle_logs")).toBe(true);
    expect(names.includes("atoss_table")).toBe(true);
    expect(names.includes("__drizzle_custom")).toBe(false);
    expect(names.includes("_toss_custom")).toBe(false);
  });

  testWithTmp("openDb returns drizzle client for engine tables", async () => {
    const { dbPath } = createTestContext();
    await initDb({ dbPath });

    const db = openDb(dbPath);
    try {
      const rows = db.select().from(MetaTable).limit(1).all();
      expect(rows.length).toBeGreaterThan(0);
    } finally {
      db.$client.close(false);
    }
  });
});
