import { describe, expect, test } from "bun:test";
import { listUserTables, resolveDbPath, withInitializedDatabase } from "../src/engine/db";
import { isTossError } from "../src/errors";
import { initDatabase } from "../src";
import { createTestContext, withTmpDirCleanup } from "./helpers";

const testWithTmp = (name: string, fn: () => void | Promise<void>) => test(name, withTmpDirCleanup(fn));

describe("db path resolution", () => {
  test("resolveDbPath returns CONFIG_ERROR when no home env is available", () => {
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
        expect(isTossError(error)).toBe(true);
        if (isTossError(error)) {
          expect(error.code).toBe("CONFIG_ERROR");
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
    await initDatabase({ dbPath });
    withInitializedDatabase(({ db }) => {
      db.run("CREATE TABLE abdrizzle_logs(id INTEGER PRIMARY KEY)");
      db.run("CREATE TABLE atoss_table(id INTEGER PRIMARY KEY)");
      db.run("CREATE TABLE __drizzle_custom(id INTEGER PRIMARY KEY)");
      db.run("CREATE TABLE _toss_custom(id INTEGER PRIMARY KEY)");
    });

    const names = withInitializedDatabase(({ db }) => listUserTables(db));
    expect(names.includes("abdrizzle_logs")).toBe(true);
    expect(names.includes("atoss_table")).toBe(true);
    expect(names.includes("__drizzle_custom")).toBe(false);
    expect(names.includes("_toss_custom")).toBe(false);
  });
});
