import { describe, expect, test } from "bun:test";
import { resolveDbPath } from "../src/db";
import { isTossError } from "../src/errors";

describe("db path resolution", () => {
  test("resolveDbPath returns CONFIG_ERROR when no home env is available", () => {
    const env = {
      HOME: process.env.HOME,
      USERPROFILE: process.env.USERPROFILE,
      TOSS_DB_PATH: process.env.TOSS_DB_PATH,
    };

    delete process.env.HOME;
    delete process.env.USERPROFILE;
    delete process.env.TOSS_DB_PATH;

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
      if (env.TOSS_DB_PATH === undefined) {
        delete process.env.TOSS_DB_PATH;
      } else {
        process.env.TOSS_DB_PATH = env.TOSS_DB_PATH;
      }
    }
  });
});
