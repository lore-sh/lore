import { describe, expect, test } from "bun:test";
import { drizzle } from "drizzle-orm/bun-sqlite";
import { assertNoForeignKeyViolations } from "../src/effect";
import * as schema from "../src/schema";

describe("effect helpers", () => {
  test("assertNoForeignKeyViolations passes for valid data", () => {
    const db = drizzle({ connection: ":memory:", schema });
    try {
      db.$client.run("PRAGMA foreign_keys=ON");
      db.$client.run("CREATE TABLE parents(id INTEGER PRIMARY KEY)");
      db.$client.run("CREATE TABLE children(id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parents(id))");
      db.$client.run("INSERT INTO parents(id) VALUES (1)");
      db.$client.run("INSERT INTO children(id, parent_id) VALUES (1, 1)");
      expect(() => assertNoForeignKeyViolations(db, "REVERT_FAILED", "check")).not.toThrow();
    } finally {
      db.$client.close(false);
    }
  });

  test("assertNoForeignKeyViolations throws on invalid refs", () => {
    const db = drizzle({ connection: ":memory:", schema });
    try {
      db.$client.run("PRAGMA foreign_keys=OFF");
      db.$client.run("CREATE TABLE parents(id INTEGER PRIMARY KEY)");
      db.$client.run("CREATE TABLE children(id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parents(id))");
      db.$client.run("INSERT INTO children(id, parent_id) VALUES (1, 999)");
      expect(() => assertNoForeignKeyViolations(db, "REVERT_FAILED", "check")).toThrow();
    } finally {
      db.$client.close(false);
    }
  });
});
