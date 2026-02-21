import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { assertNoForeignKeyViolations } from "../../src/engine/effect";

describe("effect helpers", () => {
  test("assertNoForeignKeyViolations passes for valid data", () => {
    const db = new Database(":memory:");
    try {
      db.run("PRAGMA foreign_keys=ON");
      db.run("CREATE TABLE parents(id INTEGER PRIMARY KEY)");
      db.run("CREATE TABLE children(id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parents(id))");
      db.run("INSERT INTO parents(id) VALUES (1)");
      db.run("INSERT INTO children(id, parent_id) VALUES (1, 1)");
      expect(() => assertNoForeignKeyViolations(db, "REVERT_FAILED", "check")).not.toThrow();
    } finally {
      db.close(false);
    }
  });

  test("assertNoForeignKeyViolations throws on invalid refs", () => {
    const db = new Database(":memory:");
    try {
      db.run("PRAGMA foreign_keys=OFF");
      db.run("CREATE TABLE parents(id INTEGER PRIMARY KEY)");
      db.run("CREATE TABLE children(id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parents(id))");
      db.run("INSERT INTO children(id, parent_id) VALUES (1, 999)");
      expect(() => assertNoForeignKeyViolations(db, "REVERT_FAILED", "check")).toThrow();
    } finally {
      db.close(false);
    }
  });
});
