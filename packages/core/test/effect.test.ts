import { describe, expect, test } from "bun:test";
import { drizzle } from "drizzle-orm/bun-sqlite";
import { assertForeignKeys } from "../src/effect";
import * as schema from "../src/schema";

describe("effect helpers", () => {
  test("assertForeignKeys passes for valid data", () => {
    const db = drizzle({ connection: ":memory:", schema });
    try {
      db.$client.run("PRAGMA foreign_keys=ON");
      db.$client.run("CREATE TABLE parents(id INTEGER PRIMARY KEY)");
      db.$client.run("CREATE TABLE children(id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parents(id))");
      db.$client.run("INSERT INTO parents(id) VALUES (1)");
      db.$client.run("INSERT INTO children(id, parent_id) VALUES (1, 1)");
      expect(() => assertForeignKeys(db, "REVERT_FAILED", "check")).not.toThrow();
    } finally {
      db.$client.close(false);
    }
  });

  test("assertForeignKeys throws on invalid refs", () => {
    const db = drizzle({ connection: ":memory:", schema });
    try {
      db.$client.run("PRAGMA foreign_keys=OFF");
      db.$client.run("CREATE TABLE parents(id INTEGER PRIMARY KEY)");
      db.$client.run("CREATE TABLE children(id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parents(id))");
      db.$client.run("INSERT INTO children(id, parent_id) VALUES (1, 999)");
      expect(() => assertForeignKeys(db, "REVERT_FAILED", "check")).toThrow();
    } finally {
      db.$client.close(false);
    }
  });
});
