import { describe, expect, test } from "bun:test";
import { drizzle } from "drizzle-orm/bun-sqlite";
import { applyRowEffects, assertForeignKeys } from "../src/effect";
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

  test("applyRowEffects restores dropped triggers on failure", () => {
    const db = drizzle({ connection: ":memory:", schema });
    try {
      db.$client.run("PRAGMA foreign_keys=ON");
      db.$client.run("CREATE TABLE items(id INTEGER PRIMARY KEY, note TEXT NOT NULL)");
      db.$client.run("CREATE TABLE audit(id INTEGER PRIMARY KEY AUTOINCREMENT, message TEXT NOT NULL)");
      db.$client.run(`
        CREATE TRIGGER trg_items_ai
        AFTER INSERT ON items
        BEGIN
          INSERT INTO audit(message) VALUES ('ins:' || NEW.id);
        END
      `);
      db.$client.run("INSERT INTO items(id, note) VALUES (1, 'before')");

      expect(() =>
        applyRowEffects(
          db,
          [
            {
              tableName: "items",
              pk: { id: "1" },
              opKind: "update",
              beforeRow: {
                id: { storageClass: "integer", sqlLiteral: "1" },
                note: { storageClass: "text", sqlLiteral: "CAST(X'77726f6e67' AS TEXT)" },
              },
              afterRow: {
                id: { storageClass: "integer", sqlLiteral: "1" },
                note: { storageClass: "text", sqlLiteral: "CAST(X'6166746572' AS TEXT)" },
              },
              beforeHash: null,
              afterHash: null,
            },
          ],
          "forward",
          { disableTableTriggers: true },
        )).toThrow();

      const trigger = db.$client
        .query<{ name: string }, []>("SELECT name FROM sqlite_master WHERE type='trigger' AND name='trg_items_ai' LIMIT 1")
        .get();
      expect(trigger?.name).toBe("trg_items_ai");

      db.$client.run("INSERT INTO items(id, note) VALUES (2, 'still works')");
      const audit = db.$client.query<{ c: number }, []>("SELECT COUNT(*) AS c FROM audit").get();
      expect(audit?.c).toBe(2);
    } finally {
      db.$client.close(false);
    }
  });
});
