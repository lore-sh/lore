import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { schema, initDb, openDb, CodedError } from "../src";
import { schemaHash } from "../src/engine/inspect";
import { createTestContext, withTmpDirCleanup, currentDb } from "./helpers";

const testWithTmp = (name: string, fn: () => void | Promise<void>) => test(name, withTmpDirCleanup(fn));

describe("schema", () => {
  testWithTmp("returns detailed schema metadata and schema hash", async () => {
    const { dbPath } = createTestContext();
    await initDb({ dbPath });

    const direct = new Database(dbPath);
    direct.run("PRAGMA foreign_keys=ON");
    direct.run("CREATE TABLE orgs (id INTEGER PRIMARY KEY, name TEXT NOT NULL)");
    direct.run(`
      CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        org_id INTEGER NOT NULL,
        email TEXT NOT NULL UNIQUE,
        amount INTEGER NOT NULL CHECK(amount > 0),
        FOREIGN KEY(org_id) REFERENCES orgs(id)
      )
    `);
    direct.run("CREATE INDEX idx_users_org_id ON users(org_id)");
    direct.run(`
      CREATE TRIGGER trg_users_ai
      AFTER INSERT ON users
      BEGIN
        UPDATE users SET amount = amount WHERE id = NEW.id;
      END
    `);
    direct.run("INSERT INTO orgs(id, name) VALUES (1, 'acme')");
    direct.run("INSERT INTO users(id, org_id, email, amount) VALUES (1, 1, 'a@example.com', 10)");
    direct.close(false);
    const expectedDb = openDb(dbPath);
    const expectedHash = schemaHash(expectedDb);
    expectedDb.$client.close(false);

    const dbSchema = schema(currentDb());
    expect(dbSchema.schemaHash).toBe(expectedHash);
    expect(dbSchema.tables.length).toBeGreaterThanOrEqual(2);

    const users = dbSchema.tables.find((table) => table.name === "users");
    expect(users).toBeDefined();
    if (!users) {
      throw new Error("users table not found");
    }
    expect(users.rowCount).toBe(1);
    expect(users.columns.some((column) => column.name === "email")).toBe(true);
    expect(users.foreignKeys.length).toBe(1);
    expect(users.indexes.length).toBeGreaterThan(0);
    expect(users.triggers.some((trigger) => trigger.name === "trg_users_ai")).toBe(true);
    expect(users.checks.length).toBeGreaterThan(0);
  });

  testWithTmp("supports table filter", async () => {
    const { dbPath } = createTestContext();
    await initDb({ dbPath });

    const direct = new Database(dbPath);
    direct.run("CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)");
    direct.close(false);

    const dbSchema = schema(currentDb(), { table: "notes" });
    expect(dbSchema.tables).toHaveLength(1);
    expect(dbSchema.tables[0]?.name).toBe("notes");
  });

  testWithTmp("does not fold Unicode case while filtering table name", async () => {
    const { dbPath } = createTestContext();
    await initDb({ dbPath });

    const direct = new Database(dbPath);
    direct.run('CREATE TABLE "Ä" (id INTEGER PRIMARY KEY)');
    direct.run('CREATE TABLE "ä" (id INTEGER PRIMARY KEY)');
    direct.close(false);

    const upper = schema(currentDb(), { table: "Ä" });
    expect(upper.tables).toHaveLength(1);
    expect(upper.tables[0]?.name).toBe("Ä");

    const lower = schema(currentDb(), { table: "ä" });
    expect(lower.tables).toHaveLength(1);
    expect(lower.tables[0]?.name).toBe("ä");
  });

  testWithTmp("matches punctuation and space names with ASCII case-insensitive filter", async () => {
    const { dbPath } = createTestContext();
    await initDb({ dbPath });

    const direct = new Database(dbPath);
    direct.run('CREATE TABLE "Foo-Bar" (id INTEGER PRIMARY KEY)');
    direct.run('CREATE TABLE "Task List" (id INTEGER PRIMARY KEY)');
    direct.close(false);

    const dashed = schema(currentDb(), { table: "foo-bar" });
    expect(dashed.tables).toHaveLength(1);
    expect(dashed.tables[0]?.name).toBe("Foo-Bar");

    const spaced = schema(currentDb(), { table: "task list" });
    expect(spaced.tables).toHaveLength(1);
    expect(spaced.tables[0]?.name).toBe("Task List");
  });

  testWithTmp("throws NOT_FOUND when filtered table does not exist", async () => {
    const { dbPath } = createTestContext();
    await initDb({ dbPath });

    try {
      schema(currentDb(), { table: "missing_table" });
      throw new Error("schema should throw for missing table");
    } catch (error) {
      expect(CodedError.is(error)).toBe(true);
      if (CodedError.is(error)) {
        expect(error.code).toBe("NOT_FOUND");
      }
    }
  });
});
