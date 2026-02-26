import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { drizzle } from "drizzle-orm/bun-sqlite";
import {
  apply,
  listCommits,
  parsePlan,
  status,
  initDb,
  CodedError,
  query,
} from "../src";
import { schemaHash } from "../src/inspect";
import { executeOperation } from "../src/operation";
import type { Operation } from "../src";
import * as engineSchema from "../src/schema";
import { applyPlan, createTestContext, writePlanFile, withTmpDirCleanup, currentDb } from "./helpers";

const testWithTmp = (name: string, fn: () => void | Promise<void>) => test(name, withTmpDirCleanup(fn));

describe("applyPlan", () => {
  testWithTmp("init -> apply -> status -> history works", async () => {
    const { dir, dbPath } = createTestContext();
    await initDb({ dbPath });

    const createPlanPath = await writePlanFile(dir, "create.json", {
      message: "create expenses table",
      operations: [
        {
          type: "create_table",
          table: "expenses",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "item", type: "TEXT", notNull: true },
            { name: "amount", type: "INTEGER", notNull: true },
          ],
        },
      ],
    });
    const insertPlanPath = await writePlanFile(dir, "insert.json", {
      message: "insert dinner",
      operations: [{ type: "insert", table: "expenses", values: { id: 1, item: "dinner", amount: 1200 } }],
    });

    await applyPlan(currentDb(), createPlanPath);
    const insertCommit = await applyPlan(currentDb(), insertPlanPath);

    const currentStatus = status(currentDb());
    expect(currentStatus.tableCount).toBe(1);
    expect(currentStatus.headCommit?.commitId).toBe(insertCommit.commitId);
    expect(currentStatus.snapshotCount).toBe(0);
    expect(currentStatus.lastVerifiedAt).toBeNull();
    expect(currentStatus.lastVerifiedOk).toBeNull();

    const commits = listCommits(currentDb(), true);
    expect(commits).toHaveLength(2);
    expect(commits[0]?.commitId).toBe(insertCommit.commitId);
    expect(commits[0]?.parentCount).toBe(1);
    expect(commits[0]?.stateHashAfter.length).toBeGreaterThan(10);
  });

  testWithTmp("operations fail for table without primary key", async () => {
    const { dir, dbPath } = createTestContext();
    await initDb({ dbPath });

    const direct = new Database(dbPath);
    direct.run("CREATE TABLE no_pk (name TEXT)");
    direct.close(false);

    const insertPlan = await writePlanFile(dir, "insert-no-pk.json", {
      message: "insert no pk",
      operations: [{ type: "insert", table: "no_pk", values: { name: "a" } }],
    });

    try {
      await applyPlan(currentDb(), insertPlan);
      throw new Error("applyPlan should fail for table without PK");
    } catch (error) {
      expect(CodedError.is(error)).toBe(true);
      if (CodedError.is(error)) {
        expect(error.code).toBe("NO_PRIMARY_KEY");
      }
    }
  });

  testWithTmp("operations fail when observed table contains NULL primary-key values", async () => {
    const { dir, dbPath } = createTestContext();
    await initDb({ dbPath });

    const direct = new Database(dbPath);
    direct.run("CREATE TABLE weak_pk (k TEXT PRIMARY KEY, v TEXT)");
    direct.run("INSERT INTO weak_pk(k, v) VALUES (NULL, 'a')");
    direct.run("INSERT INTO weak_pk(k, v) VALUES (NULL, 'b')");
    direct.close(false);

    const plan = await writePlanFile(dir, "plan-with-nullable-pk-values.json", {
      message: "noop create",
      operations: [
        {
          type: "create_table",
          table: "safe_table",
          columns: [{ name: "id", type: "INTEGER", primaryKey: true }],
        },
      ],
    });

    try {
      await applyPlan(currentDb(), plan);
      throw new Error("applyPlan should fail when NULL PK values exist in tracked table");
    } catch (error) {
      expect(CodedError.is(error)).toBe(true);
      if (CodedError.is(error)) {
        expect(error.code).toBe("APPLY_FAILED");
      }
    }
  });

  testWithTmp("create_table supports SQL timestamp defaults without planner-provided now values", async () => {
    const { dir, dbPath } = createTestContext();
    await initDb({ dbPath });

    const createPlanPath = await writePlanFile(dir, "create-events.sql-default.json", {
      message: "create events with sql default timestamp",
      operations: [
        {
          type: "create_table",
          table: "events",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "title", type: "TEXT", notNull: true },
            {
              name: "recorded_at",
              type: "TEXT",
              notNull: true,
              default: { kind: "sql", expr: "CURRENT_TIMESTAMP" },
            },
          ],
        },
      ],
    });
    await applyPlan(currentDb(), createPlanPath);

    const insertPlanPath = await writePlanFile(dir, "insert-events.sql-default.json", {
      message: "insert event without recorded_at",
      operations: [{ type: "insert", table: "events", values: { id: 1, title: "release" } }],
    });
    await applyPlan(currentDb(), insertPlanPath);

    const rows = query(currentDb(), "SELECT id, title, recorded_at FROM events");
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({ id: 1, title: "release" });
    expect(typeof rows[0]?.recorded_at).toBe("string");
    expect((rows[0]?.recorded_at as string).length).toBeGreaterThan(0);
  });

  testWithTmp("add_column with SQL default is rejected for non-empty tables", async () => {
    const { dir, dbPath } = createTestContext();
    await initDb({ dbPath });

    const setup = await writePlanFile(dir, "add-column-sql-default-setup.json", {
      message: "setup tasks with one row",
      operations: [
        {
          type: "create_table",
          table: "tasks",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "title", type: "TEXT", notNull: true },
          ],
        },
        {
          type: "insert",
          table: "tasks",
          values: { id: 1, title: "first" },
        },
      ],
    });
    await applyPlan(currentDb(), setup);

    const addColumnPlan = await writePlanFile(dir, "add-column-sql-default-fail.json", {
      message: "try to add created_at with sql default",
      operations: [
        {
          type: "add_column",
          table: "tasks",
          column: {
            name: "created_at",
            type: "TEXT",
            default: { kind: "sql", expr: "CURRENT_TIMESTAMP" },
          },
        },
      ],
    });

    await expect(applyPlan(currentDb(), addColumnPlan)).rejects.toThrow(
      /add_column with SQL default is only allowed on empty tables/i,
    );
  });

  testWithTmp("apply fails with STALE_PLAN when baseSchemaHash is outdated", async () => {
    const { dbPath } = createTestContext();
    await initDb({ dbPath });

    const initialHash = schemaHash(currentDb());
    const createPlan = parsePlan(
      JSON.stringify({
        baseSchemaHash: initialHash,
        message: "create stale_test",
        operations: [
          {
            type: "create_table",
            table: "stale_test",
            columns: [{ name: "id", type: "INTEGER", primaryKey: true }],
          },
        ],
      }),
    );
    await apply(currentDb(), createPlan);

    const stalePlan = parsePlan(
      JSON.stringify({
        baseSchemaHash: initialHash,
        message: "insert using stale plan",
        operations: [{ type: "insert", table: "stale_test", values: { id: 1 } }],
      }),
    );
    await expect(apply(currentDb(), stalePlan)).rejects.toMatchObject({
      code: "STALE_PLAN",
    });
  });

  testWithTmp("apply accepts uppercase baseSchemaHash when schema matches", async () => {
    const { dbPath } = createTestContext();
    await initDb({ dbPath });

    const initialHash = schemaHash(currentDb());
    const createPlan = parsePlan(
      JSON.stringify({
        baseSchemaHash: initialHash,
        message: "create hash_case_test",
        operations: [
          {
            type: "create_table",
            table: "hash_case_test",
            columns: [{ name: "id", type: "INTEGER", primaryKey: true }],
          },
        ],
      }),
    );
    await apply(currentDb(), createPlan);

    const currentHash = schemaHash(currentDb());
    const uppercasePlan = parsePlan(
      JSON.stringify({
        baseSchemaHash: currentHash.toUpperCase(),
        message: "insert with uppercase base hash",
        operations: [{ type: "insert", table: "hash_case_test", values: { id: 1 } }],
      }),
    );
    await apply(currentDb(), uppercasePlan);

    const rows = query(currentDb(), "SELECT id FROM hash_case_test ORDER BY id");
    expect(rows).toEqual([{ id: 1 }]);
  });

  testWithTmp("drop_column resolves target column name case-insensitively", async () => {
    const { dir, dbPath } = createTestContext();
    await initDb({ dbPath });

    const direct = new Database(dbPath);
    direct.run('CREATE TABLE case_drop_items (id INTEGER PRIMARY KEY, "Bar" TEXT)');
    direct.close(false);

    const planPath = await writePlanFile(dir, "drop-column-case-insensitive.json", {
      message: "drop bar with lowercase operation column",
      operations: [{ type: "drop_column", table: "case_drop_items", column: "bar" }],
    });
    await applyPlan(currentDb(), planPath);

    const columns = query(currentDb(), "SELECT name FROM pragma_table_info('case_drop_items') ORDER BY cid");
    expect(columns).toEqual([{ name: "id" }]);
  });

  testWithTmp("drop_column fails fast with DEPENDENCY_CONFLICT and suggestedOps", async () => {
    const { dir, dbPath } = createTestContext();
    await initDb({ dbPath });

    const direct = new Database(dbPath);
    direct.run("CREATE TABLE dep_items (id INTEGER PRIMARY KEY, note TEXT)");
    direct.run(`
      CREATE TRIGGER trg_dep_items_note
      AFTER INSERT ON dep_items
      BEGIN
        UPDATE dep_items SET note = upper(note) WHERE id = NEW.id;
      END
    `);
    direct.close(false);

    const planPath = await writePlanFile(dir, "drop-note-with-trigger.json", {
      message: "drop note",
      operations: [{ type: "drop_column", table: "dep_items", column: "note" }],
    });

    try {
      await applyPlan(currentDb(), planPath);
      throw new Error("expected drop_column dependency conflict");
    } catch (error) {
      expect(CodedError.is(error)).toBe(true);
      if (CodedError.is(error)) {
        expect(error.code).toBe("DEPENDENCY_CONFLICT");
        const detail = error.detail as { suggestedOps?: unknown[]; conflicts?: Array<{ type?: string }> } | undefined;
        expect(Array.isArray(detail?.suggestedOps)).toBe(true);
        expect(detail?.suggestedOps).toContainEqual({
          type: "drop_trigger",
          table: "dep_items",
          name: "trg_dep_items_note",
        });
        expect(detail?.conflicts?.some((conflict) => conflict.type === "trigger")).toBe(true);
      }
    }
  });

  testWithTmp("drop_table fails fast for inbound FK dependencies", async () => {
    const { dir, dbPath } = createTestContext();
    await initDb({ dbPath });

    const direct = new Database(dbPath);
    direct.run("PRAGMA foreign_keys=ON");
    direct.run("CREATE TABLE parent_items (id INTEGER PRIMARY KEY)");
    direct.run("CREATE TABLE child_items (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent_items(id))");
    direct.close(false);

    const planPath = await writePlanFile(dir, "drop-parent-with-inbound-fk.json", {
      message: "drop parent table",
      operations: [{ type: "drop_table", table: "parent_items" }],
    });
    await expect(applyPlan(currentDb(), planPath)).rejects.toMatchObject({
      code: "DEPENDENCY_CONFLICT",
    });
  });

  testWithTmp("drop_table ignores inbound FK whose table differs only by non-ASCII case", async () => {
    const { dir, dbPath } = createTestContext();
    await initDb({ dbPath });

    const direct = new Database(dbPath);
    direct.run('CREATE TABLE "Üsers" (id INTEGER PRIMARY KEY)');
    direct.run('CREATE TABLE child_items (id INTEGER PRIMARY KEY, user_id INTEGER REFERENCES "üsers"(id))');
    direct.close(false);

    const planPath = await writePlanFile(dir, "drop-nonascii-parent-with-inbound-fk-case-variant.json", {
      message: "drop non-ascii parent table",
      operations: [{ type: "drop_table", table: "Üsers" }],
    });

    await applyPlan(currentDb(), planPath);
    const dropped = query(currentDb(), "SELECT name FROM sqlite_master WHERE type='table' AND name='Üsers' LIMIT 1");
    expect(dropped).toEqual([]);
  });

  testWithTmp("drop_table ignores view string literals when checking dependencies", async () => {
    const { dir, dbPath } = createTestContext();
    await initDb({ dbPath });

    const direct = new Database(dbPath);
    direct.run("CREATE TABLE users (id INTEGER PRIMARY KEY)");
    direct.run("CREATE VIEW v_literal AS SELECT 'users' AS label");
    direct.close(false);

    const planPath = await writePlanFile(dir, "drop-users-with-literal-view.json", {
      message: "drop users table",
      operations: [{ type: "drop_table", table: "users" }],
    });

    await applyPlan(currentDb(), planPath);
    const dropped = query(currentDb(), "SELECT name FROM sqlite_master WHERE type='table' AND name='users' LIMIT 1");
    expect(dropped).toEqual([]);
  });

  testWithTmp("drop_table ignores view aliases that match target table name", async () => {
    const { dir, dbPath } = createTestContext();
    await initDb({ dbPath });

    const direct = new Database(dbPath);
    direct.run("CREATE TABLE users (id INTEGER PRIMARY KEY)");
    direct.run("CREATE VIEW v_alias AS SELECT 1 AS users");
    direct.close(false);

    const planPath = await writePlanFile(dir, "drop-users-with-alias-view.json", {
      message: "drop users table",
      operations: [{ type: "drop_table", table: "users" }],
    });

    await applyPlan(currentDb(), planPath);
    const dropped = query(currentDb(), "SELECT name FROM sqlite_master WHERE type='table' AND name='users' LIMIT 1");
    expect(dropped).toEqual([]);
  });

  testWithTmp("drop_table does not treat table-owned triggers as external dependencies", async () => {
    const { dir, dbPath } = createTestContext();
    await initDb({ dbPath });

    const direct = new Database(dbPath);
    direct.run("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
    direct.run(`
      CREATE TRIGGER users_ai
      AFTER INSERT ON users
      BEGIN
        UPDATE users SET name = upper(NEW.name) WHERE id = NEW.id;
      END
    `);
    direct.close(false);

    const planPath = await writePlanFile(dir, "drop-users-with-owned-trigger.json", {
      message: "drop users table",
      operations: [{ type: "drop_table", table: "users" }],
    });

    await applyPlan(currentDb(), planPath);
    const dropped = query(currentDb(), "SELECT name FROM sqlite_master WHERE type='table' AND name='users' LIMIT 1");
    expect(dropped).toEqual([]);
  });

  testWithTmp("drop_column ignores unrelated view column tokens with same name", async () => {
    const { dir, dbPath } = createTestContext();
    await initDb({ dbPath });

    const direct = new Database(dbPath);
    direct.run("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
    direct.run("CREATE TABLE posts (id INTEGER PRIMARY KEY, name TEXT)");
    direct.run("CREATE VIEW v_user_post AS SELECT users.id, posts.name FROM users JOIN posts ON posts.id = users.id");
    direct.close(false);

    const planPath = await writePlanFile(dir, "drop-users-name-with-unrelated-view-name-token.json", {
      message: "drop users.name",
      operations: [{ type: "drop_column", table: "users", column: "name" }],
    });

    await applyPlan(currentDb(), planPath);
    const columns = query(currentDb(), "SELECT name FROM pragma_table_info('users') ORDER BY cid");
    expect(columns).toEqual([{ name: "id" }]);
  });

  testWithTmp("drop_column ignores trigger string literals when checking dependencies", async () => {
    const { dir, dbPath } = createTestContext();
    await initDb({ dbPath });

    const direct = new Database(dbPath);
    direct.run("CREATE TABLE dep_items (id INTEGER PRIMARY KEY, note TEXT, body TEXT)");
    direct.run(`
      CREATE TRIGGER dep_items_literal_trigger
      AFTER INSERT ON dep_items
      BEGIN
        SELECT 'note' AS label;
      END
    `);
    direct.close(false);

    const planPath = await writePlanFile(dir, "drop-note-with-literal-trigger.json", {
      message: "drop note",
      operations: [{ type: "drop_column", table: "dep_items", column: "note" }],
    });

    await applyPlan(currentDb(), planPath);
    const columns = query(currentDb(), "SELECT name FROM pragma_table_info('dep_items') ORDER BY cid");
    expect(columns).toEqual([{ name: "id" }, { name: "body" }]);
  });

  testWithTmp("drop_column ignores same-name column references on other tables in trigger body", async () => {
    const { dir, dbPath } = createTestContext();
    await initDb({ dbPath });

    const direct = new Database(dbPath);
    direct.run("CREATE TABLE dep_items (id INTEGER PRIMARY KEY, note TEXT)");
    direct.run("CREATE TABLE posts (id INTEGER PRIMARY KEY, note TEXT)");
    direct.run(`
      CREATE TRIGGER dep_items_other_table_note_trigger
      AFTER INSERT ON dep_items
      BEGIN
        UPDATE posts SET note = upper(note) WHERE id = NEW.id;
      END
    `);
    direct.close(false);

    const planPath = await writePlanFile(dir, "drop-note-with-other-table-note-trigger.json", {
      message: "drop note",
      operations: [{ type: "drop_column", table: "dep_items", column: "note" }],
    });

    await applyPlan(currentDb(), planPath);
    const columns = query(currentDb(), "SELECT name FROM pragma_table_info('dep_items') ORDER BY cid");
    expect(columns).toEqual([{ name: "id" }]);
  });

  testWithTmp("drop_column ignores inbound FK whose table differs only by non-ASCII case", async () => {
    const { dir, dbPath } = createTestContext();
    await initDb({ dbPath });

    const direct = new Database(dbPath);
    direct.run('CREATE TABLE "Üsers" (id INTEGER PRIMARY KEY, code TEXT, note TEXT)');
    direct.run('CREATE TABLE child_items (id INTEGER PRIMARY KEY, user_code TEXT REFERENCES "üsers"(code))');
    direct.close(false);

    const planPath = await writePlanFile(dir, "drop-nonascii-parent-column-with-inbound-fk-case-variant.json", {
      message: "drop non-ascii parent column",
      operations: [{ type: "drop_column", table: "Üsers", column: "code" }],
    });

    await applyPlan(currentDb(), planPath);
    const columns = query(currentDb(), "SELECT name FROM pragma_table_info('Üsers') ORDER BY cid");
    expect(columns).toEqual([{ name: "id" }, { name: "note" }]);
  });

  testWithTmp("drop_index rejects table mismatch instead of dropping by name only", async () => {
    const { dir, dbPath } = createTestContext();
    await initDb({ dbPath });

    const direct = new Database(dbPath);
    direct.run("CREATE TABLE idx_owner_a (id INTEGER PRIMARY KEY, value TEXT)");
    direct.run("CREATE TABLE idx_owner_b (id INTEGER PRIMARY KEY, value TEXT)");
    direct.run("CREATE INDEX idx_owner_b_value ON idx_owner_b(value)");
    direct.close(false);

    const planPath = await writePlanFile(dir, "drop-index-mismatch.json", {
      message: "try dropping index with wrong table",
      operations: [{ type: "drop_index", table: "idx_owner_a", name: "idx_owner_b_value" }],
    });
    await expect(applyPlan(currentDb(), planPath)).rejects.toMatchObject({
      code: "INVALID_OPERATION",
    });

    const indexes = query(
      currentDb(),
      "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_owner_b_value' LIMIT 1",
    );
    expect(indexes).toEqual([{ name: "idx_owner_b_value" }]);
  });

  testWithTmp("drop_trigger rejects table mismatch instead of dropping by name only", async () => {
    const { dir, dbPath } = createTestContext();
    await initDb({ dbPath });

    const direct = new Database(dbPath);
    direct.run("CREATE TABLE trg_owner_a (id INTEGER PRIMARY KEY, value TEXT)");
    direct.run("CREATE TABLE trg_owner_b (id INTEGER PRIMARY KEY, value TEXT)");
    direct.run(`
      CREATE TRIGGER trg_owner_b_ai
      AFTER INSERT ON trg_owner_b
      BEGIN
        UPDATE trg_owner_b SET value = upper(NEW.value) WHERE id = NEW.id;
      END
    `);
    direct.close(false);

    const planPath = await writePlanFile(dir, "drop-trigger-mismatch.json", {
      message: "try dropping trigger with wrong table",
      operations: [{ type: "drop_trigger", table: "trg_owner_a", name: "trg_owner_b_ai" }],
    });
    await expect(applyPlan(currentDb(), planPath)).rejects.toMatchObject({
      code: "INVALID_OPERATION",
    });

    const triggers = query(
      currentDb(),
      "SELECT name FROM sqlite_master WHERE type='trigger' AND name='trg_owner_b_ai' LIMIT 1",
    );
    expect(triggers).toEqual([{ name: "trg_owner_b_ai" }]);
  });

  testWithTmp("restore_table malformed row value fails with INVALID_OPERATION instead of TypeError", () => {
    const db = drizzle({ connection: ":memory:", schema: engineSchema });
    const operation: Operation = {
      type: "restore_table",
      table: "users",
      ddlSql: "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
      rows: [
        {
          id: { storageClass: "integer", sqlLiteral: "1" },
          name: { storageClass: "text", sqlLiteral: "'alice'" },
        },
        {
          id: { storageClass: "integer", sqlLiteral: "2" },
          name: undefined as unknown as { storageClass: "text"; sqlLiteral: string },
        },
      ],
      secondaryObjects: [],
    };

    try {
      executeOperation(db, operation);
      throw new Error("executeOperation should fail for malformed restore row");
    } catch (error) {
      expect(error).not.toBeInstanceOf(TypeError);
      expect(CodedError.is(error)).toBe(true);
      if (CodedError.is(error)) {
        expect(error.code).toBe("INVALID_OPERATION");
        expect(error.message).toContain("restore_table row contains unsupported encoded value");
      }
      const restored = db.$client
        .query<{ name: string }, []>("SELECT name FROM sqlite_master WHERE type='table' AND name='users' LIMIT 1")
        .get() as { name: string } | null;
      expect(restored).toBeNull();
    } finally {
      db.$client.close(false);
    }
  });
});
