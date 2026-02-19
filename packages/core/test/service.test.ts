import { afterEach, describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { existsSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { COMMIT_TABLE, EFFECT_ROW_TABLE, ENGINE_META_TABLE } from "../src/db";
import { schemaHash } from "../src/rows";
import {
  applyPlan,
  getHistory,
  getStatus,
  initDatabase,
  isTossError,
  readQuery,
  recoverFromSnapshot,
  revertCommit,
  verifyDatabase,
} from "../src";

const tmpDirs: string[] = [];

function createTestContext(): { dir: string; dbPath: string } {
  const dir = mkdtempSync(join(tmpdir(), "toss-core-test-"));
  tmpDirs.push(dir);
  const dbPath = join(dir, "toss.db");
  return { dir, dbPath };
}

function writePlanFile(dir: string, name: string, payload: unknown): string {
  const path = join(dir, name);
  writeFileSync(path, JSON.stringify(payload), "utf8");
  return path;
}

afterEach(() => {
  while (tmpDirs.length > 0) {
    const dir = tmpDirs.pop();
    if (dir) {
      rmSync(dir, { recursive: true, force: true });
    }
  }
});

describe("toss canonical engine", () => {
  test("init -> apply -> status -> history works", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const createPlanPath = writePlanFile(dir, "create.json", {
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
    const insertPlanPath = writePlanFile(dir, "insert.json", {
      message: "insert dinner",
      operations: [{ type: "insert", table: "expenses", values: { id: 1, item: "dinner", amount: 1200 } }],
    });

    await applyPlan(createPlanPath, { dbPath });
    const insertCommit = await applyPlan(insertPlanPath, { dbPath });

    const status = getStatus({ dbPath });
    expect(status.tableCount).toBe(1);
    expect(status.headCommit?.commitId).toBe(insertCommit.commitId);
    expect(status.snapshotCount).toBe(0);
    expect(status.lastVerifiedAt).toBeNull();
    expect(status.lastVerifiedOk).toBeNull();
    expect(status.lastVerifiedOkAt).toBeNull();

    const history = getHistory({ dbPath, verbose: true });
    expect(history).toHaveLength(2);
    expect(history[0]?.commitId).toBe(insertCommit.commitId);
    expect(history[0]?.parentIds).toHaveLength(1);
    expect(history[0]?.stateHashAfter.length).toBeGreaterThan(10);
  });

  test("force-new reinitializes database", async () => {
    const { dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const direct = new Database(dbPath);
    direct.run('CREATE TABLE foo (id INTEGER PRIMARY KEY, v TEXT)');
    direct.run("INSERT INTO foo(id, v) VALUES(1, 'x')");
    direct.close(false);

    const reinit = await initDatabase({ dbPath, forceNew: true });
    expect(reinit.dbPath).toBe(dbPath);
    const status = getStatus({ dbPath });
    expect(status.tableCount).toBe(0);
    expect(status.headCommit).toBeNull();
  });

  test("operations fail for table without primary key", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const direct = new Database(dbPath);
    direct.run("CREATE TABLE no_pk (name TEXT)");
    direct.close(false);

    const insertPlan = writePlanFile(dir, "insert-no-pk.json", {
      message: "insert no pk",
      operations: [{ type: "insert", table: "no_pk", values: { name: "a" } }],
    });

    try {
      await applyPlan(insertPlan, { dbPath });
      throw new Error("applyPlan should fail for table without PK");
    } catch (error) {
      expect(isTossError(error)).toBe(true);
      if (isTossError(error)) {
        expect(error.code).toBe("TABLE_WITHOUT_PRIMARY_KEY");
      }
    }
  });

  test("schemaHash includes UNIQUE/CHECK/FOREIGN KEY constraints", async () => {
    const ctxA = createTestContext();
    await initDatabase({ dbPath: ctxA.dbPath });
    const dbA = new Database(ctxA.dbPath);
    dbA.run(`
      CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        email TEXT UNIQUE,
        org_id INTEGER,
        CHECK (length(email) > 0),
        FOREIGN KEY(org_id) REFERENCES orgs(id)
      )
    `);
    const hashA = schemaHash(dbA);
    dbA.close(false);

    const ctxB = createTestContext();
    await initDatabase({ dbPath: ctxB.dbPath });
    const dbB = new Database(ctxB.dbPath);
    dbB.run(`
      CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        email TEXT,
        org_id INTEGER
      )
    `);
    const hashB = schemaHash(dbB);
    dbB.close(false);

    expect(hashA).not.toBe(hashB);
  });

  test("schemaHash preserves whitespace inside SQL string literals", async () => {
    const ctxA = createTestContext();
    await initDatabase({ dbPath: ctxA.dbPath });
    const dbA = new Database(ctxA.dbPath);
    dbA.run(`
      CREATE TABLE checks (
        id INTEGER PRIMARY KEY,
        value TEXT CHECK (value <> 'a  b')
      )
    `);
    const hashA = schemaHash(dbA);
    dbA.close(false);

    const ctxB = createTestContext();
    await initDatabase({ dbPath: ctxB.dbPath });
    const dbB = new Database(ctxB.dbPath);
    dbB.run(`
      CREATE TABLE checks (
        id INTEGER PRIMARY KEY,
        value TEXT CHECK (value <> 'a b')
      )
    `);
    const hashB = schemaHash(dbB);
    dbB.close(false);

    expect(hashA).not.toBe(hashB);
  });

  test("schemaHash normalizes SQL keyword case outside literals", async () => {
    const ctxA = createTestContext();
    await initDatabase({ dbPath: ctxA.dbPath });
    const dbA = new Database(ctxA.dbPath);
    dbA.run("create table users (id integer primary key, email text check (length(email) > 0))");
    dbA.run("create index idx_users_email on users(email)");
    dbA.run("create trigger trg_users_ai after insert on users begin select 1; end");
    const hashA = schemaHash(dbA);
    dbA.close(false);

    const ctxB = createTestContext();
    await initDatabase({ dbPath: ctxB.dbPath });
    const dbB = new Database(ctxB.dbPath);
    dbB.run("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT CHECK (LENGTH(email) > 0))");
    dbB.run("CREATE INDEX idx_users_email ON users(email)");
    dbB.run("CREATE TRIGGER trg_users_ai AFTER INSERT ON users BEGIN SELECT 1; END");
    const hashB = schemaHash(dbB);
    dbB.close(false);

    expect(hashA).toBe(hashB);
  });

  test("schemaHash includes COLLATE and generated-column expressions", async () => {
    const ctxA = createTestContext();
    await initDatabase({ dbPath: ctxA.dbPath });
    const dbA = new Database(ctxA.dbPath);
    dbA.run(`
      CREATE TABLE docs (
        id INTEGER PRIMARY KEY,
        title TEXT COLLATE NOCASE,
        normalized TEXT GENERATED ALWAYS AS (trim(lower(title))) STORED
      )
    `);
    const hashA = schemaHash(dbA);
    dbA.close(false);

    const ctxB = createTestContext();
    await initDatabase({ dbPath: ctxB.dbPath });
    const dbB = new Database(ctxB.dbPath);
    dbB.run(`
      CREATE TABLE docs (
        id INTEGER PRIMARY KEY,
        title TEXT COLLATE BINARY,
        normalized TEXT GENERATED ALWAYS AS (title) STORED
      )
    `);
    const hashB = schemaHash(dbB);
    dbB.close(false);

    expect(hashA).not.toBe(hashB);
  });

  test("drop_table revert restores table definition and rows", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const setup = writePlanFile(dir, "setup.json", {
      message: "setup",
      operations: [
        {
          type: "create_table",
          table: "expenses",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "item", type: "TEXT", notNull: true },
          ],
        },
        { type: "insert", table: "expenses", values: { id: 1, item: "dinner" } },
      ],
    });
    const drop = writePlanFile(dir, "drop.json", {
      message: "drop table",
      operations: [{ type: "drop_table", table: "expenses" }],
    });

    await applyPlan(setup, { dbPath });
    const dropCommit = await applyPlan(drop, { dbPath });

    const revert = revertCommit(dropCommit.commitId, { dbPath });
    expect(revert.ok).toBe(true);

    const rows = readQuery("SELECT id, item FROM expenses", { dbPath });
    expect(rows).toEqual([{ id: 1, item: "dinner" }]);
  });

  test("revert works when original DDL used unquoted table name", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const direct = new Database(dbPath);
    direct.run("CREATE TABLE my_table (id INTEGER PRIMARY KEY, v TEXT)");
    direct.run("INSERT INTO my_table(id, v) VALUES (1, 'x')");
    direct.close(false);

    const drop = writePlanFile(dir, "drop-unquoted.json", {
      message: "drop table",
      operations: [{ type: "drop_table", table: "my_table" }],
    });

    const dropCommit = await applyPlan(drop, { dbPath });
    const reverted = revertCommit(dropCommit.commitId, { dbPath });
    expect(reverted.ok).toBe(true);
    const rows = readQuery("SELECT id, v FROM my_table", { dbPath });
    expect(rows).toEqual([{ id: 1, v: "x" }]);
  });

  test("drop_column and alter_column_type revert restore schema and values", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const setup = writePlanFile(dir, "setup.json", {
      message: "setup ledger",
      operations: [
        {
          type: "create_table",
          table: "ledger",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "amount", type: "TEXT", notNull: true },
            { name: "note", type: "TEXT" },
          ],
        },
        { type: "insert", table: "ledger", values: { id: 1, amount: "1200", note: "meal" } },
      ],
    });
    const mutate = writePlanFile(dir, "mutate.json", {
      message: "schema mutate",
      operations: [
        { type: "alter_column_type", table: "ledger", column: "amount", newType: "INTEGER" },
        { type: "drop_column", table: "ledger", column: "note" },
      ],
    });

    await applyPlan(setup, { dbPath });
    const mutateCommit = await applyPlan(mutate, { dbPath });

    const revert = revertCommit(mutateCommit.commitId, { dbPath });
    expect(revert.ok).toBe(true);

    const typeRow = readQuery("SELECT typeof(amount) AS t, note FROM ledger WHERE id=1", { dbPath });
    expect(typeRow).toEqual([{ t: "text", note: "meal" }]);
  });

  test("alter_column_type preserves FOREIGN KEY constraints", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const direct = new Database(dbPath);
    direct.run("PRAGMA foreign_keys=ON");
    direct.run("CREATE TABLE parents (id INTEGER PRIMARY KEY)");
    direct.run(`
      CREATE TABLE children (
        id INTEGER PRIMARY KEY,
        parent_id INTEGER NOT NULL,
        amount TEXT NOT NULL,
        FOREIGN KEY(parent_id) REFERENCES parents(id) ON DELETE CASCADE
      )
    `);
    direct.run("INSERT INTO parents(id) VALUES (1)");
    direct.run("INSERT INTO children(id, parent_id, amount) VALUES (1, 1, '42')");
    direct.close(false);

    const alter = writePlanFile(dir, "alter-child-amount.json", {
      message: "convert child amount",
      operations: [{ type: "alter_column_type", table: "children", column: "amount", newType: "INTEGER" }],
    });
    await applyPlan(alter, { dbPath });

    const verifyDb = new Database(dbPath);
    verifyDb.run("PRAGMA foreign_keys=ON");
    const fkRows = verifyDb.query("PRAGMA foreign_key_list('children')").all() as Array<{ table: string }>;
    expect(fkRows).toHaveLength(1);
    expect(fkRows[0]?.table).toBe("parents");
    verifyDb.run("DELETE FROM parents WHERE id=1");
    const remaining = verifyDb.query("SELECT COUNT(*) AS c FROM children").get() as { c: number };
    expect(remaining.c).toBe(0);
    verifyDb.close(false);
  });

  test("alter_column_type supports self-referential FOREIGN KEY tables", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const direct = new Database(dbPath);
    direct.run("PRAGMA foreign_keys=ON");
    direct.run(`
      CREATE TABLE tree_nodes (
        id INTEGER PRIMARY KEY,
        parent_id INTEGER,
        weight TEXT NOT NULL,
        FOREIGN KEY(parent_id) REFERENCES tree_nodes(id)
      )
    `);
    direct.run("INSERT INTO tree_nodes(id, parent_id, weight) VALUES (1, NULL, '10')");
    direct.run("INSERT INTO tree_nodes(id, parent_id, weight) VALUES (2, 1, '20')");
    direct.close(false);

    const alter = writePlanFile(dir, "alter-self-fk-tree-nodes.json", {
      message: "alter self fk table",
      operations: [{ type: "alter_column_type", table: "tree_nodes", column: "weight", newType: "INTEGER" }],
    });
    await applyPlan(alter, { dbPath });

    const verifyDb = new Database(dbPath);
    verifyDb.run("PRAGMA foreign_keys=ON");
    const typeRow = verifyDb.query("SELECT typeof(weight) AS t FROM tree_nodes WHERE id=2").get() as { t: string };
    expect(typeRow.t).toBe("integer");
    const fkRows = verifyDb.query("PRAGMA foreign_key_list('tree_nodes')").all() as Array<{ table: string }>;
    expect(fkRows).toHaveLength(1);
    expect(fkRows[0]?.table).toBe("tree_nodes");
    verifyDb.run("INSERT INTO tree_nodes(id, parent_id, weight) VALUES (3, 2, 30)");
    verifyDb.close(false);
  });

  test("alter_column_type supports legacy single-quoted identifiers in DDL", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const direct = new Database(dbPath);
    direct.run("CREATE TABLE 'sq_users' ('id' INTEGER PRIMARY KEY, 'amount' TEXT)");
    direct.run("INSERT INTO 'sq_users'('id', 'amount') VALUES (1, '44')");
    direct.close(false);

    const alter = writePlanFile(dir, "alter-single-quoted-identifiers.json", {
      message: "alter legacy single-quoted identifiers",
      operations: [{ type: "alter_column_type", table: "sq_users", column: "amount", newType: "INTEGER" }],
    });
    await applyPlan(alter, { dbPath });

    const rows = readQuery('SELECT typeof("amount") AS t FROM "sq_users" WHERE "id" = 1', { dbPath });
    expect(rows).toEqual([{ t: "integer" }]);
  });

  test("alter_column_type works for quoted keyword column names", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const direct = new Database(dbPath);
    direct.run(`
      CREATE TABLE kw_cols (
        "primary" TEXT PRIMARY KEY,
        note TEXT
      )
    `);
    direct.run(`INSERT INTO kw_cols("primary", note) VALUES ('7', 'x')`);
    direct.close(false);

    const alter = writePlanFile(dir, "alter-quoted-keyword-column.json", {
      message: "convert quoted keyword column type",
      operations: [{ type: "alter_column_type", table: "kw_cols", column: "primary", newType: "INTEGER" }],
    });
    await applyPlan(alter, { dbPath });

    const rows = readQuery(`SELECT typeof("primary") AS t, note FROM kw_cols`, { dbPath });
    expect(rows).toEqual([{ t: "integer", note: "x" }]);
  });

  test("alter_column_type handles leading comments in column segments", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const direct = new Database(dbPath);
    direct.run(`
      CREATE TABLE commented_cols (
        id INTEGER PRIMARY KEY,
        a TEXT,
        -- this comment starts the next segment
        b TEXT,
        c TEXT
      )
    `);
    direct.run(`INSERT INTO commented_cols(id, a, b, c) VALUES (1, 'x', '11', 'y')`);
    direct.close(false);

    const alter = writePlanFile(dir, "alter-commented-column.json", {
      message: "convert commented column type",
      operations: [{ type: "alter_column_type", table: "commented_cols", column: "b", newType: "INTEGER" }],
    });
    await applyPlan(alter, { dbPath });

    const rows = readQuery("SELECT typeof(b) AS t, c FROM commented_cols WHERE id=1", { dbPath });
    expect(rows).toEqual([{ t: "integer", c: "y" }]);
  });

  test("alter_column_type preserves newline-terminated line comments around commas", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const direct = new Database(dbPath);
    direct.run(`
      CREATE TABLE line_comment_cols (
        id INTEGER PRIMARY KEY,
        a TEXT -- comment before comma
        , b TEXT -- trailing comment before close
      )
    `);
    direct.run(`INSERT INTO line_comment_cols(id, a, b) VALUES (1, 'x', '12')`);
    direct.close(false);

    const alter = writePlanFile(dir, "alter-line-comment-column.json", {
      message: "convert line-commented column type",
      operations: [{ type: "alter_column_type", table: "line_comment_cols", column: "b", newType: "INTEGER" }],
    });
    await applyPlan(alter, { dbPath });

    const rows = readQuery("SELECT typeof(a) AS ta, typeof(b) AS tb FROM line_comment_cols WHERE id=1", { dbPath });
    expect(rows).toEqual([{ ta: "text", tb: "integer" }]);
  });

  test("alter_column_type preserves trailing line comment on last column segment", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const direct = new Database(dbPath);
    direct.run(`
      CREATE TABLE trailing_comment_last (
        id INTEGER PRIMARY KEY,
        b TEXT -- final line comment
      )
    `);
    direct.run(`INSERT INTO trailing_comment_last(id, b) VALUES (1, '34')`);
    direct.close(false);

    const alter = writePlanFile(dir, "alter-trailing-comment-last.json", {
      message: "convert last commented column type",
      operations: [{ type: "alter_column_type", table: "trailing_comment_last", column: "b", newType: "INTEGER" }],
    });
    await applyPlan(alter, { dbPath });

    const rows = readQuery("SELECT typeof(b) AS tb FROM trailing_comment_last WHERE id=1", { dbPath });
    expect(rows).toEqual([{ tb: "integer" }]);
  });

  test("alter_column_type resolves table DDL case-insensitively and preserves secondary objects", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const direct = new Database(dbPath);
    direct.run("CREATE TABLE Users (id INTEGER PRIMARY KEY, amount TEXT, note TEXT)");
    direct.run("CREATE INDEX idx_users_note ON Users(note)");
    direct.run(`
      CREATE TRIGGER trg_users_upper_note
      AFTER INSERT ON Users
      BEGIN
        UPDATE Users SET note = upper(note) WHERE id = NEW.id;
      END
    `);
    direct.run("INSERT INTO Users(id, amount, note) VALUES (1, '12', 'a')");
    direct.close(false);

    const alter = writePlanFile(dir, "alter-users-case-insensitive.json", {
      message: "alter users amount",
      operations: [{ type: "alter_column_type", table: "users", column: "amount", newType: "INTEGER" }],
    });
    await applyPlan(alter, { dbPath });

    const db = new Database(dbPath);
    const indexes = db.query("PRAGMA index_list('Users')").all() as Array<{ name: string }>;
    expect(indexes.some((index) => index.name === "idx_users_note")).toBe(true);
    db.run("INSERT INTO Users(id, amount, note) VALUES (2, '9', 'b')");
    const row = db.query("SELECT note FROM Users WHERE id=2").get() as { note: string } | null;
    expect(row?.note).toBe("B");
    db.close(false);
  });

  test("alter_column_type supports non-ASCII bare identifiers", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const direct = new Database(dbPath);
    direct.run("CREATE TABLE 사용자 (id INTEGER PRIMARY KEY, 이름 TEXT)");
    direct.run("INSERT INTO 사용자(id, 이름) VALUES (1, '42')");
    direct.close(false);

    const alter = writePlanFile(dir, "alter-unicode-bare-identifiers.json", {
      message: "alter unicode bare column",
      operations: [{ type: "alter_column_type", table: "사용자", column: "이름", newType: "INTEGER" }],
    });
    await applyPlan(alter, { dbPath });

    const rows = readQuery('SELECT typeof("이름") AS t FROM "사용자" WHERE id=1', { dbPath });
    expect(rows).toEqual([{ t: "integer" }]);
  });

  test("alter_column_type supports non-BMP bare identifiers", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const direct = new Database(dbPath);
    direct.run("CREATE TABLE astral_cols (id INTEGER PRIMARY KEY, 𐐷 TEXT)");
    direct.run("INSERT INTO astral_cols(id, 𐐷) VALUES (1, '56')");
    direct.close(false);

    const alter = writePlanFile(dir, "alter-astral-bare-identifiers.json", {
      message: "alter astral bare column",
      operations: [{ type: "alter_column_type", table: "astral_cols", column: "𐐷", newType: "INTEGER" }],
    });
    await applyPlan(alter, { dbPath });

    const rows = readQuery('SELECT typeof("𐐷") AS t FROM "astral_cols" WHERE id=1', { dbPath });
    expect(rows).toEqual([{ t: "integer" }]);
  });

  test("alter_column_type does not fold Unicode case when matching columns", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const direct = new Database(dbPath);
    direct.run('CREATE TABLE unicode_case_cols (id INTEGER PRIMARY KEY, "Ä" TEXT, "ä" TEXT)');
    direct.run('INSERT INTO unicode_case_cols(id, "Ä", "ä") VALUES (1, \'10\', \'11\')');
    direct.close(false);

    const alter = writePlanFile(dir, "alter-unicode-case-sensitive-column.json", {
      message: "alter only capital-a-umlaut",
      operations: [{ type: "alter_column_type", table: "unicode_case_cols", column: "Ä", newType: "INTEGER" }],
    });
    await applyPlan(alter, { dbPath });

    const rows = readQuery('SELECT typeof("Ä") AS upper_t, typeof("ä") AS lower_t FROM "unicode_case_cols" WHERE id=1', {
      dbPath,
    });
    expect(rows).toEqual([{ upper_t: "integer", lower_t: "text" }]);
  });

  test("restore_table path handles self-referential FOREIGN KEY tables", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const direct = new Database(dbPath);
    direct.run("PRAGMA foreign_keys=ON");
    direct.run(`
      CREATE TABLE graph_nodes (
        id INTEGER PRIMARY KEY,
        parent_id INTEGER,
        name TEXT,
        tag TEXT,
        FOREIGN KEY(parent_id) REFERENCES graph_nodes(id)
      )
    `);
    direct.run("INSERT INTO graph_nodes(id, parent_id, name, tag) VALUES (1, NULL, 'root', 'a')");
    direct.run("INSERT INTO graph_nodes(id, parent_id, name, tag) VALUES (2, 1, 'child', 'b')");
    direct.close(false);

    const dropColumn = writePlanFile(dir, "drop-tag-self-fk.json", {
      message: "drop tag",
      operations: [{ type: "drop_column", table: "graph_nodes", column: "tag" }],
    });
    const dropped = await applyPlan(dropColumn, { dbPath });
    const reverted = revertCommit(dropped.commitId, { dbPath });
    expect(reverted.ok).toBe(true);

    const verifyDb = new Database(dbPath);
    verifyDb.run("PRAGMA foreign_keys=ON");
    const rows = verifyDb
      .query("SELECT id, parent_id, tag FROM graph_nodes ORDER BY id")
      .all() as Array<{ id: number; parent_id: number | null; tag: string | null }>;
    expect(rows).toEqual([
      { id: 1, parent_id: null, tag: "a" },
      { id: 2, parent_id: 1, tag: "b" },
    ]);
    const fkRows = verifyDb.query("PRAGMA foreign_key_list('graph_nodes')").all() as Array<{ table: string }>;
    expect(fkRows).toHaveLength(1);
    expect(fkRows[0]?.table).toBe("graph_nodes");
    verifyDb.close(false);
  });

  test("restore_table path supports legacy single-quoted table identifiers", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const direct = new Database(dbPath);
    direct.run("CREATE TABLE 'sq_restore' ('id' INTEGER PRIMARY KEY, 'name' TEXT, 'tag' TEXT)");
    direct.run("INSERT INTO 'sq_restore'('id', 'name', 'tag') VALUES (1, 'a', 'x')");
    direct.close(false);

    const dropColumn = writePlanFile(dir, "drop-tag-single-quoted-table.json", {
      message: "drop single-quoted tag",
      operations: [{ type: "drop_column", table: "sq_restore", column: "tag" }],
    });
    const dropped = await applyPlan(dropColumn, { dbPath });
    const reverted = revertCommit(dropped.commitId, { dbPath });
    expect(reverted.ok).toBe(true);

    const rows = readQuery('SELECT "id", "name", "tag" FROM "sq_restore"', { dbPath });
    expect(rows).toEqual([{ id: 1, name: "a", tag: "x" }]);
  });

  test("update/delete revert detects row conflicts", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const setup = writePlanFile(dir, "setup.json", {
      message: "setup",
      operations: [
        {
          type: "create_table",
          table: "items",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "name", type: "TEXT", notNull: true },
            { name: "qty", type: "INTEGER", notNull: true },
          ],
        },
        { type: "insert", table: "items", values: { id: 1, name: "apple", qty: 3 } },
      ],
    });
    const updateA = writePlanFile(dir, "update-a.json", {
      message: "set qty 5",
      operations: [{ type: "update", table: "items", values: { qty: 5 }, where: { id: 1 } }],
    });
    const updateB = writePlanFile(dir, "update-b.json", {
      message: "set qty 7",
      operations: [{ type: "update", table: "items", values: { qty: 7 }, where: { id: 1 } }],
    });

    await applyPlan(setup, { dbPath });
    const target = await applyPlan(updateA, { dbPath });
    await applyPlan(updateB, { dbPath });

    const result = revertCommit(target.commitId, { dbPath });
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.conflicts.length).toBeGreaterThan(0);
      expect(result.conflicts[0]?.kind).toBe("row");
    }
  });

  test("revert delete returns structured conflict when later commit reinserted same PK", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const setup = writePlanFile(dir, "setup-delete-conflict.json", {
      message: "setup",
      operations: [
        {
          type: "create_table",
          table: "users",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "name", type: "TEXT", notNull: true },
          ],
        },
        { type: "insert", table: "users", values: { id: 1, name: "alice" } },
      ],
    });
    const deletePlan = writePlanFile(dir, "delete-user.json", {
      message: "delete user",
      operations: [{ type: "delete", table: "users", where: { id: 1 } }],
    });
    const reinsertPlan = writePlanFile(dir, "reinsert-user.json", {
      message: "reinsert user",
      operations: [{ type: "insert", table: "users", values: { id: 1, name: "alice" } }],
    });

    await applyPlan(setup, { dbPath });
    const deleted = await applyPlan(deletePlan, { dbPath });
    await applyPlan(reinsertPlan, { dbPath });

    const result = revertCommit(deleted.commitId, { dbPath });
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.conflicts.length).toBeGreaterThan(0);
      expect(result.conflicts[0]?.kind).toBe("row");
      expect(result.conflicts[0]?.reason.includes("PRIMARY KEY")).toBe(true);
    }
  });

  test("revert of revert is supported", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const setup = writePlanFile(dir, "setup.json", {
      message: "setup",
      operations: [
        {
          type: "create_table",
          table: "events",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "title", type: "TEXT", notNull: true },
          ],
        },
        { type: "insert", table: "events", values: { id: 1, title: "dentist" } },
      ],
    });
    const drop = writePlanFile(dir, "drop.json", {
      message: "drop events",
      operations: [{ type: "drop_table", table: "events" }],
    });

    await applyPlan(setup, { dbPath });
    const dropped = await applyPlan(drop, { dbPath });
    const first = revertCommit(dropped.commitId, { dbPath });
    expect(first.ok).toBe(true);
    if (!first.ok) {
      throw new Error("expected first revert success");
    }

    const second = revertCommit(first.revertCommit.commitId, { dbPath });
    expect(second.ok).toBe(true);
    if (!second.ok) {
      throw new Error("expected second revert success");
    }

    const tableCount = readQuery("SELECT COUNT(*) AS c FROM sqlite_master WHERE type='table' AND name='events'", { dbPath });
    expect(tableCount).toEqual([{ c: 0 }]);
  });

  test("revert throws when update effect is missing beforeRow", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const setup = writePlanFile(dir, "setup-missing-before.json", {
      message: "setup",
      operations: [
        {
          type: "create_table",
          table: "audit_items",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "name", type: "TEXT", notNull: true },
          ],
        },
        { type: "insert", table: "audit_items", values: { id: 1, name: "old" } },
      ],
    });
    const update = writePlanFile(dir, "update-missing-before.json", {
      message: "update",
      operations: [{ type: "update", table: "audit_items", values: { name: "new" }, where: { id: 1 } }],
    });
    await applyPlan(setup, { dbPath });
    const updated = await applyPlan(update, { dbPath });

    const tamper = new Database(dbPath);
    tamper
      .query(`UPDATE ${EFFECT_ROW_TABLE} SET before_row_json=NULL WHERE commit_id=? AND op_kind='update'`)
      .run(updated.commitId);
    tamper.close(false);

    try {
      revertCommit(updated.commitId, { dbPath });
      throw new Error("revertCommit should fail when beforeRow is missing");
    } catch (error) {
      expect(isTossError(error)).toBe(true);
      if (isTossError(error)) {
        expect(error.code).toBe("REVERT_FAILED");
      }
    }
  });

  test("revert throws when update beforeRow contains non-primitive values", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const setup = writePlanFile(dir, "setup-non-primitive-before.json", {
      message: "setup",
      operations: [
        {
          type: "create_table",
          table: "complex_items",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "payload", type: "TEXT", notNull: true },
          ],
        },
        { type: "insert", table: "complex_items", values: { id: 1, payload: "a" } },
      ],
    });
    const update = writePlanFile(dir, "update-non-primitive-before.json", {
      message: "update payload",
      operations: [{ type: "update", table: "complex_items", values: { payload: "b" }, where: { id: 1 } }],
    });
    await applyPlan(setup, { dbPath });
    const updated = await applyPlan(update, { dbPath });

    const tamper = new Database(dbPath);
    tamper
      .query(`UPDATE ${EFFECT_ROW_TABLE} SET before_row_json=? WHERE commit_id=? AND op_kind='update'`)
      .run('{"id":1,"payload":{"nested":true}}', updated.commitId);
    tamper.close(false);

    try {
      revertCommit(updated.commitId, { dbPath });
      throw new Error("revertCommit should fail when beforeRow contains non-primitive values");
    } catch (error) {
      expect(isTossError(error)).toBe(true);
      if (isTossError(error)) {
        expect(error.code).toBe("REVERT_FAILED");
      }
    }
  });

  test("verify stores last_verified_ok and preserves last_verified_ok_at on failure", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const setup = writePlanFile(dir, "setup.json", {
      message: "setup",
      operations: [
        {
          type: "create_table",
          table: "notes",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "body", type: "TEXT", notNull: true },
          ],
        },
      ],
    });
    await applyPlan(setup, { dbPath });

    const quick = verifyDatabase({ dbPath });
    expect(quick.ok).toBe(true);
    expect(quick.mode).toBe("quick");

    const firstStatus = getStatus({ dbPath });
    expect(firstStatus.lastVerifiedAt).not.toBeNull();
    expect(firstStatus.lastVerifiedOk).toBe(true);
    expect(firstStatus.lastVerifiedOkAt).not.toBeNull();

    const tamper = new Database(dbPath);
    tamper.query(`UPDATE ${COMMIT_TABLE} SET message='tampered' WHERE seq=1`).run();
    tamper.close(false);

    const broken = verifyDatabase({ dbPath, full: true });
    expect(broken.ok).toBe(false);
    expect(broken.mode).toBe("full");

    const secondStatus = getStatus({ dbPath });
    expect(secondStatus.lastVerifiedOk).toBe(false);
    expect(secondStatus.lastVerifiedOkAt).toBe(firstStatus.lastVerifiedOkAt);
  });

  test("snapshot recover restores and replays exact commit ids", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const tweak = new Database(dbPath);
    tweak
      .query(`UPDATE ${ENGINE_META_TABLE} SET value='1' WHERE key='snapshot_interval'`)
      .run();
    tweak
      .query(`UPDATE ${ENGINE_META_TABLE} SET value='10' WHERE key='snapshot_retain'`)
      .run();
    tweak.close(false);

    const create = writePlanFile(dir, "create.json", {
      message: "create logs",
      operations: [
        {
          type: "create_table",
          table: "logs",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "msg", type: "TEXT", notNull: true },
          ],
        },
      ],
    });
    const insert = writePlanFile(dir, "insert.json", {
      message: "insert log",
      operations: [{ type: "insert", table: "logs", values: { id: 1, msg: "hello" } }],
    });

    const firstCommit = await applyPlan(create, { dbPath });
    const secondCommit = await applyPlan(insert, { dbPath });

    const result = await recoverFromSnapshot(firstCommit.commitId, { dbPath });
    expect(result.replayedCommits).toBeGreaterThanOrEqual(1);

    const rows = readQuery("SELECT id, msg FROM logs", { dbPath });
    expect(rows).toEqual([{ id: 1, msg: "hello" }]);

    const history = getHistory({ dbPath, verbose: true });
    expect(history[0]?.commitId).toBe(secondCommit.commitId);
    expect(history[0]?.kind).toBe(secondCommit.kind);
    expect(history[0]?.createdAt).toBe(secondCommit.createdAt);
  });

  test("recover failure during replay does not overwrite original database", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const tweak = new Database(dbPath);
    tweak
      .query(`UPDATE ${ENGINE_META_TABLE} SET value='1' WHERE key='snapshot_interval'`)
      .run();
    tweak
      .query(`UPDATE ${ENGINE_META_TABLE} SET value='10' WHERE key='snapshot_retain'`)
      .run();
    tweak.close(false);

    const create = writePlanFile(dir, "create-safe-recover.json", {
      message: "create table",
      operations: [
        {
          type: "create_table",
          table: "recover_guard",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "value", type: "TEXT", notNull: true },
          ],
        },
      ],
    });
    const insertA = writePlanFile(dir, "insert-a-safe-recover.json", {
      message: "insert a",
      operations: [{ type: "insert", table: "recover_guard", values: { id: 1, value: "a" } }],
    });
    const insertB = writePlanFile(dir, "insert-b-safe-recover.json", {
      message: "insert b",
      operations: [{ type: "insert", table: "recover_guard", values: { id: 2, value: "b" } }],
    });

    const base = await applyPlan(create, { dbPath });
    await applyPlan(insertA, { dbPath });
    const latest = await applyPlan(insertB, { dbPath });

    const tamper = new Database(dbPath);
    tamper
      .query(`UPDATE ${COMMIT_TABLE} SET state_hash_after='broken-state-hash' WHERE commit_id=?`)
      .run(latest.commitId);
    tamper.close(false);

    try {
      await recoverFromSnapshot(base.commitId, { dbPath });
      throw new Error("recoverFromSnapshot should fail due to tampered replay metadata");
    } catch (error) {
      expect(isTossError(error)).toBe(true);
      if (isTossError(error)) {
        expect(error.code).toBe("RECOVER_FAILED");
      }
    }

    const rowsAfterFailure = readQuery("SELECT id, value FROM recover_guard ORDER BY id", { dbPath });
    expect(rowsAfterFailure).toEqual([
      { id: 1, value: "a" },
      { id: 2, value: "b" },
    ]);
  });

  test("snapshot creation does not leak tmp wal/shm sidecars", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const tweak = new Database(dbPath);
    tweak
      .query(`UPDATE ${ENGINE_META_TABLE} SET value='1' WHERE key='snapshot_interval'`)
      .run();
    tweak
      .query(`UPDATE ${ENGINE_META_TABLE} SET value='10' WHERE key='snapshot_retain'`)
      .run();
    tweak.close(false);

    const create = writePlanFile(dir, "create-snap-clean.json", {
      message: "create snapshots table",
      operations: [
        {
          type: "create_table",
          table: "snap_clean",
          columns: [{ name: "id", type: "INTEGER", primaryKey: true }],
        },
      ],
    });
    await applyPlan(create, { dbPath });

    const snapshotDir = join(dir, ".toss", "snapshots");
    const names = readdirSync(snapshotDir).sort();
    expect(names.length).toBeGreaterThan(0);
    expect(names.some((name) => name.startsWith("tmp-"))).toBe(false);
    expect(names.some((name) => name.endsWith(".db-wal"))).toBe(false);
    expect(names.some((name) => name.endsWith(".db-shm"))).toBe(false);
  });

  test("snapshot recover succeeds when untouched pre-existing tables exist", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const direct = new Database(dbPath);
    direct.run("CREATE TABLE external_data (id INTEGER PRIMARY KEY, body TEXT)");
    direct.run("INSERT INTO external_data(id, body) VALUES(1, 'stable')");
    direct.close(false);

    const tweak = new Database(dbPath);
    tweak
      .query(`UPDATE ${ENGINE_META_TABLE} SET value='1' WHERE key='snapshot_interval'`)
      .run();
    tweak
      .query(`UPDATE ${ENGINE_META_TABLE} SET value='10' WHERE key='snapshot_retain'`)
      .run();
    tweak.close(false);

    const create = writePlanFile(dir, "create-orders.json", {
      message: "create orders",
      operations: [
        {
          type: "create_table",
          table: "orders",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "item", type: "TEXT", notNull: true },
          ],
        },
      ],
    });
    const insert = writePlanFile(dir, "insert-orders.json", {
      message: "insert order",
      operations: [{ type: "insert", table: "orders", values: { id: 1, item: "book" } }],
    });

    const snapshotBase = await applyPlan(create, { dbPath });
    const replayed = await applyPlan(insert, { dbPath });

    const recovered = await recoverFromSnapshot(snapshotBase.commitId, { dbPath });
    expect(recovered.replayedCommits).toBe(1);

    const extRows = readQuery("SELECT id, body FROM external_data", { dbPath });
    expect(extRows).toEqual([{ id: 1, body: "stable" }]);
    const orderRows = readQuery("SELECT id, item FROM orders", { dbPath });
    expect(orderRows).toEqual([{ id: 1, item: "book" }]);

    const history = getHistory({ dbPath, verbose: true });
    expect(history[0]?.commitId).toBe(replayed.commitId);
  });

  test("init generates toss skill with migration guidance", async () => {
    const { dir, dbPath } = createTestContext();
    const result = await initDatabase({ dbPath, generateSkills: true, workspacePath: dir });
    expect(result.generatedSkills).not.toBeNull();
    if (!result.generatedSkills) {
      throw new Error("generatedSkills should exist");
    }

    expect(existsSync(result.generatedSkills.skillPath)).toBe(true);
    const skill = readFileSync(result.generatedSkills.skillPath, "utf8");
    expect(skill.includes("toss history --verbose")).toBe(true);
    expect(skill.includes("toss verify --quick")).toBe(true);
    expect(skill.includes("staged migrations")).toBe(true);
  });
});
