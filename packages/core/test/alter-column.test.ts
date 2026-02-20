import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { applyPlan, initDatabase, readQuery } from "../src";
import { createTestContext, writePlanFile, withTmpDirCleanup } from "./helpers";

const testWithTmp = (name: string, fn: () => void | Promise<void>) => test(name, withTmpDirCleanup(fn));


describe("alter_column_type", () => {
  testWithTmp("alter_column_type preserves FOREIGN KEY constraints", async () => {
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

    const alter = await writePlanFile(dir, "alter-child-amount.json", {
      message: "convert child amount",
      operations: [{ type: "alter_column_type", table: "children", column: "amount", newType: "INTEGER" }],
    });
    await applyPlan(alter);

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

  testWithTmp("alter_column_type supports self-referential FOREIGN KEY tables", async () => {
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

    const alter = await writePlanFile(dir, "alter-self-fk-tree-nodes.json", {
      message: "alter self fk table",
      operations: [{ type: "alter_column_type", table: "tree_nodes", column: "weight", newType: "INTEGER" }],
    });
    await applyPlan(alter);

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

  testWithTmp("alter_column_type supports legacy single-quoted identifiers in DDL", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const direct = new Database(dbPath);
    direct.run("CREATE TABLE 'sq_users' ('id' INTEGER PRIMARY KEY, 'amount' TEXT)");
    direct.run("INSERT INTO 'sq_users'('id', 'amount') VALUES (1, '44')");
    direct.close(false);

    const alter = await writePlanFile(dir, "alter-single-quoted-identifiers.json", {
      message: "alter legacy single-quoted identifiers",
      operations: [{ type: "alter_column_type", table: "sq_users", column: "amount", newType: "INTEGER" }],
    });
    await applyPlan(alter);

    const rows = readQuery('SELECT typeof("amount") AS t FROM "sq_users" WHERE "id" = 1');
    expect(rows).toEqual([{ t: "integer" }]);
  });

  testWithTmp("alter_column_type works for quoted keyword column names", async () => {
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

    const alter = await writePlanFile(dir, "alter-quoted-keyword-column.json", {
      message: "convert quoted keyword column type",
      operations: [{ type: "alter_column_type", table: "kw_cols", column: "primary", newType: "INTEGER" }],
    });
    await applyPlan(alter);

    const rows = readQuery(`SELECT typeof("primary") AS t, note FROM kw_cols`);
    expect(rows).toEqual([{ t: "integer", note: "x" }]);
  });

  testWithTmp("alter_column_type handles leading comments in column segments", async () => {
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

    const alter = await writePlanFile(dir, "alter-commented-column.json", {
      message: "convert commented column type",
      operations: [{ type: "alter_column_type", table: "commented_cols", column: "b", newType: "INTEGER" }],
    });
    await applyPlan(alter);

    const rows = readQuery("SELECT typeof(b) AS t, c FROM commented_cols WHERE id=1");
    expect(rows).toEqual([{ t: "integer", c: "y" }]);
  });

  testWithTmp("alter_column_type preserves newline-terminated line comments around commas", async () => {
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

    const alter = await writePlanFile(dir, "alter-line-comment-column.json", {
      message: "convert line-commented column type",
      operations: [{ type: "alter_column_type", table: "line_comment_cols", column: "b", newType: "INTEGER" }],
    });
    await applyPlan(alter);

    const rows = readQuery("SELECT typeof(a) AS ta, typeof(b) AS tb FROM line_comment_cols WHERE id=1");
    expect(rows).toEqual([{ ta: "text", tb: "integer" }]);
  });

  testWithTmp("alter_column_type preserves trailing line comment on last column segment", async () => {
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

    const alter = await writePlanFile(dir, "alter-trailing-comment-last.json", {
      message: "convert last commented column type",
      operations: [{ type: "alter_column_type", table: "trailing_comment_last", column: "b", newType: "INTEGER" }],
    });
    await applyPlan(alter);

    const rows = readQuery("SELECT typeof(b) AS tb FROM trailing_comment_last WHERE id=1");
    expect(rows).toEqual([{ tb: "integer" }]);
  });

  testWithTmp("alter_column_type resolves table DDL case-insensitively and preserves secondary objects", async () => {
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

    const alter = await writePlanFile(dir, "alter-users-case-insensitive.json", {
      message: "alter users amount",
      operations: [{ type: "alter_column_type", table: "users", column: "amount", newType: "INTEGER" }],
    });
    await applyPlan(alter);

    const db = new Database(dbPath);
    const indexes = db.query("PRAGMA index_list('Users')").all() as Array<{ name: string }>;
    expect(indexes.some((index) => index.name === "idx_users_note")).toBe(true);
    db.run("INSERT INTO Users(id, amount, note) VALUES (2, '9', 'b')");
    const row = db.query("SELECT note FROM Users WHERE id=2").get() as { note: string } | null;
    expect(row?.note).toBe("B");
    db.close(false);
  });

  testWithTmp("alter_column_type supports non-ASCII bare identifiers", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const direct = new Database(dbPath);
    direct.run("CREATE TABLE 사용자 (id INTEGER PRIMARY KEY, 이름 TEXT)");
    direct.run("INSERT INTO 사용자(id, 이름) VALUES (1, '42')");
    direct.close(false);

    const alter = await writePlanFile(dir, "alter-unicode-bare-identifiers.json", {
      message: "alter unicode bare column",
      operations: [{ type: "alter_column_type", table: "사용자", column: "이름", newType: "INTEGER" }],
    });
    await applyPlan(alter);

    const rows = readQuery('SELECT typeof("이름") AS t FROM "사용자" WHERE id=1');
    expect(rows).toEqual([{ t: "integer" }]);
  });

  testWithTmp("alter_column_type supports non-BMP bare identifiers", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const direct = new Database(dbPath);
    direct.run("CREATE TABLE astral_cols (id INTEGER PRIMARY KEY, 𐐷 TEXT)");
    direct.run("INSERT INTO astral_cols(id, 𐐷) VALUES (1, '56')");
    direct.close(false);

    const alter = await writePlanFile(dir, "alter-astral-bare-identifiers.json", {
      message: "alter astral bare column",
      operations: [{ type: "alter_column_type", table: "astral_cols", column: "𐐷", newType: "INTEGER" }],
    });
    await applyPlan(alter);

    const rows = readQuery('SELECT typeof("𐐷") AS t FROM "astral_cols" WHERE id=1');
    expect(rows).toEqual([{ t: "integer" }]);
  });

  testWithTmp("alter_column_type does not fold Unicode case when matching columns", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const direct = new Database(dbPath);
    direct.run('CREATE TABLE unicode_case_cols (id INTEGER PRIMARY KEY, "Ä" TEXT, "ä" TEXT)');
    direct.run('INSERT INTO unicode_case_cols(id, "Ä", "ä") VALUES (1, \'10\', \'11\')');
    direct.close(false);

    const alter = await writePlanFile(dir, "alter-unicode-case-sensitive-column.json", {
      message: "alter only capital-a-umlaut",
      operations: [{ type: "alter_column_type", table: "unicode_case_cols", column: "Ä", newType: "INTEGER" }],
    });
    await applyPlan(alter);

    const rows = readQuery('SELECT typeof("Ä") AS upper_t, typeof("ä") AS lower_t FROM "unicode_case_cols" WHERE id=1');
    expect(rows).toEqual([{ upper_t: "integer", lower_t: "text" }]);
  });
});
