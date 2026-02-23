import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { applyPlan, initDatabase, CodedError, planCheck, readQuery } from "../../src";
import { createTestContext, writePlanFile, withTmpDirCleanup } from "../helpers";

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

  testWithTmp("alter_column_type preserves AUTOINCREMENT sqlite_sequence during table rebuild", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const direct = new Database(dbPath);
    direct.run("CREATE TABLE auto_items (id INTEGER PRIMARY KEY AUTOINCREMENT, amount TEXT NOT NULL)");
    direct.run("INSERT INTO auto_items(amount) VALUES ('10'), ('20'), ('30')");
    direct.run("DELETE FROM auto_items WHERE id = 3");
    direct.close(false);

    const alter = await writePlanFile(dir, "alter-autoincrement-preserve-sequence.json", {
      message: "alter type and keep sqlite_sequence",
      operations: [{ type: "alter_column_type", table: "auto_items", column: "amount", newType: "INTEGER" }],
    });
    await applyPlan(alter);

    const verifyDb = new Database(dbPath);
    try {
      const seq = verifyDb.query("SELECT seq FROM sqlite_sequence WHERE name='auto_items'").get() as { seq: number } | null;
      expect(seq?.seq).toBe(3);
      verifyDb.run("INSERT INTO auto_items(amount) VALUES (40)");
      const inserted = verifyDb.query("SELECT id, typeof(amount) AS t FROM auto_items WHERE amount = 40").get() as
        | { id: number; t: string }
        | null;
      expect(inserted?.id).toBe(4);
      expect(inserted?.t).toBe("integer");
    } finally {
      verifyDb.close(false);
    }
  });
});

describe("check operations", () => {
  testWithTmp("add_check enforces constraint for toss writes and raw SQL writes", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const create = await writePlanFile(dir, "check-create-tasks.json", {
      message: "create tasks",
      operations: [
        {
          type: "create_table",
          table: "tasks",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "status", type: "TEXT", notNull: true },
          ],
        },
      ],
    });
    await applyPlan(create);

    const addCheck = await writePlanFile(dir, "check-add-status-enum.json", {
      message: "add status check",
      operations: [
        {
          type: "add_check",
          table: "tasks",
          expression: "status IN ('todo', 'doing', 'done')",
        },
      ],
    });
    await applyPlan(addCheck);

    const insertValid = await writePlanFile(dir, "check-insert-valid.json", {
      message: "insert valid status",
      operations: [{ type: "insert", table: "tasks", values: { id: 1, status: "todo" } }],
    });
    await applyPlan(insertValid);

    const insertInvalid = await writePlanFile(dir, "check-insert-invalid.json", {
      message: "insert invalid status",
      operations: [{ type: "insert", table: "tasks", values: { id: 2, status: "cancelled" } }],
    });
    await expect(applyPlan(insertInvalid)).rejects.toThrow(/CHECK constraint failed/i);

    const direct = new Database(dbPath);
    try {
      expect(() => {
        direct.run("INSERT INTO tasks(id, status) VALUES (3, 'cancelled')");
      }).toThrow(/CHECK constraint failed/i);
    } finally {
      direct.close(false);
    }
  });

  testWithTmp("validator allows comment-like tokens inside CHECK string literals", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const create = await writePlanFile(dir, "check-literal-token-create.json", {
      message: "create tasks",
      operations: [
        {
          type: "create_table",
          table: "tasks",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "status", type: "TEXT", notNull: true },
          ],
        },
      ],
    });
    await applyPlan(create);

    const addCheck = await writePlanFile(dir, "check-literal-token-add.json", {
      message: "add literal token check",
      operations: [
        {
          type: "add_check",
          table: "tasks",
          expression: "status NOT IN ('--', '/*', '*/')",
        },
      ],
    });
    await applyPlan(addCheck);
  });

  testWithTmp("validator rejects SQL comments outside literals in CHECK expression", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const setup = await writePlanFile(dir, "check-comment-reject-setup.json", {
      message: "create tasks",
      operations: [
        {
          type: "create_table",
          table: "tasks",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "status", type: "TEXT", notNull: true },
          ],
        },
      ],
    });
    await applyPlan(setup);

    const invalid = await writePlanFile(dir, "check-comment-reject.json", {
      message: "bad comment expression",
      operations: [
        {
          type: "add_check",
          table: "tasks",
          expression: "status <> 'done' -- trailing comment",
        },
      ],
    });

    const result = await planCheck(invalid);
    expect(result.ok).toBe(false);
    expect(result.errors.some((error) => error.code === "INVALID_OPERATION")).toBe(true);
    expect(result.errors.some((error) => error.message.includes("must not contain SQL comments"))).toBe(true);
  });

  testWithTmp("validator rejects semicolons outside SQLite double-quoted identifiers", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const setup = await writePlanFile(dir, "check-semicolon-quote-rule-setup.json", {
      message: "create tasks",
      operations: [
        {
          type: "create_table",
          table: "tasks",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "status", type: "TEXT", notNull: true },
          ],
        },
      ],
    });
    await applyPlan(setup);

    const invalid = await writePlanFile(dir, "check-semicolon-quote-rule.json", {
      message: "bad expression with semicolon outside sqlite double-quoted identifier",
      operations: [
        {
          type: "add_check",
          table: "tasks",
          expression: `"a\\\";"b" = 1`,
        },
      ],
    });

    const result = await planCheck(invalid);
    expect(result.ok).toBe(false);
    expect(result.errors.some((error) => error.code === "INVALID_OPERATION")).toBe(true);
    expect(result.errors.some((error) => error.message.includes("single SQL expression"))).toBe(true);
  });

  testWithTmp("validator rejects add_check payload that escapes into extra table constraints", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const setup = await writePlanFile(dir, "check-constraint-injection-setup.json", {
      message: "create tasks",
      operations: [
        {
          type: "create_table",
          table: "tasks",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "status", type: "TEXT", notNull: true },
          ],
        },
      ],
    });
    await applyPlan(setup);

    const injected = await writePlanFile(dir, "check-constraint-injection.json", {
      message: "attempt injected check payload",
      operations: [
        {
          type: "add_check",
          table: "tasks",
          expression: "status IN ('todo')) , UNIQUE(status",
        },
      ],
    });

    const result = await planCheck(injected);
    expect(result.ok).toBe(false);
    expect(result.errors.some((error) => error.code === "INVALID_OPERATION")).toBe(true);
    expect(result.errors.some((error) => error.message.includes("single SQL expression"))).toBe(true);
  });

  testWithTmp("drop_check removes matching constraint by expression", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const setup = await writePlanFile(dir, "check-drop-setup.json", {
      message: "setup tasks with check",
      operations: [
        {
          type: "create_table",
          table: "tasks",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "status", type: "TEXT", notNull: true },
          ],
        },
        {
          type: "add_check",
          table: "tasks",
          expression: "status IN ('todo','doing','done')",
        },
      ],
    });
    await applyPlan(setup);

    const dropCheck = await writePlanFile(dir, "check-drop-status-enum.json", {
      message: "drop status check",
      operations: [
        {
          type: "drop_check",
          table: "tasks",
          expression: " status   in ( 'todo' , 'doing' , 'done' ) ",
        },
      ],
    });
    await applyPlan(dropCheck);

    const direct = new Database(dbPath);
    try {
      direct.run("INSERT INTO tasks(id, status) VALUES (1, 'cancelled')");
    } finally {
      direct.close(false);
    }
    const rows = readQuery("SELECT status FROM tasks WHERE id=1");
    expect(rows).toEqual([{ status: "cancelled" }]);
  });

  testWithTmp("drop_check recognizes CHECK constraints with comments between CONSTRAINT and CHECK", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const direct = new Database(dbPath);
    try {
      direct.run(`
        CREATE TABLE tasks (
          id INTEGER PRIMARY KEY,
          status TEXT NOT NULL,
          CONSTRAINT chk_tasks_status /* note */ CHECK (status IN ('todo','doing'))
        )
      `);
    } finally {
      direct.close(false);
    }

    const dropCheck = await writePlanFile(dir, "check-drop-commented-constraint.json", {
      message: "drop commented named check",
      operations: [{ type: "drop_check", table: "tasks", expression: "status IN ('todo','doing')" }],
    });
    await applyPlan(dropCheck);

    const verify = new Database(dbPath);
    try {
      verify.run("INSERT INTO tasks(id, status) VALUES (1, 'done')");
    } finally {
      verify.close(false);
    }
  });

  testWithTmp("add_check duplicate detection recognizes constraints with CONSTRAINT/CHECK comments", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const direct = new Database(dbPath);
    try {
      direct.run(`
        CREATE TABLE tasks (
          id INTEGER PRIMARY KEY,
          status TEXT NOT NULL,
          CONSTRAINT chk_tasks_status /* note */ CHECK (status IN ('todo','doing'))
        )
      `);
    } finally {
      direct.close(false);
    }

    const addDuplicate = await writePlanFile(dir, "check-add-duplicate-commented-constraint.json", {
      message: "try duplicate named check",
      operations: [{ type: "add_check", table: "tasks", expression: "status IN ('todo','doing')" }],
    });

    const result = await planCheck(addDuplicate);
    expect(result.ok).toBe(false);
    expect(result.errors.some((error) => error.code === "INVALID_OPERATION")).toBe(true);
    expect(result.errors.some((error) => error.message.includes("Equivalent CHECK constraint already exists"))).toBe(true);
  });

  testWithTmp("drop_check matches CHECK expression regardless of whitespace before opening parenthesis", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const direct = new Database(dbPath);
    try {
      direct.run(`
        CREATE TABLE tasks (
          id INTEGER PRIMARY KEY,
          status TEXT NOT NULL,
          CHECK (status IN('todo','doing'))
        )
      `);
    } finally {
      direct.close(false);
    }

    const dropCheck = await writePlanFile(dir, "check-drop-spacing-before-paren.json", {
      message: "drop check with spacing variant before opening parenthesis",
      operations: [{ type: "drop_check", table: "tasks", expression: "status IN ('todo','doing')" }],
    });
    await applyPlan(dropCheck);

    const verify = new Database(dbPath);
    try {
      verify.run("INSERT INTO tasks(id, status) VALUES (1, 'done')");
    } finally {
      verify.close(false);
    }
  });

  testWithTmp("add_check duplicate detection ignores whitespace before opening parenthesis", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const direct = new Database(dbPath);
    try {
      direct.run(`
        CREATE TABLE tasks (
          id INTEGER PRIMARY KEY,
          status TEXT NOT NULL,
          CHECK (status IN('todo','doing'))
        )
      `);
    } finally {
      direct.close(false);
    }

    const addDuplicate = await writePlanFile(dir, "check-add-duplicate-spacing-before-paren.json", {
      message: "try duplicate check with spacing variant before opening parenthesis",
      operations: [{ type: "add_check", table: "tasks", expression: "status IN ('todo','doing')" }],
    });

    const result = await planCheck(addDuplicate);
    expect(result.ok).toBe(false);
    expect(result.errors.some((error) => error.code === "INVALID_OPERATION")).toBe(true);
    expect(result.errors.some((error) => error.message.includes("Equivalent CHECK constraint already exists"))).toBe(true);
  });

  testWithTmp("add_check preserves AUTOINCREMENT sqlite_sequence during table rebuild", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const direct = new Database(dbPath);
    try {
      direct.run(`
        CREATE TABLE tasks_auto (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          status TEXT NOT NULL
        )
      `);
      direct.run("INSERT INTO tasks_auto(status) VALUES ('todo'), ('doing'), ('todo')");
      direct.run("DELETE FROM tasks_auto WHERE id = 3");
    } finally {
      direct.close(false);
    }

    const addCheck = await writePlanFile(dir, "check-add-autoincrement-preserve-seq.json", {
      message: "add check and keep sqlite_sequence",
      operations: [{ type: "add_check", table: "tasks_auto", expression: "status IN ('todo','doing')" }],
    });
    await applyPlan(addCheck);

    const verify = new Database(dbPath);
    try {
      const seq = verify.query("SELECT seq FROM sqlite_sequence WHERE name='tasks_auto'").get() as { seq: number } | null;
      expect(seq?.seq).toBe(3);
      verify.run("INSERT INTO tasks_auto(status) VALUES ('doing')");
      const inserted = verify.query("SELECT id FROM tasks_auto WHERE status='doing' ORDER BY id DESC LIMIT 1").get() as
        | { id: number }
        | null;
      expect(inserted?.id).toBe(4);
    } finally {
      verify.close(false);
    }
  });

  testWithTmp("drop_check preserves AUTOINCREMENT sqlite_sequence during table rebuild", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const direct = new Database(dbPath);
    try {
      direct.run(`
        CREATE TABLE tasks_auto (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          status TEXT NOT NULL,
          CHECK (status IN ('todo','doing'))
        )
      `);
      direct.run("INSERT INTO tasks_auto(status) VALUES ('todo'), ('doing'), ('todo')");
      direct.run("DELETE FROM tasks_auto WHERE id = 3");
    } finally {
      direct.close(false);
    }

    const dropCheck = await writePlanFile(dir, "check-drop-autoincrement-preserve-seq.json", {
      message: "drop check and keep sqlite_sequence",
      operations: [{ type: "drop_check", table: "tasks_auto", expression: "status IN ('todo','doing')" }],
    });
    await applyPlan(dropCheck);

    const verify = new Database(dbPath);
    try {
      const seq = verify.query("SELECT seq FROM sqlite_sequence WHERE name='tasks_auto'").get() as { seq: number } | null;
      expect(seq?.seq).toBe(3);
      verify.run("INSERT INTO tasks_auto(status) VALUES ('done')");
      const inserted = verify.query("SELECT id FROM tasks_auto WHERE status='done' ORDER BY id DESC LIMIT 1").get() as
        | { id: number }
        | null;
      expect(inserted?.id).toBe(4);
    } finally {
      verify.close(false);
    }
  });

  testWithTmp("drop_check matches CHECK expression even when comment contains parenthesis tokens", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const direct = new Database(dbPath);
    try {
      direct.run(`
        CREATE TABLE tasks (
          id INTEGER PRIMARY KEY,
          status TEXT NOT NULL,
          CHECK (status IN ('todo','doing') /* ( */)
        )
      `);
    } finally {
      direct.close(false);
    }

    const dropCheck = await writePlanFile(dir, "check-drop-comment-paren.json", {
      message: "drop check with parenthesis in comment",
      operations: [{ type: "drop_check", table: "tasks", expression: "status IN ('todo','doing')" }],
    });
    await applyPlan(dropCheck);

    const verify = new Database(dbPath);
    try {
      verify.run("INSERT INTO tasks(id, status) VALUES (1, 'done')");
    } finally {
      verify.close(false);
    }
  });

  testWithTmp("add_check fails when existing rows violate expression and keeps data intact", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const setup = await writePlanFile(dir, "check-invalid-existing-setup.json", {
      message: "setup invalid existing row",
      operations: [
        {
          type: "create_table",
          table: "tasks",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "status", type: "TEXT", notNull: true },
          ],
        },
        { type: "insert", table: "tasks", values: { id: 1, status: "cancelled" } },
      ],
    });
    await applyPlan(setup);

    const addCheck = await writePlanFile(dir, "check-invalid-existing-add-check.json", {
      message: "try strict status check",
      operations: [
        {
          type: "add_check",
          table: "tasks",
          expression: "status IN ('todo','doing','done')",
        },
      ],
    });

    await expect(applyPlan(addCheck)).rejects.toThrow(/CHECK constraint failed/i);
    expect(readQuery("SELECT id, status FROM tasks")).toEqual([{ id: 1, status: "cancelled" }]);
  });

  testWithTmp("planCheck marks drop_check as destructive/high risk", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const setup = await writePlanFile(dir, "check-plan-setup.json", {
      message: "setup tasks with check",
      operations: [
        {
          type: "create_table",
          table: "tasks",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "status", type: "TEXT", notNull: true },
          ],
        },
        {
          type: "add_check",
          table: "tasks",
          expression: "status IN ('todo','doing','done')",
        },
      ],
    });
    await applyPlan(setup);

    const drop = await writePlanFile(dir, "check-plan-drop-check.json", {
      message: "drop status check",
      operations: [{ type: "drop_check", table: "tasks", expression: "status IN ('todo','doing','done')" }],
    });
    const result = await planCheck(drop);
    expect(result.ok).toBe(true);
    expect(result.risk).toBe("high");
    expect(result.warnings.some((warning) => warning.code === "DESTRUCTIVE_OPERATION")).toBe(true);
  });

  testWithTmp("drop_check returns INVALID_OPERATION when expression does not exist", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const setup = await writePlanFile(dir, "check-drop-missing-setup.json", {
      message: "create tasks",
      operations: [
        {
          type: "create_table",
          table: "tasks",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "status", type: "TEXT", notNull: true },
          ],
        },
      ],
    });
    await applyPlan(setup);

    const dropMissing = await writePlanFile(dir, "check-drop-missing.json", {
      message: "drop missing check",
      operations: [{ type: "drop_check", table: "tasks", expression: "status IN ('todo')" }],
    });

    try {
      await applyPlan(dropMissing);
      throw new Error("applyPlan should fail for missing CHECK expression");
    } catch (error) {
      expect(CodedError.is(error)).toBe(true);
      if (CodedError.is(error)) {
        expect(error.code).toBe("INVALID_OPERATION");
        expect(error.message).toContain("CHECK constraint not found");
      }
    }
  });
});
