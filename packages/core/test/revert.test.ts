import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { initDatabase, readQuery, revertCommit } from "../src";
import { applyPlan, createTestContext, writePlanFile, withTmpDirCleanup, currentDb } from "./helpers";

const testWithTmp = (name: string, fn: () => void | Promise<void>) => test(name, withTmpDirCleanup(fn));

describe("revertCommit", () => {
  testWithTmp("drop_table revert restores table definition and rows", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const setup = await writePlanFile(dir, "setup.json", {
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
    const drop = await writePlanFile(dir, "drop.json", {
      message: "drop table",
      operations: [{ type: "drop_table", table: "expenses" }],
    });

    await applyPlan(currentDb(), setup);
    const dropCommit = await applyPlan(currentDb(), drop);

    const reverted = revertCommit(currentDb(), dropCommit.commitId);
    expect(reverted.ok).toBe(true);

    const rows = readQuery(currentDb(), "SELECT id, item FROM expenses");
    expect(rows).toEqual([{ id: 1, item: "dinner" }]);
  });

  testWithTmp("drop_table revert succeeds for AUTOINCREMENT tables with sqlite_sequence side effects", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const direct = new Database(dbPath);
    direct.run("CREATE TABLE auto_drop (id INTEGER PRIMARY KEY AUTOINCREMENT, body TEXT NOT NULL)");
    direct.run("INSERT INTO auto_drop(body) VALUES ('a')");
    direct.close(false);

    const drop = await writePlanFile(dir, "drop-autoincrement-table.json", {
      message: "drop autoincrement table",
      operations: [{ type: "drop_table", table: "auto_drop" }],
    });

    const dropped = await applyPlan(currentDb(), drop);
    const reverted = revertCommit(currentDb(), dropped.commitId);
    expect(reverted.ok).toBe(true);
    if (!reverted.ok) {
      throw new Error("expected revert success for AUTOINCREMENT drop_table");
    }

    const rows = readQuery(currentDb(), "SELECT id, body FROM auto_drop ORDER BY id");
    expect(rows).toEqual([{ id: 1, body: "a" }]);
  });

  testWithTmp("drop_table revert restores FK-related tables in dependency-safe schema order", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const direct = new Database(dbPath);
    direct.run("PRAGMA foreign_keys=ON");
    direct.run("CREATE TABLE a_parent (id INTEGER PRIMARY KEY, body TEXT NOT NULL)");
    direct.run(`
      CREATE TABLE z_child (
        id INTEGER PRIMARY KEY,
        parent_id INTEGER NOT NULL,
        body TEXT NOT NULL,
        FOREIGN KEY(parent_id) REFERENCES a_parent(id)
      )
    `);
    direct.run("INSERT INTO a_parent(id, body) VALUES (1, 'p1')");
    direct.run("INSERT INTO z_child(id, parent_id, body) VALUES (1, 1, 'c1')");
    direct.close(false);

    const dropBoth = await writePlanFile(dir, "drop-fk-related-tables.json", {
      message: "drop child then parent",
      operations: [
        { type: "drop_table", table: "z_child" },
        { type: "drop_table", table: "a_parent" },
      ],
    });
    const dropped = await applyPlan(currentDb(), dropBoth);

    const reverted = revertCommit(currentDb(), dropped.commitId);
    expect(reverted.ok).toBe(true);
    if (!reverted.ok) {
      throw new Error("expected revert success for multi-table drop with FK dependencies");
    }

    const verify = new Database(dbPath);
    verify.run("PRAGMA foreign_keys=ON");
    const parentRows = verify.query("SELECT id, body FROM a_parent ORDER BY id").all() as Array<{ id: number; body: string }>;
    const childRows = verify
      .query("SELECT id, parent_id, body FROM z_child ORDER BY id")
      .all() as Array<{ id: number; parent_id: number; body: string }>;
    expect(parentRows).toEqual([{ id: 1, body: "p1" }]);
    expect(childRows).toEqual([{ id: 1, parent_id: 1, body: "c1" }]);
    const fkCheck = verify.query("PRAGMA foreign_key_check").all() as unknown[];
    expect(fkCheck).toEqual([]);
    verify.close(false);
  });

  testWithTmp("revert restores ON DELETE CASCADE side effects with dependency-safe order", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const direct = new Database(dbPath);
    direct.run("PRAGMA foreign_keys=ON");
    direct.run("CREATE TABLE a_parent (id INTEGER PRIMARY KEY, name TEXT NOT NULL)");
    direct.run(`
      CREATE TABLE z_child (
        id INTEGER PRIMARY KEY,
        parent_id INTEGER NOT NULL,
        body TEXT NOT NULL,
        FOREIGN KEY(parent_id) REFERENCES a_parent(id) ON DELETE CASCADE
      )
    `);
    direct.run("INSERT INTO a_parent(id, name) VALUES (1, 'p1')");
    direct.run("INSERT INTO z_child(id, parent_id, body) VALUES (1, 1, 'c1')");
    direct.run("INSERT INTO z_child(id, parent_id, body) VALUES (2, 1, 'c2')");
    direct.close(false);

    const deleteParent = await writePlanFile(dir, "delete-parent.json", {
      message: "delete parent 1",
      operations: [{ type: "delete", table: "a_parent", where: { id: 1 } }],
    });
    const deleted = await applyPlan(currentDb(), deleteParent);

    const reverted = revertCommit(currentDb(), deleted.commitId);
    expect(reverted.ok).toBe(true);

    const parentRows = readQuery(currentDb(), "SELECT id, name FROM a_parent ORDER BY id");
    const childRows = readQuery(currentDb(), "SELECT id, parent_id, body FROM z_child ORDER BY id");
    expect(parentRows).toEqual([{ id: 1, name: "p1" }]);
    expect(childRows).toEqual([
      { id: 1, parent_id: 1, body: "c1" },
      { id: 2, parent_id: 1, body: "c2" },
    ]);
  });

  testWithTmp("revert restores trigger side effects", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const direct = new Database(dbPath);
    direct.run("CREATE TABLE account (id INTEGER PRIMARY KEY, balance INTEGER NOT NULL)");
    direct.run("CREATE TABLE ledger (id INTEGER PRIMARY KEY, account_id INTEGER NOT NULL, amount INTEGER NOT NULL)");
    direct.run(`
      CREATE TRIGGER "trg-ledger-ai"
      AFTER INSERT ON ledger
      BEGIN
        UPDATE account SET balance = balance + NEW.amount WHERE id = NEW.account_id;
      END
    `);
    direct.run("INSERT INTO account(id, balance) VALUES (1, 0)");
    direct.close(false);

    const insertLedger = await writePlanFile(dir, "insert-ledger.json", {
      message: "insert ledger row",
      operations: [{ type: "insert", table: "ledger", values: { id: 1, account_id: 1, amount: 7 } }],
    });
    const committed = await applyPlan(currentDb(), insertLedger);
    const reverted = revertCommit(currentDb(), committed.commitId);
    expect(reverted.ok).toBe(true);

    const accountRows = readQuery(currentDb(), "SELECT id, balance FROM account");
    const ledgerRows = readQuery(currentDb(), "SELECT id, account_id, amount FROM ledger");
    expect(accountRows).toEqual([{ id: 1, balance: 0 }]);
    expect(ledgerRows).toEqual([]);
  });

  testWithTmp("revert succeeds when inverse child insert requires schema-restored parent table", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const direct = new Database(dbPath);
    direct.run("PRAGMA foreign_keys=ON");
    direct.run("CREATE TABLE parent_nodes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)");
    direct.run(`
      CREATE TABLE child_nodes (
        id INTEGER PRIMARY KEY,
        parent_id INTEGER NOT NULL,
        body TEXT NOT NULL,
        FOREIGN KEY(parent_id) REFERENCES parent_nodes(id)
      )
    `);
    direct.run("INSERT INTO parent_nodes(id, body) VALUES (1, 'p1')");
    direct.run("INSERT INTO child_nodes(id, parent_id, body) VALUES (1, 1, 'c1')");
    direct.close(false);

    const destructive = await writePlanFile(dir, "delete-child-drop-parent.json", {
      message: "delete child then drop parent",
      operations: [
        { type: "delete", table: "child_nodes", where: { id: 1 } },
        { type: "drop_table", table: "parent_nodes" },
      ],
    });
    const committed = await applyPlan(currentDb(), destructive);

    const reverted = revertCommit(currentDb(), committed.commitId);
    expect(reverted.ok).toBe(true);
    if (!reverted.ok) {
      throw new Error("expected revert success for delete-child+drop-parent commit");
    }

    const verify = new Database(dbPath);
    verify.run("PRAGMA foreign_keys=ON");
    const parentRows = verify.query("SELECT id, body FROM parent_nodes ORDER BY id").all() as Array<{ id: number; body: string }>;
    const childRows = verify
      .query("SELECT id, parent_id, body FROM child_nodes ORDER BY id")
      .all() as Array<{ id: number; parent_id: number; body: string }>;
    expect(parentRows).toEqual([{ id: 1, body: "p1" }]);
    expect(childRows).toEqual([{ id: 1, parent_id: 1, body: "c1" }]);
    const fkCheck = verify.query("PRAGMA foreign_key_check").all() as unknown[];
    expect(fkCheck).toEqual([]);
    verify.close(false);
  });

  testWithTmp("revert reports structured conflict for UNIQUE constraint ahead of SQL crash", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const setup = await writePlanFile(dir, "setup-unique.json", {
      message: "setup users",
      operations: [
        {
          type: "create_table",
          table: "users",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "email", type: "TEXT", notNull: true, unique: true },
          ],
        },
        { type: "insert", table: "users", values: { id: 1, email: "a@example.com" } },
      ],
    });
    const deleteUser = await writePlanFile(dir, "delete-user.json", {
      message: "delete user 1",
      operations: [{ type: "delete", table: "users", where: { id: 1 } }],
    });
    const insertConflicting = await writePlanFile(dir, "insert-conflicting-user.json", {
      message: "insert conflicting user",
      operations: [{ type: "insert", table: "users", values: { id: 2, email: "a@example.com" } }],
    });

    await applyPlan(currentDb(), setup);
    const deleted = await applyPlan(currentDb(), deleteUser);
    await applyPlan(currentDb(), insertConflicting);

    const result = revertCommit(currentDb(), deleted.commitId);
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.conflicts.length).toBeGreaterThan(0);
      expect(result.conflicts[0]?.kind).toBe("row");
      expect(result.conflicts[0]?.table).toBe("users");
      expect(result.conflicts[0]?.reason.toUpperCase().includes("UNIQUE")).toBe(true);
    }
  });

  testWithTmp("schema rebuild + revert preserves FK/UNIQUE/CHECK/trigger semantics", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const direct = new Database(dbPath);
    direct.run("PRAGMA foreign_keys=ON");
    direct.run("CREATE TABLE parents (id INTEGER PRIMARY KEY)");
    direct.run(`
      CREATE TABLE constrained_items (
        id INTEGER PRIMARY KEY,
        parent_id INTEGER NOT NULL,
        code TEXT NOT NULL UNIQUE,
        amount TEXT NOT NULL CHECK(length(amount) > 0),
        note TEXT,
        FOREIGN KEY(parent_id) REFERENCES parents(id) ON DELETE CASCADE
      )
    `);
    direct.run(`
      CREATE TRIGGER trg_constrained_items_upper_note
      AFTER INSERT ON constrained_items
      BEGIN
        UPDATE constrained_items SET note = upper(note) WHERE id = NEW.id;
      END
    `);
    direct.run("INSERT INTO parents(id) VALUES (1)");
    direct.run("INSERT INTO constrained_items(id, parent_id, code, amount, note) VALUES (1, 1, 'A', '10', 'seed')");
    direct.close(false);

    const dropNote = await writePlanFile(dir, "drop-note.json", {
      message: "drop note",
      operations: [{ type: "drop_column", table: "constrained_items", column: "note" }],
    });
    const dropped = await applyPlan(currentDb(), dropNote);
    const reverted = revertCommit(currentDb(), dropped.commitId);
    expect(reverted.ok).toBe(true);

    const verify = new Database(dbPath);
    verify.run("PRAGMA foreign_keys=ON");
    const fkRows = verify.query("PRAGMA foreign_key_list('constrained_items')").all() as Array<{ table: string }>;
    expect(fkRows).toHaveLength(1);
    expect(fkRows[0]?.table).toBe("parents");

    expect(() =>
      verify.run("INSERT INTO constrained_items(id, parent_id, code, amount, note) VALUES (2, 1, 'A', '11', 'x')"),
    ).toThrow();
    expect(() =>
      verify.run("INSERT INTO constrained_items(id, parent_id, code, amount, note) VALUES (3, 1, 'B', '', 'x')"),
    ).toThrow();

    verify.run("INSERT INTO constrained_items(id, parent_id, code, amount, note) VALUES (4, 1, 'C', '12', 'x')");
    const triggerRow = verify.query("SELECT note FROM constrained_items WHERE id=4").get() as { note: string } | null;
    expect(triggerRow?.note).toBe("X");

    verify.run("DELETE FROM parents WHERE id=1");
    const remaining = verify.query("SELECT COUNT(*) AS c FROM constrained_items").get() as { c: number };
    expect(remaining.c).toBe(0);
    verify.close(false);
  });

  testWithTmp("schema rebuild + revert preserves self-referential FK targets", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const direct = new Database(dbPath);
    direct.run("PRAGMA foreign_keys=ON");
    direct.run(`
      CREATE TABLE self_fk_nodes (
        id INTEGER PRIMARY KEY,
        parent_id INTEGER REFERENCES self_fk_nodes(id),
        note TEXT
      )
    `);
    direct.run("INSERT INTO self_fk_nodes(id, parent_id, note) VALUES (1, NULL, 'root')");
    direct.run("INSERT INTO self_fk_nodes(id, parent_id, note) VALUES (2, 1, 'child')");
    direct.close(false);

    const dropNote = await writePlanFile(dir, "drop-self-fk-note.json", {
      message: "drop note from self fk table",
      operations: [{ type: "drop_column", table: "self_fk_nodes", column: "note" }],
    });
    const dropped = await applyPlan(currentDb(), dropNote);

    const reverted = revertCommit(currentDb(), dropped.commitId);
    expect(reverted.ok).toBe(true);
    if (!reverted.ok) {
      throw new Error("expected self-referential FK revert success");
    }

    const verify = new Database(dbPath);
    verify.run("PRAGMA foreign_keys=ON");

    const fkRows = verify.query("PRAGMA foreign_key_list('self_fk_nodes')").all() as Array<{ table: string }>;
    expect(fkRows).toHaveLength(1);
    expect(fkRows[0]?.table).toBe("self_fk_nodes");

    const fkCheck = verify.query("PRAGMA foreign_key_check").all() as unknown[];
    expect(fkCheck).toEqual([]);

    verify.run("INSERT INTO self_fk_nodes(id, parent_id, note) VALUES (3, 1, 'grandchild')");
    expect(() => verify.run("INSERT INTO self_fk_nodes(id, parent_id, note) VALUES (4, 999, 'bad')")).toThrow();
    verify.close(false);
  });

  testWithTmp("revert of schema-changing commit fails when later rows touched same table", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const setup = await writePlanFile(dir, "schema-row-conflict-setup.json", {
      message: "setup conflict table",
      operations: [
        {
          type: "create_table",
          table: "conflict_items",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "note", type: "TEXT" },
          ],
        },
        { type: "insert", table: "conflict_items", values: { id: 1, note: "a" } },
      ],
    });
    const dropColumn = await writePlanFile(dir, "schema-row-conflict-drop.json", {
      message: "drop note",
      operations: [{ type: "drop_column", table: "conflict_items", column: "note" }],
    });
    const laterInsert = await writePlanFile(dir, "schema-row-conflict-later-insert.json", {
      message: "later insert",
      operations: [{ type: "insert", table: "conflict_items", values: { id: 2 } }],
    });

    await applyPlan(currentDb(), setup);
    const dropped = await applyPlan(currentDb(), dropColumn);
    await applyPlan(currentDb(), laterInsert);

    const reverted = revertCommit(currentDb(), dropped.commitId);
    expect(reverted.ok).toBe(false);
    if (!reverted.ok) {
      expect(reverted.conflicts.some((conflict) => conflict.kind === "schema" && conflict.table === "conflict_items")).toBe(
        true,
      );
    }

    const rows = readQuery(currentDb(), "SELECT id FROM conflict_items ORDER BY id");
    expect(rows).toEqual([{ id: 1 }, { id: 2 }]);
  });

  testWithTmp("revert returns structured conflict when later drop_table removed target row table", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const setup = await writePlanFile(dir, "missing-table-conflict-setup.json", {
      message: "setup",
      operations: [
        {
          type: "create_table",
          table: "missing_table_conflict",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "body", type: "TEXT", notNull: true },
          ],
        },
        { type: "insert", table: "missing_table_conflict", values: { id: 1, body: "a" } },
      ],
    });
    const updatePlan = await writePlanFile(dir, "missing-table-conflict-update.json", {
      message: "update row",
      operations: [{ type: "update", table: "missing_table_conflict", values: { body: "b" }, where: { id: 1 } }],
    });
    const dropPlan = await writePlanFile(dir, "missing-table-conflict-drop.json", {
      message: "drop table later",
      operations: [{ type: "drop_table", table: "missing_table_conflict" }],
    });

    await applyPlan(currentDb(), setup);
    const updated = await applyPlan(currentDb(), updatePlan);
    await applyPlan(currentDb(), dropPlan);

    const result = revertCommit(currentDb(), updated.commitId);
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(
        result.conflicts.some(
          (conflict) =>
            conflict.kind === "schema" &&
            conflict.table === "missing_table_conflict" &&
            conflict.reason.includes("Current table is missing"),
        ),
      ).toBe(true);
    }
  });

  testWithTmp("typed and BLOB values round-trip losslessly through commit and revert", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const direct = new Database(dbPath);
    direct.run("CREATE TABLE blobs (id INTEGER PRIMARY KEY, data BLOB NOT NULL, n REAL NOT NULL, tag TEXT NOT NULL)");
    direct.run("INSERT INTO blobs(id, data, n, tag) VALUES (1, X'001122334455', 1.5, 'a')");
    direct.close(false);

    const updateTag = await writePlanFile(dir, "update-tag.json", {
      message: "update blob row tag",
      operations: [{ type: "update", table: "blobs", values: { tag: "b" }, where: { id: 1 } }],
    });
    const updated = await applyPlan(currentDb(), updateTag);
    const reverted = revertCommit(currentDb(), updated.commitId);
    expect(reverted.ok).toBe(true);

    const verify = new Database(dbPath);
    const row = verify
      .query("SELECT hex(data) AS hex_data, typeof(data) AS t_data, typeof(n) AS t_n, n, tag FROM blobs WHERE id=1")
      .get() as { hex_data: string; t_data: string; t_n: string; n: number; tag: string } | null;
    verify.close(false);

    expect(row).toEqual({
      hex_data: "001122334455",
      t_data: "blob",
      t_n: "real",
      n: 1.5,
      tag: "a",
    });
  });

  testWithTmp("TEXT with embedded NUL bytes is preserved losslessly through commit and revert", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const direct = new Database(dbPath);
    direct.run("CREATE TABLE text_nul_items (id INTEGER PRIMARY KEY, payload TEXT NOT NULL, tag TEXT NOT NULL)");
    direct.run("INSERT INTO text_nul_items(id, payload, tag) VALUES (1, CAST(X'410042' AS TEXT), 'a')");
    direct.close(false);

    const updateTag = await writePlanFile(dir, "update-text-nul-tag.json", {
      message: "update tag for nul-text row",
      operations: [{ type: "update", table: "text_nul_items", values: { tag: "b" }, where: { id: 1 } }],
    });
    const updated = await applyPlan(currentDb(), updateTag);
    const reverted = revertCommit(currentDb(), updated.commitId);
    expect(reverted.ok).toBe(true);

    const rows = readQuery(currentDb(), 
      "SELECT id, hex(CAST(payload AS BLOB)) AS payload_hex, length(CAST(payload AS BLOB)) AS payload_len, tag FROM text_nul_items",
    );
    expect(rows).toEqual([{ id: 1, payload_hex: "410042", payload_len: 3, tag: "a" }]);
  });

  testWithTmp("revert applies sqlite_sequence inverse effects exactly", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const direct = new Database(dbPath);
    direct.run("CREATE TABLE auto_items (id INTEGER PRIMARY KEY AUTOINCREMENT, body TEXT NOT NULL)");
    direct.close(false);

    const insertA = await writePlanFile(dir, "autoinc-insert-a.json", {
      message: "insert autoincrement row a",
      operations: [{ type: "insert", table: "auto_items", values: { body: "a" } }],
    });
    const insertB = await writePlanFile(dir, "autoinc-insert-b.json", {
      message: "insert autoincrement row b",
      operations: [{ type: "insert", table: "auto_items", values: { body: "b" } }],
    });

    const committed = await applyPlan(currentDb(), insertA);
    const reverted = revertCommit(currentDb(), committed.commitId);
    expect(reverted.ok).toBe(true);

    await applyPlan(currentDb(), insertB);
    const rows = readQuery(currentDb(), "SELECT id, body FROM auto_items ORDER BY id");
    expect(rows).toEqual([{ id: 1, body: "b" }]);
  });

  testWithTmp("revert returns conflict on later sqlite_sequence-only drift", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const direct = new Database(dbPath);
    direct.run("CREATE TABLE auto_items_later (id INTEGER PRIMARY KEY AUTOINCREMENT, body TEXT NOT NULL)");
    direct.close(false);

    const insertA = await writePlanFile(dir, "autoinc-later-insert-a.json", {
      message: "insert a",
      operations: [{ type: "insert", table: "auto_items_later", values: { body: "a" } }],
    });
    const insertB = await writePlanFile(dir, "autoinc-later-insert-b.json", {
      message: "insert b",
      operations: [{ type: "insert", table: "auto_items_later", values: { body: "b" } }],
    });

    const first = await applyPlan(currentDb(), insertA);
    await applyPlan(currentDb(), insertB);

    const reverted = revertCommit(currentDb(), first.commitId);
    expect(reverted.ok).toBe(false);
    if (!reverted.ok) {
      expect(
        reverted.conflicts.some((conflict) => conflict.kind === "row" && conflict.table === "sqlite_sequence"),
      ).toBe(true);
    }
  });

  testWithTmp("revert reports conflict when sqlite_sequence drift remains after later insert/delete", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const direct = new Database(dbPath);
    direct.run("CREATE TABLE auto_items_hist (id INTEGER PRIMARY KEY AUTOINCREMENT, body TEXT NOT NULL)");
    direct.close(false);

    const insertA = await writePlanFile(dir, "autoinc-hist-insert-a.json", {
      message: "insert a",
      operations: [{ type: "insert", table: "auto_items_hist", values: { body: "a" } }],
    });
    const insertB = await writePlanFile(dir, "autoinc-hist-insert-b.json", {
      message: "insert b",
      operations: [{ type: "insert", table: "auto_items_hist", values: { body: "b" } }],
    });
    const deleteB = await writePlanFile(dir, "autoinc-hist-delete-b.json", {
      message: "delete b",
      operations: [{ type: "delete", table: "auto_items_hist", where: { id: 2 } }],
    });
    const first = await applyPlan(currentDb(), insertA);
    await applyPlan(currentDb(), insertB);
    await applyPlan(currentDb(), deleteB);

    const reverted = revertCommit(currentDb(), first.commitId);
    expect(reverted.ok).toBe(false);
    if (!reverted.ok) {
      expect(
        reverted.conflicts.some((conflict) => conflict.kind === "row" && conflict.table === "sqlite_sequence"),
      ).toBe(true);
    }
  });

  testWithTmp("revert of revert is supported", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const setup = await writePlanFile(dir, "setup.json", {
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
    const drop = await writePlanFile(dir, "drop.json", {
      message: "drop events",
      operations: [{ type: "drop_table", table: "events" }],
    });

    await applyPlan(currentDb(), setup);
    const dropped = await applyPlan(currentDb(), drop);
    const first = revertCommit(currentDb(), dropped.commitId);
    expect(first.ok).toBe(true);
    if (!first.ok) {
      throw new Error("expected first revert success");
    }

    const second = revertCommit(currentDb(), first.revertCommit.commitId);
    expect(second.ok).toBe(true);
    if (!second.ok) {
      throw new Error("expected second revert success");
    }
    const tableCount = readQuery(currentDb(), "SELECT COUNT(*) AS c FROM sqlite_master WHERE type='table' AND name='events'");
    expect(tableCount).toEqual([{ c: 0 }]);
  });
});
