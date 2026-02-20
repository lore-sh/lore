import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { applyPlan, initDatabase, isTossError, planCheck, readQuery } from "../src";
import { createTestContext, writePlanFile, withTmpDirCleanup } from "./helpers";

const testWithTmp = (name: string, fn: () => void | Promise<void>) => test(name, withTmpDirCleanup(fn));

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
      expect(isTossError(error)).toBe(true);
      if (isTossError(error)) {
        expect(error.code).toBe("INVALID_OPERATION");
        expect(error.message).toContain("CHECK constraint not found");
      }
    }
  });
});
