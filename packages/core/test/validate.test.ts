import { describe, expect, test } from "bun:test";
import { parsePlan } from "../src/operation";

describe("parsePlan", () => {
  test("accepts valid plan", () => {
    const plan = parsePlan(
      JSON.stringify({
        message: "create users",
        operations: [{ type: "create_table", table: "users", columns: [{ name: "id", type: "INTEGER", primaryKey: true }] }],
      }),
    );
    expect(plan.operations).toHaveLength(1);
    expect(plan.operations[0]?.type).toBe("create_table");
  });

  test("rejects internal restore_table", () => {
    expect(() =>
      parsePlan(
        JSON.stringify({
          message: "invalid",
          operations: [{ type: "restore_table", table: "t", ddlSql: "CREATE TABLE t(id INTEGER)", rows: [] }],
        }),
      ),
    ).toThrow();
  });

  test("rejects commented add_check expression", () => {
    expect(() =>
      parsePlan(
        JSON.stringify({
          message: "invalid check",
          operations: [{ type: "add_check", table: "users", expression: "id > 0 -- bad" }],
        }),
      ),
    ).toThrow();
  });
});
