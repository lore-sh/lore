import { describe, expect, test } from "bun:test";
import { parseAndValidateOperationPlan } from "../../src/engine/validate";

describe("parseAndValidateOperationPlan", () => {
  test("accepts valid plan", () => {
    const plan = parseAndValidateOperationPlan(
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
      parseAndValidateOperationPlan(
        JSON.stringify({
          message: "invalid",
          operations: [{ type: "restore_table", table: "t", ddlSql: "CREATE TABLE t(id INTEGER)", rows: [] }],
        }),
      ),
    ).toThrow();
  });

  test("rejects commented add_check expression", () => {
    expect(() =>
      parseAndValidateOperationPlan(
        JSON.stringify({
          message: "invalid check",
          operations: [{ type: "add_check", table: "users", expression: "id > 0 -- bad" }],
        }),
      ),
    ).toThrow();
  });
});
