import { describe, expect, test } from "bun:test";
import type { CommitDetailPayload } from "../../src/client/lib/api";
import { renderOperationLine, renderRowEffectLines } from "../../src/client/lib/commit-render";

type RowEffect = CommitDetailPayload["effects"]["rows"][number];

describe("commit rendering", () => {
  test("renders operations in readable one-line format", () => {
    expect(
      renderOperationLine({
        type: "add_column",
        table: "expenses",
        column: { name: "category", type: "TEXT" },
      }),
    ).toBe("ADD COLUMN category TEXT to expenses");

    expect(
      renderOperationLine({
        type: "insert",
        table: "expenses",
        values: { id: 1, item: "dinner", amount: 1200 },
      }),
    ).toContain("INSERT into expenses");
  });

  test("renders row effect diff lines for insert/update/delete", () => {
    const insert: RowEffect = {
      tableName: "expenses",
      pk: { id: "1" },
      opKind: "insert",
      beforeRow: null,
      afterRow: {
        id: { storageClass: "integer", sqlLiteral: "1" },
        item: { storageClass: "text", sqlLiteral: "CAST(X'64696e6e6572' AS TEXT)" },
      },
      beforeHash: null,
      afterHash: "after",
    };
    const insertLines = renderRowEffectLines(insert);
    expect(insertLines[0]?.kind).toBe("add");
    expect(insertLines.map((line) => line.text).join("\n")).toContain("+ item: \"dinner\"");

    const update: RowEffect = {
      tableName: "expenses",
      pk: { id: "1" },
      opKind: "update",
      beforeRow: {
        amount: { storageClass: "integer", sqlLiteral: "1200" },
      },
      afterRow: {
        amount: { storageClass: "integer", sqlLiteral: "1500" },
      },
      beforeHash: "before",
      afterHash: "after",
    };
    const updateLines = renderRowEffectLines(update);
    expect(updateLines.map((line) => line.text)).toEqual(["- amount: 1200", "+ amount: 1500"]);

    const remove: RowEffect = {
      tableName: "expenses",
      pk: { id: "1" },
      opKind: "delete",
      beforeRow: {
        id: { storageClass: "integer", sqlLiteral: "1" },
      },
      afterRow: null,
      beforeHash: "before",
      afterHash: null,
    };
    const removeLines = renderRowEffectLines(remove);
    expect(removeLines[0]?.kind).toBe("remove");
    expect(removeLines[0]?.text).toBe("- id: 1");
  });
});
