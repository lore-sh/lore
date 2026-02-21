import { describe, expect, test } from "bun:test";
import { quoteIdentifier, validateReadSql } from "../../src/engine/sql";

describe("sql helpers", () => {
  test("quoteIdentifier escapes valid identifiers", () => {
    expect(quoteIdentifier("users")).toBe('"users"');
    expect(quoteIdentifier("_ok123")).toBe('"_ok123"');
  });

  test("quoteIdentifier rejects invalid identifier", () => {
    expect(() => quoteIdentifier("bad name")).toThrow();
  });

  test("validateReadSql allows SELECT/WITH and strips trailing semicolon", () => {
    expect(validateReadSql("SELECT 1;")).toBe("SELECT 1");
    expect(validateReadSql("WITH t AS (SELECT 1) SELECT * FROM t")).toBe("WITH t AS (SELECT 1) SELECT * FROM t");
  });

  test("validateReadSql rejects multiple statements and write keywords", () => {
    expect(() => validateReadSql("SELECT 1; SELECT 2")).toThrow();
    expect(() => validateReadSql("DELETE FROM users")).toThrow();
  });
});
