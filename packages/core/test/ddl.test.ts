import { describe, expect, test } from "bun:test";
import {
  extractCheckConstraints,
  parseColumnDefinitionsFromCreateTable,
  rewriteAddCheckInCreateTable,
  rewriteColumnTypeInCreateTable,
  rewriteCreateTableName,
  rewriteDropCheckInCreateTable,
} from "../src/sql";

describe("ddl helpers", () => {
  test("parseColumnDefinitionsFromCreateTable ignores table constraints", () => {
    const defs = parseColumnDefinitionsFromCreateTable(`
      CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        email TEXT NOT NULL,
        CONSTRAINT uq_users_email UNIQUE(email)
      )
    `);
    expect(defs.get("id")?.includes("PRIMARY KEY")).toBe(true);
    expect(defs.get("email")?.includes("NOT NULL")).toBe(true);
    expect(defs.has("constraint")).toBe(false);
  });

  test("extractCheckConstraints collects normalized expressions", () => {
    const checks = extractCheckConstraints("CREATE TABLE t (id INTEGER, CHECK ( id > 0 ), CHECK(id < 100))");
    expect(checks).toEqual(["ID < 100", "ID > 0"]);
  });

  test("rewriteColumnTypeInCreateTable rewrites only target column", () => {
    const sql = "CREATE TABLE users(id INTEGER PRIMARY KEY, age TEXT, name TEXT)";
    const rewritten = rewriteColumnTypeInCreateTable(sql, "age", "INTEGER");
    expect(rewritten).toContain("age INTEGER");
    expect(rewritten).toContain("name TEXT");
  });

  test("rewriteCreateTableName rewrites self-referential foreign key target", () => {
    const sql = "CREATE TABLE tree_nodes(id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES tree_nodes(id))";
    const rewritten = rewriteCreateTableName(sql, "__tmp_tree_nodes");
    expect(rewritten).toContain('CREATE TABLE "__tmp_tree_nodes"');
    expect(rewritten).toContain('REFERENCES "__tmp_tree_nodes"');
  });

  test("rewriteAddCheckInCreateTable and rewriteDropCheckInCreateTable are inverse for same expression", () => {
    const original = "CREATE TABLE users(id INTEGER PRIMARY KEY, age INTEGER)";
    const added = rewriteAddCheckInCreateTable(original, "age > 0");
    const dropped = rewriteDropCheckInCreateTable(added, " age  >  0 ");
    expect(dropped).toContain("CREATE TABLE users");
    expect(dropped).not.toContain("CHECK");
  });
});
