import { describe, expect, test } from "bun:test";
import { computeSchemaHash, withTmpDirCleanup } from "./helpers";

const testWithTmp = (name: string, fn: () => void | Promise<void>) => test(name, withTmpDirCleanup(fn));


describe("schemaHash", () => {
  testWithTmp("schemaHash includes UNIQUE/CHECK/FOREIGN KEY constraints", async () => {
    const hashA = await computeSchemaHash([`
      CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        email TEXT UNIQUE,
        org_id INTEGER,
        CHECK (length(email) > 0),
        FOREIGN KEY(org_id) REFERENCES orgs(id)
      )
    `]);
    const hashB = await computeSchemaHash([`
      CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        email TEXT,
        org_id INTEGER
      )
    `]);

    expect(hashA).not.toBe(hashB);
  });

  testWithTmp("schemaHash preserves whitespace inside SQL string literals", async () => {
    const hashA = await computeSchemaHash([`
      CREATE TABLE checks (
        id INTEGER PRIMARY KEY,
        value TEXT CHECK (value <> 'a  b')
      )
    `]);
    const hashB = await computeSchemaHash([`
      CREATE TABLE checks (
        id INTEGER PRIMARY KEY,
        value TEXT CHECK (value <> 'a b')
      )
    `]);

    expect(hashA).not.toBe(hashB);
  });

  testWithTmp("schemaHash normalizes SQL keyword case outside literals", async () => {
    const hashA = await computeSchemaHash([
      "create table users (id integer primary key, email text check (length(email) > 0))",
      "create index idx_users_email on users(email)",
      "create trigger trg_users_ai after insert on users begin select 1; end",
    ]);
    const hashB = await computeSchemaHash([
      "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT CHECK (LENGTH(email) > 0))",
      "CREATE INDEX idx_users_email ON users(email)",
      "CREATE TRIGGER trg_users_ai AFTER INSERT ON users BEGIN SELECT 1; END",
    ]);

    expect(hashA).toBe(hashB);
  });

  testWithTmp("schemaHash includes COLLATE and generated-column expressions", async () => {
    const hashA = await computeSchemaHash([`
      CREATE TABLE docs (
        id INTEGER PRIMARY KEY,
        title TEXT COLLATE NOCASE,
        normalized TEXT GENERATED ALWAYS AS (trim(lower(title))) STORED
      )
    `]);
    const hashB = await computeSchemaHash([`
      CREATE TABLE docs (
        id INTEGER PRIMARY KEY,
        title TEXT COLLATE BINARY,
        normalized TEXT GENERATED ALWAYS AS (title) STORED
      )
    `]);

    expect(hashA).not.toBe(hashB);
  });
});
