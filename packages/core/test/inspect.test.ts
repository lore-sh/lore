import { describe, expect, test } from "bun:test";
import { initDb } from "../src";
import { normalizePage, normalizePageSize, normalizeRow, stateHash, whereClause } from "../src/inspect";
import { computeSchemaHash, createTestContext, currentDb, withDbPath, withTmpDirCleanup } from "./helpers";

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

describe("inspect helpers", () => {
  testWithTmp("stateHash distinguishes adjacent int64 values", async () => {
    const ctx = createTestContext();
    await initDb({ dbPath: ctx.dbPath });

    await withDbPath(ctx.dbPath, async () => {
      currentDb().$client.run("CREATE TABLE ledger (id INTEGER PRIMARY KEY, amount INTEGER NOT NULL)");
      currentDb().$client.run("INSERT INTO ledger(id, amount) VALUES (1, 9223372036854775806)");
      const hashA = stateHash(currentDb());
      currentDb().$client.run("UPDATE ledger SET amount = 9223372036854775807 WHERE id = 1");
      const hashB = stateHash(currentDb());
      expect(hashA).not.toBe(hashB);
    });
  });

  test("normalizeRow serializes non-finite/bytes/object values", () => {
    const normalized = normalizeRow({
      amount: Infinity,
      blob: new Uint8Array([0, 255]),
      extra: { nested: true },
      title: "ok",
      enabled: true,
      nullable: null,
    });
    expect(normalized).toEqual({
      amount: null,
      blob: "AP8=",
      extra: '{"nested":true}',
      title: "ok",
      enabled: true,
      nullable: null,
    });
  });

  test("whereClause builds SQL with bindings and IS NULL predicates", () => {
    const { clause, bindings } = whereClause({ id: 1, deletedAt: null });
    expect(clause).toContain('"id" = ?');
    expect(clause).toContain('"deletedAt" IS NULL');
    expect(bindings).toEqual([1]);
  });

  test("whereClause rejects empty predicate", () => {
    expect(() => whereClause({})).toThrow(/where must not be empty/i);
  });

  test("normalizePageSize/normalizePage clamp invalid values", () => {
    expect(normalizePageSize(undefined)).toBe(50);
    expect(normalizePageSize(0)).toBe(50);
    expect(normalizePageSize(9999)).toBe(500);
    expect(normalizePageSize(12.7)).toBe(12);

    expect(normalizePage(undefined)).toBe(1);
    expect(normalizePage(0)).toBe(1);
    expect(normalizePage(3.8)).toBe(3);
  });
});
