import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { existsSync, readFileSync } from "node:fs";
import { initDatabase, getStatus } from "../src";
import { createTestContext, withTmpDirCleanup } from "./helpers";

const testWithTmp = (name: string, fn: () => void | Promise<void>) => test(name, withTmpDirCleanup(fn));


describe("initDatabase", () => {
  testWithTmp("force-new reinitializes database", async () => {
    const { dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const direct = new Database(dbPath);
    direct.run('CREATE TABLE foo (id INTEGER PRIMARY KEY, v TEXT)');
    direct.run("INSERT INTO foo(id, v) VALUES(1, 'x')");
    direct.close(false);

    const reinit = await initDatabase({ dbPath, forceNew: true });
    expect(reinit.dbPath).toBe(dbPath);
    const status = getStatus({ dbPath });
    expect(status.tableCount).toBe(0);
    expect(status.headCommit).toBeNull();
  });

  testWithTmp("init generates toss skill with migration guidance", async () => {
    const { dir, dbPath } = createTestContext();
    const result = await initDatabase({ dbPath, generateSkills: true, workspacePath: dir });
    expect(result.generatedSkills).not.toBeNull();
    if (!result.generatedSkills) {
      throw new Error("generatedSkills should exist");
    }

    expect(existsSync(result.generatedSkills.skillPath)).toBe(true);
    const skill = readFileSync(result.generatedSkills.skillPath, "utf8");
    expect(skill.includes("toss history --verbose")).toBe(true);
    expect(skill.includes("toss verify --quick")).toBe(true);
    expect(skill.includes("staged migrations")).toBe(true);
  });
});
