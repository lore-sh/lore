import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
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

    expect(await Bun.file(result.generatedSkills.skillPath).exists()).toBe(true);
    const skill = await Bun.file(result.generatedSkills.skillPath).text();
    expect(skill.includes("toss history --verbose")).toBe(true);
    expect(skill.includes("toss verify --quick")).toBe(true);
    expect(skill.includes("staged migrations")).toBe(true);
  });
});
