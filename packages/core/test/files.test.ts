import { describe, expect, test } from "bun:test";
import { deleteIfExists, deleteWithSidecars } from "../src/db";
import { createTestContext, withTmpDirCleanup } from "./helpers";

const testWithTmp = (name: string, fn: () => void | Promise<void>) => test(name, withTmpDirCleanup(fn));

describe("files", () => {
  testWithTmp("deleteIfExists is idempotent under concurrent deletion", async () => {
    const { dir } = createTestContext();
    const path = `${dir}/race.tmp`;

    for (let i = 0; i < 200; i++) {
      await Bun.write(path, "x");
      await Promise.all(Array.from({ length: 8 }, () => deleteIfExists(path)));
    }

    expect(await Bun.file(path).exists()).toBe(false);
  });

  testWithTmp("deleteWithSidecars is idempotent under concurrent deletion", async () => {
    const { dir } = createTestContext();
    const path = `${dir}/toss.db`;

    for (let i = 0; i < 200; i++) {
      await Promise.all([
        Bun.write(path, "db"),
        Bun.write(`${path}-wal`, "wal"),
        Bun.write(`${path}-shm`, "shm"),
      ]);
      await Promise.all(Array.from({ length: 4 }, () => deleteWithSidecars(path)));
    }

    expect(await Bun.file(path).exists()).toBe(false);
    expect(await Bun.file(`${path}-wal`).exists()).toBe(false);
    expect(await Bun.file(`${path}-shm`).exists()).toBe(false);
  });
});
