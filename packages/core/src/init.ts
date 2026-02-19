import { rm } from "node:fs/promises";
import {
  closeDatabase,
  detectLegacySchema,
  getMetaValue,
  initializeStorage,
  openDatabase,
} from "./db";
import { TossError } from "./errors";
import { generateSkills, type GeneratedSkills } from "./skills";
import type { InitDatabaseOptions } from "./types";

export async function removeExistingDbFiles(dbPath: string): Promise<void> {
  await rm(dbPath, { force: true });
  await rm(`${dbPath}-shm`, { force: true });
  await rm(`${dbPath}-wal`, { force: true });
}

export async function initDatabase(
  options: InitDatabaseOptions = {},
): Promise<{ dbPath: string; generatedSkills: GeneratedSkills | null }> {
  const { db, dbPath } = openDatabase(options.dbPath);
  let shouldClose = true;
  try {
    if (options.forceNew) {
      closeDatabase(db);
      shouldClose = false;
      await removeExistingDbFiles(dbPath);
      const reopened = openDatabase(dbPath);
      try {
        initializeStorage(reopened.db);
      } finally {
        closeDatabase(reopened.db);
      }
    } else {
      const hasRepoMetaTable =
        (db
          .query("SELECT 1 AS ok FROM sqlite_master WHERE type='table' AND name='_toss_repo_meta' LIMIT 1")
          .get() as { ok?: number } | null)?.ok === 1;
      const hasFormatGenerationMeta = hasRepoMetaTable && getMetaValue(db, "format_generation") !== null;
      const hasHistoryEngineMeta = hasRepoMetaTable && getMetaValue(db, "history_engine") !== null;

      if (detectLegacySchema(db) || (hasFormatGenerationMeta && !hasHistoryEngineMeta)) {
        throw new TossError(
          "FORMAT_MISMATCH",
          `Legacy toss format detected at ${dbPath}. Run \`toss init --force-new\` to reinitialize.`,
        );
      }
      initializeStorage(db);
    }
  } finally {
    if (shouldClose) {
      closeDatabase(db);
    }
  }

  const generatedSkills = options.generateSkills ? await generateSkills(options.workspacePath) : null;
  return { dbPath, generatedSkills };
}
