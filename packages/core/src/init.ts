import {
  closeDatabase,
  initializeStorage,
  openDatabase,
} from "./db";
import { deleteWithSidecars } from "./fsx";
import { generateSkills, type GeneratedSkills } from "./skills";
import type { InitDatabaseOptions } from "./types";

export async function removeExistingDbFiles(dbPath: string): Promise<void> {
  await deleteWithSidecars(dbPath);
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
