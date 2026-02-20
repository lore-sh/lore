import { closeClient } from "./engine/client";
import { initializeStorage, resolveDbPath, withDatabaseAtPath } from "./db";
import { deleteWithSidecars } from "./fsx";
import { generateSkills, type GeneratedSkills } from "./skills";
import type { InitDatabaseOptions } from "./types";

export async function removeExistingDbFiles(dbPath: string): Promise<void> {
  await deleteWithSidecars(dbPath);
}

export async function initDatabase(
  options: InitDatabaseOptions = {},
): Promise<{ dbPath: string; generatedSkills: GeneratedSkills | null }> {
  const dbPath = resolveDbPath(options.dbPath);
  if (options.forceNew) {
    closeClient();
    await removeExistingDbFiles(dbPath);
  }
  withDatabaseAtPath(dbPath, () => {
    initializeStorage();
  });

  const generatedSkills = options.generateSkills
    ? await generateSkills(options.skillPlatforms ? { platforms: options.skillPlatforms } : {})
    : null;
  return { dbPath, generatedSkills };
}
