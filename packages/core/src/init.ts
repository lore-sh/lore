import { closeClient } from "./engine/client";
import { configureDatabase, initializeStorage, resolveDbPath } from "./db";
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
  configureDatabase(dbPath);
  initializeStorage();

  const generatedSkills = options.generateSkills
    ? await generateSkills({
        platforms: options.skillPlatforms,
        openclawHeartbeat: options.openclawHeartbeat,
      })
    : null;
  return { dbPath, generatedSkills };
}
