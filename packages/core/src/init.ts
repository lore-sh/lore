import { closeClient } from "./engine/client";
import { configureDatabase, initializeStorage, resolveDbPath } from "./engine/db";
import { deleteWithSidecars } from "./engine/files";
import type { InitDatabaseOptions } from "./types";

export async function removeExistingDbFiles(dbPath: string): Promise<void> {
  await deleteWithSidecars(dbPath);
}

export async function initDatabase(
  options: InitDatabaseOptions = {},
): Promise<{ dbPath: string }> {
  const dbPath = resolveDbPath(options.dbPath);
  if (options.forceNew) {
    closeClient();
    await removeExistingDbFiles(dbPath);
  }
  configureDatabase(dbPath);
  initializeStorage();
  return { dbPath };
}
