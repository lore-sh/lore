import { initDb } from "./engine/db";
import type { InitDatabaseOptions } from "./types";

export async function initDatabase(
  options: InitDatabaseOptions = {},
): Promise<{ dbPath: string }> {
  const initialized = await initDb({
    dbPath: options.dbPath,
    forceNew: options.forceNew,
  });
  return { dbPath: initialized.path };
}
