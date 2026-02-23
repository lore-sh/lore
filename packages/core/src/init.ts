import { initDb as initEngineDb } from "./engine/db";
import type { InitDbOptions } from "./types";

export async function initDb(options: InitDbOptions = {}): Promise<{ path: string }> {
  const initialized = await initEngineDb({
    dbPath: options.dbPath,
    forceNew: options.forceNew,
  });
  return { path: initialized.path };
}
