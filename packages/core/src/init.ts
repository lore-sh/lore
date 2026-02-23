import { initDb as initEngineDb } from "./engine/db";
import type { InitDbOptions } from "./types";

export async function initDb(options: InitDbOptions = {}): Promise<{ path: string }> {
  return initEngineDb(options);
}
