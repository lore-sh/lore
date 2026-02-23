import { initDb as initEngineDb } from "./engine/db";

export type SkillPlatform = "claude" | "cursor" | "codex" | "opencode" | "openclaw";

export interface InitDbOptions {
  dbPath?: string;
  forceNew?: boolean;
}

export async function initDb(options: InitDbOptions = {}): Promise<{ path: string }> {
  return initEngineDb(options);
}
