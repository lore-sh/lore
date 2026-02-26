import { sync, type Database } from "@lore/core";
import { toJson } from "../format";
import { parseNoArgs } from "../parse";

export function parseSyncArgs(args: string[]): void {
  parseNoArgs(args, "sync");
}

export async function runSync(db: Database): Promise<void> {
  const result = await sync(db);
  console.log(toJson({ status: "ok", ...result }));
}
