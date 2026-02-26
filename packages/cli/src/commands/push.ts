import { push, type Database } from "@lore/core";
import { toJson } from "../format";
import { parseNoArgs } from "../parse";

export function parsePushArgs(args: string[]): void {
  parseNoArgs(args, "push");
}

export async function runPush(db: Database): Promise<void> {
  const result = await push(db);
  console.log(toJson({ status: "ok", ...result }));
}
