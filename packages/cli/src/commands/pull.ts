import { pull, type Database } from "@lore/core";
import { toJson } from "../format";
import { parseNoArgs } from "../parse";

export function parsePullArgs(args: string[]): void {
  parseNoArgs(args, "pull");
}

export async function runPull(db: Database): Promise<void> {
  const result = await pull(db);
  console.log(toJson({ status: "ok", ...result }));
}
