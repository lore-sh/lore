import { sync, type Database } from "@lore/core";
import { z } from "zod";
import { toJson } from "../format";

export const SyncArgsSchema = z.array(z.string()).length(0, "sync does not accept arguments");

export function parseSyncArgs(args: string[]): void {
  SyncArgsSchema.parse(args);
}

export async function runSync(db: Database): Promise<void> {
  const result = await sync(db);
  console.log(toJson({ status: "ok", ...result }));
}
