import { push, type Database } from "@lore/core";
import { z } from "zod";
import { toJson } from "../format";

export const PushArgsSchema = z.array(z.string()).length(0, "push does not accept arguments");

export function parsePushArgs(args: string[]): void {
  PushArgsSchema.parse(args);
}

export async function runPush(db: Database): Promise<void> {
  const result = await push(db);
  console.log(toJson({ status: "ok", ...result }));
}
