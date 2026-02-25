import { pull, type Database } from "@lore/core";
import { z } from "zod";
import { toJson } from "../format";

export const PullArgsSchema = z.array(z.string()).length(0, "pull does not accept arguments");

export function parsePullArgs(args: string[]): void {
  PullArgsSchema.parse(args);
}

export async function runPull(db: Database): Promise<void> {
  const result = await pull(db);
  console.log(toJson({ status: "ok", ...result }));
}
