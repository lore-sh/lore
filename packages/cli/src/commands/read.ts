import { query, type Database } from "@lore/core";
import { z } from "zod";
import { printTable, toJson } from "../format";
import { parseCliArgs } from "../parse";

export const ReadArgsSchema = z.object({ sql: z.string().min(1), json: z.boolean() });

export function parseReadArgs(args: string[]): z.infer<typeof ReadArgsSchema> {
  const parsed = parseCliArgs(args, {
    options: {
      sql: { type: "string" },
      json: { type: "boolean" },
    },
  });
  return ReadArgsSchema.parse({
    sql: parsed.values.sql,
    json: parsed.values.json ?? false,
  });
}

export function runRead(db: Database, args: z.infer<typeof ReadArgsSchema>): void {
  const { sql, json } = args;
  const rows = query(db, sql);
  if (json) {
    console.log(toJson(rows));
    return;
  }
  console.log(printTable(rows));
}
