import { type Database, describeDb } from "@toss/core";
import { parseArgs } from "node:util";
import { z } from "zod";
import { toJson } from "../format";

export const SchemaArgsSchema = z.object({ table: z.string().optional() });

export function parseSchemaArgs(args: string[]): z.infer<typeof SchemaArgsSchema> {
  const parsed = parseArgs({
    strict: true,
    args,
    allowPositionals: true,
    options: {},
  });
  const positionals = z.array(z.string()).max(1).parse(parsed.positionals);
  return SchemaArgsSchema.parse({ table: positionals[0] });
}

export function runSchema(db: Database, args: z.infer<typeof SchemaArgsSchema>): void {
  const { table } = args;
  console.log(toJson(describeDb(db, { table })));
}
